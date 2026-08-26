import logging
import hashlib
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
import io

# [Refactor] 移除 CONFIG, RESOURCE_DIR, PROJECT_ROOT 全局引用
from src.shared_kernel.config_model import AppConfig
from src.yield_domain.infrastructure.repositories.yield_repository import (
    PanelRepository,
    build_yield_snapshot_path,
)
from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

# --- Core (Processors) ---
from yield_domain.core.mwd_trend.mwd_trend_processor import MWDTrendProcessor
from yield_domain.core.mwd_trend.modifier_table import (
    compute_scale_factors,
    resolve_monthly_targets,
    specified_signature,
    sync_modifier_table,
)
from yield_domain.core.sheet_lot.sheet_lot_processor import (
    calculate_lot_defect_rates, 
    calculate_sheet_defect_rates
)
from yield_domain.core.mapping.mapping_processor import prepare_mapping_data
from src.yield_domain.core.defect_modifier import (
    apply_defect_multipliers
)

class YieldAnalysisService:
    """
    [应用服务层] YieldAnalysisService (V4.0 极速静态版)
    [Refactor Note] 
    所有方法现在必须接收 `config` 和 `resource_dir` (或 `project_root`)。
    Config 对象被放置在第一个参数位置，以确保 Streamlit 缓存机制能正确感知配置变化。
    """
    
    # ==========================================================================
    #  1. 基础数据源 (L1 & L2 Cache)
    # ==========================================================================

    # 更新时间需使用 datetime(2026, 3, 31)
    _custom_end_date: Optional[datetime] = None

    @classmethod
    def set_analysis_end_date(cls, end_date: datetime):
        """允许外部注入并锁定结束时间"""
        cls._custom_end_date = end_date
        start_date = end_date - relativedelta(months=3)
        logging.info(f"分析时间窗口已人工锁定: {start_date.date()} -> {end_date.date()}")

    @classmethod
    def get_time_window(cls) -> Tuple[datetime, datetime]:
        current_end = cls._custom_end_date or datetime.now()
        # 1. 往前推3个月
        three_months_ago = current_end - relativedelta(months=3)
        # 2. [核心修复]：强制将起始日期对齐到该月的 1 号的 0点0分0秒，确保自然月的完整性
        current_start = three_months_ago.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        return current_start, current_end
        
    @staticmethod
    def compute_snapshot_signature(snapshot_path: Path) -> str:
        """
        [企业级缓存签名] 计算本地快照文件的签名，用于 Streamlit 缓存失效感知。
        当文件被删除、重建或修改时，签名改变，触发 L1 Cache Miss。
        """
        if not snapshot_path.exists():
            return "NOT_EXISTS"
        stat = snapshot_path.stat()
        return hashlib.md5(f"{stat.st_mtime}_{stat.st_size}".encode()).hexdigest()[:8]

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_raw_panel_details(
        query_config_json: str,
        data_policy_json: str,
        _db_manager: Optional['DatabaseManager'] = None,
        snapshot_signature: str = "",
    ) -> pd.DataFrame:
        """
        [L1 Cache] 从数据库加载原始 Panel 数据。
        基于 JSON 序列化的 DTO + 快照签名进行缓存追踪。
        """
        logging.info("--- [L1 Cache Miss] 加载原始 Panel 数据... ---")
        
        # 1. 严格实例化 DTO
        query = YieldQueryConfig.model_validate_json(query_config_json)
        data_policy = YieldDataPolicy.model_validate_json(data_policy_json)
        
        # 2. 动态路由隔离路径 (Service 层自己决定存哪，不再依赖 AppConfig)
        snapshot_path = build_yield_snapshot_path(
            Path("data"), query.product_code, data_policy
        )
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 3. 实例化 Repo 并透传 DTO 与 db_manager（依赖注入）
        repo = PanelRepository(
            snapshot_path=snapshot_path,
            data_policy=data_policy,
            use_snapshot=True,
            db_manager=_db_manager,
        )
        
        return repo.get_panel_details(query=query)

    @staticmethod
    def _build_panel_request(
        config: AppConfig,
    ) -> tuple[YieldQueryConfig, YieldDataPolicy]:
        """在 Yield 数据边界一次构造动态查询与静态数据策略。"""
        start_dt, end_dt = YieldAnalysisService.get_time_window()
        query = YieldQueryConfig(
            start_date=start_dt.strftime("%Y-%m-%d"),
            end_date=end_dt.strftime("%Y-%m-%d"),
            product_code=config.data_source.product_code,
        )
        return query, YieldDataPolicy.from_app_config(config)

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_modified_panel_details(config: 'AppConfig', _db_manager: Optional['DatabaseManager'] = None, snapshot_signature: str = "") -> pd.DataFrame:
        """
        [L2 Cache] 获取经过修饰(分散/衰减)后的 Panel 数据
        """
        import logging
        import pandas as pd
        query, data_policy = YieldAnalysisService._build_panel_request(config)
        
        # 1. 获取 L1 数据 (向下传递严格序列化后的 JSON 字符串 + 签名)
        raw_df = YieldAnalysisService.get_raw_panel_details(
            query.model_dump_json(),
            data_policy.model_dump_json(),
            _db_manager,
            snapshot_signature,
        )
        
        if raw_df.empty: 
            return pd.DataFrame()
        
        # 2. 应用修饰
        processed_df = raw_df.copy()

        # 缺陷衰减 (从 config.processing 获取)
        multipliers_config = config.processing.get('defect_multipliers', {})
        logging.info(f"[CACHE_DEBUG] get_modified_panel_details 被执行, defect_multipliers={multipliers_config}")
        if multipliers_config:
            logging.info("应用缺陷衰减...")
            try:
                # 假设 apply_defect_multipliers 在 core 层，按需调整引入路径
                processed_df = apply_defect_multipliers(processed_df, multipliers_config)
            except Exception as e:
                logging.error(f"应用缺陷衰减失败: {e}")
                
        return processed_df

    # ==========================================================================
    #  2. 趋势图业务 (Trend Analysis)
    # ==========================================================================

    DEFAULT_MODIFIER_TABLE_NAME = "入库良率修饰表.xlsx"

    @staticmethod
    def resolve_modifier_table_path(config: AppConfig, product_dir: Path) -> Path:
        """解析入库良率修饰表路径（config.paths['yield_modifier_config'] 可覆盖文件名）。"""
        modifier_res = config.paths.get('yield_modifier_config')
        file_name = (
            modifier_res.file_name
            if modifier_res
            else YieldAnalysisService.DEFAULT_MODIFIER_TABLE_NAME
        )
        return Path(product_dir).parent / file_name

    @staticmethod
    def _build_modifier_context(
        config: AppConfig,
        product_dir: Path,
        panel_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """同步入库良率修饰表并产出趋势/Mapping 所需上下文。

        在 cached 方法内部（cache miss 时）调用：
        - 回写当月良损（仅当月行）与缩放倍数（指定签名变化时）；
        - 返回 targets（Code 级）、group_targets（Group 级）、factors（Code 级缩放
          倍数）和 signature（两级指定签名，供缓存 key 使用）。
        当月良损与趋势图使用同一份 panel 明细计算，保证 Mapping 口径：
        展示不良数 = 展示原始数 × (指定 / 展示原始) = 指定水准。
        """
        start_dt, end_dt = YieldAnalysisService.get_time_window()
        months = pd.period_range(start_dt, end_dt, freq="M").strftime("%Y-%m").tolist()
        current_month = end_dt.strftime("%Y-%m")
        table_path = YieldAnalysisService.resolve_modifier_table_path(config, product_dir)
        table = sync_modifier_table(
            table_path,
            config.data_source.product_code,
            panel_df,
            current_month,
        )
        return {
            "targets": resolve_monthly_targets(table["code"], months),
            "group_targets": resolve_monthly_targets(table["group"], months),
            "factors": compute_scale_factors(table["code"]),
            "signature": (
                f"{specified_signature(table['code'])}"
                f"-{specified_signature(table['group'])}"
            ),
        }

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_mwd_trend_data(
        config: AppConfig, 
        product_dir: Path, 
        _db_manager: Optional['DatabaseManager'] = None,
        snapshot_signature: str = "",
        modifier_signature: str = "",
        ) -> Dict[str, pd.DataFrame] | None:
        """获取月/周/天趋势数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _db_manager, snapshot_signature)
        if panel_df.empty: return None
        
        # [核心修复] 获取目标截止日期，用于数据补齐
        _, target_end_dt = YieldAnalysisService.get_time_window()
        
        modifier_context = YieldAnalysisService._build_modifier_context(
            config,
            product_dir,
            panel_df,
        )

        return MWDTrendProcessor.create_mwd_trend_data(
            panel_details_df=panel_df,
            config=config,
            modifier_targets=modifier_context["group_targets"],
            target_end_date=target_end_dt  # [核心修复] 传入目标截止日期
        )

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_code_level_trend_data(
        config: AppConfig, 
        product_dir: Path,
        _db_manager: Optional['DatabaseManager'] = None,
        snapshot_signature: str = "",
        modifier_signature: str = "",
        ) -> Dict[str, pd.DataFrame] | None:
        """获取 Code 级趋势数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _db_manager, snapshot_signature)
        if panel_df.empty: 
            logging.error("获取基础Panel级数据失败，无法生成Code级趋势图。")
            return None
        
        # [核心修复] 获取目标截止日期，用于数据补齐
        _, target_end_dt = YieldAnalysisService.get_time_window()
        
        # 同步入库良率修饰表（cache miss 时顺带写回）并取目标良损
        modifier_context = YieldAnalysisService._build_modifier_context(
            config, product_dir, panel_df
        )

        return MWDTrendProcessor.create_code_level_mwd_trend_data(
            panel_details_df=panel_df, 
            config=config,
            modifier_targets=modifier_context["targets"],
            target_end_date=target_end_dt  # [核心修复] 传入目标截止日期
        )

    # ==========================================================================
    #  3. Sheet & Lot 级计算 (Heavy Calculation)
    # ==========================================================================
    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_lot_defect_rates(
        config: AppConfig, 
        product_dir: Path,
        _db_manager: Optional['DatabaseManager'] = None,
        snapshot_signature: str = "") -> Dict[str, Any] | None:
        """[重构] 计算 Lot 级良率 (现在它是独立的第一顺位)"""
        logging.info("--- [Cache Miss] 计算 Lot 级良率... ---")

        panel_df = YieldAnalysisService.get_modified_panel_details(config, _db_manager, snapshot_signature)
        if panel_df.empty: return None

        # 1. 独立获取 Array Time (不再依赖 Sheet 结果)
        lot_ids = panel_df['lot_id'].unique().tolist()
        array_times_df = YieldAnalysisService._get_array_times(
            tuple(lot_ids),
            config,
            _db_manager,
            snapshot_signature,
        )

        # 2. 依赖 MWD 数据
        mwd_code_data = YieldAnalysisService.get_code_level_trend_data(
            config, product_dir, _db_manager, snapshot_signature
        )
        warning_lines = YieldAnalysisService.load_static_warning_lines(
            config,
            product_dir,
            snapshot_signature,
        )

        # 3. 核心计算
        return calculate_lot_defect_rates(
            panel_details_df=panel_df,
            array_input_times_df=array_times_df, # 传入原生时间表
            mwd_code_data=mwd_code_data,
            config=config,
            product_dir=product_dir,
            warning_lines=warning_lines
        )

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_sheet_defect_rates(
        config: AppConfig, 
        product_dir: Path,
        _db_manager: Optional['DatabaseManager'] = None,
        snapshot_signature: str = "") -> Dict[str, Any] | None:
        """[重构] 计算 Sheet 级良率 (听命于 Lot 级数据)"""
        logging.info("--- [Cache Miss] 计算 Sheet 级良率... ---")
        
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _db_manager, snapshot_signature)
        if panel_df.empty: return None

        lot_ids = panel_df['lot_id'].unique().tolist()
        array_times_df = YieldAnalysisService._get_array_times(
            tuple(lot_ids),
            config,
            _db_manager,
            snapshot_signature,
        )
        
        # [核心变动]：先拿 Lot 结果作为“发牌官”
        lot_results = YieldAnalysisService.get_lot_defect_rates(
            config, product_dir, _db_manager, snapshot_signature
        )
        if not lot_results: return None

        return calculate_sheet_defect_rates(
            panel_details_df=panel_df,
            array_input_times_df=array_times_df,
            lot_results=lot_results, # 注入 Lot 结果
            config=config,
            product_dir=product_dir
        )

    # ==========================================================================
    #  4. Mapping 业务
    # ==========================================================================
    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_mapping_data(config: AppConfig, scaling_factor: float = 1.0, _db_manager: Optional['DatabaseManager'] = None, snapshot_signature: str = "", product_dir: Path | None = None, modifier_signature: str = "") -> pd.DataFrame:
        """准备 Mapping 数据（不良数 = 全局倍率 × 月度缩放倍数 × 级联衰减）"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _db_manager, snapshot_signature)
        if panel_df.empty: return pd.DataFrame()
        monthly_factors = None
        if product_dir is not None:
            modifier_context = YieldAnalysisService._build_modifier_context(
                config, product_dir, panel_df
            )
            monthly_factors = modifier_context["factors"]
        return prepare_mapping_data(
            panel_details_df=panel_df,
            scaling_factor=scaling_factor,
            hotspot_scripts=config.processing.get('mapping_hotspot_script', []),
            product_code=config.data_source.product_code,
            mapping_layout=config.processing.get('mapping_layout'),
            monthly_factors=monthly_factors,
        )

    # ==========================================================================
    #  内部辅助方法 (依然需要缓存)
    # ==========================================================================
    @staticmethod
    @st.cache_data(show_spinner=False)
    def _get_array_times(
        lot_ids: Tuple[str, ...],
        config: AppConfig,
        _db_manager: Optional['DatabaseManager'] = None,
        snapshot_signature: str = "",
    ) -> pd.DataFrame:
        """独立的 Array Time 查询缓存"""
        if not lot_ids: return pd.DataFrame()
        
        # 为了实例化 Repo，我们需要 snapshot_path，但 get_array_input_times 其实不依赖 snapshot。
        # 这里我们仅为了满足 __init__ 签名传入 dummy path，或者从 config 获取。
        processing_conf = config.processing
        snapshot_path = Path(processing_conf.get('snapshot_path', 'dummy.parquet'))
        
        repo = PanelRepository(
            snapshot_path=snapshot_path,
            data_policy=YieldDataPolicy(),
            use_snapshot=False,
            db_manager=_db_manager,
        )
        
        # 从 config 获取自定义时间
        input_time_conf = processing_conf.get('array_input_time', {})
        custom_times = input_time_conf.get('custom_times', {})
        
        return repo.get_array_input_times(list(lot_ids), custom_times)
    
    @staticmethod
    @st.cache_data(show_spinner=False)
    def load_static_warning_lines(
        config: AppConfig,
        product_dir: Path,
        snapshot_signature: str = "",
    ) -> Dict[str, Any]:
        """
        [新功能 - 降维打击版]
        读取警戒线配置 (完全依赖注入，重构为高内聚的列提取逻辑)
        """
        try:
            # [Refactor] 从 config.paths 获取 FileResource 对象
            warning_res = config.paths.get('static_warning_lines')
            if not warning_res:
                logging.warning("Config 中未找到 'static_warning_lines' 配置。")
                return {}

            # 构建完整路径
            file_path = product_dir.parent / warning_res.file_name
            sheet_name = warning_res.sheet_name or "Sheet1"

            if not file_path.exists():
                logging.warning(f"警戒线文件不存在: {file_path}")
                return {}

            # --- 步骤 1: 读取 Excel 并“降维”为 CSV ---
            df_raw = pd.read_excel(
                file_path, header=None, dtype=str, engine='openpyxl', sheet_name=sheet_name
            )
            csv_buffer = io.StringIO()
            df_raw.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)
            df_clean = pd.read_csv(csv_buffer, header=None, dtype=str).fillna("")
            
            logging.info(f"Excel 已在内存中清洗为纯文本矩阵，形状: {df_clean.shape}")

            # =================================================================
            # 🛠️ 步骤 2: 定义通用提取器 (DRY 原则重构)
            # =================================================================
            header_row = df_clean.iloc[0]
            
            def _get_col_index(keywords: list) -> int:
                """根据关键词列表动态寻找列索引"""
                for idx, val in enumerate(header_row):
                    val_str = str(val).strip().lower()
                    if any(k in val_str for k in keywords):
                        return idx
                return -1

            def _parse_rate(val_raw) -> float | None:
                """统一的数值解析器：处理百分号、空值与浮点数转换"""
                v_str = str(val_raw).strip()
                if not v_str: return None
                try:
                    return float(v_str.replace('%', '')) / 100.0 if '%' in v_str else float(v_str)
                except ValueError:
                    return None

            # 动态获取所有必需和非必需列的索引
            code_col_idx = _get_col_index(['code'])
            upper_col_idx = _get_col_index(['预警线', 'warning', 'limit'])
            lower_col_idx = _get_col_index(['下限'])

            # 核心防呆校验
            if code_col_idx == -1 or upper_col_idx == -1:
                error_msg = f"表头验证失败：未找到包含 'Code' 或 '预警线/Limit' 的必需列。"
                logging.error(error_msg)
                st.error(error_msg) 
                return {}

            # =================================================================
            # 🚀 步骤 3: 遍历提取数据 (双轨结构)
            # =================================================================
            warning_lines = {}
            valid_count = 0
            
            for curr_r in range(1, len(df_clean)):
                code_str = str(df_clean.iloc[curr_r, code_col_idx]).strip()
                if not code_str: continue
                
                # 极简复用：提取上限
                upper_val = _parse_rate(df_clean.iloc[curr_r, upper_col_idx])
                if upper_val is None: continue # 上限是核心，没有上限则此行无效
                
                # 极简复用：提取下限 (允许不存在，默认 0.0)
                lower_val = 0.0
                if lower_col_idx != -1:
                    parsed_lower = _parse_rate(df_clean.iloc[curr_r, lower_col_idx])
                    if parsed_lower is not None:
                        lower_val = parsed_lower
                
                # 装载到字典
                warning_lines[code_str] = {
                    'upper': upper_val,
                    'lower': lower_val
                }
                valid_count += 1

            logging.info(f"✅ 警戒线加载成功，共提取 {valid_count} 条配置 (含上下限)。")
            return warning_lines

        except Exception as e:
            logging.error(f"读取警戒线配置失败: {e}", exc_info=True)
            return {}
        
    @staticmethod
    def safe_refresh_snapshots(
        _db_manager: Optional['DatabaseManager'],
        config: AppConfig,
    ) -> bool:
        """
        [生命周期钩子] 代理 UI 的强刷指令，触发底层的安全覆写。
        """
        import logging
        from pathlib import Path
        
        try:
            query, data_policy = YieldAnalysisService._build_panel_request(config)
            
            snapshot_path = build_yield_snapshot_path(
                Path("data"), query.product_code, data_policy
            )
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            
            repo = PanelRepository(
                snapshot_path=snapshot_path,
                data_policy=data_policy,
                use_snapshot=True,
                db_manager=_db_manager,
            )
            
            logging.info(f"🔄 [YieldService] 向底层下发 {query.product_code} 强刷指令 (Force Refresh)...")
            
            # 穿透强刷指令
            df = repo.get_panel_details(query=query, force_refresh=True)
            
            return not df.empty
            
        except Exception as e:
            logging.error(f"❌ Yield 快照安全覆写代理调度失败: {e}")
            return False
