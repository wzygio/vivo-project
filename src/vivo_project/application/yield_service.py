import logging, os
import numpy as np
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
import io

# [Refactor] 移除 CONFIG, RESOURCE_DIR, PROJECT_ROOT 全局引用
from vivo_project.config_model import AppConfig
from vivo_project.infrastructure.repositories.panel_repository import PanelRepository

# --- Core (Processors) ---
from vivo_project.core.mwd_trend_processor import (
    create_mwd_trend_data, 
    create_code_level_mwd_trend_data, 
    create_current_month_trend_data
)
from vivo_project.core.sheet_lot_processor import (
    calculate_lot_defect_rates, 
    calculate_sheet_defect_rates
)
from vivo_project.core.mapping_processor import prepare_mapping_data
from vivo_project.core.defect_modifier import (
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

    _end_date: datetime = datetime.now()
    _start_date: datetime = _end_date - relativedelta(months=3)

    @classmethod
    def set_analysis_end_date(cls, end_date: datetime):
        """允许外部注入结束时间"""
        cls._end_date = end_date
        cls._start_date = end_date - relativedelta(months=3)
        logging.info(f"分析时间窗口已更新: {cls._start_date.date()} -> {cls._end_date.date()}")

    @staticmethod
    def _get_core_revision(project_root: Path) -> float:
        """
        [热重载核心] 获取 Core 层代码的最新修改时间戳。
        [Refactor] 接收 project_root 路径。
        """
        try:
            core_dir = project_root / "src" / "vivo_project" / "core"
            max_mtime = 0.0
            if core_dir.exists():
                for f in core_dir.glob("*.py"):
                    mtime = f.stat().st_mtime
                    if mtime > max_mtime:
                        max_mtime = mtime
            return max_mtime
        except Exception:
            return 0.0
        
    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_raw_panel_details(config: AppConfig, _core_revision: float = 0.0) -> pd.DataFrame:
        """
        [L1 Cache] 从数据库加载原始 Panel 数据。
        注意: TTL 由 cache_ttl_hours 决定，但静态装饰器无法动态读取 config。
        建议在 UI 层调用 st.cache_data.clear() 或使用 session_state 管理强刷。
        """
        logging.info("--- [L1 Cache Miss] 加载原始 Panel 数据... ---")
        
        # 1. 提取仓库配置
        processing_conf = config.processing
        snapshot_path_str = processing_conf.get('snapshot_path', 'data/panel_details_snapshot.parquet')
        # 假设 snapshot 位于项目根目录下，这里需要注意 snapshot_path 的解析方式。
        # 暂时假定运行目录即为根目录，或者在 Repo 内部处理。
        # 为了稳健，Repo 接收 Path 对象。
        # 这里的路径相对性取决于 ConfigLoader 如何定义 root。
        # 通常建议 snapshot_path 是相对路径，在 Repo 内部结合 root 使用，或者在这里结合 cwd。
        # 这里为了简化，直接传 Path 对象。
        snapshot_path = Path(snapshot_path_str) 
        
        use_snapshot = processing_conf.get('use_local_snapshot', True)

        # 2. 实例化 Repo (注入配置)
        repo = PanelRepository(
            snapshot_path=snapshot_path,
            use_snapshot=use_snapshot
        )
        
        end_date = YieldAnalysisService._end_date
        start_date = YieldAnalysisService._start_date
        
        logging.info(f"当前查询时间窗口: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")

        # 3. 提取查询参数
        ds_config = config.data_source
        
        return repo.get_panel_details(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            product_code=ds_config.product_code,
            work_order_types=ds_config.work_order_types,
            target_defect_groups=ds_config.target_defect_groups 
        )

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_modified_panel_details(config: AppConfig, _core_revision: float = 0.0) -> pd.DataFrame:
        """[L2 Cache] 获取经过修饰(分散/衰减)后的 Panel 数据"""
        
        # 1. 获取 L1 数据 (传递 config)
        raw_df = YieldAnalysisService.get_raw_panel_details(config, _core_revision)
        
        if raw_df.empty: return pd.DataFrame()
        
        # 2. 应用修饰
        processed_df = raw_df.copy()

        # 缺陷衰减 (从 config.processing 获取)
        multipliers_config = config.processing.get('defect_multipliers', {})
        if multipliers_config:
            logging.info("应用缺陷衰减...")
            processed_df = apply_defect_multipliers(processed_df, multipliers_config)
            
        return processed_df

    # ==========================================================================
    #  2. 趋势图业务 (Trend Analysis)
    # ==========================================================================

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_mwd_trend_data(config: AppConfig, resource_dir: Path, ema_span: int = 14, scaling_factor: float = 1, _core_revision: float = 0.0) -> Dict[str, pd.DataFrame] | None:
        """获取月/周/天趋势数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _core_revision)
        if panel_df.empty: return None
        
        # [Refactor] 传入 config 和 resource_dir 给 Core 层
        return create_mwd_trend_data(
            panel_details_df=panel_df,
            config=config,
            resource_dir=resource_dir,
            ema_span=ema_span,
            scaling_factor=scaling_factor
        )

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_current_month_trend_data(config: AppConfig, _core_revision: float = 0.0) -> pd.DataFrame | None:
        """获取当月日度趋势"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _core_revision)
        if panel_df.empty: return None
        
        return create_current_month_trend_data(panel_details_df=panel_df)

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_code_level_trend_data(config: AppConfig, resource_dir: Path, ema_span: int = 14, scaling_factor: float = 0.7, _core_revision: float = 0.0) -> Dict[str, pd.DataFrame] | None:
        """获取 Code 级趋势数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _core_revision)
        if panel_df.empty: 
            logging.error("获取基础Panel级数据失败，无法生成Code级趋势图。")
            return None
            
        return create_code_level_mwd_trend_data(
            panel_details_df=panel_df, 
            config=config,
            resource_dir=resource_dir,
            ema_span=ema_span, 
            scaling_factor=scaling_factor
        )

    # ==========================================================================
    #  3. Sheet & Lot 级计算 (Heavy Calculation)
    # ==========================================================================
    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_sheet_defect_rates(config: AppConfig, resource_dir: Path, _core_revision: float = 0.0) -> Dict[str, Any] | None:
        """计算 Sheet 级良率 (注入警戒线)"""
        logging.info("--- [Cache Miss] 计算 Sheet 级良率... ---")
        
        # 1. 主数据
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _core_revision)
        if panel_df.empty: return None

        # 2. 依赖数据
        lot_ids = panel_df['lot_id'].unique().tolist()
        array_times_df = YieldAnalysisService._get_array_times(tuple(lot_ids), config)
        
        # 生成辅助的 MWD Code 数据 (用于模拟热点)
        mwd_code_data = create_code_level_mwd_trend_data(
            panel_details_df=panel_df, 
            config=config,
            resource_dir=resource_dir,
            ema_span=30, 
            scaling_factor=0.7
        )

        # 3. 加载警戒线配置
        warning_lines = YieldAnalysisService.load_static_warning_lines(config, resource_dir)

        # 4. 核心计算 (传入 config 和 resource_dir)
        return calculate_sheet_defect_rates(
            panel_details_df=panel_df,
            array_input_times_df=array_times_df,
            mwd_code_data=mwd_code_data,
            start_date=YieldAnalysisService._start_date, 
            config=config,
            resource_dir=resource_dir,
            warning_lines=warning_lines
        )

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_lot_defect_rates(config: AppConfig, resource_dir: Path, _core_revision: float = 0.0) -> Dict[str, Any] | None:
        """计算 Lot 级良率 (注入警戒线)"""
        logging.info("--- [Cache Miss] 计算 Lot 级良率... ---")

        # 1. 主数据
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _core_revision)
        if panel_df.empty: return None

        # 2. 依赖 Sheet 结果 (传入 config)
        sheet_results = YieldAnalysisService.get_sheet_defect_rates(config, resource_dir, _core_revision)
        if not sheet_results: return None

        # 3. 依赖 MWD 数据
        mwd_code_data = create_code_level_mwd_trend_data(
            panel_details_df=panel_df, 
            config=config,
            resource_dir=resource_dir,
            ema_span=30, 
            scaling_factor=0.7
        )
        
        # 4. 加载警戒线配置
        warning_lines = YieldAnalysisService.load_static_warning_lines(config, resource_dir)

        # 5. 核心计算
        return calculate_lot_defect_rates(
            panel_details_df=panel_df,
            sheet_results=sheet_results,
            mwd_code_data=mwd_code_data,
            start_date=YieldAnalysisService._start_date,
            config=config,
            resource_dir=resource_dir,
            warning_lines=warning_lines
        )

    # ==========================================================================
    #  4. Mapping 业务
    # ==========================================================================

    @staticmethod
    @st.cache_data(show_spinner=False)
    def get_mapping_data(config: AppConfig, _core_revision: float = 0.0) -> pd.DataFrame:
        """准备 Mapping 数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details(config, _core_revision)
        if panel_df.empty: return pd.DataFrame()
        return prepare_mapping_data(panel_details_df=panel_df)

    # ==========================================================================
    #  内部辅助方法 (依然需要缓存)
    # ==========================================================================

    @staticmethod
    @st.cache_data(show_spinner=False)
    def _get_array_times(lot_ids: Tuple[str, ...], config: AppConfig) -> pd.DataFrame:
        """独立的 Array Time 查询缓存"""
        if not lot_ids: return pd.DataFrame()
        
        # 为了实例化 Repo，我们需要 snapshot_path，但 get_array_input_times 其实不依赖 snapshot。
        # 这里我们仅为了满足 __init__ 签名传入 dummy path，或者从 config 获取。
        processing_conf = config.processing
        snapshot_path = Path(processing_conf.get('snapshot_path', 'dummy.parquet'))
        
        repo = PanelRepository(snapshot_path=snapshot_path, use_snapshot=False)
        
        # 从 config 获取自定义时间
        input_time_conf = processing_conf.get('array_input_time', {})
        custom_times = input_time_conf.get('custom_times', {})
        
        return repo.get_array_input_times(list(lot_ids), custom_times)
    
    @staticmethod
    @st.cache_data(show_spinner=False)
    def load_static_warning_lines(config: AppConfig, resource_dir: Path) -> Dict[str, Any]:
        """
        [新功能 - 降维打击版]
        读取警戒线配置 (完全依赖注入)
        """
        try:
            # [Refactor] 从 config.paths 获取 FileResource 对象
            warning_res = config.paths.get('static_warning_lines')
            if not warning_res:
                logging.warning("Config 中未找到 'static_warning_lines' 配置。")
                return {}

            # 构建完整路径
            file_path = resource_dir / warning_res.file_name
            sheet_name = warning_res.sheet_name or "Sheet1"

            if not file_path.exists():
                logging.warning(f"警戒线文件不存在: {file_path}")
                return {}

            # --- 步骤 1: 读取 Excel 并“降维”为 CSV ---
            # 使用 openpyxl 引擎读取
            df_raw = pd.read_excel(
                file_path, 
                header=None, 
                dtype=str, 
                engine='openpyxl',
                sheet_name=sheet_name
            )
            
            # [关键] 模拟“另存为 CSV”的过程
            csv_buffer = io.StringIO()
            df_raw.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)
            df_clean = pd.read_csv(csv_buffer, header=None, dtype=str).fillna("")
            
            logging.info(f"Excel 已在内存中清洗为纯文本矩阵，形状: {df_clean.shape}")

            # --- 步骤 2: 使用固定的列位置 ---
            header_row_idx = 0  # 第一行
            code_col_idx = 1    # B列
            limit_col_idx = 5   # F列
            
            # 验证表头内容
            code_header = str(df_clean.iloc[0, code_col_idx]).strip().lower()
            limit_header = str(df_clean.iloc[0, limit_col_idx]).strip().lower()
            
            if code_header != 'code' or not any(keyword in limit_header for keyword in ['预警线', 'warning', 'limit']):
                error_msg = f"表头验证失败：B列应为'Code'（实际：{code_header}），F列应包含'预警线'相关关键词（实际：{limit_header}）"
                logging.error(error_msg)
                # 注意：Service 层尽量不要直接调 st.error，除非是单纯的工具类。
                # 但这里保持原逻辑。
                st.error(error_msg) 
                return {}

            # --- 步骤 3: 精准提取数据 ---
            warning_lines = {}
            valid_count = 0
            
            for curr_r in range(header_row_idx + 1, len(df_clean)):
                raw_code = df_clean.iloc[curr_r, code_col_idx]
                raw_val = df_clean.iloc[curr_r, limit_col_idx]
                
                code_str = str(raw_code).strip()
                val_str = str(raw_val).strip()
                
                try:
                    final_val = 0.0
                    if '%' in val_str:
                        final_val = float(val_str.replace('%', '')) / 100.0
                    else:
                        final_val = float(val_str)
                    
                    warning_lines[code_str] = final_val
                    valid_count += 1
                    
                except ValueError:
                    continue

            logging.info(f"✅ 警戒线加载成功，共提取 {valid_count} 条配置。")
            return warning_lines

        except Exception as e:
            logging.error(f"读取警戒线配置失败: {e}", exc_info=True)
            return {}