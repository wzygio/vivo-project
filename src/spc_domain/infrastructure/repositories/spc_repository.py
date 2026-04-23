import pandas as pd
import numpy as np
import logging, re
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


from src.spc_domain.infrastructure.data_loader import(
    load_spc_measurements, 
    load_spc_spec_limits, 
    load_valid_spc_params
)
from src.spc_domain.application.dtos import SpcQueryConfig
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.utils.data_inspector import export_probed_details

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

class SpcRepository:
    """
    [仓储层] SPC 数据仓储引擎
    职责：拦截DB直连，维护 Parquet 快照，处理增量更新。
    """
    SNAPSHOT_TTL_HOURS = 8 
    INCREMENTAL_BUFFER_DAYS = 2 

    def __init__(self, snapshot_dir: Path, use_snapshot: bool = True, db_manager: Optional['DatabaseManager'] = None):
        self.snapshot_dir = snapshot_dir
        self.use_snapshot = use_snapshot
        self.db = db_manager

    # ==========================================
    # 🆕 新增接口：规格线数据拉取代理
    # ==========================================
    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        """
        提取产品管控规格线，并应用 YAML 配置覆盖。
        职责代理：让 Service 层彻底与 data_loader 解耦。
        （由于规格表数据量极小且变动不频繁，此处可选择直接透传或后期加入轻量级缓存）
        """
        logging.info(f"[SpcRepo] 代理拉取 {prod_code} 规格基准线...")
        if self.db is None:
            raise ValueError("数据库引擎未初始化。")
        
        # 1. 从数据库获取原始规格
        spec_df = load_spc_spec_limits(self.db, prod_code)
        
        # 2. 从 YAML 配置读取规格覆盖项
        spec_overrides = self._load_spec_overrides_from_yaml(prod_code)
        
        # 3. 应用覆盖
        if spec_overrides and not spec_df.empty:
            spec_df = self._apply_spec_overrides(spec_df, spec_overrides, prod_code)
        
        return spec_df
    
    def _load_spec_overrides_from_yaml(self, prod_code: str) -> list:
        """
        从产品的 YAML 配置文件中读取 spc_spec_override 配置。
        
        Returns:
            list: 规格覆盖配置列表，每个元素为 dict 包含 step_id, param_name, ucl/lcl/usl/lsl 等
        """
        try:
            project_root = ConfigLoader.get_project_root()
            product_yaml_path = project_root / "config" / "products" / f"{prod_code}.yaml"
            
            if not product_yaml_path.exists():
                return []
            
            yaml_config = ConfigLoader._load_yaml(product_yaml_path)
            overrides = yaml_config.get('spc_spec_override', [])
            
            if overrides:
                logging.info(f"[SpcRepo] 从 YAML 加载到 {len(overrides)} 条规格覆盖配置")
            
            return overrides if isinstance(overrides, list) else []
            
        except Exception as e:
            logging.warning(f"[SpcRepo] 读取规格覆盖配置失败: {e}")
            return []
    
    def _apply_spec_overrides(
        self, 
        spec_df: pd.DataFrame, 
        overrides: list, 
        prod_code: str
    ) -> pd.DataFrame:
        """
        将 YAML 中的规格覆盖应用到数据库规格数据框。
        匹配条件：prod_code + step_id + param_name 三者必须完全匹配
        
        Args:
            spec_df: 原始规格数据框
            overrides: YAML 中的覆盖配置列表
            prod_code: 产品代码
            
        Returns:
            pd.DataFrame: 应用覆盖后的规格数据框
        """
        df = spec_df.copy()
        applied_count = 0
        
        for override in overrides:
            # 三重匹配条件：prod_code + step_id + param_name
            override_prod = override.get('prod_code')
            step_id = override.get('step_id')
            param_name = override.get('param_name')
            
            # 1. 检查 prod_code 是否匹配（如果配置了的话）
            if override_prod and override_prod != prod_code:
                continue
            
            # 2. 构建精确匹配条件（step_id 和 param_name 必须同时指定）
            if not step_id or not param_name:
                logging.warning(
                    f"[SpcRepo] 规格覆盖配置不完整，跳过: prod_code={override_prod or prod_code}, "
                    f"step_id={step_id}, param_name={param_name}"
                )
                continue
            
            # 3. 三重匹配：prod_code（已验证）+ step_id + param_name
            mask = (df['step_id'] == step_id) & (df['param_name'] == param_name)
            
            # 4. 应用覆盖值
            if mask.any():
                for col in ['ucl', 'lcl', 'usl', 'lsl', 'target']:
                    if col in override and override[col] is not None:
                        old_val = df.loc[mask, col].iloc[0] if mask.sum() > 0 else None
                        df.loc[mask, col] = override[col]
                        logging.info(
                            f"[SpcRepo] 规格覆盖: {prod_code}-{step_id}-{param_name} {col}: "
                            f"{old_val} → {override[col]}"
                        )
                applied_count += 1
            else:
                logging.warning(
                    f"[SpcRepo] 未找到匹配规格: {prod_code}-{step_id}-{param_name}，跳过覆盖"
                )
        
        if applied_count > 0:
            logging.info(f"[SpcRepo] 共成功应用 {applied_count} 条规格覆盖配置")
        
        return df

    # ==========================================
    # 🔄 优化接口：量测明细拉取 (强制 3 个月看板逻辑)
    # ==========================================
    # [架构升级] 新增 force_refresh 强刷参数
    def get_spc_measurements(self, config: SpcQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        """
        获取量测数据，处理增量更新。
        包含: 强刷指令拦截 (Safe Overwrite) 与 数据库防断连容灾降级。
        """
        req_end_dt = datetime.strptime(config.end_date, "%Y-%m-%d")
        req_start_dt = req_end_dt - relativedelta(months=3) 
        actual_start_str = req_start_dt.strftime("%Y-%m-%d")
        snapshot_path = self.snapshot_dir / f"spc_snapshot_{config.prod_code}.parquet"


        df_cache = pd.DataFrame()
        cache_exists, is_cache_fresh = False, False
        time_col = 'sheet_start_time'

        # --- Phase 1: 加载快照与指令拦截 ---
        if self.use_snapshot and snapshot_path.exists():
            try:
                stat = snapshot_path.stat()
                age_hours = (datetime.now() - datetime.fromtimestamp(stat.st_mtime)).total_seconds() / 3600
                
                df_cache = pd.read_parquet(snapshot_path)
                if not df_cache.empty and time_col in df_cache.columns:
                    df_cache[time_col] = pd.to_datetime(df_cache[time_col])
                    cache_exists = True
                    
                    # ==============================================================
                    # 🚨 [通用探针] 检查本地旧快照中的数据驻留情况
                    # ==============================================================
                    export_probed_details(df_cache, "02_Repo层-历史Parquet缓存")

                    # [核心] 拦截强刷指令
                    if force_refresh:
                        logging.info(f"⚡ [SpcRepo] 收到强刷指令，忽略缓存，执行全量刷新！")
                        cache_exists = False  # [核心修复] 强制视为无缓存，走全量刷新而非增量更新
                        is_cache_fresh = False
                    else:
                        if age_hours < self.SNAPSHOT_TTL_HOURS:
                            if df_cache[time_col].max() >= req_end_dt:
                                is_cache_fresh = True
            except Exception as e:
                logging.warning(f"⚠️ 读取 SPC 快照失败: {e}")
                cache_exists = False

        # --- Phase 2: 智能路由与容灾降级 ---
        df_final = pd.DataFrame()
        need_save = False

        if cache_exists and is_cache_fresh:
            logging.info("🚀 [SpcRepo] 命中 3 个月滚动快照，跳过数据库直连。")
            df_final = df_cache
        elif cache_exists and not df_cache.empty:
            logging.info("🔄 [SpcRepo] 执行增量更新 (Safe Overwrite 模式)...")
            delta_start_dt = df_cache[time_col].max() - timedelta(days=self.INCREMENTAL_BUFFER_DAYS)
            
            if delta_start_dt < req_end_dt:
                try:
                    df_delta = load_spc_measurements(self.db, delta_start_dt.strftime("%Y-%m-%d"), config.end_date, config.prod_code)
                    if not df_delta.empty:
                        df_delta[time_col] = pd.to_datetime(df_delta[time_col])
                        df_combined = pd.concat([df_cache, df_delta], ignore_index=True)
                        df_combined = df_combined.sort_values(by=time_col, ascending=True)
                        df_combined.drop_duplicates(
                            subset=['prod_code', 'factory', 'sheet_id', 'step_id', 'param_name', 'site_name'], keep='last', inplace=True)
                        df_final = df_combined
                        need_save = True
                    else:
                        df_final = df_cache
                except Exception as e:
                    # [容灾防线 1] 增量拉取挂掉，无损回退旧快照
                    logging.warning(f"🚨 数据库增量拉取失败 ({e})，安全回退至陈旧快照！")
                    df_final = df_cache
            else:
                df_final = df_cache
        else:
            logging.info(f"🆕 [SpcRepo] 执行全量刷新 ({actual_start_str} 至 {config.end_date})")
            try:
                df_final = load_spc_measurements(self.db, actual_start_str, config.end_date, config.prod_code)
                if not df_final.empty:
                    df_final[time_col] = pd.to_datetime(df_final[time_col])
                    df_final.drop_duplicates(
                        subset=['prod_code', 'factory', 'sheet_id', 'step_id', 'param_name', 'site_name'], keep='last', inplace=True)
                    need_save = True
                elif cache_exists and not df_cache.empty:
                    # [容灾防线 2] 数据库假死返回空，无损回退
                    logging.warning("🚨 数据库全量拉取返回空数据，安全回退至陈旧快照！")
                    df_final = df_cache
            except Exception as e:
                # [容灾防线 3] 彻底断连，无损回退
                logging.error(f"❌ 数据库全量拉取崩溃 ({e})")
                if cache_exists and not df_cache.empty:
                    logging.warning("🚨 触发极端容灾降级，强行启用本地历史快照续命！")
                    df_final = df_cache

        # --- Phase 3: 持久化与内存过滤 ---
        if not df_final.empty:
            if need_save and self.use_snapshot:
                # 滚动抛弃：只保留 req_start_dt 之后的三个月数据写入硬盘（⚠️ 此处写入的是不挑参数的全量数据！）
                df_to_save = df_final[df_final[time_col] >= req_start_dt]
                try:
                    self.snapshot_dir.mkdir(parents=True, exist_ok=True)
                    df_to_save.to_parquet(snapshot_path, index=False)
                    if force_refresh:
                        logging.info("✅ [SpcRepo] 安全覆写 (Safe Overwrite) 完成！")
                except Exception as e:
                    logging.error(f"❌ 快照覆写保存失败: {e}")
                df_final = df_to_save

            mask_time = (df_final[time_col] >= req_start_dt) & (df_final[time_col] <= req_end_dt)
            # 引入 copy() 防止后续赋值触发 Pandas 的 SettingWithCopyWarning
            df_filtered = df_final[mask_time].copy() 

            # =================================================================
            # [核心修复] 动态内存过滤：索要当前产品的专属白名单
            # [扩展] 支持按 data_type 筛选: SPC, CTQ, AOI, ALL
            # =================================================================
            data_type_filter = getattr(config, 'data_type_filter', 'ALL')
            valid_params_df = load_valid_spc_params(self.db, config.prod_code, data_type_filter)
            
            if valid_params_df is not None:
                if not valid_params_df.empty:
                    # 1. 统一转大写建立连接键
                    df_filtered['param_name_upper'] = df_filtered['param_name'].str.upper()
                    
                    # 2. 内连接：既过滤了不合规的参数，又顺路带回了 data_type 列
                    df_filtered = df_filtered.merge(
                        valid_params_df,
                        left_on='param_name_upper',
                        right_on='ref_param_name',
                        how='inner'
                    )
                    
                    # 3. 清理联结产生的临时字段
                    df_filtered = df_filtered.drop(columns=['param_name_upper', 'ref_param_name'])
                    
                    logging.info(f"[SpcRepo] 内存过滤与 data_type 注入完成，下发 {len(valid_params_df)} 种专属参数。")
                else:
                    logging.warning(f"[SpcRepo] 警告：产品 {config.prod_code} 查无 SPC 参数白名单！已清空本批次数据。")
                    df_filtered = df_filtered.iloc[0:0] 
            else:
                logging.error("[SpcRepo] 严重警告：拉取 SPC 参数白名单失败，下发全量参数并标记未知类型。")
                df_filtered['data_type'] = 'UNKNOWN'

            df_filtered = self._apply_outlier_filters(df_filtered, config.prod_code)

            # 原有的维度过滤
            if config.factory:
                df_filtered = df_filtered[df_filtered['factory'] == config.factory.upper()]
            if config.step_id:
                df_filtered = df_filtered[df_filtered['step_id'] == config.step_id]
            if config.param_name:
                df_filtered = df_filtered[df_filtered['param_name'] == config.param_name]

            # =================================================================
            # 🚨 [追踪矩阵 1]：Repo 层彻底处理完毕，即将离开 Repo 
            # =================================================================
            export_probed_details(df_filtered, "Track_01_Repo即将返回")

            return df_filtered.reset_index(drop=True)

        return df_final
    
    def _apply_outlier_filters(self, df: pd.DataFrame, prod_code: str) -> pd.DataFrame:
        """
        [物理级拦截器] 根据 Excel 预设的数字边界剔除异常点位 (site_name)。
        逻辑：value <= lower_col 或 value >= upper_col 的数据将被物理剔除。
        """
        import io
        from src.shared_kernel.config import ConfigLoader

        # 1. 路径锁定
        project_root = ConfigLoader.get_project_root()
        rule_file = project_root / "resources" / "spc_outlier_filters.xlsx"
        
        if not rule_file.exists() or df.empty:
            return df

        try:
            # 2. 降维读取：Excel -> CSV Buffer -> DataFrame (免疫格式干扰) 
            df_raw = pd.read_excel(rule_file, header=None, dtype=str, engine='openpyxl')
            csv_buffer = io.StringIO()
            df_raw.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)
            df_clean = pd.read_csv(csv_buffer, header=None, dtype=str).fillna("")

            if len(df_clean) < 2:
                return df

            # 3. 提取表头索引 (严格匹配您提供的列名)
            header_row = df_clean.iloc[0].astype(str).str.strip()
            col_indices = {col_name: idx for idx, col_name in enumerate(header_row)}

            # 核心校验
            if not all(k in col_indices for k in ['step_col', 'param_col']):
                logging.warning(f"⚠️ [SpcRepo] 过滤规则表头缺失核心字段。提取到的表头: {header_row.tolist()}")
                return df

            # 4. 初始化掩码与数值准备
            outlier_mask = pd.Series(False, index=df.index)
            # 统一转为数字类型以便对比 [cite: 11]
            df_vals = pd.to_numeric(df['param_value'], errors='coerce')

            applied_count = 0

            # 5. 遍历规则行
            for curr_r in range(1, len(df_clean)):
                rule = df_clean.iloc[curr_r]
                
                r_prod = str(rule[col_indices['prod_col']]).strip().upper() if 'prod_col' in col_indices else 'ALL'
                r_step = str(rule[col_indices['step_col']]).strip()
                r_param = str(rule[col_indices['param_col']]).strip()

                if not r_step or not r_param: continue

                # 产品匹配
                if r_prod and r_prod != 'ALL' and r_prod != prod_code.upper():
                    continue

                # 锁定靶向范围
                target_mask = (df['step_id'] == r_step) & (df['param_name'].str.upper() == r_param.upper())
                if not target_mask.any():
                    continue

                # --- [极简逻辑实现] ---
                # 提取下限值：如果数据 <= 该值，则标记为异常
                if 'lower_col' in col_indices:
                    l_val = pd.to_numeric(rule[col_indices['lower_col']], errors='coerce')
                    if not pd.isna(l_val):
                        outlier_mask |= (target_mask & (df_vals <= l_val))

                # 提取上限值：如果数据 >= 该值，则标记为异常
                if 'upper_col' in col_indices:
                    u_val = pd.to_numeric(rule[col_indices['upper_col']], errors='coerce')
                    if not pd.isna(u_val):
                        outlier_mask |= (target_mask & (df_vals >= u_val))

                applied_count += 1

            # 6. 执行剔除
            if outlier_mask.any():
                drop_count = outlier_mask.sum()
                df = df[~outlier_mask].copy()
                logging.info(f"🛡️ [SpcRepo] 物理防线触发：基于数字边界剔除了 {drop_count} 个异常测量点。")
            else:
                logging.info(f"✅ [SpcRepo] 物理防线扫描完毕，未发现越界点。")

            return df

        except Exception as e:
            logging.error(f"❌ [SpcRepo] 物理过滤执行失败: {e}")
            return df

    # =========================================================================
    # 🆕 新增接口：报废数据适配器
    # =========================================================================
    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        """
        [报废数据适配器] 从 resources/scrap_sheets.xlsx 读取报废数据，
        按 prod_code 过滤后转换为与 SPC 管道兼容的格式（OOC 伪装）。
        
        注意：scrap_sheets.xlsx 是统一文件，内部已包含所有产品的数据，
              通过 '产品型号' 列进行过滤。
        """
        try:
            project_root = ConfigLoader.get_project_root()
            scrap_path = project_root / "resources" / "scrap_sheets.xlsx"
            logging.info(f"🚧 [ScrapTrace][Repo-L1] scrap_path={scrap_path}, exists={scrap_path.exists()}")
            
            if not scrap_path.exists():
                logging.warning(f"[SpcRepo] 报废数据文件不存在: {scrap_path}")
                return pd.DataFrame()
            
            # 1. 读取 Excel（尝试多种引擎）
            df = pd.DataFrame()
            engines = ['openpyxl', 'xlrd']
            for engine in engines:
                try:
                    df = pd.read_excel(scrap_path, engine=engine)
                    logging.info(f"🚧 [ScrapTrace][Repo-L2] 使用引擎 {engine} 读取成功, shape={df.shape}, columns={df.columns.tolist()}")
                    break
                except Exception as e:
                    logging.info(f"🚧 [ScrapTrace][Repo-L2] 引擎 {engine} 失败: {e}")
                    continue
            
            if df.empty:
                logging.info(f"[SpcRepo] 报废数据为空或无法读取: {scrap_path}")
                return pd.DataFrame()
            
            # 2. 列名标准化（支持中文和英文列名）
            col_mapping = {
                '产品型号': 'prod_code',
                'Sheet_ID': 'sheet_id',
                'sheet_id': 'sheet_id',
                '报废时间': 'sheet_start_time',
                '报废时间(yyyy-mm-dd)': 'sheet_start_time',
                'warehousing_time': 'sheet_start_time',
                '报废站点': 'step_id',
                '报废站点(五位代码)': 'step_id',
                'scrap_step': 'step_id',
            }
            
            rename_dict = {src: dst for src, dst in col_mapping.items() if src in df.columns}
            logging.info(f"🚧 [ScrapTrace][Repo-L3] rename_dict={rename_dict}")
            if rename_dict:
                df = df.rename(columns=rename_dict)
            
            # 3. 确保必要列存在
            required_cols = ['prod_code', 'sheet_id', 'sheet_start_time', 'step_id']
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                logging.error(f"[SpcRepo] 报废数据缺少必要列: {missing}，实际列: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # 4. 按产品型号过滤（统一文件内包含多产品数据）
            df['prod_code'] = df['prod_code'].astype(str).str.strip()
            before_filter = len(df)
            df = df[df['prod_code'].str.upper() == prod_code.upper()].copy()
            logging.info(f"🚧 [ScrapTrace][Repo-L4] 按 prod_code={prod_code} 过滤: {before_filter} -> {len(df)} 条")
            
            if df.empty:
                logging.info(f"[SpcRepo] 产品 {prod_code} 在报废数据中无记录")
                return pd.DataFrame()
            
            # 5. 类型转换与清洗
            df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce')
            before_dropna = len(df)
            df = df.dropna(subset=['sheet_start_time'])
            logging.info(f"🚧 [ScrapTrace][Repo-L5] dropna 后: {before_dropna} -> {len(df)} 条, 时间样本: {df['sheet_start_time'].head(3).tolist()}")
            
            # 6. 推断厂别
            df['factory'] = df['step_id'].astype(str).apply(self._infer_factory_from_step)
            logging.info(f"🚧 [ScrapTrace][Repo-L6] 厂别推断: {df['factory'].unique().tolist()}")
            
            # 7. 状态伪装（伪装成 OOC，使 aggregate_spc_metrics 无感知处理）
            df['is_ooc'] = 1
            df['is_oos'] = 0
            df['is_soos'] = 0
            df['param_name'] = '报废'
            df['site_name'] = '报废'
            df['data_type'] = '报废'
            df['spc_status'] = 'OOC'
            
            # 8. 添加必要的占位列（与 apply_spc_rules 输出格式兼容）
            for col in ['sheet_mean', 'sheet_max', 'sheet_min', 'usl', 'lsl', 'ucl', 'lcl']:
                if col not in df.columns:
                    df[col] = np.nan
            
            logging.info(f"🚧 [ScrapTrace][Repo-L7] 最终返回: {len(df)} 条, columns={df.columns.tolist()}")
            return df
            
        except Exception as e:
            logging.error(f"[SpcRepo] 加载报废数据失败: {e}", exc_info=True)
            return pd.DataFrame()

    @staticmethod
    def _infer_factory_from_step(step_id: str) -> str:
        """
        根据报废站点代码推断厂别。
        优先级：精确映射 > 前缀推断 > UNKNOWN
        """
        try:
            config = ConfigLoader.get_scrap_factory_mapping()
            step = str(step_id).strip().upper()
            
            # 1. 精确匹配
            mappings = config.get('mappings', {})
            if step in mappings:
                return mappings[step]
            
            # 2. 前缀推断
            prefix_rules = config.get('default_prefix_rules', {})
            for prefix, factory in prefix_rules.items():
                if step.startswith(str(prefix).upper()):
                    return factory
            
            return 'UNKNOWN'
        except Exception:
            return 'UNKNOWN'