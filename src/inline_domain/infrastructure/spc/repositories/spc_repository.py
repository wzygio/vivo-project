import pandas as pd
import numpy as np
import logging, re

# [Phase 1] 调试追踪专用 Logger
trace_logger = logging.getLogger("trace")
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from src.inline_domain.infrastructure.spc.main_process_trace import (
    apply_main_process_history,
    attach_main_process_spec,
)
from src.inline_domain.infrastructure.spc.measurement_preprocessor import (
    filter_excluded_spc_param_names,
)
from src.inline_domain.core.monitor.monitor_param_classifier import classify_param_type
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.ports.measurement_snapshot import (
    MainProcessHistoryPort,
    MeasurementMetadataPort,
    MeasurementSnapshotPort,
)
from src.shared_kernel.config import ConfigLoader

class SpcRepository:
    """Derive the SPC contract from monitor-owned data ports."""

    def __init__(
        self,
        raw_measurements: MeasurementSnapshotPort,
        metadata: MeasurementMetadataPort,
        main_process_history: MainProcessHistoryPort,
    ) -> None:
        if raw_measurements is None or metadata is None or main_process_history is None:
            raise ValueError(
                "SPC repository requires raw measurement, metadata, and history ports"
            )
        self.raw_measurements = raw_measurements
        self.metadata = metadata
        self.main_process_history = main_process_history

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
        # 1. 从数据库获取原始规格
        spec_df = self.metadata.get_parameter_specs(prod_code)
        
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
    def get_spc_measurements(self, config: SpcQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        """Return an SPC projection derived exclusively from monitor-owned ports."""
        raw = self.raw_measurements.get_measurements(
            config.prod_code,
            config.end_date,
            force_refresh,
        )
        return self._prepare_shared_measurements(raw, config)

    def _prepare_shared_measurements(
        self, raw: pd.DataFrame, config: SpcQueryConfig
    ) -> pd.DataFrame:
        if raw.empty:
            return raw.copy()
        prepared = raw.rename(columns={"start_time": "sheet_start_time"}).copy()
        prepared["sheet_start_time"] = pd.to_datetime(
            prepared["sheet_start_time"], errors="coerce"
        )
        prepared["param_value"] = pd.to_numeric(
            prepared["param_value"], errors="coerce"
        )
        prepared = prepared.dropna(subset=["sheet_start_time", "param_value"])
        prepared = filter_excluded_spc_param_names(prepared)
        prepared = prepared.sort_values("sheet_start_time").drop_duplicates(
            subset=["prod_code", "factory", "sheet_id", "step_id", "param_name", "site_name"],
            keep="last",
        )
        catalog = self.metadata.get_parameter_catalog(config.prod_code)
        if catalog is None:
            prepared["data_type"] = "UNKNOWN"
        elif catalog.empty:
            return prepared.iloc[0:0]
        else:
            typed = catalog.copy()
            typed["data_type"] = typed["data_type"].apply(classify_param_type)
            target = (config.data_type_filter or "ALL").upper()
            if target != "ALL":
                typed = typed[typed["data_type"].eq(target)]
            prepared["param_name_upper"] = prepared["param_name"].astype(str).str.upper()
            prepared = prepared.merge(
                typed,
                left_on="param_name_upper",
                right_on="ref_param_name",
                how="inner",
            ).drop(columns=["param_name_upper", "ref_param_name"])
        prepared = self._apply_outlier_filters(prepared, config.prod_code)
        start = pd.Timestamp(config.start_date)
        end = pd.Timestamp(config.end_date) + pd.Timedelta(days=1)
        prepared = prepared[
            prepared["sheet_start_time"].ge(start) & prepared["sheet_start_time"].lt(end)
        ].copy()
        if config.factory:
            prepared = prepared[prepared["factory"].eq(config.factory.upper())]
        if config.step_id:
            prepared = prepared[prepared["step_id"].eq(config.step_id)]
        if config.param_name:
            prepared = prepared[prepared["param_name"].eq(config.param_name)]
        if prepared.empty:
            return prepared.reset_index(drop=True)
        specs = self.metadata.get_parameter_specs(config.prod_code)
        routed = attach_main_process_spec(prepared.reset_index(drop=True), specs)
        history = self.main_process_history.get_main_process_history(
            routed,
            history_start=start - relativedelta(months=1),
            history_end=pd.Timestamp(config.end_date),
        )
        return apply_main_process_history(routed, history)
    
    def _apply_outlier_filters(self, df: pd.DataFrame, prod_code: str) -> pd.DataFrame:
        """
        [物理级拦截器] 根据 CSV 预设的数字边界剔除异常点位 (site_name)。
        逻辑：value <= lower_col 或 value >= upper_col 的数据将被物理剔除。

        [核心流程] xlsx(加密) → COM 解密 → 另存为 CSV → 读取 CSV → 过滤
        xlsx 被企业加密软件锁定，openpyxl 无法直接读取。
        通过 Windows COM (Excel.Application) 透明解密后，立即另存为 CSV
        覆盖旧文件，再从 CSV 加载规则进行过滤。
        [降级] 当 COM 不可用时（如无 Excel），降级到已有 CSV（带内容校验）。
        """
        import io
        from src.shared_kernel.config import ConfigLoader

        # 1. 路径锁定
        project_root = ConfigLoader.get_project_root()
        rule_file = project_root / "resources" / "spc_outlier_filters.xlsx"
        csv_fallback = project_root / "output" / "decrypted_files" / "spc_outlier_filters.csv"
        
        if df.empty:
            return df

        df_clean = None
        read_source = None

        # ---- 策略 1 (主路径): xlsx(COM解密) → 另存 CSV → 读取 CSV ----
        # xlsx 已被企业加密软件锁定，openpyxl 必然失败。
        # 通过 COM (Excel.Application) 透明解密，立即另存为 CSV 覆盖旧文件。
        if rule_file.exists():
            try:
                from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com
                df_com = _read_encrypted_xlsx_via_com(rule_file)
                if df_com is not None and not df_com.empty:
                    # 立即另存为 CSV，覆盖可能损坏的旧文件
                    csv_fallback.parent.mkdir(parents=True, exist_ok=True)
                    df_com.to_csv(csv_fallback, index=False, encoding='utf-8-sig')
                    logging.info(
                        f"✅ [SpcRepo] COM 解密 xlsx 并另存为 CSV 成功: "
                        f"{csv_fallback.name} (shape={df_com.shape})"
                    )

                    # 从刚保存的 CSV 读取规则（优先走文件读取以保持一致性）
                    try:
                        df_clean = pd.read_csv(csv_fallback, header=None, dtype=str).fillna("")
                        read_source = "csv"
                    except Exception:
                        # CSV 可能刚写出就被加密软件锁定，回退到内存数据
                        csv_buffer = io.StringIO()
                        df_com.to_csv(csv_buffer, index=False, header=True)
                        csv_buffer.seek(0)
                        df_clean = pd.read_csv(csv_buffer, header=None, dtype=str).fillna("")
                        read_source = "excel_com"
                        logging.warning(
                            f"⚠️ [SpcRepo] 刚保存的 CSV 被加密软件锁定，"
                            f"回退到内存数据路径 (来源: excel_com)"
                        )
            except Exception as com_e:
                logging.warning(f"⚠️ [SpcRepo] COM 直读加密 xlsx 失败: {com_e}")

        # ---- 策略 2 (降级): 尝试已有 CSV（带内容有效性校验） ----
        if df_clean is None and csv_fallback.exists():
            try:
                df_csv = pd.read_csv(csv_fallback, dtype=str).fillna("")
                if not df_csv.empty:
                    csv_buffer = io.StringIO()
                    df_csv.to_csv(csv_buffer, index=False, header=True)
                    csv_buffer.seek(0)
                    df_clean = pd.read_csv(csv_buffer, header=None, dtype=str).fillna("")
                    
                    # CSV 内容有效性校验：防止二进制垃圾文件被误用
                    if df_clean is not None and len(df_clean) > 0:
                        probe_header = df_clean.iloc[0].astype(str).str.strip().tolist()
                        if 'step_col' not in probe_header or 'param_col' not in probe_header:
                            logging.warning(
                                f"⚠️ [SpcRepo] CSV 备用文件 {csv_fallback.name} 内容异常"
                                f"（缺少 step_col/param_col，实际表头: {probe_header}），"
                                f"跳过过滤。"
                            )
                            df_clean = None
                        else:
                            read_source = "csv"
                            logging.info(
                                f"✅ [SpcRepo] 从现有 CSV 加载过滤规则: {csv_fallback.name}"
                            )
            except Exception as e:
                logging.warning(f"⚠️ [SpcRepo] 读取 CSV 备用文件失败: {e}")

        # 无任何可用规则
        if df_clean is None:
            logging.warning(
                f"🛡️ [SpcRepo] 无可用的物理过滤规则（COM 不可用且 CSV 无效），"
                f"跳过异常值剔除。当前产品: {prod_code}"
            )
            return df

        if len(df_clean) < 2:
            return df

        try:
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
                # [类型归一化修复] CSV 中 step 值可能带有 ".0" 后缀（如 "21230.0"），
                # 而 SPC 数据中的 step_id 可能是整数 21230 或字符串 "21230"。
                # 将两边都转为字符串并同时尝试带/不带 ".0" 的变体，确保匹配。
                r_step_clean = r_step.rstrip('0').rstrip('.') if '.' in r_step else r_step
                step_variants = [r_step]
                if r_step_clean != r_step:
                    step_variants.append(r_step_clean)
                target_mask = (df['step_id'].astype(str).str.strip().isin(step_variants)) & \
                              (df['param_name'].str.upper() == r_param.upper())
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
                logging.info(
                    f"🛡️ [SpcRepo] 物理防线触发：基于数字边界剔除了 {drop_count} 个异常测量点"
                    f" (来源: {read_source})。"
                )
            else:
                logging.info(f"✅ [SpcRepo] 物理防线扫描完毕，未发现越界点 (来源: {read_source})。")

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
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L1] scrap_path={scrap_path}, exists={scrap_path.exists()}")
            
            if not scrap_path.exists():
                logging.warning(f"[SpcRepo] 报废数据文件不存在: {scrap_path}")
                return pd.DataFrame()
            
            # 1. 读取 Excel（尝试多种引擎）
            df = pd.DataFrame()
            engines = ['openpyxl', 'xlrd']
            for engine in engines:
                try:
                    df = pd.read_excel(scrap_path, engine=engine)
                    trace_logger.info(f"🚧 [ScrapTrace][Repo-L2] 使用引擎 {engine} 读取成功, shape={df.shape}, columns={df.columns.tolist()}")
                    break
                except Exception as e:
                    trace_logger.info(f"🚧 [ScrapTrace][Repo-L2] 引擎 {engine} 失败: {e}")
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
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L3] rename_dict={rename_dict}")
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
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L4] 按 prod_code={prod_code} 过滤: {before_filter} -> {len(df)} 条")
            
            if df.empty:
                logging.info(f"[SpcRepo] 产品 {prod_code} 在报废数据中无记录")
                return pd.DataFrame()
            
            # 5. 类型转换与清洗
            df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce')
            before_dropna = len(df)
            df = df.dropna(subset=['sheet_start_time'])
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L5] dropna 后: {before_dropna} -> {len(df)} 条, 时间样本: {df['sheet_start_time'].head(3).tolist()}")
            
            # 6. 推断厂别
            df['factory'] = df['step_id'].astype(str).apply(self._infer_factory_from_step)
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L6] 厂别推断: {df['factory'].unique().tolist()}")
            
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
            
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L7] 最终返回: {len(df)} 条, columns={df.columns.tolist()}")
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
