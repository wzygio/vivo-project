import pandas as pd
import logging, re
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


from spc_domain.infrastructure.data_loader import(
    load_spc_measurements, 
    load_spc_spec_limits, 
    load_valid_spc_params
)
from spc_domain.application.dtos import SpcQueryConfig
from src.shared_kernel.config import ConfigLoader

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from shared_kernel.infrastructure.db_handler import DatabaseManager

class SpcRepository:
    """
    [仓储层] SPC 数据仓储引擎
    职责：拦截DB直连，维护 Parquet 快照，处理增量更新。
    """
    SNAPSHOT_TTL_HOURS = 12 
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
        提取产品管控规格线。
        职责代理：让 Service 层彻底与 data_loader 解耦。
        （由于规格表数据量极小且变动不频繁，此处可选择直接透传或后期加入轻量级缓存）
        """
        logging.info(f"[SpcRepo] 代理拉取 {prod_code} 规格基准线...")
        if self.db is None:
            raise ValueError("数据库引擎未初始化。")
        return load_spc_spec_limits(self.db, prod_code)

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

        # --- Phase 1: 加载快照与指令拦截 ---
        if self.use_snapshot and snapshot_path.exists():
            try:
                stat = snapshot_path.stat()
                age_hours = (datetime.now() - datetime.fromtimestamp(stat.st_mtime)).total_seconds() / 3600
                
                df_cache = pd.read_parquet(snapshot_path)
                if not df_cache.empty and 'sheet_start_time' in df_cache.columns:
                    df_cache['sheet_start_time'] = pd.to_datetime(df_cache['sheet_start_time'])
                    cache_exists = True

                    # [核心] 拦截强刷指令
                    if force_refresh:
                        logging.info(f"⚡ [SpcRepo] 收到强刷指令，强制标记快照为过期，准备安全覆写！")
                        is_cache_fresh = False
                    else:
                        if age_hours < self.SNAPSHOT_TTL_HOURS:
                            if df_cache['sheet_start_time'].max() >= req_end_dt:
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
            delta_start_dt = df_cache['sheet_start_time'].max() - timedelta(days=self.INCREMENTAL_BUFFER_DAYS)
            
            if delta_start_dt < req_end_dt:
                try:
                    df_delta = load_spc_measurements(self.db, delta_start_dt.strftime("%Y-%m-%d"), config.end_date, config.prod_code)
                    if not df_delta.empty:
                        df_delta['sheet_start_time'] = pd.to_datetime(df_delta['sheet_start_time'])
                        df_combined = pd.concat([df_cache, df_delta], ignore_index=True)
                        df_combined = df_combined.sort_values(by='sheet_start_time', ascending=True)
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
                    df_final['sheet_start_time'] = pd.to_datetime(df_final['sheet_start_time'])
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
                df_to_save = df_final[df_final['sheet_start_time'] >= req_start_dt]
                try:
                    self.snapshot_dir.mkdir(parents=True, exist_ok=True)
                    df_to_save.to_parquet(snapshot_path, index=False)
                    if force_refresh:
                        logging.info("✅ [SpcRepo] 安全覆写 (Safe Overwrite) 完成！")
                except Exception as e:
                    logging.error(f"❌ 快照覆写保存失败: {e}")
                df_final = df_to_save

            mask_time = (df_final['sheet_start_time'] >= req_start_dt) & (df_final['sheet_start_time'] <= req_end_dt)
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

            return df_filtered.reset_index(drop=True)

        return df_final
    
    def _apply_outlier_filters(self, df: pd.DataFrame, prod_code: str) -> pd.DataFrame:
        """
        [物理级拦截器] 读取 Excel 规则，剔除点位级别 (site_name) 的极端脏数据。
        支持产品通配符 (ALL) 与动态逻辑运算符 (<=, <, >=, >)。
        """
        # [核心修复 2] 绝对路径锁定：利用 ConfigLoader 动态获取根目录
        project_root = ConfigLoader.get_project_root()
        rule_file = project_root / "resources" / "spc_outlier_filters.xlsx"
        
        if not rule_file.exists() or df.empty:
            return df

        try:
            # 1. 加载规则表
            rules_df = pd.read_excel(rule_file, dtype=str).fillna("")
            
            # [核心修复 1] 移除冗余映射，直接实施强表头校验
            required_cols = ['step_col', 'param_col']
            if not all(k in rules_df.columns for k in required_cols):
                logging.warning("⚠️ [SpcRepo] 异常过滤规则表头缺失核心字段(step_col/param_col)，跳过物理过滤。")
                return df

            # 2. 初始化掩码与辅助解析器
            outlier_mask = pd.Series(False, index=df.index)
            df_vals = pd.to_numeric(df['param_value'], errors='coerce')

            def parse_condition(cond_str):
                if not cond_str: return None, None
                match = re.match(r'(<=|>=|<|>|=)?\s*([+-]?\d+\.?\d*)', str(cond_str).strip())
                if match:
                    return match.group(1) or '=', float(match.group(2))
                return None, None

            applied_count = 0

            # 3. 遍历规则库执行靶向捕获
            for _, rule in rules_df.iterrows():
                r_prod = str(rule.get('prod_col', '')).strip().upper()
                r_step = str(rule['step_col']).strip()
                r_param = str(rule['param_col']).strip()

                # 产品白名单过滤
                if r_prod and r_prod != 'ALL' and r_prod != prod_code.upper():
                    continue

                # 锁定靶向数据范围
                target_mask = (df['step_id'] == r_step) & (df['param_name'].str.upper() == r_param.upper())
                if not target_mask.any():
                    continue

                # 获取严格列名下的限制值
                lower_op, lower_val = parse_condition(rule.get('lower_col', ''))
                upper_op, upper_val = parse_condition(rule.get('upper_col', ''))

                # 执行下限捕获
                if lower_val is not None:
                    if lower_op == '<=': outlier_mask |= (target_mask & (df_vals <= lower_val))
                    elif lower_op == '<': outlier_mask |= (target_mask & (df_vals < lower_val))
                    elif lower_op == '>=': outlier_mask |= (target_mask & (df_vals >= lower_val))
                    elif lower_op == '>': outlier_mask |= (target_mask & (df_vals > lower_val))
                    elif lower_op == '=': outlier_mask |= (target_mask & (df_vals == lower_val))

                # 执行上限捕获
                if upper_val is not None:
                    if upper_op == '<=': outlier_mask |= (target_mask & (df_vals <= upper_val))
                    elif upper_op == '<': outlier_mask |= (target_mask & (df_vals < upper_val))
                    elif upper_op == '>=': outlier_mask |= (target_mask & (df_vals >= upper_val))
                    elif upper_op == '>': outlier_mask |= (target_mask & (df_vals > upper_val))
                    elif upper_op == '=': outlier_mask |= (target_mask & (df_vals == upper_val))

                applied_count += 1

            # 4. 执行物理剔除
            if outlier_mask.any():
                drop_count = outlier_mask.sum()
                df = df[~outlier_mask].copy()
                logging.info(f"🛡️ [SpcRepo] 物理防线触发：根据 {applied_count} 条规则，成功剔除了 {drop_count} 个异常量测点！")
            else:
                logging.info(f"✅ [SpcRepo] 物理防线扫描完毕：检查了 {applied_count} 条规则，未发现异常点。")

            return df

        except Exception as e:
            logging.error(f"❌ [SpcRepo] 应用点位异常过滤规则时崩溃: {e}")
            return df