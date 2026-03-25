# src/vivo_project/infrastructure/repositories/panel_repository.py
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta

from shared_kernel.infrastructure.db_handler import DatabaseManager
from yield_domain.infrastructure.data_loader import load_panel_details, load_array_input_times

class PanelRepository:
    """
    [仓储层] PanelRepository
    职责：Service 层与数据库的接口。
    [能力]: 
    1. TTL 缓存保护 (12h 内不连库)
    2. 增量更新 (只查最近 3 天)
    3. 滚动窗口 (自动裁剪过期数据)
    """
    
    # [还原] 缓存有效期：12小时
    SNAPSHOT_TTL_HOURS = 12
    # [新增] 增量缓冲：3天
    INCREMENTAL_BUFFER_DAYS = 3 

    def __init__(
        self, 
        snapshot_path: Path,
        use_snapshot: bool = True,
        db_manager: Optional[DatabaseManager] = None
    ):
        if db_manager:
            self.db = db_manager
        else:
            self.db = DatabaseManager()
            
        self.snapshot_path = snapshot_path
        self.use_snapshot = use_snapshot

    def get_panel_details(
        self, 
        start_date: str, 
        end_date: str, 
        product_code: str,
        work_order_types: List[str],
        target_defect_groups: List[str],
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取 Panel 数据 (TTL 保护 + 增量更新)。
        包含基于业务的安全去重逻辑、强刷指令拦截与数据库容灾降级。
        """
        req_start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        req_end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        df_cache = pd.DataFrame()
        cache_exists = False
        is_cache_fresh = False  

        # --- Phase 1: 加载缓存 & 检查 TTL (注入强刷拦截) ---
        if self.use_snapshot and self.snapshot_path.exists():
            try:
                stat = self.snapshot_path.stat()
                mtime = datetime.fromtimestamp(stat.st_mtime)
                age_hours = (datetime.now() - mtime).total_seconds() / 3600

                df_cache = pd.read_parquet(self.snapshot_path)
                if not df_cache.empty and 'warehousing_time' in df_cache.columns:
                    df_cache['warehousing_time'] = pd.to_datetime(df_cache['warehousing_time'])
                    cache_exists = True

                    # [核心升级] 拦截强刷指令
                    if force_refresh:
                        logging.info("⚡ [YieldRepo] 收到强刷指令，强制标记快照为过期，准备安全覆写！")
                        is_cache_fresh = False
                    else:
                        if age_hours < self.SNAPSHOT_TTL_HOURS:
                            max_cached_date = df_cache['warehousing_time'].max()
                            if max_cached_date >= req_end_dt:
                                is_cache_fresh = True
                                logging.info(f"⏱️ [YieldRepo] 缓存有效 (年龄 {age_hours:.1f}h < {self.SNAPSHOT_TTL_HOURS}h)。")
                            else:
                                logging.info("⏰ [YieldRepo] 缓存虽未过12h，但缺少目标尾部数据，触发增量拉取！")
                        else:
                            logging.info(f"⏰ [YieldRepo] 缓存已过期 (年龄 {age_hours:.1f}h)，准备执行增量更新。")
            except Exception as e:
                logging.warning(f"⚠️ 缓存读取失败: {e}")
                cache_exists = False

        # --- Phase 2: 决策逻辑与数据库容灾降级 ---
        df_final = pd.DataFrame()
        need_save = False

        if cache_exists and is_cache_fresh:
            logging.info("🚀 [YieldRepo] 命中有效缓存，跳过数据库查询。")
            df_final = df_cache
        elif cache_exists and not df_cache.empty:
            # === 增量更新模式 ===
            logging.info("🔄 [YieldRepo] 执行增量更新 (Safe Overwrite)...")
            max_cached_date = df_cache['warehousing_time'].max()
            delta_start_dt = max_cached_date - timedelta(days=self.INCREMENTAL_BUFFER_DAYS)
            
            if delta_start_dt < req_end_dt:
                delta_s_str = delta_start_dt.strftime("%Y-%m-%d")
                try:
                    df_delta = self._fetch_from_db_in_chunks(
                        delta_s_str, end_date, 
                        product_code, work_order_types, target_defect_groups
                    )
                    
                    if not df_delta.empty:
                        df_delta['warehousing_time'] = pd.to_datetime(df_delta['warehousing_time'])
                        logging.info(f"   >> 合并: 缓存({len(df_cache)}) + 增量({len(df_delta)})")
                        df_combined = pd.concat([df_cache, df_delta], ignore_index=True)
                        
                        if 'defect_desc' in df_combined.columns:
                            df_combined.drop_duplicates(subset=['panel_id', 'defect_desc'], keep='last', inplace=True)
                        else:
                            df_combined.drop_duplicates(subset=['panel_id'], keep='last', inplace=True)
                            
                        df_final = df_combined
                        need_save = True
                    else:
                        logging.info("   >> 增量查询为空，沿用旧缓存。")
                        df_final = df_cache
                except Exception as e:
                    # [容灾防线 1] 增量拉取挂掉，无损回退旧快照
                    logging.warning(f"🚨 数据库增量拉取失败 ({e})，安全回退至陈旧快照！")
                    df_final = df_cache
            else:
                df_final = df_cache
        else:
            # === 全量刷新模式 ===
            logging.info("🆕 [YieldRepo] 执行全量刷新 (Full Refresh)...")
            try:
                df_final = self._fetch_from_db_in_chunks(
                    start_date, end_date, 
                    product_code, work_order_types, target_defect_groups
                )
                if not df_final.empty:
                    df_final['warehousing_time'] = pd.to_datetime(df_final['warehousing_time'])
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

        # --- Phase 3: 全局安全去重、滚动裁剪 & 持久化 ---
        if not df_final.empty:
            # ====================================================================
            # ✅ [核心新增：业务级全局去重]
            # 1. 按照入库时间升序排列，确保同一 panel_id 的最新状态在 DataFrame 末尾
            df_final = df_final.sort_values(by='warehousing_time', ascending=True)
            
            # 2. 安全去重 (保留最新状态，并且绝不吞掉同一片玻璃上的多个不良)
            if 'defect_desc' in df_final.columns:
                df_final = df_final.drop_duplicates(subset=['panel_id', 'defect_desc'], keep='last')
            else:
                df_final = df_final.drop_duplicates(subset=['panel_id'], keep='last')
            # ====================================================================
            
            # 持久化逻辑：仅当发生了数据库查询(need_save)时才写入磁盘
            if need_save and self.use_snapshot:
                df_to_save = df_final[df_final['warehousing_time'] >= req_start_dt]
                
                try:
                    self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                    df_to_save.to_parquet(self.snapshot_path, index=False)
                    logging.info(f"💾 [Repo] 快照已更新 (Rolling Window)。")
                except Exception as e:
                    logging.error(f"❌ 快照保存失败: {e}")
                
                df_final = df_to_save

            # 最后的防御性过滤：确保返回给业务层的数据严格符合请求范围
            mask = (df_final['warehousing_time'] >= req_start_dt) & \
                   (df_final['warehousing_time'] <= req_end_dt)
            
            return df_final[mask].reset_index(drop=True)

        return df_final

    def _fetch_from_db_in_chunks(
        self, start_str, end_str, prod, wo_types, target_groups
    ) -> pd.DataFrame:
        """
        内部辅助：分片执行数据库查询
        """
        try:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d")
            end_dt = datetime.strptime(end_str, "%Y-%m-%d")
            chunk_days = 30 # 增量模式下通常只会循环一次
            
            current_start = start_dt
            all_chunks = []
            
            while current_start <= end_dt:
                current_end = current_start + timedelta(days=chunk_days)
                if current_end > end_dt: current_end = end_dt
                
                s_s = current_start.strftime("%Y-%m-%d")
                e_s = current_end.strftime("%Y-%m-%d")
                
                df = load_panel_details(
                    self.db, s_s, e_s, prod, wo_types, target_groups
                )
                if not df.empty: all_chunks.append(df)
                
                current_start = current_end + timedelta(days=1)
                
            if all_chunks:
                return pd.concat(all_chunks, ignore_index=True)
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"❌ 数据库查询失败: {e}")
            return pd.DataFrame()

    def get_array_input_times(self, lot_ids: List[str], custom_times: Optional[dict] = None) -> pd.DataFrame:
        # 保持原有逻辑不变
        if not lot_ids: return pd.DataFrame()
        return load_array_input_times(
            db_manager=self.db,
            lot_ids=lot_ids,
            enable_custom_times=True if custom_times else False,
            custom_times=custom_times
        )