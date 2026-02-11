# src/vivo_project/infrastructure/repositories/panel_repository.py
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta

from vivo_project.infrastructure.db_handler import DatabaseManager
from vivo_project.infrastructure.data_loader import load_panel_details, load_array_input_times

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
        """
        req_start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        req_end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        df_cache = pd.DataFrame()
        cache_exists = False
        is_cache_fresh = False  # 标记缓存是否在有效期内

        # --- Phase 1: 加载缓存 & 检查 TTL ---
        if self.use_snapshot and self.snapshot_path.exists():
            try:
                # 1.1 读取文件属性
                stat = self.snapshot_path.stat()
                mtime = datetime.fromtimestamp(stat.st_mtime)
                age_hours = (datetime.now() - mtime).total_seconds() / 3600
                
                # 1.2 判定新鲜度
                if age_hours < self.SNAPSHOT_TTL_HOURS:
                    is_cache_fresh = True
                    logging.info(f"⏱️ [Repo] 缓存有效 (年龄 {age_hours:.1f}h < {self.SNAPSHOT_TTL_HOURS}h)。")
                else:
                    logging.info(f"⏰ [Repo] 缓存已过期 (年龄 {age_hours:.1f}h)，准备执行增量更新。")

                # 1.3 只有在 (有效且不强制刷新) 或者 (需要基于旧数据做增量) 时才读取
                # 这里为了简单，总是先读出来
                df_cache = pd.read_parquet(self.snapshot_path)
                if not df_cache.empty and 'warehousing_time' in df_cache.columns:
                    df_cache['warehousing_time'] = pd.to_datetime(df_cache['warehousing_time'])
                    cache_exists = True
                    
            except Exception as e:
                logging.warning(f"⚠️ 缓存读取失败: {e}")
                cache_exists = False

        # --- Phase 2: 决策逻辑 ---
        df_final = pd.DataFrame()
        need_save = False

        # 场景 A: 缓存有效 且 未强制刷新 -> 直接返回 (0 IO)
        if cache_exists and is_cache_fresh and not force_refresh:
            logging.info("🚀 [Repo] 命中缓存，跳过数据库查询。")
            df_final = df_cache
            
        # 场景 B: 缓存过期 或 强制刷新 -> 执行增量/全量更新
        else:
            if cache_exists and not df_cache.empty:
                # === 增量更新模式 ===
                logging.info("🔄 [Repo] 执行增量更新 (Incremental Update)...")
                
                # 回溯 N 天，确保覆盖迟到数据
                max_cached_date = df_cache['warehousing_time'].max()
                delta_start_dt = max_cached_date - timedelta(days=self.INCREMENTAL_BUFFER_DAYS)
                
                # 只有当请求的结束时间晚于 (缓存最大时间 - 缓冲) 时才需要查
                if delta_start_dt < req_end_dt:
                    delta_s_str = delta_start_dt.strftime("%Y-%m-%d")
                    # 增量查询
                    df_delta = self._fetch_from_db_in_chunks(
                        delta_s_str, end_date, 
                        product_code, work_order_types, target_defect_groups
                    )
                    
                    if not df_delta.empty:
                        df_delta['warehousing_time'] = pd.to_datetime(df_delta['warehousing_time'])
                        logging.info(f"   >> 合并: 缓存({len(df_cache)}) + 增量({len(df_delta)})")
                        
                        # 合并 + 去重 (保留最新)
                        df_combined = pd.concat([df_cache, df_delta], ignore_index=True)
                        df_combined.drop_duplicates(subset=['panel_id'], keep='last', inplace=True)
                        
                        df_final = df_combined
                        need_save = True
                    else:
                        logging.info("   >> 增量查询为空，沿用旧缓存。")
                        df_final = df_cache
                        # 即使增量为空，也建议更新一下文件时间戳，避免下次马上又过期？
                        # 或者不更新，让它下次继续试。这里选择更新文件时间戳以重置 TTL
                        need_save = True 
                else:
                    df_final = df_cache
            else:
                # === 全量刷新模式 ===
                logging.info("🆕 [Repo] 执行全量刷新 (Full Refresh)...")
                df_final = self._fetch_from_db_in_chunks(
                    start_date, end_date, 
                    product_code, work_order_types, target_defect_groups
                )
                if not df_final.empty:
                    df_final['warehousing_time'] = pd.to_datetime(df_final['warehousing_time'])
                    need_save = True

        # --- Phase 3: 滚动裁剪 & 持久化 ---
        if not df_final.empty:
            # 裁剪：移除早于 start_date 的数据 (Rolling Window)
            # 注意：仅在需要保存(发生过更新)或者缓存范围过大时才裁剪
            # 为了保持逻辑简单，总是对返回结果进行过滤
            
            # 持久化逻辑：仅当发生了数据库查询(need_save)时才写入磁盘
            if need_save and self.use_snapshot:
                # 在保存前，先执行一次物理裁剪，防止文件无限膨胀
                # 保留范围：本次请求的 start_date ~ end_date (或者稍微宽一点)
                # 这里策略是：磁盘上保留“请求窗口”内的数据
                df_to_save = df_final[df_final['warehousing_time'] >= req_start_dt]
                
                try:
                    self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                    df_to_save.to_parquet(self.snapshot_path, index=False)
                    logging.info(f"💾 [Repo] 快照已更新 (Rolling Window)。")
                except Exception as e:
                    logging.error(f"❌ 快照保存失败: {e}")
                
                # 更新内存中的 df_final 指向裁剪后的数据
                df_final = df_to_save

            # 最后的防御性过滤：确保返回给业务层的数据严格符合请求范围
            # (这层过滤是内存级的，不影响磁盘)
            mask = (df_final['warehousing_time'] >= req_start_dt) & \
                   (df_final['warehousing_time'] <= req_end_dt)
            return df_final[mask]

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