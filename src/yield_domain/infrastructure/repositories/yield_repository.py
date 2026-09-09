# src/vivo_project/infrastructure/repositories/panel_repository.py
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, List, Optional, Sequence
from datetime import datetime, timedelta

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_health import attach_data_health, get_data_health, make_data_health
from src.shared_kernel.snapshot_paths import snapshot_directory, snapshot_component
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.yield_domain.application.dtos import YieldDataPolicy, YieldQueryConfig
from src.yield_domain.application.errors import YieldSourceReadError
from src.yield_domain.infrastructure.data_loader import load_panel_details, load_array_input_times


def build_yield_snapshot_path(
    data_dir: Path,
    product_code: str,
    data_policy: YieldDataPolicy,
) -> Path:
    """构造包含静态数据策略签名的 Yield 快照路径。"""
    return (
        snapshot_directory(data_dir, "yield_domain", "yield")
        / f"yield_snapshot_{snapshot_component(product_code)}_{data_policy.signature}.parquet"
    )


class PanelRepository:
    """
    [仓储层] PanelRepository
    职责：Service 层与数据库的接口。
    [能力]: 
    1. TTL 缓存保护（有效期内不连库，TTL 见 global.yaml 的 application.cache_ttl_hours）
    2. 增量更新 (只查最近 3 天)
    3. 滚动窗口 (自动裁剪过期数据)
    """

    INCREMENTAL_BUFFER_DAYS = 2 # 增量缓冲

    def __init__(
        self, 
        snapshot_path: Path,
        data_policy: YieldDataPolicy,
        use_snapshot: bool = True,
        db_manager: Optional[DatabaseManager] = None
    ):
        if db_manager:
            self.db = db_manager
        else:
            self.db = DatabaseManager()
            
        self.snapshot_path = snapshot_path
        self.data_policy = data_policy
        self.use_snapshot = use_snapshot
        # 缓存有效期，统一来自 global.yaml 的 application.cache_ttl_hours
        self.SNAPSHOT_TTL_HOURS = ConfigLoader.get_snapshot_ttl_hours()
        self.data_forward_policy = ConfigLoader.get_data_forward_policy()

    def get_panel_details(self, query: YieldQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        """Publish only complete source windows; preserve old snapshots on failure."""
        req_start = pd.Timestamp(query.start_date)
        req_end = pd.Timestamp(query.end_date)
        source_start, source_end = self.data_forward_policy.to_source_window(req_start, req_end)
        time_col = "warehousing_time"
        cache = None
        cache_health = make_data_health("unknown")
        fresh = False
        if self.use_snapshot and self.snapshot_path.exists():
            try:
                cache = pd.read_parquet(self.snapshot_path)
                cache_health = get_data_health(cache)
                if not cache.empty:
                    cache[time_col] = pd.to_datetime(cache[time_col])
                modified = datetime.fromtimestamp(self.snapshot_path.stat().st_mtime)
                age = (datetime.now() - modified).total_seconds() / 3600
                # New snapshots carry coverage, including successful zero-row windows.
                covered = bool(
                    cache_health["source_start"] and cache_health["source_end"]
                    and pd.Timestamp(cache_health["source_start"]) <= source_start
                    and pd.Timestamp(cache_health["source_end"]) >= source_end
                )
                # Legacy snapshots keep their established TTL behavior, but unknown health.
                legacy_covered = (
                    cache_health["status"] == "unknown" and not cache.empty
                    and cache[time_col].max().date() >= min(source_end, pd.Timestamp.now()).date()
                )
                fresh = age < self.SNAPSHOT_TTL_HOURS and (covered or legacy_covered)
            except Exception:
                logging.warning("YIELD_SNAPSHOT_READ_FAILED")
                cache = None

        if cache is not None and fresh and not force_refresh:
            result = cache
            health = cache_health
        else:
            try:
                fetch_start = source_start
                incremental = (
                    cache is not None and not cache.empty and not force_refresh
                    and bool(cache_health["source_start"])
                    and pd.Timestamp(cache_health["source_start"]) <= source_start
                )
                if incremental:
                    fetch_start = max(source_start, cache[time_col].max() - timedelta(days=self.INCREMENTAL_BUFFER_DAYS))
                delta = self._fetch_from_db_in_chunks(
                    fetch_start.strftime("%Y-%m-%d"), source_end.strftime("%Y-%m-%d"),
                    query.product_code, self.data_policy.work_order_types,
                )
                if not delta.empty:
                    delta[time_col] = pd.to_datetime(delta[time_col])
                # Successful empty chunks are facts, not connection failures.
                result = pd.concat([cache, delta], ignore_index=True) if incremental else delta
                if not result.empty:
                    result = result.sort_values(time_col)
                    keys = ["panel_id", "defect_desc"] if "defect_desc" in result else ["panel_id"]
                    result = result.drop_duplicates(subset=keys, keep="last")
                    result = result[result[time_col] >= source_start].copy()
                health = make_data_health(
                    "fresh", source_start=source_start.strftime("%Y-%m-%d"),
                    source_end=source_end.strftime("%Y-%m-%d"),
                    refreshed_at=datetime.now().isoformat(timespec="seconds"),
                )
                attach_data_health(result, health)
                if self.use_snapshot:
                    try:
                        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                        result.to_parquet(self.snapshot_path, index=False)
                    except Exception:
                        logging.error("YIELD_SNAPSHOT_WRITE_FAILED")
            except Exception:
                logging.warning("YIELD_SOURCE_UNAVAILABLE: retaining previous snapshot when available")
                result = cache if cache is not None else pd.DataFrame()
                health = make_data_health(
                    "stale" if cache is not None else "unavailable",
                    source_start=cache_health["source_start"], source_end=cache_health["source_end"],
                    refreshed_at=cache_health["refreshed_at"], error_code="YIELD_SOURCE_UNAVAILABLE",
                )
        if not result.empty:
            result = self.data_forward_policy.shift_frame(result, (time_col,))
            result = result[(result[time_col] >= req_start) & (result[time_col] <= req_end)].copy()
            result = self._apply_data_policy(result).reset_index(drop=True)
        return attach_data_health(result, health)

    def _fetch_from_db_in_chunks(
        self,
        start_str: str,
        end_str: str,
        prod: str,
        wo_types: Sequence[str],
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
                    self.db, s_s, e_s, prod, wo_types
                )
                if not df.empty: all_chunks.append(df)
                
                current_start = current_end + timedelta(days=1)
                
            if all_chunks:
                return pd.concat(all_chunks, ignore_index=True)
            return pd.DataFrame()
            
        except Exception:
            logging.error("YIELD_SOURCE_UNAVAILABLE: complete window query aborted")
            raise YieldSourceReadError() from None

    def _apply_data_policy(self, panel_df: pd.DataFrame) -> pd.DataFrame:
        """在向上层返回前统一应用注入的 Defect Group 策略。"""
        target_groups = self.data_policy.target_defect_groups
        if panel_df.empty or not target_groups or 'defect_group' not in panel_df.columns:
            return panel_df

        mask_non_target = (
            ~panel_df['defect_group'].isin(target_groups)
            & panel_df['defect_group'].notna()
        )
        cleaned_count = int(mask_non_target.sum())
        if cleaned_count:
            cols_to_clean = ['defect_code', 'defect_desc', 'defect_group']
            existing_cols = [col for col in cols_to_clean if col in panel_df.columns]
            panel_df.loc[mask_non_target, existing_cols] = np.nan
            logging.info(
                "🛡️ [YieldRepo] 统一数据策略已抹除 %s 条非目标 Group 记录 "
                "(Target: %s)。",
                cleaned_count,
                target_groups,
            )
        return panel_df

    def get_array_input_times(
        self,
        lot_ids: List[str],
        custom_times: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        # 保持原有逻辑不变
        if not lot_ids: return pd.DataFrame()
        times = load_array_input_times(
            db_manager=self.db,
            lot_ids=lot_ids,
            enable_custom_times=True if custom_times else False,
            custom_times=custom_times
        )
        return self.data_forward_policy.shift_frame(times, ("array_input_time",))
