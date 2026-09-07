"""PostgreSQL adapter backed by one rolling Q-Time snapshot per shop."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import bindparam, text

from src.indicator_domain.application.qtime.dtos import (
    QTimeQuery,
    QTimeStepOption,
    Shop,
)
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.application.qtime.ports import QTimeSnapshotRefresh
from src.indicator_domain.infrastructure.qtime.snapshot_store import (
    QTimeSnapshot,
    QTimeSnapshotStore,
)
from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


DETAIL_COLUMNS = [
    "step_desc",
    "lot_id",
    "prod_qty",
    "sub_prod_type",
    "f_step",
    "t_step",
    "q_spec",
    "wait_time",
    "timekey",
    "shop",
    "prodcode",
]
SAFE_DATA_ERROR = "Q-Time 数据读取失败，请联系系统管理员确认数据库权限。"
INCREMENTAL_OVERLAP_DAYS = 2
logger = logging.getLogger(__name__)


class QTimeRepository:
    """Read Q-Time facts from a shared rolling snapshot or PostgreSQL."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        *,
        snapshot_dir: Path | None = None,
        snapshot_ttl_hours: int | None = None,
    ) -> None:
        self._engine = db_manager.engine
        self._data_forward_policy = ConfigLoader.get_data_forward_policy()
        ttl_hours = (
            ConfigLoader.get_snapshot_ttl_hours()
            if snapshot_ttl_hours is None
            else snapshot_ttl_hours
        )
        self._snapshot_store = (
            QTimeSnapshotStore(Path(snapshot_dir), ttl_hours)
            if snapshot_dir is not None
            else None
        )

    def list_products(self) -> tuple[str, ...]:
        snapshot_products = self._products_from_snapshots()
        if snapshot_products:
            return snapshot_products
        statement = text(
            "SELECT DISTINCT productspecname "
            "FROM eda.imp_qtime_tzbjx ORDER BY productspecname"
        )
        return self._products_from_frame(self._read_frame(statement))

    def list_step_options(self, shop: Shop) -> tuple[QTimeStepOption, ...]:
        snapshot = self._read_snapshot(shop, fresh_only=False)
        if snapshot is not None:
            options = self._step_options_from_frame(snapshot.frame)
            if options:
                return options

        statement = text(
            "SELECT DISTINCT step_desc, f_step, t_step FROM ("
            "SELECT CASE WHEN f_step LIKE '1%' THEN 'ARRAY' "
            "WHEN f_step LIKE '2%' THEN 'OLED' ELSE 'TP' END AS shop, "
            "step_desc, f_step, t_step FROM mdw.qtime_tzbjx"
            ") AS qtime_steps WHERE shop = :shop "
            "ORDER BY step_desc, f_step, t_step"
        )
        try:
            return self._step_options_from_frame(
                self._read_frame(statement, params={"shop": shop})
            )
        except QTimeDataAccessError:
            if snapshot is not None:
                return self._step_options_from_frame(snapshot.frame)
            raise

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        if self._snapshot_store is None:
            source = self._fetch_filtered_source(query)
        else:
            source_start, source_end = self._rolling_source_window(query.end_time)
            source = self._load_shop_source(
                query.shop,
                source_start=source_start,
                source_end=source_end,
                force_refresh=False,
            ).frame
        return self._to_display_details(source, query)

    def refresh_snapshot(
        self,
        shop: Shop,
        *,
        as_of: date | None = None,
    ) -> QTimeSnapshotRefresh:
        """Force one incremental shop refresh for the rolling report window."""
        if self._snapshot_store is None:
            raise RuntimeError("Q-Time snapshot store is not configured")
        report_date = as_of or date.today()
        display_end = datetime.combine(report_date + timedelta(days=1), time.min)
        source_start, source_end = self._rolling_source_window(display_end)
        loaded = self._load_shop_source(
            shop,
            source_start=source_start,
            source_end=source_end,
            force_refresh=True,
        )
        return QTimeSnapshotRefresh(
            shop=shop,
            row_count=len(loaded.frame),
            refreshed_from_database=loaded.refreshed_from_database,
            source_start=loaded.snapshot.metadata.source_start,
            source_end=loaded.snapshot.metadata.source_end,
            refreshed_at=loaded.snapshot.metadata.refreshed_at,
        )

    def _fetch_filtered_source(self, query: QTimeQuery) -> pd.DataFrame:
        source_start, source_end = self._data_forward_policy.to_source_window(
            pd.Timestamp(query.start_time),
            pd.Timestamp(query.end_time),
        )
        sql = f"{self._detail_select_sql()} AND step_desc IN :step_descriptions"
        params: dict[str, object] = {
            "start_time": source_start.strftime("%Y%m%d%H%M%S"),
            "end_time": source_end.strftime("%Y%m%d%H%M%S"),
            "shop": query.shop,
            "step_descriptions": query.step_descriptions,
        }
        if query.products:
            sql = f"{sql} AND prodcode IN :products"
            params["products"] = query.products
        statement = text(f"{sql} ORDER BY step_desc, lot_id, timekey").bindparams(
            bindparam("step_descriptions", expanding=True)
        )
        if query.products:
            statement = statement.bindparams(bindparam("products", expanding=True))
        return self._normalize_source_details(
            self._read_frame(statement, params=params)
        )

    def _load_shop_source(
        self,
        shop: Shop,
        *,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
        force_refresh: bool,
    ) -> "_LoadedShopSource":
        if self._snapshot_store is None:
            raise RuntimeError("Q-Time snapshot store is not configured")
        fresh = self._read_snapshot(shop, fresh_only=True)
        if (
            not force_refresh
            and fresh is not None
            and self._snapshot_store.covers(fresh, source_start, source_end)
        ):
            return _LoadedShopSource(fresh.frame, fresh, False)

        source_path = self._snapshot_store.source_path(shop)
        with self._snapshot_store.lock_for(source_path):
            fresh = self._read_snapshot(shop, fresh_only=True)
            if (
                not force_refresh
                and fresh is not None
                and self._snapshot_store.covers(fresh, source_start, source_end)
            ):
                return _LoadedShopSource(fresh.frame, fresh, False)
            return self._refresh_shop_source(shop, source_start, source_end)

    def _refresh_shop_source(
        self,
        shop: Shop,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
    ) -> "_LoadedShopSource":
        if self._snapshot_store is None:
            raise RuntimeError("Q-Time snapshot store is not configured")
        stale = self._read_snapshot(shop, fresh_only=False)
        refresh_start = self._refresh_start(stale, source_start, source_end)
        params: dict[str, object] = {
            "start_time": refresh_start.strftime("%Y%m%d%H%M%S"),
            "end_time": source_end.strftime("%Y%m%d%H%M%S"),
            "shop": shop,
        }
        statement = text(
            f"{self._detail_select_sql()} ORDER BY step_desc, lot_id, timekey"
        )
        try:
            refreshed = self._normalize_source_details(
                self._read_frame(statement, params=params)
            )
        except QTimeDataAccessError:
            if stale is None:
                raise
            logger.warning(
                "Using stale Q-Time %s snapshot after database refresh failure",
                shop,
            )
            return _LoadedShopSource(stale.frame, stale, False)

        merged = self._merge_refresh(
            stale.frame if stale is not None else pd.DataFrame(columns=DETAIL_COLUMNS),
            refreshed,
            refresh_start=refresh_start,
            source_start=source_start,
            source_end=source_end,
        )
        metadata = self._snapshot_store.write(
            shop,
            merged,
            source_start=source_start,
            source_end=source_end,
            refreshed_at=datetime.now().astimezone(),
        )
        snapshot = QTimeSnapshot(frame=merged, metadata=metadata)
        return _LoadedShopSource(merged, snapshot, True)

    @staticmethod
    def _refresh_start(
        stale: QTimeSnapshot | None,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
    ) -> pd.Timestamp:
        if stale is None:
            return source_start
        covered_start = pd.Timestamp(stale.metadata.source_start)
        if covered_start > source_start:
            return source_start
        covered_end = min(pd.Timestamp(stale.metadata.source_end), source_end)
        return max(
            source_start,
            covered_end - pd.Timedelta(days=INCREMENTAL_OVERLAP_DAYS),
        )

    def _merge_refresh(
        self,
        existing: pd.DataFrame,
        refreshed: pd.DataFrame,
        *,
        refresh_start: pd.Timestamp,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
    ) -> pd.DataFrame:
        existing_times = pd.to_datetime(existing.get("timekey"), errors="coerce")
        retained = existing.loc[existing_times.lt(refresh_start)].copy()
        frames = [frame for frame in (retained, refreshed) if not frame.empty]
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=DETAIL_COLUMNS)
        )
        combined_times = pd.to_datetime(combined["timekey"], errors="coerce")
        in_window = combined_times.ge(source_start) & combined_times.lt(source_end)
        return (
            combined.loc[in_window]
            .sort_values(["step_desc", "lot_id", "timekey"], kind="stable")
            .reset_index(drop=True)
        )

    def _rolling_source_window(
        self,
        display_end: datetime,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        report_date = (pd.Timestamp(display_end) - pd.Timedelta(microseconds=1)).date()
        current_month_start = report_date.replace(day=1)
        previous_month_start = (
            pd.Timestamp(current_month_start) - pd.DateOffset(months=1)
        )
        return self._data_forward_policy.to_source_window(
            previous_month_start,
            pd.Timestamp(display_end),
        )

    @staticmethod
    def _detail_select_sql() -> str:
        return """
            SELECT
                step_desc,
                lot_id,
                prod_qty,
                sub_prod_type,
                f_step,
                t_step,
                q_spec,
                wait_time,
                timekey,
                CASE
                    WHEN f_step LIKE '1%' THEN 'ARRAY'
                    WHEN f_step LIKE '2%' THEN 'OLED'
                    ELSE 'TP'
                END AS shop,
                prodcode
            FROM mdw.qtime_tzbjx
            WHERE timekey >= :start_time
              AND timekey < :end_time
              AND CASE
                    WHEN f_step LIKE '1%' THEN 'ARRAY'
                    WHEN f_step LIKE '2%' THEN 'OLED'
                    ELSE 'TP'
                  END = :shop
        """

    def _read_frame(
        self,
        statement: object,
        *,
        params: dict[str, object] | None = None,
    ) -> pd.DataFrame:
        try:
            if self._engine is None:
                raise RuntimeError("database engine unavailable")
            if params is None:
                return pd.read_sql(statement, self._engine)
            return pd.read_sql(statement, self._engine, params=params)
        except Exception as exc:
            logger.error("Q-Time database read failed: %s", type(exc).__name__)
            raise QTimeDataAccessError(SAFE_DATA_ERROR) from exc

    @staticmethod
    def _normalize_source_details(frame: pd.DataFrame) -> pd.DataFrame:
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        normalized = normalized.reindex(columns=DETAIL_COLUMNS)
        for column in ("prod_qty", "q_spec", "wait_time"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        source_time = pd.to_datetime(
            normalized["timekey"],
            format="%Y%m%d%H%M%S",
            errors="coerce",
        )
        normalized["timekey"] = source_time.dt.strftime("%Y%m%d%H%M%S")
        return normalized

    def _to_display_details(
        self,
        source: pd.DataFrame,
        query: QTimeQuery,
    ) -> pd.DataFrame:
        displayed = self._data_forward_policy.shift_frame(source, ("timekey",))
        display_time = pd.to_datetime(displayed["timekey"], errors="coerce")
        mask = display_time.ge(query.start_time) & display_time.lt(query.end_time)
        mask &= displayed["step_desc"].isin(query.step_descriptions)
        if query.products:
            mask &= displayed["prodcode"].isin(query.products)
        displayed["timekey"] = display_time.dt.strftime("%Y%m%d%H%M%S")
        return displayed.loc[mask].reset_index(drop=True)

    def _read_snapshot(
        self,
        shop: Shop,
        *,
        fresh_only: bool,
    ) -> QTimeSnapshot | None:
        if self._snapshot_store is None:
            return None
        return self._snapshot_store.read(
            shop,
            fresh_only=fresh_only,
            normalizer=self._normalize_source_details,
        )

    def _products_from_snapshots(self) -> tuple[str, ...]:
        products: set[str] = set()
        for shop in ("ARRAY", "OLED", "TP"):
            snapshot = self._read_snapshot(shop, fresh_only=False)
            if snapshot is None or "prodcode" not in snapshot.frame:
                continue
            values = snapshot.frame["prodcode"].dropna().astype(str).str.strip()
            products.update(value for value in values if value)
        return tuple(sorted(products))

    @staticmethod
    def _products_from_frame(frame: pd.DataFrame) -> tuple[str, ...]:
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        if "productspecname" not in normalized:
            return ()
        values = normalized["productspecname"].dropna().astype(str).str.strip()
        return tuple(sorted(value for value in values.unique() if value))

    @staticmethod
    def _step_options_from_frame(
        frame: pd.DataFrame,
    ) -> tuple[QTimeStepOption, ...]:
        required = ["step_desc", "f_step", "t_step"]
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        if normalized.empty or not set(required).issubset(normalized.columns):
            return ()
        normalized = normalized.loc[:, required].dropna().copy()
        for column in required:
            normalized[column] = normalized[column].astype(str).str.strip()
        normalized = normalized.loc[(normalized != "").all(axis=1)].drop_duplicates()
        normalized = normalized.sort_values(required)
        return tuple(
            QTimeStepOption(row.step_desc, row.f_step, row.t_step)
            for row in normalized.itertuples(index=False)
        )


@dataclass(frozen=True, slots=True)
class _LoadedShopSource:
    frame: pd.DataFrame
    snapshot: QTimeSnapshot
    refreshed_from_database: bool
