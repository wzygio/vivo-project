"""PostgreSQL adapter backed by one rolling Q-Time snapshot per shop."""

from __future__ import annotations

import logging
import hashlib
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
from src.indicator_domain.infrastructure.shared.date_exclusion import apply_indicator_date_exclusion
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_health import attach_data_health, make_data_health
from src.shared_kernel.snapshot_window import manual_refresh_start

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    from src.indicator_domain.infrastructure.qtime.array_transfer_source import ArrayTransferQTimeSource


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
OUTPUT_POLICY_VERSION = "qtime-earliest-station-lot-v1"
logger = logging.getLogger(__name__)


class QTimeRepository:
    """Read Q-Time facts from a shared rolling snapshot or PostgreSQL."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        *,
        snapshot_dir: Path | None = None,
        snapshot_ttl_hours: int | None = None,
        supplemental_source: ArrayTransferQTimeSource | None = None,
    ) -> None:
        self._engine = db_manager.engine
        self._supplemental_source = supplemental_source
        self._data_forward_policy = ConfigLoader.get_data_forward_policy()
        ttl_hours = (
            ConfigLoader.get_snapshot_ttl_hours()
            if snapshot_ttl_hours is None
            else snapshot_ttl_hours
        )
        self._snapshot_store = (
            QTimeSnapshotStore(
                Path(snapshot_dir), ttl_hours,
                policy_suffixes={"ARRAY": hashlib.sha256(repr(supplemental_source.source_signature()).encode()).hexdigest()}
                if supplemental_source is not None else None,
            )
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
                return self._with_supplemental_option(options, shop)

        statement = text(
            "SELECT DISTINCT step_desc, f_step, t_step FROM ("
            "SELECT CASE WHEN f_step LIKE '1%' THEN 'ARRAY' "
            "WHEN f_step LIKE '2%' THEN 'OLED' ELSE 'TP' END AS shop, "
            "step_desc, f_step, t_step FROM mdw.qtime_tzbjx "
            "WHERE timekey > :step_options_after"
            ") AS qtime_steps WHERE shop = :shop "
            "ORDER BY step_desc, f_step, t_step"
        )
        try:
            return self._with_supplemental_option(self._step_options_from_frame(
                self._read_frame(
                    statement, params={"shop": shop, "step_options_after": "20260101"}
                )
            ), shop)
        except QTimeDataAccessError:
            if snapshot is not None:
                return self._with_supplemental_option(self._step_options_from_frame(snapshot.frame), shop)
            raise

    def _with_supplemental_option(
        self, options: tuple[QTimeStepOption, ...], shop: Shop,
    ) -> tuple[QTimeStepOption, ...]:
        if self._supplemental_source is None or shop != "ARRAY":
            return options
        return tuple(dict.fromkeys((*options, self._supplemental_source.step_option)))

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        if self._snapshot_store is None:
            source = self._fetch_filtered_source(query)
            source_start, source_end = self._data_forward_policy.to_source_window(
                pd.Timestamp(query.start_time), pd.Timestamp(query.end_time),
            )
            health = make_data_health(
                "fresh", source_start=source_start.isoformat(),
                source_end=source_end.isoformat(),
                refreshed_at=datetime.now().astimezone().isoformat(),
            )
        else:
            source_start, source_end = self._rolling_source_window(query.end_time)
            loaded = self._load_shop_source(
                query.shop,
                source_start=source_start,
                source_end=source_end,
                force_refresh=False,
            )
            source = loaded.frame
            metadata = loaded.snapshot.metadata
            health = make_data_health(
                "stale" if loaded.degraded else "fresh",
                source_start=metadata.source_start,
                source_end=metadata.source_end,
                refreshed_at=metadata.refreshed_at,
                error_code="QTIME_SOURCE_READ_FAILED" if loaded.degraded else "",
            )
        return attach_data_health(self._to_display_details(source, query), health)

    def cache_signature(self, shop: Shop) -> tuple[object, ...]:
        signature = (
            QTimeSnapshotStore.POLICY_VERSION, OUTPUT_POLICY_VERSION,
            self._data_forward_policy.signature,
            ConfigLoader.get_report_cutoff_policy().signature,
            ConfigLoader.get_indicator_data_exclusion(),
            self._supplemental_source.source_signature()
            if shop == "ARRAY" and self._supplemental_source is not None else (),
        )
        if self._snapshot_store is None:
            return signature
        try:
            stat = self._snapshot_store.source_path(shop).stat()
        except FileNotFoundError:
            return (*signature, None)
        return (*signature, stat.st_mtime_ns, stat.st_size)

    def refresh_snapshot(
        self,
        shop: Shop,
        *,
        as_of: date | None = None,
        full_refresh: bool = False,
        refresh_days: int | None = None,
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
            full_refresh=full_refresh,
            refresh_days=refresh_days,
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
        sql = f"{self._detail_select_sql(current_specs_only=self._supplemental_source is not None)} AND step_desc IN :step_descriptions"
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
            self._read_source(statement, params=params)
        )

    def _load_shop_source(
        self,
        shop: Shop,
        *,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
        force_refresh: bool,
        full_refresh: bool = False,
        refresh_days: int | None = None,
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
            return self._refresh_shop_source(
                shop, source_start, source_end, full_refresh=full_refresh, refresh_days=refresh_days,
            )

    def _refresh_shop_source(
        self,
        shop: Shop,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
        *,
        full_refresh: bool = False,
        refresh_days: int | None = None,
    ) -> "_LoadedShopSource":
        if self._snapshot_store is None:
            raise RuntimeError("Q-Time snapshot store is not configured")
        stale = self._read_snapshot(shop, fresh_only=False)
        if full_refresh:
            refresh_start = source_start
        elif refresh_days is not None:
            covered_until = None
            if stale is not None and pd.Timestamp(stale.metadata.source_start) <= source_start:
                covered_until = pd.Timestamp(stale.metadata.source_end)
            refresh_start = manual_refresh_start(source_start, source_end, covered_until, refresh_days)
        else:
            refresh_start = self._refresh_start(stale, source_start, source_end)
        params: dict[str, object] = {
            "start_time": refresh_start.strftime("%Y%m%d%H%M%S"),
            "end_time": source_end.strftime("%Y%m%d%H%M%S"),
            "shop": shop,
        }
        statement = text(
            f"{self._detail_select_sql(current_specs_only=self._supplemental_source is not None)} ORDER BY step_desc, lot_id, timekey"
        )
        try:
            refreshed = self._normalize_source_details(
                self._read_source(statement, params=params, supplemental_start=source_start)
            )
        except QTimeDataAccessError:
            if stale is None:
                raise
            logger.warning(
                "Using stale Q-Time %s snapshot after database refresh failure",
                shop,
            )
            return _LoadedShopSource(stale.frame, stale, False, degraded=True)

        existing = stale.frame if stale is not None else pd.DataFrame(columns=DETAIL_COLUMNS)
        if self._supplemental_source is not None and shop == "ARRAY":
            # All starts in the rolling window are rebuilt: a late target may
            # complete a start older than the base source's incremental overlap.
            existing = self._without_supplemental_route(existing)
        merged = self._merge_refresh(
            existing,
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

    def _without_supplemental_route(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        option = self._supplemental_source.step_option
        return frame.loc[~(frame["f_step"].eq(option.f_step) & frame["t_step"].eq(option.t_step))].copy()

    def _read_source(
        self, statement: object, *, params: dict[str, object],
        supplemental_start: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        base = self._read_frame(statement, params=params)
        source = self._supplemental_source
        if source is None or params["shop"] != "ARRAY":
            return base
        base = self._without_supplemental_route(base)
        steps = params.get("step_descriptions", ())
        if steps and source.step_option.step_desc not in steps:
            return base
        extra = source.read(
            start_time=supplemental_start if supplemental_start is not None else pd.Timestamp(params["start_time"]),
            end_time=pd.Timestamp(params["end_time"]),
        )
        products = params.get("products", ())
        if products:
            extra = extra.loc[extra["prodcode"].isin(products)]
        frames = [frame for frame in (base, extra) if not frame.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=DETAIL_COLUMNS)

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
            covered_end - pd.Timedelta(days=ConfigLoader.get_incremental_refresh_days()),
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
    def _detail_select_sql(*, current_specs_only: bool = False) -> str:
        # Production observes current maintained specifications, not just records
        # updated inside the source-fact window. Keep injected legacy adapters compatible.
        spec_time = "AND (spec.update_time IS NULL OR spec.update_time <= CURRENT_TIMESTAMP)" if current_specs_only else ""
        return f"""
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
            FROM (
                SELECT facts.step_desc, facts.lot_id, facts.prod_qty,
                       facts.sub_prod_type, facts.f_step, facts.t_step,
                       COALESCE(CAST(spec.q_spec AS NUMERIC), facts.q_spec) AS q_spec,
                       facts.wait_time, facts.timekey, facts.prodcode
                FROM mdw.qtime_tzbjx AS facts
                LEFT JOIN eda.imp_qtime_tzbjx AS spec
                  ON spec.productspecname = facts.prodcode
                 AND spec.f_step_id = facts.f_step
                 AND spec.t_step_id = facts.t_step
                 {spec_time}
            ) AS qtime
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
        precise_time = normalized.get("_source_event_time")
        normalized = normalized.reindex(columns=DETAIL_COLUMNS)
        for column in ("prod_qty", "q_spec", "wait_time"):
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
        # The source uses YYYYMMDDHHMMSSffffff; snapshots/report keys use seconds.
        # Accept both contracts without treating the microsecond suffix as invalid.
        raw_timekeys = normalized["timekey"].astype("string")
        timekeys = raw_timekeys.str.replace(
            r"^(\d{14})\d{6}$", r"\1", regex=True
        )
        source_time = pd.to_datetime(
            timekeys,
            format="%Y%m%d%H%M%S",
            errors="coerce",
        )
        normalized["timekey"] = source_time.dt.strftime("%Y%m%d%H%M%S")
        # Retain source precision for the inclusive cutoff; public keys stay seconds.
        if precise_time is None:
            micros = pd.to_numeric(raw_timekeys.str[14:].where(raw_timekeys.str.len().eq(20), "0"), errors="coerce")
            precise_time = source_time + pd.to_timedelta(micros, unit="us")
        normalized["_source_event_time"] = pd.to_datetime(precise_time, errors="coerce")
        return normalized

    def _to_display_details(
        self,
        source: pd.DataFrame,
        query: QTimeQuery,
    ) -> pd.DataFrame:
        displayed = self._data_forward_policy.shift_frame(source, ("timekey", "_source_event_time"))
        displayed = ConfigLoader.get_report_cutoff_policy().filter_frame(
            displayed, "_source_event_time" if "_source_event_time" in displayed else "timekey",
        )
        displayed = apply_indicator_date_exclusion(
            displayed, time_column="_source_event_time" if "_source_event_time" in displayed else "timekey",
        )
        display_time = pd.to_datetime(displayed["timekey"], errors="coerce")
        mask = display_time.ge(query.start_time) & display_time.lt(query.end_time)
        mask &= displayed["step_desc"].isin(query.step_descriptions)
        if query.products:
            mask &= displayed["prodcode"].isin(query.products)
        displayed["timekey"] = display_time.dt.strftime("%Y%m%d%H%M%S")
        eligible = displayed.loc[mask].reset_index(drop=True)
        # Keep the earliest entry in the selected window for each station path/Lot.
        # Preserve source precision and survivor ordering; raw L1 facts stay intact.
        time_column = "_source_event_time" if "_source_event_time" in eligible else "timekey"
        earliest = (
            eligible.sort_values(time_column, kind="stable")
            .drop_duplicates(["f_step", "t_step", "lot_id"], keep="first")
            .sort_index()
        )
        return earliest.drop(columns=["_source_event_time"], errors="ignore").reset_index(drop=True)

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
    degraded: bool = False
