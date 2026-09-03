"""PostgreSQL adapter for the prepared Q-Time report dataset."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
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
from src.indicator_domain.infrastructure.qtime.snapshot_store import QTimeSnapshotStore
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.snapshot_window import snapshot_window_start

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
logger = logging.getLogger(__name__)


class QTimeRepository:
    """Read Q-Time options and details from the report-owned database tables."""

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
        statement = text(
            "SELECT DISTINCT productspecname "
            "FROM eda.imp_qtime_tzbjx ORDER BY productspecname"
        )
        frame = self._load_option_source(
            statement,
            params=None,
            snapshot_path=self._option_snapshot_path("products"),
            normalizer=self._normalize_products,
            label="products",
        )
        if frame.empty:
            return ()
        return tuple(frame["productspecname"].tolist())

    def list_step_options(self, shop: Shop) -> tuple[QTimeStepOption, ...]:
        statement = text(
            "SELECT DISTINCT step_desc, f_step, t_step FROM ("
            "SELECT CASE WHEN f_step LIKE '1%' THEN 'ARRAY' "
            "WHEN f_step LIKE '2%' THEN 'OLED' ELSE 'TP' END AS shop, "
            "step_desc, f_step, t_step FROM mdw.qtime_tzbjx"
            ") AS qtime_steps WHERE shop = :shop "
            "ORDER BY step_desc, f_step, t_step"
        )
        normalized = self._load_option_source(
            statement,
            params={"shop": shop},
            snapshot_path=self._option_snapshot_path(f"step_options_{shop.lower()}"),
            normalizer=self._normalize_step_options,
            label=f"{shop} step options",
        )
        if normalized.empty:
            return ()
        return tuple(
            QTimeStepOption(
                step_desc=row.step_desc,
                f_step=row.f_step,
                t_step=row.t_step,
            )
            for row in normalized.itertuples(index=False)
        )

    @staticmethod
    def _normalize_products(frame: pd.DataFrame) -> pd.DataFrame:
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        if "productspecname" not in normalized:
            return pd.DataFrame(columns=["productspecname"])
        products = normalized["productspecname"].dropna().astype(str).str.strip()
        unique_products = sorted(product for product in products.unique() if product)
        return pd.DataFrame({"productspecname": unique_products})

    @staticmethod
    def _normalize_step_options(frame: pd.DataFrame) -> pd.DataFrame:
        required_columns = ["step_desc", "f_step", "t_step"]
        normalized = frame.copy()
        normalized.columns = normalized.columns.str.lower()
        if normalized.empty or not set(required_columns).issubset(normalized.columns):
            return pd.DataFrame(columns=required_columns)
        normalized = normalized.loc[:, required_columns].dropna().copy()
        for column in required_columns:
            normalized[column] = normalized[column].astype(str).str.strip()
        normalized = normalized.loc[(normalized != "").all(axis=1)].drop_duplicates()
        return normalized.sort_values(required_columns).reset_index(drop=True)

    def _load_option_source(
        self,
        statement: object,
        *,
        params: dict[str, object] | None,
        snapshot_path: Path | None,
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
        label: str,
    ) -> pd.DataFrame:
        cached = self._read_option_snapshot(
            snapshot_path,
            fresh_only=True,
            normalizer=normalizer,
            label=label,
        )
        if cached is not None:
            return cached
        if snapshot_path is None:
            return normalizer(self._read_frame(statement, params=params))

        with self._lock_for(snapshot_path):
            cached = self._read_option_snapshot(
                snapshot_path,
                fresh_only=True,
                normalizer=normalizer,
                label=label,
            )
            if cached is not None:
                return cached
            try:
                source = normalizer(self._read_frame(statement, params=params))
            except QTimeDataAccessError:
                fallback = self._read_option_snapshot(
                    snapshot_path,
                    fresh_only=False,
                    normalizer=normalizer,
                    label=label,
                )
                if fallback is not None:
                    logger.warning("Using stale Q-Time %s snapshot", label)
                    return fallback
                raise
            self._write_source_snapshot(snapshot_path, source)
            return source

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame:
        sql = """
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
              AND step_desc IN :step_descriptions
              AND CASE
                    WHEN f_step LIKE '1%' THEN 'ARRAY'
                    WHEN f_step LIKE '2%' THEN 'OLED'
                    ELSE 'TP'
                  END = :shop
        """
        if self._snapshot_store is None:
            source_start, source_end = self._data_forward_policy.to_source_window(
                pd.Timestamp(query.start_time),
                pd.Timestamp(query.end_time),
            )
        else:
            source_start = snapshot_window_start(query.end_time)
            source_end = pd.Timestamp(query.end_time)
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

        snapshot_path = self._detail_snapshot_path(params)
        source = self._load_detail_source(statement, params, snapshot_path)
        return self._to_display_details(source, query)

    def _load_detail_source(
        self,
        statement: object,
        params: dict[str, object],
        snapshot_path: Path | None,
    ) -> pd.DataFrame:
        cached = self._read_detail_snapshot(snapshot_path, fresh_only=True)
        if cached is not None:
            return cached
        if snapshot_path is None:
            return self._normalize_source_details(
                self._read_frame(statement, params=params)
            )

        with self._lock_for(snapshot_path):
            cached = self._read_detail_snapshot(snapshot_path, fresh_only=True)
            if cached is not None:
                return cached
            try:
                source = self._normalize_source_details(
                    self._read_frame(statement, params=params)
                )
            except QTimeDataAccessError:
                fallback = self._read_detail_snapshot(snapshot_path, fresh_only=False)
                if fallback is not None:
                    logger.warning(
                        "Using stale Q-Time detail snapshot after database failure"
                    )
                    return fallback
                raise
            self._write_source_snapshot(snapshot_path, source)
            return source

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

    def _normalize_source_details(self, frame: pd.DataFrame) -> pd.DataFrame:
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
        in_window = display_time.ge(query.start_time) & display_time.lt(query.end_time)
        displayed["timekey"] = display_time.dt.strftime("%Y%m%d%H%M%S")
        return displayed.loc[in_window].reset_index(drop=True)

    def _detail_snapshot_path(self, params: dict[str, object]) -> Path | None:
        if self._snapshot_store is None:
            return None
        return self._snapshot_store.detail_path(params)

    def _option_snapshot_path(self, name: str) -> Path | None:
        if self._snapshot_store is None:
            return None
        return self._snapshot_store.option_path(name)

    def _read_option_snapshot(
        self,
        snapshot_path: Path | None,
        *,
        fresh_only: bool,
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
        label: str,
    ) -> pd.DataFrame | None:
        if snapshot_path is None or self._snapshot_store is None:
            return None
        return self._snapshot_store.read(
            snapshot_path,
            fresh_only=fresh_only,
            normalizer=normalizer,
            label=label,
        )

    def _read_detail_snapshot(
        self,
        snapshot_path: Path | None,
        *,
        fresh_only: bool,
    ) -> pd.DataFrame | None:
        if snapshot_path is None or self._snapshot_store is None:
            return None
        return self._snapshot_store.read(
            snapshot_path,
            fresh_only=fresh_only,
            normalizer=self._normalize_source_details,
            label="detail",
        )

    def _write_source_snapshot(
        self,
        snapshot_path: Path,
        source: pd.DataFrame,
    ) -> None:
        if self._snapshot_store is None:
            raise RuntimeError("Q-Time snapshot store is not configured")
        self._snapshot_store.write(snapshot_path, source)

    def _lock_for(self, snapshot_path: Path) -> threading.Lock:
        if self._snapshot_store is None:
            raise RuntimeError("Q-Time snapshot store is not configured")
        return self._snapshot_store.lock_for(snapshot_path)
