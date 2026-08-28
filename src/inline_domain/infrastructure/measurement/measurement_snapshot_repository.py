"""Reusable product-level Parquet snapshot adapter for Inline measurements."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
import threading
from typing import Optional, TYPE_CHECKING
from uuid import uuid4

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.inline_domain.infrastructure.measurement.measurement_data_loader import (
    RAW_MEASUREMENT_COLUMNS,
    load_raw_measurements,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MeasurementLoader = Callable[["DatabaseManager", str, str, str], pd.DataFrame]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MeasurementRefreshResult:
    """Outcome of loading measurements, distinguishing data content from DB success.

    ``refreshed_from_db`` is True only when the loader returned normally during
    this call (an empty window is still a successful refresh); it is False when
    the load failed and the result fell back to the previous snapshot, or when
    a cached snapshot was served without hitting the database.
    """

    measurements: pd.DataFrame
    refreshed_from_db: bool


class InlineMeasurementSnapshotRepository:
    """Load a product's raw measurements once and reuse its fresh snapshot."""

    SNAPSHOT_TTL_HOURS = 8
    SNAPSHOT_POLICY_VERSION = "inline-measurement-raw-v1"
    _locks_guard = threading.Lock()
    _snapshot_locks: dict[str, threading.Lock] = {}

    def __init__(
        self,
        snapshot_dir: Path,
        db_manager: "DatabaseManager",
        measurement_loader: MeasurementLoader = load_raw_measurements,
    ) -> None:
        self.snapshot_dir = snapshot_dir
        self.db_manager = db_manager
        self.measurement_loader = measurement_loader
        # 最近一次强制刷新是否真正从数据库成功（None 表示尚未强制刷新）。
        self.last_refresh_from_db: Optional[bool] = None

    def get_measurements(
        self,
        prod_code: str,
        end_date: str,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        result = self._load_measurements(prod_code, end_date, force_refresh)
        if force_refresh:
            self.last_refresh_from_db = result.refreshed_from_db
        return result.measurements

    def refresh_measurements(
        self,
        prod_code: str,
        end_date: str,
    ) -> MeasurementRefreshResult:
        """Force a DB refresh and report whether the data truly came from the DB.

        An empty data window counts as success when the loader returned
        normally; only loader failures (served via the old snapshot fallback)
        report ``refreshed_from_db=False``.
        """
        result = self._load_measurements(prod_code, end_date, force_refresh=True)
        self.last_refresh_from_db = result.refreshed_from_db
        return result

    def _load_measurements(
        self,
        prod_code: str,
        end_date: str,
        force_refresh: bool,
    ) -> MeasurementRefreshResult:
        end_timestamp = pd.Timestamp(end_date)
        start_date = (end_timestamp - relativedelta(months=3)).strftime("%Y-%m-%d")
        snapshot_path = self.snapshot_dir / f"inline_measurements_{prod_code}.parquet"

        if not force_refresh and self._is_fresh(snapshot_path, end_timestamp):
            return MeasurementRefreshResult(self._read_snapshot(snapshot_path), False)

        with self._lock_for(snapshot_path):
            if not force_refresh and self._is_fresh(snapshot_path, end_timestamp):
                return MeasurementRefreshResult(self._read_snapshot(snapshot_path), False)
            try:
                measurements = self.measurement_loader(
                    self.db_manager,
                    start_date,
                    end_date,
                    prod_code,
                )
            except Exception:
                logger.exception("Failed to refresh Inline measurement snapshot for %s", prod_code)
                return MeasurementRefreshResult(self._fallback(snapshot_path), False)
            if measurements.empty:
                return MeasurementRefreshResult(self._fallback(snapshot_path), True)

            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self._write_snapshot(snapshot_path, measurements)
            return MeasurementRefreshResult(measurements.copy(), True)

    def _is_fresh(self, snapshot_path: Path, end_timestamp: pd.Timestamp) -> bool:
        if not snapshot_path.exists():
            return False
        try:
            current_policy = self._policy_path(snapshot_path).read_text(encoding="utf-8").strip()
        except OSError:
            return False
        if current_policy != self.SNAPSHOT_POLICY_VERSION:
            return False
        age_hours = (
            datetime.now() - datetime.fromtimestamp(snapshot_path.stat().st_mtime)
        ).total_seconds() / 3600
        if age_hours >= self.SNAPSHOT_TTL_HOURS:
            return False
        snapshot = self._read_snapshot(snapshot_path)
        return not snapshot.empty and snapshot["start_time"].max() >= end_timestamp

    @staticmethod
    def _read_snapshot(snapshot_path: Path) -> pd.DataFrame:
        snapshot = pd.read_parquet(snapshot_path)
        snapshot["start_time"] = pd.to_datetime(snapshot["start_time"], errors="coerce")
        return snapshot

    def _fallback(self, snapshot_path: Path) -> pd.DataFrame:
        if not snapshot_path.exists():
            return pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)
        try:
            return self._read_snapshot(snapshot_path)
        except Exception:
            logger.exception("Failed to read fallback Inline snapshot %s", snapshot_path)
            return pd.DataFrame(columns=RAW_MEASUREMENT_COLUMNS)

    def _write_snapshot(self, snapshot_path: Path, measurements: pd.DataFrame) -> None:
        snapshot_temp = snapshot_path.with_name(f"{snapshot_path.name}.{uuid4().hex}.tmp")
        policy_path = self._policy_path(snapshot_path)
        policy_temp = policy_path.with_name(f"{policy_path.name}.{uuid4().hex}.tmp")
        try:
            measurements.to_parquet(snapshot_temp, index=False)
            policy_temp.write_text(self.SNAPSHOT_POLICY_VERSION, encoding="utf-8")
            os.replace(snapshot_temp, snapshot_path)
            os.replace(policy_temp, policy_path)
        finally:
            snapshot_temp.unlink(missing_ok=True)
            policy_temp.unlink(missing_ok=True)

    @classmethod
    def _lock_for(cls, snapshot_path: Path) -> threading.Lock:
        key = str(snapshot_path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())

    @staticmethod
    def _policy_path(snapshot_path: Path) -> Path:
        return snapshot_path.with_suffix(".policy")
