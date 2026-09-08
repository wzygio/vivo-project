"""Reusable product-level Parquet snapshot adapter for Inline measurements."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
from pathlib import Path
import threading
from typing import Optional, TYPE_CHECKING

import pandas as pd

from src.inline_domain.infrastructure.shared.measurement_data_loader import (
    load_raw_measurements,
)
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.infrastructure.shared.rolling_snapshot import (
    read_metadata, is_fresh, incremental_start, replace_tail, publish_snapshots,
)
from src.inline_domain.infrastructure.shared.snapshot_window import (
    IncompleteMonitorSnapshotError,
    inline_snapshot_window_start,
    metadata_covers_start,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MeasurementLoader = Callable[["DatabaseManager", str, str, str], pd.DataFrame]
MeasurementCorrector = Callable[[pd.DataFrame], pd.DataFrame]
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

    # TTL 统一由 config/global.yaml 的 application.cache_ttl_hours 提供
    # v2: 快照生成前接入 SPC 数值修正（M673 PPA site[99,114] param_value-5）
    SNAPSHOT_POLICY_VERSION = "inline-measurement-raw-v4-rolling"
    _locks_guard = threading.Lock()
    _snapshot_locks: dict[str, threading.Lock] = {}

    def __init__(
        self,
        snapshot_dir: Path,
        db_manager: "DatabaseManager",
        measurement_loader: MeasurementLoader = load_raw_measurements,
        measurement_corrector: MeasurementCorrector | None = None,
    ) -> None:
        self.snapshot_dir = snapshot_dir
        self.db_manager = db_manager
        self.measurement_loader = measurement_loader
        self.measurement_corrector = measurement_corrector
        self.SNAPSHOT_TTL_HOURS = ConfigLoader.get_snapshot_ttl_hours()
        self.data_forward_policy = ConfigLoader.get_data_forward_policy()
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
        return self.data_forward_policy.shift_frame(result.measurements, ("start_time",))

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
        return MeasurementRefreshResult(
            self.data_forward_policy.shift_frame(result.measurements, ("start_time",)),
            result.refreshed_from_db,
        )

    def _load_measurements(
        self,
        prod_code: str,
        end_date: str,
        force_refresh: bool,
    ) -> MeasurementRefreshResult:
        end_timestamp = pd.Timestamp(end_date)
        start_date = inline_snapshot_window_start(end_timestamp).strftime("%Y-%m-%d")
        snapshot_path = self.snapshot_dir / f"inline_measurements_{prod_code}.parquet"

        cached = None if force_refresh else self._try_read_fresh(snapshot_path, end_timestamp)
        if cached is not None:
            return MeasurementRefreshResult(cached, False)

        with self._lock_for(snapshot_path):
            cached = None if force_refresh else self._try_read_fresh(snapshot_path, end_timestamp)
            if cached is not None:
                return MeasurementRefreshResult(cached, False)
            metadata = read_metadata(snapshot_path, self.SNAPSHOT_POLICY_VERSION, policy_file=True)
            previous = None
            if metadata is not None:
                try:
                    previous = self._read_snapshot(snapshot_path)
                except Exception:
                    logger.exception("Unreadable Inline snapshot; rebuilding %s", snapshot_path)
                    metadata = None
            load_start = incremental_start(metadata, end_timestamp)
            try:
                measurements = self.measurement_loader(
                    self.db_manager,
                    load_start.strftime("%Y-%m-%d"),
                    end_date,
                    prod_code,
                )
            except Exception:
                logger.exception("Failed to refresh Inline measurement snapshot for %s", prod_code)
                return MeasurementRefreshResult(
                    self._fallback(snapshot_path, required_start=start_date), False
                )
            if self.measurement_corrector is not None and not measurements.empty:
                measurements = self.measurement_corrector(measurements)
            measurements = replace_tail(previous, measurements, load_start, end_timestamp)
            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self._write_snapshot(snapshot_path, measurements, end_timestamp)
            return MeasurementRefreshResult(measurements.copy(), True)

    def _is_fresh(self, snapshot_path: Path, end_timestamp: pd.Timestamp) -> bool:
        return is_fresh(read_metadata(snapshot_path, self.SNAPSHOT_POLICY_VERSION, policy_file=True), end_timestamp, self.SNAPSHOT_TTL_HOURS)

    def _try_read_fresh(
        self, snapshot_path: Path, end_timestamp: pd.Timestamp,
    ) -> pd.DataFrame | None:
        if not self._is_fresh(snapshot_path, end_timestamp):
            return None
        try:
            return self._read_snapshot(snapshot_path)
        except Exception:
            logger.exception("Invalid fresh Inline snapshot %s; rebuilding", snapshot_path)
            return None

    @staticmethod
    def _read_snapshot(snapshot_path: Path) -> pd.DataFrame:
        snapshot = pd.read_parquet(snapshot_path)
        snapshot["start_time"] = pd.to_datetime(snapshot["start_time"], errors="coerce")
        return snapshot

    def _fallback(
        self, snapshot_path: Path, *, required_start: object
    ) -> pd.DataFrame:
        if not self._covers_start(snapshot_path, required_start):
            raise IncompleteMonitorSnapshotError(
                f"Inline 原始快照未覆盖 {required_start}，需成功刷新滚动窗口"
            )
        try:
            return self._read_snapshot(snapshot_path)
        except Exception as exc:
            logger.exception("Failed to read fallback Inline snapshot %s", snapshot_path)
            raise IncompleteMonitorSnapshotError("Inline 原始快照损坏，无法生成汇总") from exc

    def _write_snapshot(
        self, snapshot_path: Path, measurements: pd.DataFrame, end_timestamp: pd.Timestamp
    ) -> None:
        publish_snapshots([(snapshot_path, measurements, self.SNAPSHOT_POLICY_VERSION, end_timestamp, True)])

    @classmethod
    def _covers_start(cls, snapshot_path: Path, required_start: object) -> bool:
        metadata = read_metadata(snapshot_path, cls.SNAPSHOT_POLICY_VERSION, policy_file=True)
        return metadata is not None and metadata_covers_start(metadata, required_start)

    @classmethod
    def _lock_for(cls, snapshot_path: Path) -> threading.Lock:
        key = str(snapshot_path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())

    @staticmethod
    def _policy_path(snapshot_path: Path) -> Path:
        return snapshot_path.with_suffix(".policy")
