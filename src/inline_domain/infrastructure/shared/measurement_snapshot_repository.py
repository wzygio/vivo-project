"""Reusable product-level Parquet snapshot adapter for Inline measurements."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import json
import logging
from pathlib import Path
import threading
from typing import Optional, TYPE_CHECKING

import pandas as pd

from src.inline_domain.infrastructure.shared.measurement_data_loader import (
    load_raw_measurements,
)
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_forward import DataForwardPolicy
from src.inline_domain.infrastructure.shared.date_exclusion import apply_inline_date_exclusion
from src.inline_domain.infrastructure.shared.rolling_snapshot import (
    read_metadata, is_fresh, incremental_start, replace_tail, publish_snapshots,
    snapshot_process_lock,
)
from src.inline_domain.infrastructure.shared.snapshot_window import (
    IncompleteMonitorSnapshotError,
    inline_measurement_snapshot_window_start,
    metadata_covers_start,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

MeasurementLoader = Callable[["DatabaseManager", str, str, str], pd.DataFrame]
MeasurementCorrector = Callable[[pd.DataFrame], pd.DataFrame]
logger = logging.getLogger(__name__)


def measurement_snapshot_coverage(path: Path, policy: DataForwardPolicy) -> dict | None:
    """Bind inclusive display coverage to one validated raw-file generation."""
    metadata = read_metadata(path, InlineMeasurementSnapshotRepository.SNAPSHOT_POLICY_VERSION,
                             policy_file=True)
    if metadata is None:
        return None
    offset = pd.Timedelta(days=policy.effective_days)
    generation = hashlib.sha256(json.dumps({"metadata": metadata, "forward": policy.signature},
                                          sort_keys=True).encode()).hexdigest()
    return {"start_date": (pd.Timestamp(metadata["covered_from"]) + offset).date().isoformat(),
            "end_date": (pd.Timestamp(metadata["covered_through"]) + offset).date().isoformat(),
            "source_generation": generation}


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
    # v5: OLED 2120A 的产品归属改为 PPID -> MQC.PRODCODE；旧快照需完整重取。
    SNAPSHOT_POLICY_VERSION = "inline-measurement-raw-v5-2120a-ppid"
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
        return self._report_projection(result.measurements)

    def refresh_measurements(
        self,
        prod_code: str,
        end_date: str,
    ) -> MeasurementRefreshResult:
        """Reload the full rolling window and report actual DB refresh success.

        An empty data window counts as success when the loader returned
        normally; only loader failures (served via the old snapshot fallback)
        report ``refreshed_from_db=False``.
        """
        result = self._load_measurements(prod_code, end_date, force_refresh=True)
        self.last_refresh_from_db = result.refreshed_from_db
        return MeasurementRefreshResult(
            self._report_projection(result.measurements),
            result.refreshed_from_db,
        )

    def _report_projection(self, measurements: pd.DataFrame) -> pd.DataFrame:
        displayed = self.data_forward_policy.shift_frame(measurements, ("start_time",))
        displayed = ConfigLoader.get_report_cutoff_policy().filter_frame(displayed, "start_time")
        return apply_inline_date_exclusion(displayed)

    def _load_measurements(
        self,
        prod_code: str,
        end_date: str,
        force_refresh: bool,
    ) -> MeasurementRefreshResult:
        end_timestamp = pd.Timestamp(end_date)
        window_start = inline_measurement_snapshot_window_start(prod_code, end_timestamp)
        start_date = window_start.strftime("%Y-%m-%d")
        snapshot_path = self.snapshot_dir / f"inline_measurements_{prod_code}.parquet"

        with self._lock_for(snapshot_path), snapshot_process_lock(snapshot_path):
            cached = None if force_refresh else self._try_read_fresh(
                snapshot_path, end_timestamp, window_start=window_start)
            if cached is not None:
                return self._snapshot_result(snapshot_path, cached, False)
            metadata = read_metadata(snapshot_path, self.SNAPSHOT_POLICY_VERSION, policy_file=True)
            previous = None
            if metadata is not None:
                try:
                    previous = self._read_snapshot(snapshot_path)
                except Exception:
                    logger.exception("Unreadable Inline snapshot; rebuilding %s", snapshot_path)
                    metadata = None
            load_start = (
                window_start
                if force_refresh else incremental_start(metadata, end_timestamp, window_start=window_start)
            )
            try:
                measurements = self.measurement_loader(
                    self.db_manager,
                    load_start.strftime("%Y-%m-%d"),
                    end_date,
                    prod_code,
                )
            except Exception:
                logger.exception("Failed to refresh Inline measurement snapshot for %s", prod_code)
                return self._snapshot_result(
                    snapshot_path, self._fallback(snapshot_path, required_start=start_date), False
                )
            if self.measurement_corrector is not None and not measurements.empty:
                measurements = self.measurement_corrector(measurements)
            measurements = replace_tail(
                None if force_refresh else previous, measurements, load_start, end_timestamp,
                window_start=window_start,
            )
            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self._write_snapshot(snapshot_path, measurements, end_timestamp, window_start=window_start)
            return self._snapshot_result(snapshot_path, measurements.copy(), True)

    def _snapshot_result(self, path: Path, measurements: pd.DataFrame,
                         refreshed: bool) -> MeasurementRefreshResult:
        # Every caller is inside the same product/process lock as the data read
        # or publication. Never look up newer coverage after releasing that lock.
        measurements.attrs["spc_source_coverage"] = measurement_snapshot_coverage(
            path, self.data_forward_policy)
        return MeasurementRefreshResult(measurements, refreshed)

    def _is_fresh(self, snapshot_path: Path, end_timestamp: pd.Timestamp,
                  *, window_start: object | None = None) -> bool:
        return is_fresh(read_metadata(snapshot_path, self.SNAPSHOT_POLICY_VERSION, policy_file=True),
                        end_timestamp, self.SNAPSHOT_TTL_HOURS, window_start=window_start)

    def _try_read_fresh(
        self, snapshot_path: Path, end_timestamp: pd.Timestamp,
        *, window_start: object | None = None,
    ) -> pd.DataFrame | None:
        if not self._is_fresh(snapshot_path, end_timestamp, window_start=window_start):
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
        self, snapshot_path: Path, measurements: pd.DataFrame, end_timestamp: pd.Timestamp,
        *, window_start: object | None = None,
    ) -> None:
        publish_snapshots([(snapshot_path, measurements, self.SNAPSHOT_POLICY_VERSION, end_timestamp, True)],
                          window_start=window_start)

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
