"""Product-level rolling incremental snapshots for AOI_RS source facts."""
from __future__ import annotations

from collections.abc import Callable
import logging
from pathlib import Path
import threading
from typing import TYPE_CHECKING

import pandas as pd

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.data_loader import (
    PASS_THROUGH_COLUMNS, RS_DETAIL_COLUMNS, load_pass_through, load_rs_details, load_rs_spec_limits,
)
from src.inline_domain.infrastructure.shared.rolling_snapshot import (
    read_metadata, is_fresh, incremental_start, replace_tail, publish_snapshots,
)
from src.inline_domain.infrastructure.shared.snapshot_window import (
    IncompleteMonitorSnapshotError, inline_snapshot_window_start, metadata_covers_start,
)
from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

DetailsLoader = Callable[["DatabaseManager", AoiRsQueryConfig], pd.DataFrame]
PassThroughLoader = Callable[["DatabaseManager", AoiRsQueryConfig], pd.DataFrame]
SpecLoader = Callable[["DatabaseManager", str], pd.DataFrame]
logger = logging.getLogger(__name__)


class AoiRsSnapshotRepository:
    """Keep one rolling source generation per product and fact type."""

    supports_shared_history_persistence = True
    SNAPSHOT_POLICY_VERSION = "aoi-rs-raw-v3-rolling"
    _locks_guard = threading.Lock()
    _snapshot_locks: dict[str, threading.Lock] = {}

    def __init__(self, snapshot_dir: Path, db_manager: "DatabaseManager",
                 details_loader: DetailsLoader = load_rs_details,
                 pass_through_loader: PassThroughLoader = load_pass_through,
                 spec_loader: SpecLoader = load_rs_spec_limits,
                 require_complete_coverage: bool = False) -> None:
        self.snapshot_dir = snapshot_dir
        self.db_manager = db_manager
        self.details_loader = details_loader
        self.pass_through_loader = pass_through_loader
        self.spec_loader = spec_loader
        self.require_complete_coverage = require_complete_coverage
        self.SNAPSHOT_TTL_HOURS = ConfigLoader.get_snapshot_ttl_hours()
        self.data_forward_policy = ConfigLoader.get_data_forward_policy()

    def get_rs_details(self, query: AoiRsQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        return self._get(query, force_refresh, "aoi_rs_details", RS_DETAIL_COLUMNS, self.details_loader)

    def get_pass_through(self, query: AoiRsQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        return self._get(query, force_refresh, "aoi_rs_pass_through", PASS_THROUGH_COLUMNS, self.pass_through_loader)

    def get_rs_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return self.spec_loader(self.db_manager, prod_code)

    def _get(
        self, query: AoiRsQueryConfig, force_refresh: bool, prefix: str,
        columns: list[str], loader: DetailsLoader,
    ) -> pd.DataFrame:
        path = self.snapshot_dir / f"{prefix}_{query.prod_code}.parquet"
        with self._lock_for(path):
            metadata, old = self._existing(path, columns)
            if not force_refresh and is_fresh(metadata, query.end_date, self.SNAPSHOT_TTL_HOURS):
                return self._filter_window(old, query)
            try:
                frame = self._load_tail(query, columns, loader, metadata, old)
            except Exception:
                logger.exception("Failed to refresh AOI_RS snapshot %s", path)
                if old is not None and metadata is not None:
                    if self._requires_coverage(query) and not metadata_covers_start(metadata, query.start_date):
                        raise IncompleteMonitorSnapshotError(f"AOI_RS 原始快照未覆盖 {query.start_date}")
                    return self._filter_window(old, query)
                if self._requires_coverage(query):
                    raise IncompleteMonitorSnapshotError(f"AOI_RS 原始快照未覆盖 {query.start_date} 或已损坏")
                return pd.DataFrame(columns=columns)
            self._write_snapshot(path, frame, columns, query.end_date)
            return self._filter_window(frame, query)

    def _existing(self, path: Path, columns: list[str]) -> tuple[dict | None, pd.DataFrame | None]:
        metadata = read_metadata(path, self.SNAPSHOT_POLICY_VERSION)
        if metadata is None:
            return None, None
        try:
            return metadata, self._read_contract_snapshot(path, columns)
        except Exception:
            logger.exception("Invalid AOI_RS snapshot; rebuilding %s", path)
            return None, None

    def _load_tail(
        self, query: AoiRsQueryConfig, columns: list[str], loader: DetailsLoader,
        metadata: dict | None, old: pd.DataFrame | None,
    ) -> pd.DataFrame:
        start = incremental_start(metadata, query.end_date)
        loader_query = query.model_copy(update={"start_date": start.strftime("%Y-%m-%d")})
        frame = loader(self.db_manager, loader_query)
        if not frame.empty and set(columns).difference(frame.columns):
            raise ValueError("AOI_RS loader returned an incomplete source schema")
        frame = frame.reindex(columns=columns)
        if "code_qty" in frame:
            frame["code_qty"] = pd.to_numeric(frame["code_qty"], errors="raise")
        return replace_tail(old, frame, start, query.end_date)

    def refresh(self, query: AoiRsQueryConfig) -> bool:
        """Load both tails before publishing either; empty successful loads are valid."""
        details_path = self.snapshot_dir / f"aoi_rs_details_{query.prod_code}.parquet"
        pass_path = self.snapshot_dir / f"aoi_rs_pass_through_{query.prod_code}.parquet"
        with self._lock_for(details_path), self._lock_for(pass_path):
            try:
                details_meta, details_old = self._existing(details_path, RS_DETAIL_COLUMNS)
                pass_meta, pass_old = self._existing(pass_path, PASS_THROUGH_COLUMNS)
                details = self._load_tail(query, RS_DETAIL_COLUMNS, self.details_loader, details_meta, details_old)
                passed = self._load_tail(query, PASS_THROUGH_COLUMNS, self.pass_through_loader, pass_meta, pass_old)
                publish_snapshots([
                    (details_path, details, self.SNAPSHOT_POLICY_VERSION, query.end_date, False),
                    (pass_path, passed, self.SNAPSHOT_POLICY_VERSION, query.end_date, False),
                ])
            except Exception:
                logger.exception("Failed to refresh AOI_RS snapshot pair for %s", query.prod_code)
                return False
        return True

    def _is_fresh(self, snapshot_path: Path, requested_end_date: str) -> bool:
        return is_fresh(read_metadata(snapshot_path, self.SNAPSHOT_POLICY_VERSION), requested_end_date, self.SNAPSHOT_TTL_HOURS)

    @staticmethod
    def _read_contract_snapshot(snapshot_path: Path, columns: list[str]) -> pd.DataFrame:
        data = pd.read_parquet(snapshot_path)
        missing = set(columns).difference(data.columns)
        if missing:
            raise ValueError(f"AOI_RS snapshot is missing columns: {sorted(missing)}")
        data = data.reindex(columns=columns)
        data["start_time"] = pd.to_datetime(data["start_time"], errors="raise")
        if "code_qty" in data:
            data["code_qty"] = pd.to_numeric(data["code_qty"], errors="raise")
        return data

    @staticmethod
    def _read_details(snapshot_path: Path) -> pd.DataFrame:
        return AoiRsSnapshotRepository._read_contract_snapshot(snapshot_path, RS_DETAIL_COLUMNS)

    @staticmethod
    def _read_pass_through(snapshot_path: Path) -> pd.DataFrame:
        return AoiRsSnapshotRepository._read_contract_snapshot(snapshot_path, PASS_THROUGH_COLUMNS)

    def _write_details(
        self, snapshot_path: Path, details: pd.DataFrame, covered_through: str,
    ) -> None:
        self._write_snapshot(snapshot_path, details, RS_DETAIL_COLUMNS, covered_through)

    def _write_snapshot(
        self, snapshot_path: Path, data: pd.DataFrame, columns: list[str],
        covered_through: str,
    ) -> None:
        publish_snapshots([(snapshot_path, data.reindex(columns=columns), self.SNAPSHOT_POLICY_VERSION, covered_through, False)])

    def _requires_coverage(self, query: AoiRsQueryConfig) -> bool:
        return self.require_complete_coverage or pd.Timestamp(query.start_date) < inline_snapshot_window_start(query.end_date)

    def _filter_window(self, details: pd.DataFrame, query: AoiRsQueryConfig) -> pd.DataFrame:
        displayed = self.data_forward_policy.shift_frame(details, ("start_time",))
        start = pd.Timestamp(query.start_date)
        end = pd.Timestamp(query.end_date) + pd.Timedelta(days=1)
        time = pd.to_datetime(displayed["start_time"], errors="coerce")
        return displayed[time.ge(start) & time.lt(end)].reset_index(drop=True)

    @classmethod
    def _lock_for(cls, snapshot_path: Path) -> threading.Lock:
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(str(snapshot_path.resolve()), threading.Lock())

    @staticmethod
    def _metadata_path(snapshot_path: Path) -> Path:
        return snapshot_path.with_suffix(".snapshot.json")
