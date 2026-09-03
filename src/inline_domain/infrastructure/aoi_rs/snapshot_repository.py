"""Product-level Parquet snapshots for AOI_RS source facts."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import threading
from typing import TYPE_CHECKING
from uuid import uuid4

import pandas as pd

from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.infrastructure.aoi_rs.data_loader import (
    PASS_THROUGH_COLUMNS,
    RS_DETAIL_COLUMNS,
    load_pass_through,
    load_rs_details,
    load_rs_spec_limits,
)

from src.shared_kernel.config import ConfigLoader

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager


DetailsLoader = Callable[["DatabaseManager", AoiRsQueryConfig], pd.DataFrame]
PassThroughLoader = Callable[["DatabaseManager", AoiRsQueryConfig], pd.DataFrame]
SpecLoader = Callable[["DatabaseManager", str], pd.DataFrame]
logger = logging.getLogger(__name__)


class AoiRsSnapshotRepository:
    """Persist and reuse one product's normalized AOI_RS source facts."""

    # TTL 统一由 config/global.yaml 的 data_snapshot.ttl_hours 提供
    SNAPSHOT_POLICY_VERSION = "aoi-rs-raw-v1"
    _locks_guard = threading.Lock()
    _snapshot_locks: dict[str, threading.Lock] = {}

    def __init__(
        self,
        snapshot_dir: Path,
        db_manager: "DatabaseManager",
        details_loader: DetailsLoader = load_rs_details,
        pass_through_loader: PassThroughLoader = load_pass_through,
        spec_loader: SpecLoader = load_rs_spec_limits,
    ) -> None:
        self.snapshot_dir = snapshot_dir
        self.db_manager = db_manager
        self.details_loader = details_loader
        self.pass_through_loader = pass_through_loader
        self.spec_loader = spec_loader
        self.SNAPSHOT_TTL_HOURS = ConfigLoader.get_snapshot_ttl_hours()
        self.data_forward_policy = ConfigLoader.get_data_forward_policy()

    def get_rs_details(
        self,
        query: AoiRsQueryConfig,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Return requested RS details from a fresh snapshot or the database."""
        snapshot_path = self.snapshot_dir / f"aoi_rs_details_{query.prod_code}.parquet"
        cached = None if force_refresh else self._try_read_fresh(
            snapshot_path, query, self._read_details
        )
        if cached is not None:
            return cached

        with self._lock_for(snapshot_path):
            cached = None if force_refresh else self._try_read_fresh(
                snapshot_path, query, self._read_details
            )
            if cached is not None:
                return cached

            end_timestamp = pd.Timestamp(query.end_date)
            loader_query = query.model_copy(
                update={
                    "start_date": self.data_forward_policy.snapshot_start(
                        end_timestamp
                    ).strftime("%Y-%m-%d"),
                }
            )
            try:
                details = self.details_loader(self.db_manager, loader_query)
            except Exception:
                logger.exception("Failed to refresh AOI_RS details snapshot for %s", query.prod_code)
                return self._fallback_details(snapshot_path, query)
            if details.empty:
                return self._fallback_details(snapshot_path, query)

            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self._write_details(snapshot_path, details, query.end_date)
            return self._filter_window(details.copy(), query)

    def get_pass_through(
        self,
        query: AoiRsQueryConfig,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Return requested pass-through facts from a snapshot or database."""
        snapshot_path = self.snapshot_dir / f"aoi_rs_pass_through_{query.prod_code}.parquet"
        cached = None if force_refresh else self._try_read_fresh(
            snapshot_path, query, self._read_pass_through
        )
        if cached is not None:
            return cached

        with self._lock_for(snapshot_path):
            cached = None if force_refresh else self._try_read_fresh(
                snapshot_path, query, self._read_pass_through
            )
            if cached is not None:
                return cached

            end_timestamp = pd.Timestamp(query.end_date)
            loader_query = query.model_copy(
                update={
                    "start_date": self.data_forward_policy.snapshot_start(
                        end_timestamp
                    ).strftime("%Y-%m-%d"),
                }
            )
            try:
                pass_through = self.pass_through_loader(self.db_manager, loader_query)
            except Exception:
                logger.exception(
                    "Failed to refresh AOI_RS pass-through snapshot for %s", query.prod_code
                )
                return self._fallback_pass_through(snapshot_path, query)
            if pass_through.empty:
                return self._fallback_pass_through(snapshot_path, query)

            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
            self._write_snapshot(
                snapshot_path,
                pass_through,
                PASS_THROUGH_COLUMNS,
                query.end_date,
            )
            return self._filter_window(pass_through.copy(), query)

    def get_rs_spec_limits(self, prod_code: str) -> pd.DataFrame:
        """Load the small AOI_RS specification dataset without snapshotting it."""
        return self.spec_loader(self.db_manager, prod_code)

    def refresh(self, query: AoiRsQueryConfig) -> bool:
        """Refresh both source snapshots without mistaking fallback data for success."""
        end_timestamp = pd.Timestamp(query.end_date)
        loader_query = query.model_copy(
            update={
                "start_date": self.data_forward_policy.snapshot_start(
                    end_timestamp
                ).strftime("%Y-%m-%d"),
            }
        )
        try:
            details = self.details_loader(self.db_manager, loader_query)
            pass_through = self.pass_through_loader(self.db_manager, loader_query)
        except Exception:
            logger.exception("Failed to refresh AOI_RS snapshots for %s", query.prod_code)
            return False
        if details.empty or pass_through.empty:
            logger.error(
                "AOI_RS snapshot refresh returned empty source data for %s", query.prod_code
            )
            return False

        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._write_details(
                self.snapshot_dir / f"aoi_rs_details_{query.prod_code}.parquet",
                details,
                query.end_date,
            )
            self._write_snapshot(
                self.snapshot_dir / f"aoi_rs_pass_through_{query.prod_code}.parquet",
                pass_through,
                PASS_THROUGH_COLUMNS,
                query.end_date,
            )
        except Exception:
            logger.exception("Failed to persist AOI_RS snapshots for %s", query.prod_code)
            return False
        return True

    def _is_fresh(self, snapshot_path: Path, requested_end_date: str) -> bool:
        if not snapshot_path.exists():
            return False
        try:
            metadata = json.loads(self._metadata_path(snapshot_path).read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return False
        if metadata.get("policy_version") != self.SNAPSHOT_POLICY_VERSION:
            return False
        covered_through = metadata.get("covered_through")
        if not isinstance(covered_through, str) or not covered_through.strip():
            return False
        try:
            covered_timestamp = pd.Timestamp(covered_through)
        except (TypeError, ValueError):
            return False
        if pd.isna(covered_timestamp) or covered_timestamp < pd.Timestamp(requested_end_date):
            return False
        age_hours = (
            datetime.now() - datetime.fromtimestamp(snapshot_path.stat().st_mtime)
        ).total_seconds() / 3600
        return age_hours < self.SNAPSHOT_TTL_HOURS

    def _try_read_fresh(
        self,
        snapshot_path: Path,
        query: AoiRsQueryConfig,
        reader: Callable[[Path], pd.DataFrame],
    ) -> pd.DataFrame | None:
        if not self._is_fresh(snapshot_path, query.end_date):
            return None
        try:
            return self._filter_window(reader(snapshot_path), query)
        except Exception:
            logger.exception("Failed to read fresh AOI_RS snapshot %s", snapshot_path)
            return None

    @staticmethod
    def _read_details(snapshot_path: Path) -> pd.DataFrame:
        details = AoiRsSnapshotRepository._read_contract_snapshot(
            snapshot_path, RS_DETAIL_COLUMNS
        )
        details["start_time"] = pd.to_datetime(details["start_time"], errors="coerce")
        details["code_qty"] = pd.to_numeric(details["code_qty"], errors="coerce").fillna(0)
        return details.dropna(subset=["start_time"]).reset_index(drop=True)

    @staticmethod
    def _read_pass_through(snapshot_path: Path) -> pd.DataFrame:
        pass_through = AoiRsSnapshotRepository._read_contract_snapshot(
            snapshot_path, PASS_THROUGH_COLUMNS
        )
        pass_through["start_time"] = pd.to_datetime(
            pass_through["start_time"], errors="coerce"
        )
        return pass_through.dropna(subset=["start_time"]).reset_index(drop=True)

    @staticmethod
    def _read_contract_snapshot(snapshot_path: Path, columns: list[str]) -> pd.DataFrame:
        data = pd.read_parquet(snapshot_path)
        missing = set(columns).difference(data.columns)
        if missing:
            raise ValueError(f"AOI_RS snapshot is missing columns: {sorted(missing)}")
        return data.reindex(columns=columns)

    def _fallback_details(
        self,
        snapshot_path: Path,
        query: AoiRsQueryConfig,
    ) -> pd.DataFrame:
        if not snapshot_path.exists():
            return pd.DataFrame(columns=RS_DETAIL_COLUMNS)
        try:
            return self._filter_window(self._read_details(snapshot_path), query)
        except Exception:
            logger.exception("Failed to read fallback AOI_RS details snapshot %s", snapshot_path)
            return pd.DataFrame(columns=RS_DETAIL_COLUMNS)

    def _fallback_pass_through(
        self,
        snapshot_path: Path,
        query: AoiRsQueryConfig,
    ) -> pd.DataFrame:
        if not snapshot_path.exists():
            return pd.DataFrame(columns=PASS_THROUGH_COLUMNS)
        try:
            return self._filter_window(self._read_pass_through(snapshot_path), query)
        except Exception:
            logger.exception(
                "Failed to read fallback AOI_RS pass-through snapshot %s", snapshot_path
            )
            return pd.DataFrame(columns=PASS_THROUGH_COLUMNS)

    def _write_details(
        self,
        snapshot_path: Path,
        details: pd.DataFrame,
        covered_through: str,
    ) -> None:
        self._write_snapshot(snapshot_path, details, RS_DETAIL_COLUMNS, covered_through)

    def _write_snapshot(
        self,
        snapshot_path: Path,
        data: pd.DataFrame,
        columns: list[str],
        covered_through: str,
    ) -> None:
        snapshot_temp = snapshot_path.with_name(f"{snapshot_path.name}.{uuid4().hex}.tmp")
        metadata_path = self._metadata_path(snapshot_path)
        metadata_temp = metadata_path.with_name(f"{metadata_path.name}.{uuid4().hex}.tmp")
        metadata = {
            "policy_version": self.SNAPSHOT_POLICY_VERSION,
            "covered_through": covered_through,
        }
        try:
            data.reindex(columns=columns).to_parquet(snapshot_temp, index=False)
            metadata_temp.write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
            os.replace(snapshot_temp, snapshot_path)
            os.replace(metadata_temp, metadata_path)
        finally:
            snapshot_temp.unlink(missing_ok=True)
            metadata_temp.unlink(missing_ok=True)

    def _filter_window(
        self,
        details: pd.DataFrame,
        query: AoiRsQueryConfig,
    ) -> pd.DataFrame:
        displayed = self.data_forward_policy.shift_frame(details, ("start_time",))
        start = pd.Timestamp(query.start_date)
        end = pd.Timestamp(query.end_date) + pd.Timedelta(days=1)
        start_time = pd.to_datetime(displayed["start_time"], errors="coerce")
        return displayed[start_time.ge(start) & start_time.lt(end)].reset_index(drop=True)

    @classmethod
    def _lock_for(cls, snapshot_path: Path) -> threading.Lock:
        key = str(snapshot_path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())

    @staticmethod
    def _metadata_path(snapshot_path: Path) -> Path:
        return snapshot_path.with_suffix(".snapshot.json")
