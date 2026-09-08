"""Shared OOS history use cases for monitor and alert-matrix consumers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Iterable, Protocol

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_DECORATION_FILE_NAME,
)
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import (
    AOI_TT_OOS_DECORATION_FILE_NAME,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DECORATION_FILE_NAME,
    merge_detail_with_decoration_flags,
)
from src.inline_domain.core.shared.oos_history import (
    OosHistoryMetadata,
    OosHistorySnapshot,
    OosScopeContract,
)


COMMON_COLUMNS = [
    "scope",
    "factory",
    "prod_code",
    "step_id",
    "metric_name",
    "item_id",
    "event_time",
    "observed_value",
    "usl",
    "lsl",
    "oos_type",
    "flag",
    "upper_limit",
    "lower_limit",
    "limit_type",
]

SCOPE_DECORATION_FILE_NAME = {
    "spc": OOS_DECORATION_FILE_NAME,
    "ctq": "ctq_sheet_oos_decoration.xlsx",
    "aoi_tt": AOI_TT_OOS_DECORATION_FILE_NAME,
    "aoi_rs": AOI_RS_OOS_DECORATION_FILE_NAME,
}


@dataclass(frozen=True)
class OosProductRead:
    decorated_df: pd.DataFrame
    alerts_df: pd.DataFrame
    source: str
    refreshed_at: pd.Timestamp | None
    metadata: OosHistoryMetadata | None = None


class OosHistoryStorePort(Protocol):
    def contract_for(self, scope: str) -> OosScopeContract: ...
    def snapshot_path(self, scope: str, prod_code: str) -> Path: ...
    def read(self, scope: str, prod_code: str) -> OosHistorySnapshot | None: ...


class OosDecisionLedgerPort(Protocol):
    def load_decisions(
        self, file_name: str, prod_code: str, key_columns: Iterable[str]
    ) -> pd.DataFrame: ...
    def load_fallback(
        self, file_name: str, prod_code: str, key_columns: Iterable[str]
    ) -> pd.DataFrame: ...
    def load_refresh_time(
        self, file_name: str, scope: str, prod_code: str
    ) -> pd.Timestamp | None: ...
    def source_path(self, file_name: str) -> Path: ...


class OosHistoryService:
    """Join cache-computed facts with the mutable Excel decision ledger.

    The historical class name remains for existing report consumers. Production
    composition uses LiveHistoryStore and never persists OOS/OOC decisions.
    """

    def __init__(
        self,
        store: OosHistoryStorePort,
        decisions: OosDecisionLedgerPort,
    ) -> None:
        self._store = store
        self._decisions = decisions

    @staticmethod
    def inclusive_date_window(
        start_date: str, end_date: str
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        start = pd.to_datetime(start_date, errors="raise").normalize()
        end = pd.to_datetime(end_date, errors="raise").normalize() + pd.Timedelta(days=1)
        if start >= end:
            raise ValueError("start_date must not be after end_date")
        return start, end

    def read_product(self, scope: str, prod_code: str) -> OosProductRead:
        return self._read_product_with_file(
            scope, prod_code, SCOPE_DECORATION_FILE_NAME[scope]
        )

    def _read_product_with_file(
        self, scope: str, prod_code: str, file_name: str
    ) -> OosProductRead:
        contract = self._store.contract_for(scope)
        snapshot = self._store.read(scope, prod_code)
        if snapshot is not None:
            decisions = self._decisions.load_decisions(
                file_name, prod_code, contract.key_columns
            )
            decorated = merge_detail_with_decoration_flags(
                snapshot.frame, decisions, contract.key_columns
            )
            source = "computed_cache" if getattr(self._store, "is_computed_cache", False) else "history"
            refreshed_at = pd.to_datetime(snapshot.metadata.refreshed_at, errors="coerce")
            refreshed_at = None if pd.isna(refreshed_at) else refreshed_at
            metadata = snapshot.metadata
        else:
            decorated = self._decisions.load_fallback(
                file_name, prod_code, contract.key_columns
            )
            source = "workbook_fallback" if not decorated.empty else "missing"
            refreshed_at = self._decisions.load_refresh_time(file_name, scope, prod_code)
            metadata = None

        projected = self._project(scope, decorated)
        alert_mask = projected["flag"].apply(self._is_false_flag) if not projected.empty else []
        alerts = projected.loc[alert_mask].reset_index(drop=True) if not projected.empty else projected.copy()
        return OosProductRead(
            decorated_df=projected,
            alerts_df=alerts,
            source=source,
            refreshed_at=refreshed_at,
            metadata=metadata,
        )

    def has_history(self, scope: str, prod_code: str) -> bool:
        if getattr(self._store, "is_computed_cache", False):
            return True
        return self._store.snapshot_path(scope, prod_code).exists()

    def scope_contract(self, scope: str):
        """Expose the stable scope contract to maintenance commands."""
        return self._store.contract_for(scope)

    def source_signature(self, products: list[str], scopes: list[str]) -> str:
        """Cheap cache key covering history and mutable decision workbooks."""
        return self._source_signature_with_files(products, scopes, SCOPE_DECORATION_FILE_NAME)

    def _source_signature_with_files(
        self, products: list[str], scopes: list[str], file_names: dict[str, str]
    ) -> str:
        if getattr(self._store, "is_computed_cache", False):
            return self._store.source_signature(products, scopes)
        parts: list[str] = []
        for scope in sorted(set(scopes)):
            workbook = self._decisions.source_path(file_names[scope])
            parts.append(self._path_signature(workbook))
            for prod_code in sorted(set(products)):
                parts.append(self._path_signature(self._store.snapshot_path(scope, prod_code)))
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    @staticmethod
    def _path_signature(path: Path) -> str:
        try:
            stat = path.stat()
        except OSError:
            return f"{path.name}:missing"
        return f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}"

    @staticmethod
    def _is_false_flag(value: object) -> bool:
        if isinstance(value, bool):
            return not value
        return str(value).strip().lower() in {"false", "0", "no", "n", "否"}

    @staticmethod
    def _project(scope: str, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=[*COMMON_COLUMNS, "alarm_type"])
        projected = frame.copy()
        projected["scope"] = scope
        projected["alarm_type"] = "OOS"
        if scope in {"spc", "ctq"}:
            projected["metric_name"] = projected["param_name"]
            projected["item_id"] = projected["sheet_id"]
            projected["event_time"] = projected["sheet_start_time"]
            lower_only = projected["oos_type"].astype(str).eq("LSL")
            projected["observed_value"] = projected["sheet_max"]
            projected.loc[lower_only, "observed_value"] = projected.loc[lower_only, "sheet_min"]
        elif scope == "aoi_tt":
            projected["metric_name"] = projected["tt_name"]
            projected["item_id"] = projected["sheet_id"]
            projected["event_time"] = projected["start_time"]
            projected["observed_value"] = projected["tt_qty"]
            projected["lsl"] = pd.NA
            projected["oos_type"] = "USL"
        elif scope == "aoi_rs":
            projected["metric_name"] = projected["rs_code"]
            projected["item_id"] = projected["point_id"]
            projected["event_time"] = projected["sheet_start_time"]
            projected["observed_value"] = projected["value"]
            projected["usl"] = projected["spec"]
            projected["lsl"] = pd.NA
            projected["oos_type"] = "USL"
        projected["event_time"] = pd.to_datetime(projected["event_time"], errors="coerce")
        projected["upper_limit"] = projected["usl"]
        projected["lower_limit"] = projected["lsl"]
        projected["limit_type"] = projected["oos_type"]
        for column in COMMON_COLUMNS:
            if column not in projected.columns:
                projected[column] = pd.NA
        return projected[[*COMMON_COLUMNS, "alarm_type"]].copy()
