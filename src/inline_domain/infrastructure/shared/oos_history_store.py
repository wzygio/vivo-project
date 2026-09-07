"""Long-term, coverage-aware Parquet storage for Inline OOS facts."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_DETAIL_COLUMNS,
    AOI_RS_OOS_KEY_COLUMNS,
)
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import (
    AOI_TT_OOS_DETAIL_COLUMNS,
    AOI_TT_OOS_KEY_COLUMNS,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DETAIL_COLUMNS,
    OOS_KEY_COLUMNS,
)
from src.inline_domain.core.shared.oos_history import (
    OosHistoryMetadata,
    OosHistorySnapshot,
    OosScopeContract,
)


class OosHistoryError(RuntimeError):
    """Base error for the durable OOS history boundary."""


class OosHistoryReadError(OosHistoryError):
    """Raised when an existing history file cannot be verified."""


class OosHistoryContractError(OosHistoryError):
    """Raised when facts or coverage violate a scope contract."""


SCOPE_CONTRACTS: dict[str, OosScopeContract] = {
    "spc": OosScopeContract(tuple(OOS_KEY_COLUMNS), "sheet_start_time", tuple(OOS_DETAIL_COLUMNS)),
    "ctq": OosScopeContract(tuple(OOS_KEY_COLUMNS), "sheet_start_time", tuple(OOS_DETAIL_COLUMNS)),
    "aoi_tt": OosScopeContract(
        tuple(AOI_TT_OOS_KEY_COLUMNS), "start_time", tuple(AOI_TT_OOS_DETAIL_COLUMNS)
    ),
    "aoi_rs": OosScopeContract(
        tuple(AOI_RS_OOS_KEY_COLUMNS),
        "sheet_start_time",
        tuple(AOI_RS_OOS_DETAIL_COLUMNS),
    ),
}


class OosHistoryStore:
    """Persist one append-retaining, overlap-replacing history per scope/product."""

    POLICY_VERSION = "inline-oos-history-v1"
    METADATA_KEY = b"inline_oos_history"
    _locks_guard = threading.Lock()
    _snapshot_locks: ClassVar[dict[str, threading.Lock]] = {}

    def __init__(self, snapshot_dir: Path | str = Path("data/inline_domain/oos_history")) -> None:
        self._snapshot_dir = Path(snapshot_dir)

    @staticmethod
    def contract_for(scope: str) -> OosScopeContract:
        normalized = str(scope).strip().lower()
        try:
            return SCOPE_CONTRACTS[normalized]
        except KeyError as exc:
            raise OosHistoryContractError(f"Unsupported OOS scope: {scope!r}") from exc

    def snapshot_path(self, scope: str, prod_code: str) -> Path:
        normalized_scope = str(scope).strip().lower()
        self.contract_for(normalized_scope)
        product = str(prod_code).strip()
        if not product:
            raise OosHistoryContractError("prod_code must not be empty")
        digest = hashlib.sha256(product.encode("utf-8")).hexdigest()[:16]
        return self._snapshot_dir / f"{normalized_scope}__{digest}.parquet"

    def read(self, scope: str, prod_code: str) -> OosHistorySnapshot | None:
        path = self.snapshot_path(scope, prod_code)
        if not path.exists():
            return None
        try:
            metadata = self._read_metadata(path)
            if (
                metadata.policy != self.POLICY_VERSION
                or metadata.scope != str(scope).strip().lower()
                or metadata.prod_code != str(prod_code).strip()
            ):
                raise ValueError("history metadata identity mismatch")
            frame = pd.read_parquet(path)
            if len(frame) != metadata.row_count:
                raise ValueError("history row count mismatch")
            return OosHistorySnapshot(frame=frame, metadata=metadata)
        except Exception as exc:
            raise OosHistoryReadError(
                f"Unable to verify OOS history for {scope}/{prod_code}"
            ) from exc

    def update(
        self,
        scope: str,
        prod_code: str,
        facts: pd.DataFrame,
        *,
        coverage_start: pd.Timestamp,
        coverage_end: pd.Timestamp,
        refreshed_at: datetime | None = None,
    ) -> OosHistorySnapshot:
        normalized_scope = str(scope).strip().lower()
        contract = self.contract_for(normalized_scope)
        start = pd.Timestamp(coverage_start)
        end = pd.Timestamp(coverage_end)
        if pd.isna(start) or pd.isna(end) or start >= end:
            raise OosHistoryContractError("coverage must be a non-empty half-open interval")

        incoming = self._normalize_facts(facts, contract, str(prod_code).strip())
        event_times = pd.to_datetime(incoming[contract.event_time_column], errors="coerce")
        if event_times.isna().any():
            raise OosHistoryContractError(
                f"{normalized_scope} facts contain invalid {contract.event_time_column}"
            )
        incoming = incoming.loc[(event_times >= start) & (event_times < end)].copy()

        path = self.snapshot_path(normalized_scope, prod_code)
        with self._lock_for(path):
            current = self.read(normalized_scope, prod_code)
            if current is None:
                retained = pd.DataFrame(columns=contract.detail_columns)
            else:
                retained = current.frame.copy()
                existing_times = pd.to_datetime(
                    retained[contract.event_time_column], errors="coerce"
                )
                if existing_times.isna().any():
                    raise OosHistoryReadError(
                        f"Existing OOS history has invalid {contract.event_time_column}"
                    )
                retained = retained.loc[(existing_times < start) | (existing_times >= end)]

            frames = [frame for frame in (retained, incoming) if not frame.empty]
            merged = (
                pd.concat(frames, ignore_index=True)
                if frames
                else pd.DataFrame(columns=contract.detail_columns)
            )
            if not merged.empty:
                merged = merged.drop_duplicates(list(contract.key_columns), keep="last")
                merged = merged.sort_values(
                    [contract.event_time_column, *contract.key_columns], kind="stable"
                ).reset_index(drop=True)
            metadata = OosHistoryMetadata(
                policy=self.POLICY_VERSION,
                scope=normalized_scope,
                prod_code=str(prod_code).strip(),
                coverage_start=start.isoformat(),
                coverage_end=end.isoformat(),
                refreshed_at=(refreshed_at or datetime.now()).isoformat(),
                row_count=len(merged),
            )
            self._write(path, merged, metadata)
            return OosHistorySnapshot(merged, metadata)

    @staticmethod
    def _normalize_facts(
        facts: pd.DataFrame, contract: OosScopeContract, prod_code: str
    ) -> pd.DataFrame:
        frame = facts.copy() if isinstance(facts, pd.DataFrame) else pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=contract.detail_columns)
        missing = set(contract.detail_columns).difference(frame.columns)
        if missing:
            raise OosHistoryContractError(f"OOS facts missing columns: {sorted(missing)}")
        frame = frame.loc[:, list(contract.detail_columns)].copy()
        if not frame["prod_code"].fillna("").astype(str).eq(prod_code).all():
            raise OosHistoryContractError("OOS facts contain another product")
        for column in contract.key_columns:
            frame[column] = frame[column].fillna("").astype(str)
        return frame

    def _write(
        self, path: Path, frame: pd.DataFrame, metadata: OosHistoryMetadata
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
        try:
            table = pa.Table.from_pandas(frame, preserve_index=False)
            schema_metadata = dict(table.schema.metadata or {})
            schema_metadata[self.METADATA_KEY] = json.dumps(
                asdict(metadata), ensure_ascii=False
            ).encode("utf-8")
            pq.write_table(table.replace_schema_metadata(schema_metadata), temporary)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @classmethod
    def _read_metadata(cls, path: Path) -> OosHistoryMetadata:
        raw = (pq.read_metadata(path).metadata or {}).get(cls.METADATA_KEY)
        if raw is None:
            raise ValueError("OOS history metadata is missing")
        payload = json.loads(raw.decode("utf-8"))
        return OosHistoryMetadata(
            policy=str(payload["policy"]),
            scope=str(payload["scope"]),
            prod_code=str(payload["prod_code"]),
            coverage_start=str(payload["coverage_start"]),
            coverage_end=str(payload["coverage_end"]),
            refreshed_at=str(payload["refreshed_at"]),
            row_count=int(payload["row_count"]),
        )

    @classmethod
    def _lock_for(cls, path: Path) -> threading.Lock:
        key = str(path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())
