"""Unified local Parquet storage for Q-Time source facts."""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.indicator_domain.application.qtime.dtos import Shop

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class QTimeSnapshotMetadata:
    """Coverage contract published with one shop snapshot."""

    policy: str
    shop: Shop
    source_start: str
    source_end: str
    refreshed_at: str
    row_count: int


@dataclass(frozen=True, slots=True)
class QTimeSnapshot:
    """A normalized source frame and its verified coverage metadata."""

    frame: pd.DataFrame
    metadata: QTimeSnapshotMetadata


class QTimeSnapshotStore:
    """Persist one TTL-aware, query-independent Q-Time fact snapshot per shop."""

    # v2 could publish empty coverage after rejecting microsecond source timekeys.
    POLICY_VERSION = "qtime-source-v4"
    METADATA_KEY = b"qtime_snapshot"
    REQUIRED_SHOPS: ClassVar[tuple[Shop, ...]] = ("ARRAY", "OLED", "TP")
    LEGACY_PATTERNS = (
        "qtime_details_*.parquet",
        "qtime_*_qtime-source-v1.parquet",
    )
    _locks_guard = threading.Lock()
    _snapshot_locks: ClassVar[dict[str, threading.Lock]] = {}

    def __init__(self, snapshot_dir: Path, ttl_hours: int) -> None:
        if ttl_hours <= 0:
            raise ValueError("Q-Time snapshot TTL must be positive")
        self._snapshot_dir = Path(snapshot_dir)
        self._ttl_hours = ttl_hours

    def source_path(self, shop: Shop) -> Path:
        return self._snapshot_dir / f"qtime_source_{shop.lower()}.parquet"

    def read(
        self,
        shop: Shop,
        *,
        fresh_only: bool,
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> QTimeSnapshot | None:
        source_path = self.source_path(shop)
        if not source_path.exists():
            return None
        try:
            metadata = self._read_metadata(source_path)
            if metadata.policy != self.POLICY_VERSION or metadata.shop != shop:
                return None
            if fresh_only and not self._is_fresh(metadata):
                return None
            frame = normalizer(pd.read_parquet(source_path))
        except Exception:
            logger.exception("Failed to read Q-Time %s snapshot", shop)
            return None
        if len(frame) != metadata.row_count:
            logger.warning(
                "Q-Time %s snapshot row count differs from metadata: %s != %s",
                shop,
                len(frame),
                metadata.row_count,
            )
            return None
        return QTimeSnapshot(frame=frame, metadata=metadata)

    def write(
        self,
        shop: Shop,
        source: pd.DataFrame,
        *,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
        refreshed_at: datetime,
    ) -> QTimeSnapshotMetadata:
        self._snapshot_dir.mkdir(parents=True, exist_ok=True)
        source_path = self.source_path(shop)
        nonce = uuid4().hex
        temporary_source = source_path.with_name(f"{source_path.name}.{nonce}.tmp")
        metadata = QTimeSnapshotMetadata(
            policy=self.POLICY_VERSION,
            shop=shop,
            source_start=pd.Timestamp(source_start).isoformat(),
            source_end=pd.Timestamp(source_end).isoformat(),
            refreshed_at=refreshed_at.isoformat(),
            row_count=len(source),
        )
        try:
            table = pa.Table.from_pandas(source, preserve_index=False)
            schema_metadata = dict(table.schema.metadata or {})
            schema_metadata[self.METADATA_KEY] = json.dumps(
                asdict(metadata), ensure_ascii=False
            ).encode("utf-8")
            pq.write_table(
                table.replace_schema_metadata(schema_metadata), temporary_source
            )
            os.replace(temporary_source, source_path)
            self._remove_legacy_snapshots()
        finally:
            temporary_source.unlink(missing_ok=True)
        return metadata

    def _remove_legacy_snapshots(self) -> None:
        """Remove old L1 files only after every shop has a replacement snapshot."""
        if not all(self.source_path(shop).exists() for shop in self.REQUIRED_SHOPS):
            return
        for pattern in self.LEGACY_PATTERNS:
            for legacy_path in self._snapshot_dir.glob(pattern):
                try:
                    legacy_path.unlink()
                except OSError:
                    logger.warning(
                        "Failed to remove legacy Q-Time snapshot %s",
                        legacy_path,
                        exc_info=True,
                    )

    @staticmethod
    def covers(
        snapshot: QTimeSnapshot,
        source_start: pd.Timestamp,
        source_end: pd.Timestamp,
    ) -> bool:
        covered_start = pd.Timestamp(snapshot.metadata.source_start)
        covered_end = pd.Timestamp(snapshot.metadata.source_end)
        return covered_start <= source_start and covered_end >= source_end

    @classmethod
    def _read_metadata(cls, source_path: Path) -> QTimeSnapshotMetadata:
        parquet_metadata = pq.read_metadata(source_path).metadata or {}
        raw_payload = parquet_metadata.get(cls.METADATA_KEY)
        if raw_payload is None:
            raise ValueError("Q-Time snapshot metadata is missing")
        payload = json.loads(raw_payload.decode("utf-8"))
        return QTimeSnapshotMetadata(
            policy=str(payload["policy"]),
            shop=payload["shop"],
            source_start=str(payload["source_start"]),
            source_end=str(payload["source_end"]),
            refreshed_at=str(payload["refreshed_at"]),
            row_count=int(payload["row_count"]),
        )

    def _is_fresh(self, metadata: QTimeSnapshotMetadata) -> bool:
        refreshed_at = pd.Timestamp(metadata.refreshed_at)
        now = pd.Timestamp.now(tz=refreshed_at.tz)
        return now - refreshed_at < pd.Timedelta(hours=self._ttl_hours)

    @classmethod
    def lock_for(cls, source_path: Path) -> threading.Lock:
        key = str(source_path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())
