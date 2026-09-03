"""Local Parquet storage for Q-Time source facts and filter options."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections.abc import Callable
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

import pandas as pd

logger = logging.getLogger(__name__)


class QTimeSnapshotStore:
    """Persist query-isolated Q-Time source frames with TTL-aware reads."""

    POLICY_VERSION = "qtime-source-v1"
    _locks_guard = threading.Lock()
    _snapshot_locks: ClassVar[dict[str, threading.Lock]] = {}

    def __init__(self, snapshot_dir: Path, ttl_hours: int) -> None:
        if ttl_hours <= 0:
            raise ValueError("Q-Time snapshot TTL must be positive")
        self._snapshot_dir = Path(snapshot_dir)
        self._ttl_hours = ttl_hours

    def detail_path(self, params: dict[str, object]) -> Path:
        signature_payload = {
            "policy": self.POLICY_VERSION,
            "start_time": params["start_time"],
            "end_time": params["end_time"],
            "shop": params["shop"],
            "step_descriptions": list(params["step_descriptions"]),
            "products": list(params.get("products", ())),
        }
        signature = hashlib.sha256(
            json.dumps(
                signature_payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:16]
        return self._snapshot_dir / f"qtime_details_{signature}.parquet"

    def option_path(self, name: str) -> Path:
        return self._snapshot_dir / f"qtime_{name}_{self.POLICY_VERSION}.parquet"

    def read(
        self,
        snapshot_path: Path,
        *,
        fresh_only: bool,
        normalizer: Callable[[pd.DataFrame], pd.DataFrame],
        label: str,
    ) -> pd.DataFrame | None:
        if not snapshot_path.exists():
            return None
        if fresh_only and not self._is_fresh(snapshot_path):
            return None
        try:
            return normalizer(pd.read_parquet(snapshot_path))
        except Exception:
            logger.exception(
                "Failed to read Q-Time %s snapshot %s", label, snapshot_path
            )
            return None

    def write(self, snapshot_path: Path, source: pd.DataFrame) -> None:
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = snapshot_path.with_name(
            f"{snapshot_path.name}.{uuid4().hex}.tmp"
        )
        try:
            source.to_parquet(temporary_path, index=False)
            os.replace(temporary_path, snapshot_path)
        finally:
            temporary_path.unlink(missing_ok=True)

    def _is_fresh(self, snapshot_path: Path) -> bool:
        age_seconds = pd.Timestamp.now().timestamp() - snapshot_path.stat().st_mtime
        return age_seconds < self._ttl_hours * 3600

    @classmethod
    def lock_for(cls, snapshot_path: Path) -> threading.Lock:
        key = str(snapshot_path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())
