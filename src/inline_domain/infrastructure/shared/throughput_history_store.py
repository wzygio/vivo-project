"""Atomic, overlap-replacing Parquet history for daily Inline throughput."""

from __future__ import annotations

import hashlib
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

import pandas as pd

from src.inline_domain.core.shared.throughput_facts import THROUGHPUT_COLUMNS

THROUGHPUT_SCOPES = {"spc", "ctq", "aoi_tt", "aoi_rs"}


class ThroughputHistoryStore:
    _locks_guard = threading.Lock()
    _snapshot_locks: ClassVar[dict[str, threading.Lock]] = {}

    def __init__(
        self, snapshot_dir: Path | str = Path("data/inline_domain/throughput_history")
    ) -> None:
        self._snapshot_dir = Path(snapshot_dir)

    def snapshot_path(self, scope: str, prod_code: str) -> Path:
        normalized_scope = str(scope).strip().lower()
        product = str(prod_code).strip()
        if normalized_scope not in THROUGHPUT_SCOPES:
            raise ValueError(f"unsupported throughput scope: {scope!r}")
        if not product:
            raise ValueError("scope and prod_code must not be empty")
        digest = hashlib.sha256(product.encode("utf-8")).hexdigest()[:16]
        return self._snapshot_dir / f"{normalized_scope}__{digest}.parquet"

    def read(self, scope: str, prod_code: str) -> pd.DataFrame:
        path = self.snapshot_path(scope, prod_code)
        if not path.exists():
            return pd.DataFrame(columns=THROUGHPUT_COLUMNS)
        frame = pd.read_parquet(path)
        missing = set(THROUGHPUT_COLUMNS).difference(frame.columns)
        if missing:
            raise ValueError(f"throughput history missing columns: {sorted(missing)}")
        normalized_scope = str(scope).strip().lower()
        product = str(prod_code).strip()
        if not frame.empty and (
            not frame["scope"].astype(str).eq(normalized_scope).all()
            or not frame["prod_code"].astype(str).eq(product).all()
        ):
            raise ValueError("throughput history identity mismatch")
        if frame["item_id"].fillna("").astype(str).str.strip().eq("").any():
            raise ValueError("throughput history contains an empty item_id")
        return frame[THROUGHPUT_COLUMNS].copy()

    def update(
        self,
        scope: str,
        prod_code: str,
        facts: pd.DataFrame,
        *,
        coverage_start: pd.Timestamp,
        coverage_end: pd.Timestamp,
    ) -> pd.DataFrame:
        start = pd.Timestamp(coverage_start).normalize()
        end = pd.Timestamp(coverage_end).normalize()
        if pd.isna(start) or pd.isna(end) or start >= end:
            raise ValueError("coverage must be a non-empty half-open interval")

        incoming = facts.copy() if isinstance(facts, pd.DataFrame) else pd.DataFrame()
        if incoming.empty:
            incoming = pd.DataFrame(columns=THROUGHPUT_COLUMNS)
        missing = set(THROUGHPUT_COLUMNS).difference(incoming.columns)
        if missing:
            raise ValueError(f"throughput facts missing columns: {sorted(missing)}")
        incoming = incoming[THROUGHPUT_COLUMNS].copy()
        if not incoming.empty:
            if not incoming["scope"].astype(str).eq(str(scope).strip().lower()).all():
                raise ValueError("throughput facts contain another scope")
            if not incoming["prod_code"].astype(str).eq(str(prod_code).strip()).all():
                raise ValueError("throughput facts contain another product")
            incoming["event_date"] = pd.to_datetime(
                incoming["event_date"], errors="coerce"
            ).dt.normalize()
            if incoming["event_date"].isna().any():
                raise ValueError("throughput facts contain invalid event_date")
            incoming["item_id"] = incoming["item_id"].fillna("").astype(str)
            if incoming["item_id"].str.strip().eq("").any():
                raise ValueError("throughput facts contain an empty item_id")
            incoming = incoming[
                (incoming["event_date"] >= start) & (incoming["event_date"] < end)
            ]

        path = self.snapshot_path(scope, prod_code)
        with self._lock_for(path):
            with self._process_lock(path):
                current = self.read(scope, prod_code)
                if not current.empty:
                    current["event_date"] = pd.to_datetime(
                        current["event_date"]
                    ).dt.normalize()
                    current = current[
                        (current["event_date"] < start)
                        | (current["event_date"] >= end)
                    ]
                frames = [frame for frame in (current, incoming) if not frame.empty]
                merged = (
                    pd.concat(frames, ignore_index=True)
                    if frames
                    else pd.DataFrame(columns=THROUGHPUT_COLUMNS)
                )
                if not merged.empty:
                    merged = (
                        merged.drop_duplicates(
                            ["scope", "prod_code", "factory", "event_date", "item_id"],
                            keep="last",
                        )
                        .sort_values(["event_date", "factory", "item_id"], kind="stable")
                        .reset_index(drop=True)
                    )
                self._write(path, merged)
                return merged

    @staticmethod
    def _write(path: Path, frame: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
        try:
            frame.to_parquet(temporary, index=False)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @classmethod
    def _lock_for(cls, path: Path) -> threading.Lock:
        key = str(path.resolve())
        with cls._locks_guard:
            return cls._snapshot_locks.setdefault(key, threading.Lock())

    @staticmethod
    @contextmanager
    def _process_lock(path: Path, timeout_seconds: float = 15.0):
        """Serialize read-modify-write across Streamlit/scheduler processes."""
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_suffix(f"{path.suffix}.lock")
        deadline = time.monotonic() + timeout_seconds
        descriptor: int | None = None
        while descriptor is None:
            try:
                descriptor = os.open(
                    lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                try:
                    stale = time.time() - lock_path.stat().st_mtime > 60
                    if stale:
                        lock_path.unlink(missing_ok=True)
                        continue
                except OSError:
                    continue
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for throughput lock: {lock_path}")
                time.sleep(0.05)
        try:
            os.write(descriptor, str(os.getpid()).encode("ascii"))
        except BaseException:
            os.close(descriptor)
            lock_path.unlink(missing_ok=True)
            raise
        try:
            yield
        finally:
            os.close(descriptor)
            lock_path.unlink(missing_ok=True)


__all__ = ["ThroughputHistoryStore"]
