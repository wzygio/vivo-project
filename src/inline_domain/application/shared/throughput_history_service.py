"""Application seam shared by Inline producers and the monitor dashboard."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from src.inline_domain.core.shared.throughput_facts import build_daily_throughput_facts
from src.inline_domain.infrastructure.shared.throughput_history_store import (
    ThroughputHistoryStore,
)


class ThroughputHistoryService:
    def __init__(self, store: ThroughputHistoryStore) -> None:
        self._store = store

    def update_from_details(
        self,
        scope: str,
        prod_code: str,
        details: pd.DataFrame,
        *,
        event_time_column: str,
        item_id_column: str,
        coverage_start: pd.Timestamp,
        coverage_end: pd.Timestamp,
    ) -> None:
        facts = build_daily_throughput_facts(
            details,
            scope=scope,
            prod_code=prod_code,
            event_time_column=event_time_column,
            item_id_column=item_id_column,
        )
        self._store.update(
            scope,
            prod_code,
            facts,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
        )

    def read_product(self, scope: str, prod_code: str) -> pd.DataFrame:
        return self._store.read(scope, prod_code)

    def snapshot_path(self, scope: str, prod_code: str) -> Path:
        return self._store.snapshot_path(scope, prod_code)

    def source_signature(self, products: list[str], scopes: list[str]) -> str:
        parts: list[str] = []
        for scope in sorted(set(scopes)):
            for prod_code in sorted(set(products)):
                path = self.snapshot_path(scope, prod_code)
                try:
                    stat = path.stat()
                    parts.append(f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
                except OSError:
                    parts.append(f"{path.name}:missing")
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


__all__ = ["ThroughputHistoryService"]
