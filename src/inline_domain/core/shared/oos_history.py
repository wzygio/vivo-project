"""Persistence-neutral contracts for shared Inline OOS history."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class OosScopeContract:
    key_columns: tuple[str, ...]
    event_time_column: str
    detail_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OosHistoryMetadata:
    policy: str
    scope: str
    prod_code: str
    coverage_start: str
    coverage_end: str
    refreshed_at: str
    row_count: int


@dataclass(frozen=True, slots=True)
class OosHistorySnapshot:
    frame: pd.DataFrame
    metadata: OosHistoryMetadata
