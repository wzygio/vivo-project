"""Outbound data contract owned by the Q-Time application layer."""

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol

import pandas as pd

from src.indicator_domain.application.qtime.dtos import (
    QTimeQuery,
    QTimeStepOption,
    Shop,
)


@dataclass(frozen=True, slots=True)
class QTimeSnapshotRefresh:
    """Application-facing outcome of one shop snapshot refresh."""

    shop: Shop
    row_count: int
    refreshed_from_database: bool
    source_start: str
    source_end: str
    refreshed_at: str


class QTimeDataPort(Protocol):
    def cache_signature(self, shop: Shop) -> tuple[object, ...]: ...

    def list_step_options(self, shop: Shop) -> tuple[QTimeStepOption, ...]: ...

    def fetch_details(self, query: QTimeQuery) -> pd.DataFrame: ...

    def refresh_snapshot(
        self,
        shop: Shop,
        *,
        as_of: date | None = None,
        full_refresh: bool = False,
    ) -> QTimeSnapshotRefresh: ...


class QTimeDecorationPort(Protocol):
    @property
    def decoration_path(self) -> Path: ...

    def load_decisions(self) -> pd.DataFrame: ...

    def save_decisions(self, decisions: pd.DataFrame) -> None: ...
