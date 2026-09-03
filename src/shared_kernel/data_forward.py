"""Shared source-time to display-time policy for manufacturing facts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class DataForwardPolicy:
    """Translate manufacturing fact timestamps onto the report display axis."""

    enabled: bool = False
    offset_days: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("data forward enabled must be a boolean")
        if isinstance(self.offset_days, bool) or not isinstance(self.offset_days, int):
            raise ValueError("data forward offset_days must be an integer")
        if self.offset_days < 0:
            raise ValueError("data forward offset_days must be non-negative")

    @property
    def effective_days(self) -> int:
        return self.offset_days if self.enabled else 0

    @property
    def signature(self) -> str:
        mode = "enabled" if self.enabled else "disabled"
        return f"data-forward-v1:{mode}:{self.offset_days}d"

    def shift_frame(
        self,
        frame: pd.DataFrame,
        time_columns: Iterable[str],
    ) -> pd.DataFrame:
        """Return a copy with existing manufacturing-time columns shifted."""
        shifted = frame.copy()
        if self.effective_days == 0:
            return shifted
        offset = pd.Timedelta(days=self.effective_days)
        for column in time_columns:
            if column in shifted.columns:
                shifted[column] = pd.to_datetime(shifted[column], errors="coerce") + offset
        return shifted

    def to_source_window(
        self,
        display_start: pd.Timestamp,
        display_end: pd.Timestamp,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Translate a caller-facing display window to its source-time window."""
        offset = pd.Timedelta(days=self.effective_days)
        return pd.Timestamp(display_start) - offset, pd.Timestamp(display_end) - offset

    def snapshot_start(self, display_end: object) -> pd.Timestamp:
        """Return the configured snapshot start while preserving legacy mode."""
        normalized_end = pd.Timestamp(display_end).normalize()
        if not self.enabled:
            return normalized_end - pd.DateOffset(months=3)
        month_start = normalized_end.replace(day=1)
        return month_start - pd.DateOffset(months=3)
