"""Shared source-time window rules for product-level snapshots."""

from __future__ import annotations

import pandas as pd


def snapshot_window_start(end_date: object) -> pd.Timestamp:
    """Return the first day of the third prior natural month."""
    normalized_end = pd.Timestamp(end_date).normalize()
    month_start = normalized_end.replace(day=1)
    return month_start - pd.DateOffset(months=3)
