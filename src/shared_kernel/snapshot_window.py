"""Shared source-time window rules for product-level snapshots."""

from __future__ import annotations

import pandas as pd


def snapshot_window_start(end_date: object) -> pd.Timestamp:
    """Return the first day of the third prior natural month."""
    normalized_end = pd.Timestamp(end_date).normalize()
    month_start = normalized_end.replace(day=1)
    return month_start - pd.DateOffset(months=3)


def manual_refresh_start(
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    covered_until: pd.Timestamp | None,
    days: int,
) -> pd.Timestamp:
    """Start a manual tail read, with exclusive end dates and gap-free coverage."""
    if covered_until is None:
        return window_start
    window_days = (window_end - window_start).days
    requested_start = window_end - pd.Timedelta(days=min(days, window_days))
    return max(window_start, min(requested_start, covered_until))
