"""Enforce configured exclusions before facts leave Inline data adapters.

Inputs already use display time. Source snapshots and maintained workbooks are
never changed by this projection; callers receive a filtered copy.
"""

from __future__ import annotations

import pandas as pd

from src.inline_domain.core.shared.date_exclusion import InlineDateExclusion
from src.shared_kernel.config import ConfigLoader


def exclude_inline_factory_dates(
    details: pd.DataFrame, exclusion: InlineDateExclusion | None,
    *, time_column: str = "start_time",
) -> pd.DataFrame:
    """Filter inclusive display dates without mutating or rewriting source facts."""
    if exclusion is None or not exclusion[0] or details.empty:
        return details.copy()
    missing = {"factory", time_column} - set(details.columns)
    if missing:
        raise ValueError(f"Inline date exclusion requires columns: {sorted(missing)}")
    factories, start_date, end_date = exclusion
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date)
    if start > end:
        raise ValueError("Inline date exclusion start_date must not exceed end_date")
    event_dates = (
        pd.to_datetime(details[time_column], errors="coerce", format="mixed")
        .dt.tz_localize(None).dt.normalize()
    )
    factory_matches = details["factory"].astype("string").str.strip().str.upper().isin(
        {name.strip().upper() for name in factories},
    )
    excluded = factory_matches & event_dates.between(start, end, inclusive="both")
    return details.loc[~excluded].copy()


def apply_inline_date_exclusion(
    frame: pd.DataFrame, *, time_column: str = "start_time",
) -> pd.DataFrame:
    """Resolve current policy on every read, including fresh-cache and fallback reads."""
    return exclude_inline_factory_dates(
        frame, ConfigLoader.get_inline_data_exclusion(), time_column=time_column,
    )


def inline_report_windows(
    start: pd.Timestamp, end: pd.Timestamp, factory: str,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Split a half-open display window before SQL aggregates facts by Sheet."""
    if start >= end:
        return []
    exclusion = ConfigLoader.get_inline_data_exclusion()
    if exclusion is None or factory.upper() not in exclusion[0]:
        return [(start, end)]
    excluded_start = pd.Timestamp(exclusion[1])
    excluded_end = pd.Timestamp(exclusion[2]) + pd.Timedelta(days=1)
    return [
        (left, right) for left, right in (
            (start, min(end, excluded_start)),
            (max(start, excluded_end), end),
        ) if left < right
    ]
