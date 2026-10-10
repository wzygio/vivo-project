"""Inline exclusion policy type and rules for maintained period aggregates.

Point/fact filtering is owned by infrastructure/shared/date_exclusion.py and
executes before report decoration. Period rules retain their separate policy.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

InlineDateExclusion = tuple[tuple[str, ...], str, str]


def exclude_inline_period_records(
    records: pd.DataFrame, exclusion: InlineDateExclusion | None,
    *, as_of: pd.Timestamp,
    period_type_column: str = "period_type",
    period_label_column: str = "period_label",
    factory_column: str = "factory",
) -> pd.DataFrame:
    """Omit maintained aggregates that cannot separate excluded facts; never rewrite them."""
    if exclusion is None or not exclusion[0] or records.empty:
        return records.copy()
    required = {factory_column, period_type_column, period_label_column}
    if missing := required - set(records.columns):
        raise ValueError(f"Inline period exclusion requires columns: {sorted(missing)}")
    factories, start_date, end_date = exclusion
    excluded_factories = {name.strip().upper() for name in factories}
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date)
    if start > end:
        raise ValueError("Inline date exclusion start_date must not exceed end_date")
    excluded: list[bool] = []
    for _, row in records.iterrows():
        factory_key = str(row[factory_column]).strip().upper()
        if factory_key != "ALL" and not excluded_factories.intersection(
            name.strip() for name in factory_key.split(",")
        ):
            excluded.append(False)
            continue
        period_start, period_end = _period_dates(
            str(row[period_type_column]), str(row[period_label_column]),
        )
        period_end = min(period_end, pd.Timestamp(as_of).normalize())
        excluded.append(period_start <= period_end and period_start <= end and period_end >= start)
    return records.loc[[not value for value in excluded]].copy()


def _period_dates(kind: str, label: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    if kind in {"year", "年度"}:
        year = int(label)
        return pd.Timestamp(year, 1, 1), pd.Timestamp(year, 12, 31)
    if kind in {"quarter", "季度"}:
        year, quarter = label.split("-Q")
        start = pd.Timestamp(int(year), (int(quarter) - 1) * 3 + 1, 1)
        return start, start + pd.DateOffset(months=3) - pd.Timedelta(days=1)
    if kind in {"month", "月度"}:
        period = pd.Period(label, freq="M")
        return period.start_time, period.end_time.normalize()
    if kind in {"week", "周度"}:
        year, week = label.split("-W")
        start = pd.Timestamp(date.fromisocalendar(int(year), int(week), 1))
        return start, start + pd.Timedelta(days=6)
    if kind in {"day", "日度", "天度"}:
        day = pd.Timestamp(label).normalize()
        return day, day
    raise ValueError(f"Unsupported Inline period type: {kind}")
