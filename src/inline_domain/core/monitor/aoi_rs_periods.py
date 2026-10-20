"""Calendar periods selected by the AOI RS monitoring use case."""

from __future__ import annotations

from datetime import date

import pandas as pd


def build_monitor_period_axis(
    as_of: date,
    *,
    halfyears: tuple[int, ...],
    quarters: tuple[int, ...],
    months: tuple[int, ...],
    week_count: int,
) -> pd.DataFrame:
    """Build elapsed current-year periods and recent ISO weeks, inclusive of today.

    Calendar selections are ordered and deduplicated. Each current period ends
    at ``as_of``; preceding periods retain their complete calendar window.
    """
    cutoff = pd.Timestamp(as_of).normalize()
    if pd.isna(cutoff):
        raise ValueError("AOI RS monitoring requires a valid cutoff date")
    if isinstance(week_count, bool) or not isinstance(week_count, int) or not 1 <= week_count <= 53:
        raise ValueError("AOI RS monitoring week count must be an integer from 1 to 53")
    records: list[dict[str, object]] = []
    for kind, selections, size, marker, offset in [
        ("halfyear", halfyears, 6, "H", 100),
        ("quarter", quarters, 3, "Q", 200),
        ("month", months, 1, "M", 300),
    ]:
        for number in selections:
            if isinstance(number, bool) or not isinstance(number, int) or not 1 <= number <= 12 // size:
                raise ValueError(f"Invalid AOI RS {kind} selection")
        for number in sorted(set(selections)):
            start = pd.Timestamp(year=cutoff.year, month=(number - 1) * size + 1, day=1)
            if start > cutoff:
                raise ValueError(f"AOI RS {kind} selection has not started")
            end = start + pd.DateOffset(months=size) - pd.Timedelta(days=1)
            label_number = f"{number:02d}" if kind == "month" else str(number)
            records.append(dict(
                period_type=kind, period_label=f"{cutoff.year}-{marker}{label_number}",
                period_sort=offset + number, period_start=start, period_end=min(end, cutoff),
            ))
    current_monday = cutoff - pd.Timedelta(days=cutoff.weekday())
    for index in range(week_count):
        start = current_monday - pd.Timedelta(weeks=week_count - index - 1)
        iso = start.isocalendar()
        records.append(dict(
            period_type="week", period_label=f"{iso.year}-W{iso.week:02d}",
            period_sort=401 + index, period_start=start,
            period_end=min(start + pd.Timedelta(days=6), cutoff),
        ))
    return pd.DataFrame(records, columns=[
        "period_type", "period_label", "period_sort", "period_start", "period_end",
    ])
