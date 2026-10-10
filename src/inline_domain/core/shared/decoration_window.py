"""Pure display-date masks for time-scoped report decoration."""

from __future__ import annotations

import pandas as pd

DecorationWindow = tuple[str, str]


def decoration_window_mask(
    timestamps: pd.Series, window: DecorationWindow | None,
) -> pd.Series:
    """Include both calendar dates; unparseable times are outside active windows."""
    if window is None:
        return pd.Series(True, index=timestamps.index)
    start, end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    if start > end:
        raise ValueError("Decoration start_date must not exceed end_date")
    times = pd.to_datetime(timestamps, errors="coerce", format="mixed")
    return times.ge(start) & times.lt(end + pd.Timedelta(days=1))
