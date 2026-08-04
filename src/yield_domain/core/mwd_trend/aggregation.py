"""Monthly/weekly aggregation for Group and Code trend tables."""

from __future__ import annotations

from datetime import datetime as dt

import pandas as pd
from dateutil.relativedelta import relativedelta


def safe_trend_aggregator(
    df: pd.DataFrame,
    anchor_date: dt,
    freq: str,
    is_group_level: bool = False,
) -> pd.DataFrame:
    """Aggregate trend counts while keeping one global denominator per period."""
    if df.empty:
        return pd.DataFrame()

    working = df.copy()
    if "warehousing_time" not in working.columns:
        working = working.reset_index()
        if "index" in working.columns:
            working = working.rename(columns={"index": "warehousing_time"})
        elif "level_0" in working.columns:
            working = working.rename(columns={"level_0": "warehousing_time"})

    working["warehousing_time"] = pd.to_datetime(working["warehousing_time"])
    start = pd.to_datetime(anchor_date) - relativedelta(months=3)
    working = working[
        working["warehousing_time"].dt.to_period(freq)
        >= pd.Period(start, freq)
    ].copy()
    if working.empty:
        return pd.DataFrame()

    daily_globals = working[["warehousing_time", "total_panels"]].drop_duplicates(
        subset=["warehousing_time"]
    )
    global_totals = daily_globals.set_index("warehousing_time").resample(freq)[
        "total_panels"
    ].sum()

    if is_group_level:
        excluded = {"warehousing_time", "total_panels", "month_period"}
        group_columns = [c for c in working.columns if c not in excluded]
        numerator = working.set_index("warehousing_time").resample(freq)[
            group_columns
        ].sum()
        return numerator.join(global_totals)

    numerator = (
        working.groupby(
            [
                pd.Grouper(key="warehousing_time", freq=freq),
                "defect_group",
                "defect_desc",
            ]
        )["defect_panel_count"]
        .sum()
        .reset_index()
        .set_index("warehousing_time")
    )
    merged = numerator.join(global_totals, rsuffix="_global", how="left")
    if "total_panels_global" in merged.columns:
        merged["total_panels"] = merged["total_panels_global"]
        merged = merged.drop(columns=["total_panels_global"])
    return merged.reset_index()
