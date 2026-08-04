"""Raw data preparation and daily calendar alignment for MWD trends."""

from __future__ import annotations

import logging
from datetime import datetime as dt

import pandas as pd


def prepare_group_raw_data(
    df: pd.DataFrame,
    target_end_date: dt | None = None,
) -> tuple[pd.DataFrame, pd.Timestamp, list[str]]:
    """Convert panel details to a Group-level daily wide table."""
    working = df.copy()
    working["warehousing_time"] = pd.to_datetime(
        working["warehousing_time"], format="%Y%m%d"
    )

    data_last_day = working["warehousing_time"].max()
    last_day = pd.to_datetime(target_end_date) if target_end_date else data_last_day
    logging.info(
        "[Group Raw Data] target end=%s, data max=%s",
        last_day.strftime("%Y-%m-%d"),
        data_last_day.strftime("%Y-%m-%d"),
    )

    raw_daily = working.groupby(working["warehousing_time"].dt.date)[
        "panel_id"
    ].nunique().to_frame(name="total_panels")
    daily_defect = working.groupby(
        [working["warehousing_time"].dt.date, "defect_group"]
    )["panel_id"].nunique().unstack(level="defect_group").fillna(0)
    raw_daily = pd.concat([raw_daily, daily_defect], axis=1).fillna(0)
    raw_daily.index = pd.to_datetime(raw_daily.index)

    target_defects = sorted(working["defect_group"].dropna().unique().tolist())
    return raw_daily, last_day, target_defects


def prepare_code_raw_data(
    df: pd.DataFrame,
    target_end_date: dt | None = None,
) -> tuple[pd.DataFrame, pd.Timestamp]:
    """Convert panel details to a Code-level daily long table."""
    working = df.copy()
    working["warehousing_time"] = pd.to_datetime(
        working["warehousing_time"], format="%Y%m%d"
    )

    data_last_day = working["warehousing_time"].max()
    last_day = pd.to_datetime(target_end_date) if target_end_date else data_last_day
    logging.info(
        "[Code Raw Data] target end=%s, data max=%s",
        last_day.strftime("%Y-%m-%d"),
        data_last_day.strftime("%Y-%m-%d"),
    )

    daily_total = working.groupby(working["warehousing_time"].dt.date)[
        "panel_id"
    ].nunique().to_frame("total_panels")
    daily_code = working.groupby(
        [working["warehousing_time"].dt.date, "defect_group", "defect_desc"]
    )["panel_id"].nunique().to_frame("defect_panel_count")

    raw_daily = pd.merge(
        daily_total.reset_index(),
        daily_code.reset_index(),
        on="warehousing_time",
        how="left",
    )
    raw_daily["defect_panel_count"] = raw_daily["defect_panel_count"].fillna(0)
    raw_daily["warehousing_time"] = pd.to_datetime(raw_daily["warehousing_time"])
    raw_daily["defect_group"] = raw_daily["defect_group"].fillna("NoDefect")
    raw_daily["defect_desc"] = raw_daily["defect_desc"].fillna("NoDefect")
    return raw_daily, last_day


def pad_daily_data_to_end(
    df: pd.DataFrame,
    is_group_level: bool,
    end_date: dt | None = None,
) -> pd.DataFrame:
    """Fill missing calendar days up to the analysis end date."""
    if df.empty:
        return df.copy()

    real_end = pd.to_datetime(end_date) if end_date else pd.to_datetime(dt.now().date())
    result = df.copy()

    if is_group_level:
        if not isinstance(result.index, pd.DatetimeIndex):
            result = result.set_index("warehousing_time")
        full_dates = pd.date_range(result.index.min(), real_end, freq="D")
        result = result.reindex(full_dates).fillna(0)
        result.index.name = "warehousing_time"
        return result

    min_date = result["warehousing_time"].min()
    full_dates = pd.date_range(min_date, real_end, freq="D")
    unique_codes = result[["defect_group", "defect_desc"]].drop_duplicates()
    daily_totals = (
        result[["warehousing_time", "total_panels"]]
        .drop_duplicates()
        .set_index("warehousing_time")
        .reindex(full_dates)
        .fillna(0)
        .rename_axis("warehousing_time")
        .reset_index()
    )

    full_grid = daily_totals.assign(_key=1).merge(
        unique_codes.assign(_key=1),
        on="_key",
    ).drop(columns="_key")
    merged = full_grid.merge(
        result[["warehousing_time", "defect_desc", "defect_panel_count"]],
        on=["warehousing_time", "defect_desc"],
        how="left",
    )
    merged["defect_panel_count"] = merged["defect_panel_count"].fillna(0).astype(int)
    return merged
