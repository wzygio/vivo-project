"""Output formatting for MWD trend tables."""

from __future__ import annotations

import numpy as np
import pandas as pd


def format_group_results(
    monthly: pd.DataFrame,
    weekly: pd.DataFrame,
    daily: pd.DataFrame,
    target_defects: list[str],
) -> dict[str, pd.DataFrame]:
    """Format Group wide tables for the application/UI layer."""

    monthly_prepared = _prepare_group_table(monthly, target_defects)
    weekly_prepared = _prepare_group_table(weekly, target_defects)
    daily_prepared = _prepare_group_table(daily, target_defects)

    return {
        "monthly": _format_group_table(
            monthly_prepared, target_defects, "%Y-%m月", 3
        ),
        "weekly": _format_group_table(
            weekly_prepared, target_defects, "ISO", 3
        ),
        "daily_full": _format_group_table(
            daily_prepared, target_defects, "%Y-%m-%d", None
        ),
        "daily": _format_group_table(
            daily_prepared, target_defects, "%m-%d", 7
        ),
    }


def format_code_results(
    monthly: pd.DataFrame,
    weekly: pd.DataFrame,
    daily: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Format Code long tables for the application/UI layer."""

    monthly_prepared = _prepare_code_table(monthly)
    weekly_prepared = _prepare_code_table(weekly)
    daily_prepared = _prepare_code_table(daily)

    return {
        "monthly": _format_code_table(monthly_prepared, "%Y-%m月", 3),
        "weekly": _format_code_table(weekly_prepared, "ISO", 3),
        "weekly_full": _format_code_table(weekly_prepared, "ISO", None),
        "daily_full": _format_code_table(daily_prepared, "%Y-%m-%d", None),
        "daily": _format_code_table(daily_prepared, "%m-%d", 7),
    }


def _prepare_group_table(
    aggregate: pd.DataFrame,
    target_defects: list[str],
) -> pd.DataFrame:
    """Group 表只做一次日期解析和良损计算。"""
    if aggregate.empty:
        return pd.DataFrame()

    result = aggregate.copy()
    if "warehousing_time" not in result.columns:
        result = result.reset_index()
        if "index" in result.columns:
            result = result.rename(columns={"index": "warehousing_time"})
        elif "level_0" in result.columns:
            result = result.rename(columns={"level_0": "warehousing_time"})
    result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])

    for group in target_defects:
        rate_column = f"{group.lower()}_rate"
        if group not in result.columns:
            result[group] = 0
        result[rate_column] = np.where(
            result["total_panels"] > 0,
            result[group] / result["total_panels"],
            0.0,
        )
    return result


def _format_group_table(
    prepared: pd.DataFrame,
    target_defects: list[str],
    date_format: str,
    tail_size: int | None,
) -> pd.DataFrame:
    if prepared.empty:
        return pd.DataFrame()

    result, ordered_periods = _select_and_label_periods(
        prepared, date_format, tail_size
    )
    rate_map = {f"{group.lower()}_rate": group for group in target_defects}
    melted = result.melt(
        id_vars=["time_period", "total_panels"],
        value_vars=list(rate_map),
        var_name="defect_group_raw",
        value_name="defect_rate",
    )
    melted["defect_group"] = melted["defect_group_raw"].map(rate_map)
    melted["time_period"] = pd.Categorical(
        melted["time_period"], categories=ordered_periods, ordered=True
    )
    return melted.sort_values(["time_period", "defect_group"]).reset_index(drop=True)


def _prepare_code_table(aggregate: pd.DataFrame) -> pd.DataFrame:
    """Code 表只做一次日期解析、良损计算和 NoDefect 过滤。"""
    if aggregate.empty:
        return pd.DataFrame()

    result = aggregate.copy()
    result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])
    result["defect_rate"] = np.where(
        result["total_panels"] > 0,
        result["defect_panel_count"] / result["total_panels"],
        0.0,
    )
    return result[result["defect_desc"] != "NoDefect"].copy()


def _format_code_table(
    prepared: pd.DataFrame,
    date_format: str,
    tail_size: int | None,
) -> pd.DataFrame:
    if prepared.empty:
        return pd.DataFrame()

    result, ordered_periods = _select_and_label_periods(
        prepared, date_format, tail_size
    )
    result["time_period"] = pd.Categorical(
        result["time_period"], categories=ordered_periods, ordered=True
    )
    result = result[
        [
            column
            for column in result.columns
            if column not in {"time_period", "defect_rate"}
        ]
        + ["time_period", "defect_rate"]
    ]
    return result.sort_values(
        ["time_period", "defect_group", "defect_desc"]
    ).reset_index(drop=True)


def _select_and_label_periods(
    prepared: pd.DataFrame,
    date_format: str,
    tail_size: int | None,
) -> tuple[pd.DataFrame, list[str]]:
    """先按真实日期截取近期窗口，再只复制并标记需要展示的行。"""
    ordered_dates = (
        prepared.sort_values("warehousing_time")["warehousing_time"]
        .drop_duplicates()
        .tolist()
    )
    target_dates = ordered_dates if tail_size is None else ordered_dates[-tail_size:]
    result = prepared[prepared["warehousing_time"].isin(target_dates)].copy()
    if date_format == "ISO":
        iso = result["warehousing_time"].dt.isocalendar()
        result["time_period"] = (
            iso.year.astype(str) + "-W" + iso.week.map("{:02d}".format)
        )
    else:
        result["time_period"] = result["warehousing_time"].dt.strftime(date_format)
    ordered_periods = (
        result.sort_values("warehousing_time")["time_period"]
        .drop_duplicates()
        .tolist()
    )
    return result, ordered_periods
