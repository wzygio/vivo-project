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

    def format_table(
        aggregate: pd.DataFrame,
        date_format: str,
        tail_size: int,
    ) -> pd.DataFrame:
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
            if group in result.columns:
                result[rate_column] = np.where(
                    result["total_panels"] > 0,
                    result[group] / result["total_panels"],
                    0.0,
                )
            else:
                result[rate_column] = 0.0
                result[group] = 0

        if date_format == "ISO":
            iso = result["warehousing_time"].dt.isocalendar()
            result["time_period"] = iso.year.astype(str) + "-W" + iso.week.map(
                "{:02d}".format
            )
        else:
            result["time_period"] = result["warehousing_time"].dt.strftime(date_format)

        rate_map = {f"{group.lower()}_rate": group for group in target_defects}
        melted = result.melt(
            id_vars=["time_period", "total_panels"],
            value_vars=list(rate_map),
            var_name="defect_group_raw",
            value_name="defect_rate",
        )
        melted["defect_group"] = melted["defect_group_raw"].map(rate_map)

        ordered_periods = (
            result.sort_values("warehousing_time")["time_period"].drop_duplicates().tolist()
        )
        target_periods = ordered_periods[-tail_size:]
        melted = melted[melted["time_period"].isin(target_periods)].copy()
        melted["time_period"] = pd.Categorical(
            melted["time_period"],
            categories=target_periods,
            ordered=True,
        )
        return melted.sort_values(["time_period", "defect_group"]).reset_index(drop=True)

    return {
        "monthly": format_table(monthly, "%Y-%m月", 3),
        "weekly": format_table(weekly, "ISO", 3),
        "daily_full": format_table(daily, "%Y-%m-%d", 9999),
        "daily": format_table(daily, "%m-%d", 7),
    }


def format_code_results(
    monthly: pd.DataFrame,
    weekly: pd.DataFrame,
    daily: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Format Code long tables for the application/UI layer."""

    def format_table(
        aggregate: pd.DataFrame,
        date_format: str,
        tail_size: int,
    ) -> pd.DataFrame:
        if aggregate.empty:
            return pd.DataFrame()

        result = aggregate.copy()
        result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])
        if date_format == "ISO":
            iso = result["warehousing_time"].dt.isocalendar()
            result["time_period"] = iso.year.astype(str) + "-W" + iso.week.map(
                "{:02d}".format
            )
        else:
            result["time_period"] = result["warehousing_time"].dt.strftime(date_format)

        result["defect_rate"] = np.where(
            result["total_panels"] > 0,
            result["defect_panel_count"] / result["total_panels"],
            0.0,
        )
        result = result[result["defect_desc"] != "NoDefect"].copy()
        ordered_periods = (
            result.sort_values("warehousing_time")["time_period"].drop_duplicates().tolist()
        )
        target_periods = ordered_periods[-tail_size:]
        result = result[result["time_period"].isin(target_periods)].copy()
        result["time_period"] = pd.Categorical(
            result["time_period"],
            categories=target_periods,
            ordered=True,
        )
        return result.sort_values(
            ["time_period", "defect_group", "defect_desc"]
        ).reset_index(drop=True)

    return {
        "monthly": format_table(monthly, "%Y-%m月", 3),
        "weekly": format_table(weekly, "ISO", 3),
        "weekly_full": format_table(weekly, "ISO", 9999),
        "daily_full": format_table(daily, "%Y-%m-%d", 9999),
        "daily": format_table(daily, "%m-%d", 7),
    }
