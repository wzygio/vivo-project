"""Small orchestration pipelines for Group and Code MWD trends."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def run_manual_period_pipeline(
    automatic_daily: pd.DataFrame,
    last_day: pd.Timestamp,
    aggregate_monthly: Callable[[pd.DataFrame, pd.Timestamp], pd.DataFrame],
    aggregate_weekly: Callable[[pd.DataFrame, pd.Timestamp], pd.DataFrame],
    apply_monthly_override: Callable[[pd.DataFrame, dict], pd.DataFrame],
    apply_weekly_override: Callable[[pd.DataFrame, dict], pd.DataFrame],
    rebuild_daily_from_weekly: Callable[[pd.DataFrame, pd.DataFrame, dict], pd.DataFrame],
    apply_daily_override: Callable[[pd.DataFrame, dict], pd.DataFrame],
    monthly_values: dict,
    weekly_values: dict,
    daily_values: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the explicit daily -> weekly/monthly override sequence.

    Monthly overrides are applied only to the final monthly table. Weekly
    overrides are applied to weekly data first, then rebuilt into daily data,
    and only then aggregated to monthly data.
    """
    if automatic_daily.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    weekly = aggregate_weekly(automatic_daily, last_day)
    weekly = apply_weekly_override(weekly, weekly_values)
    daily = rebuild_daily_from_weekly(automatic_daily, weekly, weekly_values)
    daily = apply_daily_override(daily, daily_values)

    # Daily overrides are the most specific input. Rebuilding weekly after
    # them keeps the three resolutions mathematically consistent.
    if daily_values:
        weekly = aggregate_weekly(daily, last_day)

    monthly = aggregate_monthly(daily, last_day)
    monthly = apply_monthly_override(monthly, monthly_values)
    return monthly, weekly, daily
