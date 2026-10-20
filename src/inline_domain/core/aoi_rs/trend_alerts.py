"""Yesterday-density warnings with bounded sparse-date comparisons (no I/O)."""

from __future__ import annotations

from datetime import date, timedelta
from math import isclose

import numpy as np
import pandas as pd

AOI_RS_TREND_POLICY_VERSION = "yesterday-sparse-seven-day-five-day-mean-v2"
RISE_FACTOR = 1.2
LOOKBACK_DAYS = 7
BASELINE_DAYS = 5
BASELINE_LAG_DAYS = 2
HISTORY_LOOKBACK_DAYS = max(LOOKBACK_DAYS, BASELINE_DAYS + BASELINE_LAG_DAYS - 1)
INDICATOR_KEYS = ["factory", "step_id", "rs_code"]
EVALUATION_COLUMNS = [
    "prod_code", *INDICATOR_KEYS, "target_date", "target_density",
    "previous_date", "previous_density", "two_days_before_date",
    "two_days_before_density", "baseline_density",
    "target_sheet_qty", "valid_days", "baseline_days", "daily_evaluable",
    "rising_evaluable", "daily_alert", "rising_alert", "is_alert",
]


def _exceeds_rise(value: float, baseline: float) -> bool:
    """Treat binary roundoff at exactly 20% as equality (e.g. 1/5 vs 1/6)."""
    threshold = RISE_FACTOR * baseline
    return value > threshold and not isclose(value, threshold, rel_tol=1e-12, abs_tol=0)


def _evaluate_series(series: pd.DataFrame, target: pd.Timestamp) -> dict[str, object]:
    values = (
        series.set_index("day")["value"].sort_index() if not series.empty
        else pd.Series(dtype=float, index=pd.DatetimeIndex([]))
    )
    current_value = values.get(target, np.nan)
    comparisons = values.loc[
        (values.index >= target - pd.Timedelta(days=LOOKBACK_DAYS))
        & (values.index < target)
    ]
    previous_value = comparisons.iloc[-1] if len(comparisons) >= 1 else np.nan
    two_before_value = comparisons.iloc[-2] if len(comparisons) >= 2 else np.nan
    baseline_end = target - pd.Timedelta(days=BASELINE_LAG_DAYS)
    baseline_start = baseline_end - pd.Timedelta(days=BASELINE_DAYS - 1)
    baseline = values.loc[
        (values.index >= baseline_start) & (values.index <= baseline_end) & values.gt(0)
    ]
    baseline_mean = float(baseline.mean()) if not baseline.empty else np.nan
    has_pair = target in values.index and not comparisons.empty
    daily_evaluable = has_pair and len(values) >= 3 and baseline_mean > 0
    rising_evaluable = has_pair and len(comparisons) >= 2
    daily_alert = bool(
        daily_evaluable
        and _exceeds_rise(current_value, previous_value)
        and _exceeds_rise(current_value, baseline_mean)
    )
    rising_alert = bool(
        rising_evaluable
        and _exceeds_rise(current_value, previous_value)
        and _exceeds_rise(previous_value, two_before_value)
    )
    target_rows = series.loc[series["day"].eq(target)] if not series.empty else series
    return {
        "target_date": target.date().isoformat(),
        "target_density": current_value,
        "previous_date": comparisons.index[-1].date().isoformat() if len(comparisons) >= 1 else "",
        "previous_density": previous_value,
        "two_days_before_date": comparisons.index[-2].date().isoformat() if len(comparisons) >= 2 else "",
        "two_days_before_density": two_before_value,
        "baseline_density": baseline_mean,
        "target_sheet_qty": int(target_rows["sheet_qty"].iloc[0]) if not target_rows.empty else 0,
        "valid_days": len(values),
        "baseline_days": len(baseline),
        "daily_evaluable": bool(daily_evaluable),
        "rising_evaluable": bool(rising_evaluable),
        "daily_alert": daily_alert,
        "rising_alert": rising_alert,
        "is_alert": daily_alert or rising_alert,
    }


def evaluate_aoi_rs_trends(
    trend_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    *,
    reference_date: date,
) -> pd.DataFrame:
    """Evaluate yesterday, not the latest observed date, for every scoped indicator.

    P and Q are the latest two valid dates in [T-7, T), including real zero
    densities. Daily: D(T)>1.2*D(P) AND D(T)>1.2*positive_mean(T-6..T-2),
    with at least three valid observations in the calculation window. Rising:
    D(T)>1.2*D(P) AND D(P)>1.2*D(Q). Missing days never become zero; T is fixed.
    """
    if indicators_df.empty:
        return pd.DataFrame(columns=EVALUATION_COLUMNS)
    target = pd.Timestamp(reference_date - timedelta(days=1)).normalize()
    start = target - pd.Timedelta(days=HISTORY_LOOKBACK_DAYS)
    daily = pd.DataFrame(columns=[*INDICATOR_KEYS, "day", "value", "sheet_qty"])
    if not trend_df.empty:
        required = {*INDICATOR_KEYS, "period_type", "period_label", "value", "sheet_qty"}
        if not required.issubset(trend_df.columns):
            raise ValueError("AOI_RS trend input is missing required columns")
        daily = trend_df.loc[trend_df["period_type"].eq("day")].copy()
        daily["day"] = pd.to_datetime(daily["period_label"], errors="coerce").dt.normalize()
        daily = daily.loc[daily["day"].between(start, target)].copy()
        if daily.duplicated([*INDICATOR_KEYS, "day"]).any():
            raise ValueError("AOI_RS trend input has duplicate indicator dates")
        for column in ("value", "sheet_qty"):
            daily[column] = pd.to_numeric(daily[column], errors="coerce")
        daily = daily.loc[
            np.isfinite(daily["value"]) & daily["value"].ge(0)
            & np.isfinite(daily["sheet_qty"]) & daily["sheet_qty"].gt(0)
        ]
    groups = {
        tuple(str(value) for value in key): group
        for key, group in daily.groupby(INDICATOR_KEYS)
    }
    records = []
    for indicator in indicators_df.drop_duplicates(INDICATOR_KEYS).to_dict("records"):
        key = tuple(str(indicator[name]) for name in INDICATOR_KEYS)
        series = groups.get(key, daily.iloc[0:0])
        records.append({
            "prod_code": indicator.get("prod_code", ""),
            **{name: indicator[name] for name in INDICATOR_KEYS},
            **_evaluate_series(series, target),
        })
    return pd.DataFrame(records, columns=EVALUATION_COLUMNS)
