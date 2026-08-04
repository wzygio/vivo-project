"""Manual period overrides and daily reconstruction for MWD trends."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .allocation import allocate_integer_counts


def _normalise_period_key(value: object, period_type: str) -> str | None:
    text = str(value).strip().upper()
    try:
        if period_type == "monthly":
            year, month = text.replace("月", "").split("-", maxsplit=1)
            return f"{int(year):04d}-{int(month):02d}"
        year, week = text.split("-W", maxsplit=1)
        return f"{int(year):04d}-W{int(week):02d}"
    except (TypeError, ValueError):
        return None


def _period_keys(values: pd.Series, period_type: str) -> pd.Series:
    dates = pd.to_datetime(values)
    if period_type == "monthly":
        return dates.dt.strftime("%Y-%m")
    iso = dates.dt.isocalendar()
    return iso.year.astype(str) + "-W" + iso.week.map("{:02d}".format)


def _safe_rate(value: object) -> float | None:
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(rate):
        return None
    return max(0.0, rate)


def _normalise_period_freq(period_freq: str) -> str:
    return "W-SUN" if period_freq.upper() in {"W", "W-SUN"} else period_freq


def apply_group_period_overrides(
    df: pd.DataFrame,
    overrides: dict,
    period_type: str,
    target_defects: list[str],
) -> pd.DataFrame:
    """Apply Group rates to an already aggregated wide table."""
    if not overrides or df.empty:
        return df.copy()

    result = df.copy()
    dates = pd.to_datetime(result.index)
    keys = _period_keys(pd.Series(dates), period_type).to_numpy()

    for group in target_defects:
        if group not in result.columns or not isinstance(overrides.get(group), dict):
            continue
        for configured_period, configured_rate in overrides[group].items():
            period_key = _normalise_period_key(configured_period, period_type)
            rate = _safe_rate(configured_rate)
            if period_key is None or rate is None:
                continue
            mask = keys == period_key
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"], errors="coerce"
            ).fillna(0).clip(lower=0).to_numpy(dtype=float)
            weights = pd.to_numeric(
                result.loc[mask, group], errors="coerce"
            ).fillna(0).clip(lower=0).to_numpy(dtype=float)
            target_total = int(np.round(rate * capacities.sum()))
            result.loc[mask, group] = allocate_integer_counts(
                weights,
                capacities,
                target_total,
            )
            logging.info(
                "[Group override] %s @ %s = %s",
                group,
                period_key,
                rate,
            )

    return result


def apply_group_daily_overrides(
    df: pd.DataFrame,
    overrides: dict,
    target_defects: list[str],
) -> pd.DataFrame:
    """Apply exact daily Group rates after period reconstruction."""
    if not overrides or df.empty:
        return df.copy()

    result = df.copy()
    dates = pd.to_datetime(result.index).normalize()
    for group in target_defects:
        if group not in result.columns or not isinstance(overrides.get(group), dict):
            continue
        for configured_day, configured_rate in overrides[group].items():
            rate = _safe_rate(configured_rate)
            if rate is None:
                continue
            try:
                target_day = pd.to_datetime(configured_day).normalize()
            except (TypeError, ValueError):
                continue
            mask = dates == target_day
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"], errors="coerce"
            ).fillna(0).clip(lower=0).to_numpy(dtype=float)
            result.loc[mask, group] = np.minimum(
                np.round(rate * capacities).astype(int),
                capacities.astype(int),
            )
    return result


def apply_code_period_overrides(
    df: pd.DataFrame,
    overrides: dict,
    period_type: str,
) -> pd.DataFrame:
    """Apply Code rates to a long-period table."""
    if not overrides or df.empty:
        return df.copy()

    result = df.copy()
    result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])
    keys = _period_keys(result["warehousing_time"], period_type)

    for code, configured_periods in overrides.items():
        if code not in set(result["defect_desc"].dropna()) or not isinstance(
            configured_periods, dict
        ):
            continue
        code_mask = result["defect_desc"] == code
        for configured_period, configured_rate in configured_periods.items():
            period_key = _normalise_period_key(configured_period, period_type)
            rate = _safe_rate(configured_rate)
            if period_key is None or rate is None:
                continue
            mask = code_mask & (keys == period_key)
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"], errors="coerce"
            ).fillna(0).clip(lower=0).to_numpy(dtype=float)
            weights = pd.to_numeric(
                result.loc[mask, "defect_panel_count"], errors="coerce"
            ).fillna(0).clip(lower=0).to_numpy(dtype=float)
            result.loc[mask, "defect_panel_count"] = allocate_integer_counts(
                weights,
                capacities,
                int(np.round(rate * capacities.sum())),
            )
    result["defect_panel_count"] = pd.to_numeric(
        result["defect_panel_count"], errors="coerce"
    ).fillna(0).clip(lower=0).astype(int)
    return result


def apply_code_daily_overrides(df: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """Apply exact daily Code rates after period reconstruction."""
    if not overrides or df.empty:
        return df.copy()

    result = df.copy()
    result["warehousing_time"] = pd.to_datetime(result["warehousing_time"])
    for code, configured_days in overrides.items():
        if not isinstance(configured_days, dict):
            continue
        for configured_day, configured_rate in configured_days.items():
            rate = _safe_rate(configured_rate)
            if rate is None:
                continue
            try:
                target_day = pd.to_datetime(configured_day).normalize()
            except (TypeError, ValueError):
                continue
            mask = (result["defect_desc"] == code) & (
                result["warehousing_time"].dt.normalize() == target_day
            )
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"], errors="coerce"
            ).fillna(0).clip(lower=0).astype(int)
            result.loc[mask, "defect_panel_count"] = np.minimum(
                np.round(rate * capacities).astype(int),
                capacities,
            )
    return result


def _stable_noise(index: pd.Index, key: str, volatility: float, scramble: int) -> np.ndarray:
    timestamps = np.array([int(pd.Timestamp(value).timestamp() / 86400) for value in index])
    stable_key = sum(ord(char) for char in str(key)) % 9999
    return np.sin(timestamps * scramble + stable_key) * volatility


def generate_group_daily_from_period_baseline(
    daily_skeleton: pd.DataFrame,
    period_final: pd.DataFrame,
    target_defects: list[str],
    volatility: float,
    period_freq: str,
) -> pd.DataFrame:
    """Allocate Group period totals back to daily rows."""
    period_freq = _normalise_period_freq(period_freq)
    result = daily_skeleton.copy()
    if result.empty or period_final.empty:
        return result

    result.index = pd.to_datetime(result.index)
    daily_periods = result.index.to_period(period_freq)
    target = period_final.copy()
    target.index = pd.to_datetime(target.index).to_period(period_freq)

    for group in target_defects:
        if group not in target.columns:
            continue
        result[group] = 0
        for period, target_row in target.groupby(level=0, sort=False):
            mask = daily_periods == period
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"], errors="coerce"
            ).fillna(0).to_numpy(dtype=float)
            weights = capacities * (
                1 + _stable_noise(result.index[mask], group, volatility, 1_234_567)
            )
            result.loc[mask, group] = allocate_integer_counts(
                weights,
                capacities,
                int(max(0, target_row[group].iloc[0] if isinstance(target_row[group], pd.Series) else target_row[group])),
            )
    return result


def generate_code_daily_from_period_baseline(
    daily_skeleton: pd.DataFrame,
    period_final: pd.DataFrame,
    volatility: float,
    period_freq: str,
) -> pd.DataFrame:
    """Allocate Code period totals back to a daily x Code grid."""
    period_freq = _normalise_period_freq(period_freq)
    columns = [
        "warehousing_time",
        "total_panels",
        "defect_group",
        "defect_desc",
        "defect_panel_count",
    ]
    if daily_skeleton.empty or period_final.empty:
        return pd.DataFrame(columns=columns)

    target = period_final.copy()
    target["warehousing_time"] = pd.to_datetime(target["warehousing_time"])
    target["period"] = target["warehousing_time"].dt.to_period(period_freq)
    target = target.drop_duplicates(["period", "defect_desc"])

    skeleton = daily_skeleton.copy()
    skeleton["warehousing_time"] = pd.to_datetime(skeleton["warehousing_time"])
    skeleton["period"] = skeleton["warehousing_time"].dt.to_period(period_freq)
    codes = target[["defect_group", "defect_desc"]].drop_duplicates()
    grid = skeleton.assign(_join_key=1).merge(
        codes.assign(_join_key=1), on="_join_key", how="inner"
    ).drop(columns="_join_key")
    grid = grid.merge(
        target[["period", "defect_desc", "defect_panel_count"]].rename(
            columns={"defect_panel_count": "target_count"}
        ),
        on=["period", "defect_desc"],
        how="left",
    )
    grid["target_count"] = grid["target_count"].fillna(0).clip(lower=0)
    result_parts: list[pd.DataFrame] = []

    for (period, code), rows in grid.groupby(["period", "defect_desc"], sort=False):
        capacities = pd.to_numeric(rows["total_panels"], errors="coerce").fillna(0).to_numpy(dtype=float)
        weights = capacities * (
            1 + _stable_noise(rows["warehousing_time"], str(code), volatility, 999_983)
        )
        allocated = allocate_integer_counts(
            weights,
            capacities,
            int(rows["target_count"].iloc[0]),
        )
        part = rows[["warehousing_time", "total_panels", "defect_group", "defect_desc"]].copy()
        part["defect_panel_count"] = allocated
        result_parts.append(part)

    return pd.concat(result_parts, ignore_index=True)[columns]


def rebuild_group_daily_from_weekly(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    weekly_values: dict,
    target_defects: list[str],
    volatility: float,
) -> pd.DataFrame:
    """Rebuild only weekly-overridden Groups; monthly overrides never enter here."""
    if not weekly_values or daily_df.empty:
        return daily_df.copy()

    overridden = [group for group in weekly_values if group in target_defects]
    if not overridden:
        return daily_df.copy()

    generated = generate_group_daily_from_period_baseline(
        daily_df[["total_panels"]],
        weekly_df,
        overridden,
        volatility,
        period_freq="W-SUN",
    )
    result = daily_df.copy()
    for group in overridden:
        if group in generated.columns:
            result[group] = generated[group]
    return result


def rebuild_code_daily_from_weekly(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    weekly_values: dict,
    volatility: float,
) -> pd.DataFrame:
    """Rebuild only weekly-overridden Codes; monthly overrides never enter here."""
    if not weekly_values or daily_df.empty:
        return daily_df.copy()

    overridden = set(weekly_values)
    targets = weekly_df[weekly_df["defect_desc"].isin(overridden)].copy()
    if targets.empty:
        return daily_df.copy()

    # daily_df 是 (date, group, code) 长表，total_panels 在每天的所有 code 行上重复。
    # 骨架必须按天去重，否则后续 skeleton × codes 的笛卡尔积会把每个 (day, code)
    # 复制成 N 行，分配结果被拆成大量 count=1 的碎行，前端堆叠后出现白条/粗体标签。
    daily_skeleton = daily_df[["warehousing_time", "total_panels"]].drop_duplicates()
    generated = generate_code_daily_from_period_baseline(
        daily_skeleton,
        targets,
        volatility,
        period_freq="W-SUN",
    )
    preserved = daily_df[~daily_df["defect_desc"].isin(overridden)].copy()
    return pd.concat([preserved, generated], ignore_index=True)


def apply_group_manual_overrides_to_daily(
    daily_df: pd.DataFrame,
    monthly_values: dict,
    weekly_values: dict,
    daily_values: dict,
    target_defects: list[str],
) -> pd.DataFrame:
    """Compatibility helper: rebuild weekly and daily overrides only.

    Monthly overrides intentionally affect only the final monthly table and are
    therefore ignored by this daily-stage helper.
    """
    result = daily_df.copy()
    if weekly_values:
        result = _apply_group_weekly_rates_to_daily(
            result,
            weekly_values,
            target_defects,
        )
    return apply_group_daily_overrides(result, daily_values, target_defects)


def _apply_group_weekly_rates_to_daily(
    daily_df: pd.DataFrame,
    weekly_values: dict,
    target_defects: list[str],
) -> pd.DataFrame:
    result = daily_df.copy()
    dates = pd.Series(pd.to_datetime(result.index), index=result.index)
    period_keys = _period_keys(dates, "weekly")
    for group in target_defects:
        if group not in result.columns or not isinstance(weekly_values.get(group), dict):
            continue
        for configured_period, configured_rate in weekly_values[group].items():
            period_key = _normalise_period_key(configured_period, "weekly")
            rate = _safe_rate(configured_rate)
            if period_key is None or rate is None:
                continue
            mask = period_keys == period_key
            if not mask.any():
                continue
            capacities = pd.to_numeric(
                result.loc[mask, "total_panels"], errors="coerce"
            ).fillna(0).to_numpy(dtype=float)
            weights = pd.to_numeric(
                result.loc[mask, group], errors="coerce"
            ).fillna(0).to_numpy(dtype=float)
            result.loc[mask, group] = allocate_integer_counts(
                weights,
                capacities,
                int(np.round(rate * capacities.sum())),
            )
    return result


def apply_code_manual_overrides_to_daily(
    daily_df: pd.DataFrame,
    monthly_values: dict,
    weekly_values: dict,
    daily_values: dict,
) -> pd.DataFrame:
    """Legacy direct daily helper with monthly -> weekly -> daily precedence."""
    result = apply_code_period_overrides(daily_df, monthly_values, "monthly")
    result = apply_code_period_overrides(result, weekly_values, "weekly")
    return apply_code_daily_overrides(result, daily_values)
