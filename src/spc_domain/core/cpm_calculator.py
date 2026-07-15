import logging
import math
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PERIOD_SIGMA_SOURCE_SHEET_MEAN = "sheet_mean"
PERIOD_SIGMA_SOURCE_POINT_VALUE = "point_value"
PERIOD_SIGMA_SOURCE_OPTIONS = {
    PERIOD_SIGMA_SOURCE_SHEET_MEAN,
    PERIOD_SIGMA_SOURCE_POINT_VALUE,
}


def derive_lot_id(sheet_id: object) -> str:
    """Derive 9-character Lot ID from Sheet/Glass/Panel-like identifiers."""
    sheet_id_str = str(sheet_id).strip() if sheet_id is not None else ""
    if len(sheet_id_str) < 9:
        return ""
    return sheet_id_str[:9]


def calculate_cpm(
    mean_value: float,
    std_value: float,
    usl: float,
    lsl: float,
    target: Optional[float] = None,
) -> float:
    """Calculate Taguchi CPM for a two-sided specification."""
    values = [mean_value, std_value, usl, lsl]
    if any(pd.isna(value) for value in values):
        return float("nan")

    if usl <= lsl:
        return float("nan")

    resolved_target = target
    if resolved_target is None or pd.isna(resolved_target):
        resolved_target = (usl + lsl) / 2.0

    denominator = 6.0 * math.sqrt(float(std_value) ** 2 + (float(mean_value) - float(resolved_target)) ** 2)
    if denominator == 0:
        return float("inf")
    return float((usl - lsl) / denominator)


def calculate_cpk(mean_value: float, std_value: float, usl: float, lsl: float) -> float:
    """Calculate CPK from the nearest specification distance."""
    values = [mean_value, std_value, usl, lsl]
    if any(pd.isna(value) for value in values):
        return float("nan")

    if usl <= lsl or std_value < 0:
        return float("nan")

    nearest_distance = min(float(usl) - float(mean_value), float(mean_value) - float(lsl))
    denominator = 3.0 * float(std_value)
    if denominator == 0:
        if nearest_distance > 0:
            return float("inf")
        if nearest_distance == 0:
            return 0.0
        return float("-inf")
    return float(nearest_distance / denominator)


def _start_of_week(value: date) -> date:
    return value - timedelta(days=value.weekday())


def get_period_window_start(end_date: date) -> date:
    """Return the first day needed by the Task2 M/W/D report windows."""
    month = end_date.month - 1
    year = end_date.year
    if month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def build_period_axis(end_date: date) -> pd.DataFrame:
    """Build the fixed Task2 period axis: 2 months, 3 weeks, and 7 days."""
    end_date = pd.Timestamp(end_date).date()
    records: list[dict[str, object]] = []

    month_starts = [
        get_period_window_start(end_date),
        date(end_date.year, end_date.month, 1),
    ]
    for index, month_start in enumerate(month_starts, start=1):
        next_month = date(month_start.year + (month_start.month // 12), (month_start.month % 12) + 1, 1)
        month_end = min(end_date, next_month - timedelta(days=1))
        records.append(
            {
                "period_type": "month",
                "period_label": month_start.strftime("%Y-%m"),
                "period_sort": 100 + index,
                "period_start": pd.Timestamp(month_start),
                "period_end": pd.Timestamp(month_end),
            }
        )

    current_week_start = _start_of_week(end_date)
    for index, week_offset in enumerate([2, 1, 0], start=1):
        week_start = current_week_start - timedelta(weeks=week_offset)
        iso_week = week_start.isocalendar()
        records.append(
            {
                "period_type": "week",
                "period_label": f"{iso_week.year}-W{iso_week.week:02d}",
                "period_sort": 200 + index,
                "period_start": pd.Timestamp(week_start),
                "period_end": pd.Timestamp(min(end_date, week_start + timedelta(days=6))),
            }
        )

    for index, day_offset in enumerate(range(6, -1, -1), start=1):
        day = end_date - timedelta(days=day_offset)
        records.append(
            {
                "period_type": "day",
                "period_label": day.strftime("%Y-%m-%d"),
                "period_sort": 300 + index,
                "period_start": pd.Timestamp(day),
                "period_end": pd.Timestamp(day),
            }
        )

    return pd.DataFrame(records)


def build_available_period_axis(sheet_features: pd.DataFrame, end_date: date) -> pd.DataFrame:
    """Build a compact M/W/D axis from the latest periods that actually have Sheet data."""
    end_date = pd.Timestamp(end_date).date()
    if sheet_features.empty or "sheet_start_time" not in sheet_features.columns:
        return build_period_axis(end_date)

    df = sheet_features.copy()
    df["sheet_start_time"] = pd.to_datetime(df["sheet_start_time"], errors="coerce")
    df = df.dropna(subset=["sheet_start_time"]).copy()
    df = df[df["sheet_start_time"] < pd.Timestamp(end_date) + pd.Timedelta(days=1)].copy()
    if df.empty:
        return build_period_axis(end_date)

    records: list[dict[str, object]] = []

    month_periods = sorted(df["sheet_start_time"].dt.to_period("M").dropna().unique())[-2:]
    for index, month_period in enumerate(month_periods, start=1):
        month_start = month_period.to_timestamp().date()
        next_month = date(month_start.year + (month_start.month // 12), (month_start.month % 12) + 1, 1)
        month_end = min(end_date, next_month - timedelta(days=1))
        records.append(
            {
                "period_type": "month",
                "period_label": month_start.strftime("%Y-%m"),
                "period_sort": 100 + index,
                "period_start": pd.Timestamp(month_start),
                "period_end": pd.Timestamp(month_end),
            }
        )

    week_starts = sorted({ _start_of_week(ts.date()) for ts in df["sheet_start_time"] })[-3:]
    for index, week_start in enumerate(week_starts, start=1):
        iso_week = week_start.isocalendar()
        records.append(
            {
                "period_type": "week",
                "period_label": f"{iso_week.year}-W{iso_week.week:02d}",
                "period_sort": 200 + index,
                "period_start": pd.Timestamp(week_start),
                "period_end": pd.Timestamp(min(end_date, week_start + timedelta(days=6))),
            }
        )

    days = sorted({ ts.date() for ts in df["sheet_start_time"] })[-7:]
    for index, day in enumerate(days, start=1):
        records.append(
            {
                "period_type": "day",
                "period_label": day.strftime("%Y-%m-%d"),
                "period_sort": 300 + index,
                "period_start": pd.Timestamp(day),
                "period_end": pd.Timestamp(day),
            }
        )

    return pd.DataFrame(records)


def build_all_available_period_axis(sheet_features: pd.DataFrame, end_date: date) -> pd.DataFrame:
    """Build all M/W/D periods with data inside the active CPM query window."""
    end_date = pd.Timestamp(end_date).date()
    if sheet_features.empty or "sheet_start_time" not in sheet_features.columns:
        return build_period_axis(end_date)

    timestamps = pd.to_datetime(sheet_features["sheet_start_time"], errors="coerce").dropna()
    window_start = pd.Timestamp(get_period_window_start(end_date))
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
    timestamps = timestamps[(timestamps >= window_start) & (timestamps < end_ts)]
    if timestamps.empty:
        return build_period_axis(end_date)

    records: list[dict[str, object]] = []

    month_periods = sorted(timestamps.dt.to_period("M").unique())
    for index, month_period in enumerate(month_periods, start=1):
        month_start = month_period.to_timestamp().date()
        next_month = date(month_start.year + (month_start.month // 12), (month_start.month % 12) + 1, 1)
        month_end = min(end_date, next_month - timedelta(days=1))
        records.append(
            {
                "period_type": "month",
                "period_label": month_start.strftime("%Y-%m"),
                "period_sort": 100 + index,
                "period_start": pd.Timestamp(month_start),
                "period_end": pd.Timestamp(month_end),
            }
        )

    unique_days = timestamps.dt.normalize().drop_duplicates()
    week_starts = sorted({_start_of_week(ts.date()) for ts in unique_days})
    for index, week_start in enumerate(week_starts, start=1):
        iso_week = week_start.isocalendar()
        records.append(
            {
                "period_type": "week",
                "period_label": f"{iso_week.year}-W{iso_week.week:02d}",
                "period_sort": 200 + index,
                "period_start": pd.Timestamp(week_start),
                "period_end": pd.Timestamp(min(end_date, week_start + timedelta(days=6))),
            }
        )

    days = sorted({ts.date() for ts in unique_days})
    for index, day in enumerate(days, start=1):
        records.append(
            {
                "period_type": "day",
                "period_label": day.strftime("%Y-%m-%d"),
                "period_sort": 300 + index,
                "period_start": pd.Timestamp(day),
                "period_end": pd.Timestamp(day),
            }
        )

    return pd.DataFrame(records)


def _period_frame(df: pd.DataFrame, end_date: date) -> pd.DataFrame:
    period_df = df.copy()
    period_df["sheet_start_time"] = pd.to_datetime(period_df["sheet_start_time"], errors="coerce")
    period_df = period_df.dropna(subset=["sheet_start_time"]).copy()
    if period_df.empty:
        return period_df

    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
    period_axis = build_all_available_period_axis(period_df, end_date)
    period_sort_lookup = period_axis[["period_type", "period_label", "period_sort"]]
    month_start = period_axis[period_axis["period_type"] == "month"]["period_start"].min()
    week_start = period_axis[period_axis["period_type"] == "week"]["period_start"].min()
    day_start = period_axis[period_axis["period_type"] == "day"]["period_start"].min()

    frames: list[pd.DataFrame] = []
    month_df = period_df[(period_df["sheet_start_time"] >= month_start) & (period_df["sheet_start_time"] < end_ts)].copy()
    if not month_df.empty:
        month_df["period_type"] = "month"
        month_df["period_label"] = month_df["sheet_start_time"].dt.strftime("%Y-%m")
        frames.append(month_df)

    week_df = period_df[(period_df["sheet_start_time"] >= week_start) & (period_df["sheet_start_time"] < end_ts)].copy()
    if not week_df.empty:
        iso_week = week_df["sheet_start_time"].dt.isocalendar()
        week_df["period_type"] = "week"
        week_df["period_label"] = iso_week.year.astype(str) + "-W" + iso_week.week.astype(str).str.zfill(2)
        frames.append(week_df)

    day_df = period_df[(period_df["sheet_start_time"] >= day_start) & (period_df["sheet_start_time"] < end_ts)].copy()
    if not day_df.empty:
        day_df["period_type"] = "day"
        day_df["period_label"] = day_df["sheet_start_time"].dt.strftime("%Y-%m-%d")
        frames.append(day_df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).merge(period_sort_lookup, on=["period_type", "period_label"], how="inner")


def _build_period_measurement_stats(raw_measurements: pd.DataFrame | None, end_date: date) -> dict[tuple[object, ...], dict[str, float]]:
    """Return point-level sigma inputs keyed by indicator and M/W/D period."""
    required_cols = {
        "prod_code",
        "factory",
        "sheet_id",
        "step_id",
        "param_name",
        "sheet_start_time",
        "param_value",
    }
    if raw_measurements is None or raw_measurements.empty or not required_cols.issubset(raw_measurements.columns):
        return {}

    df = raw_measurements.copy()
    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df = df.dropna(subset=["param_value"]).copy()
    if df.empty:
        return {}

    period_df = _period_frame(df, end_date)
    if period_df.empty:
        return {}

    group_cols = ["prod_code", "factory", "step_id", "param_name", "period_type", "period_label"]
    aggregated = (
        period_df.groupby(group_cols, dropna=False, sort=True, as_index=False)
        .agg(
            point_count=("param_value", "count"),
            # Keep Series.std's exact floating-point behavior from the legacy
            # per-group implementation; pandas' native groupby std can round
            # near-constant values to zero and change CPK from finite to inf.
            std_value=("param_value", lambda values: float(values.std(ddof=1))),
        )
    )
    return {
        tuple(row[: len(group_cols)]): {
            "point_count": int(row.point_count),
            "std_value": float(row.std_value),
        }
        for row in aggregated.itertuples(index=False)
    }


def normalize_period_sigma_source(value: object) -> str:
    """Normalize the M/W/D capability sigma source, defaulting to Sheet Mean."""
    value_text = str(value).strip().lower() if value is not None else ""
    if value_text in PERIOD_SIGMA_SOURCE_OPTIONS:
        return value_text
    return PERIOD_SIGMA_SOURCE_SHEET_MEAN


def build_period_capability_report(
    sheet_features: pd.DataFrame,
    end_date: date,
    raw_measurements: pd.DataFrame | None = None,
    sigma_source: str = PERIOD_SIGMA_SOURCE_SHEET_MEAN,
) -> pd.DataFrame:
    """Aggregate M/W/D CPM and CPK rows with Sheet means and point-level sigma."""
    required_cols = {
        "prod_code",
        "factory",
        "sheet_id",
        "step_id",
        "param_name",
        "sheet_start_time",
        "sheet_mean",
        "usl",
        "lsl",
    }
    missing = required_cols - set(sheet_features.columns)
    if missing:
        logger.warning("[CPM] sheet_features missing required period columns: %s", sorted(missing))
        return pd.DataFrame()

    if sheet_features.empty:
        return pd.DataFrame()

    df = sheet_features.copy()
    if "target" not in df.columns:
        df["target"] = np.nan
    for col in ["ucl", "lcl"]:
        if col not in df.columns:
            df[col] = np.nan

    df = _period_frame(df, end_date)
    if df.empty:
        return pd.DataFrame()

    group_cols = ["prod_code", "factory", "step_id", "param_name", "period_type", "period_label", "period_sort"]
    resolved_sigma_source = normalize_period_sigma_source(sigma_source)
    measurement_stats = (
        _build_period_measurement_stats(raw_measurements, end_date)
        if resolved_sigma_source == PERIOD_SIGMA_SOURCE_POINT_VALUE
        else {}
    )
    valid_df = df.dropna(subset=["sheet_mean", "usl", "lsl"])
    if valid_df.empty:
        return pd.DataFrame()

    result = (
        valid_df.groupby(group_cols, dropna=False, sort=True, as_index=False)
        .agg(
            period_start=("sheet_start_time", "min"),
            period_end=("sheet_start_time", "max"),
            sample_count=("sheet_id", "nunique"),
            # These call the same Series reducers as the former Python loop.
            # Native groupby reducers use a different floating-point order.
            mean_value=("sheet_mean", lambda values: float(values.mean())),
            sheet_std_value=("sheet_mean", lambda values: float(values.std(ddof=1))),
            usl=("usl", "first"),
            lsl=("lsl", "first"),
            ucl=("ucl", "first"),
            lcl=("lcl", "first"),
            target=("target", "first"),
        )
    )

    stats_cols = ["prod_code", "factory", "step_id", "param_name", "period_type", "period_label"]
    stats_rows = [
        measurement_stats.get(tuple(keys), {})
        for keys in result[stats_cols].itertuples(index=False, name=None)
    ]
    use_point_sigma = [
        resolved_sigma_source == PERIOD_SIGMA_SOURCE_POINT_VALUE and "std_value" in stats
        for stats in stats_rows
    ]

    result["point_count"] = [
        int(stats["point_count"]) if int(stats.get("point_count", 0)) > 0 else np.nan
        for stats in stats_rows
    ]
    result["sigma_source"] = [
        PERIOD_SIGMA_SOURCE_POINT_VALUE if use_point else PERIOD_SIGMA_SOURCE_SHEET_MEAN
        for use_point in use_point_sigma
    ]
    result["std_value"] = [
        float(stats["std_value"]) if use_point else float(sheet_std)
        for stats, use_point, sheet_std in zip(
            stats_rows,
            use_point_sigma,
            result["sheet_std_value"],
            strict=True,
        )
    ]
    result["usl"] = [float(value) for value in result["usl"]]
    result["lsl"] = [float(value) for value in result["lsl"]]
    result["ucl"] = [float(value) if pd.notna(value) else np.nan for value in result["ucl"]]
    result["lcl"] = [float(value) if pd.notna(value) else np.nan for value in result["lcl"]]
    result["target"] = [
        float(target) if pd.notna(target) else (usl + lsl) / 2.0
        for target, usl, lsl in result[["target", "usl", "lsl"]].itertuples(index=False, name=None)
    ]
    result["cpm"] = [
        calculate_cpm(mean_value, std_value, usl, lsl, target)
        for mean_value, std_value, usl, lsl, target in result[
            ["mean_value", "std_value", "usl", "lsl", "target"]
        ].itertuples(index=False, name=None)
    ]
    result["cpk"] = [
        calculate_cpk(mean_value, std_value, usl, lsl)
        for mean_value, std_value, usl, lsl in result[
            ["mean_value", "std_value", "usl", "lsl"]
        ].itertuples(index=False, name=None)
    ]
    result = result.drop(columns=["sheet_std_value"])
    result = result[
        [
            "prod_code",
            "factory",
            "step_id",
            "param_name",
            "period_type",
            "period_label",
            "period_sort",
            "period_start",
            "period_end",
            "sample_count",
            "point_count",
            "sigma_source",
            "mean_value",
            "std_value",
            "usl",
            "lsl",
            "ucl",
            "lcl",
            "target",
            "cpm",
            "cpk",
        ]
    ]
    if result.empty:
        return result
    return result.sort_values(["factory", "step_id", "param_name", "period_sort"]).reset_index(drop=True)


def build_lot_cpm_report(sheet_features: pd.DataFrame, min_sheet_count: int = 2) -> pd.DataFrame:
    """Aggregate Sheet-level SPC features into Lot-level CPM by monitoring indicator."""
    required_cols = {
        "prod_code",
        "factory",
        "sheet_id",
        "step_id",
        "param_name",
        "sheet_mean",
        "usl",
        "lsl",
    }
    missing = required_cols - set(sheet_features.columns)
    if missing:
        logger.warning("[CPM] sheet_features missing required columns: %s", sorted(missing))
        return pd.DataFrame()

    if sheet_features.empty:
        return pd.DataFrame()

    df = sheet_features.copy()
    df["lot_id"] = df["sheet_id"].apply(derive_lot_id)
    df = df[df["lot_id"] != ""].copy()
    if df.empty:
        return pd.DataFrame()

    if "target" not in df.columns:
        df["target"] = np.nan

    group_cols = ["prod_code", "factory", "lot_id", "step_id", "param_name"]
    records: list[dict[str, object]] = []

    for keys, group in df.groupby(group_cols, dropna=False, sort=True):
        valid = group.dropna(subset=["sheet_mean", "usl", "lsl"])
        if len(valid) < min_sheet_count:
            continue

        prod_code, factory, lot_id, step_id, param_name = keys
        lot_mean = float(valid["sheet_mean"].mean())
        lot_std = float(valid["sheet_mean"].std(ddof=1))
        usl = float(valid["usl"].iloc[0])
        lsl = float(valid["lsl"].iloc[0])
        target_value = valid["target"].dropna().iloc[0] if valid["target"].notna().any() else np.nan
        cpm = calculate_cpm(
            mean_value=lot_mean,
            std_value=lot_std,
            usl=usl,
            lsl=lsl,
            target=float(target_value) if pd.notna(target_value) else None,
        )
        cpk = calculate_cpk(
            mean_value=lot_mean,
            std_value=lot_std,
            usl=usl,
            lsl=lsl,
        )

        records.append(
            {
                "prod_code": prod_code,
                "factory": factory,
                "lot_id": lot_id,
                "step_id": step_id,
                "param_name": param_name,
                "sheet_count": int(valid["sheet_id"].nunique()),
                "lot_mean": lot_mean,
                "lot_std": lot_std,
                "usl": usl,
                "lsl": lsl,
                "target": float(target_value) if pd.notna(target_value) else (usl + lsl) / 2.0,
                "cpm": cpm,
                "cpk": cpk,
                "first_sheet_time": pd.to_datetime(valid["sheet_start_time"], errors="coerce").min()
                if "sheet_start_time" in valid.columns
                else pd.NaT,
                "last_sheet_time": pd.to_datetime(valid["sheet_start_time"], errors="coerce").max()
                if "sheet_start_time" in valid.columns
                else pd.NaT,
            }
        )

    result = pd.DataFrame(records)
    if result.empty:
        return result
    return result.sort_values(["step_id", "param_name", "last_sheet_time", "lot_id"]).reset_index(drop=True)
