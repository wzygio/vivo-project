from __future__ import annotations

from numbers import Integral, Real
from typing import Iterable

import pandas as pd

CPK_DECORATION_FILE_NAME = "spc_cpk_cpm_decoration.xlsx"
CPK_KEY_COLUMNS = [
    "prod_code",
    "factory",
    "step_id",
    "param_name",
    "period_type",
    "period_label",
]

CAPABILITY_METRIC_CPK = "cpk"
CAPABILITY_METRIC_CPM = "cpm"
SUPPORTED_CAPABILITY_METRICS = (CAPABILITY_METRIC_CPK, CAPABILITY_METRIC_CPM)
CPM_DECORATION_SHEET_SUFFIX = "_cpm"


def _validate_metric(metric: str) -> str:
    if metric not in SUPPORTED_CAPABILITY_METRICS:
        raise ValueError(f"unsupported capability metric: {metric!r}")
    return metric


def capability_corrected_column(metric: str) -> str:
    return f"{_validate_metric(metric)}_corrected"


def capability_decorated_column(metric: str) -> str:
    return f"{_validate_metric(metric)}_decorated"


def capability_detail_columns(metric: str) -> list[str]:
    return [
        *CPK_KEY_COLUMNS,
        "period_sort",
        "period_start",
        "period_end",
        capability_corrected_column(metric),
    ]


def capability_decoration_columns(metric: str) -> list[str]:
    return [*capability_detail_columns(metric), "flag"]


CPK_DETAIL_COLUMNS = capability_detail_columns(CAPABILITY_METRIC_CPK)
CPK_DECORATION_COLUMNS = capability_decoration_columns(CAPABILITY_METRIC_CPK)


def resolve_capability_decoration_sheet(prod_code: str, metric: str) -> str:
    """Return the user-maintained sheet name for a product and metric.

    CPK 沿用既有产品 sheet 名（保留历史人工维护数据）；
    CPM 使用 ``{prod_code}_cpm``，与 CPK 表共存于同一工作簿。
    """
    if _validate_metric(metric) == CAPABILITY_METRIC_CPM:
        return f"{prod_code}{CPM_DECORATION_SHEET_SUFFIX}"
    return prod_code


def _empty_detail_frame(metric: str) -> pd.DataFrame:
    return pd.DataFrame(columns=capability_detail_columns(metric))


def _empty_decoration_frame(metric: str) -> pd.DataFrame:
    return pd.DataFrame(columns=capability_decoration_columns(metric))


def _ordered_existing_columns(df: pd.DataFrame, ordered_columns: Iterable[str]) -> pd.DataFrame:
    result = df.copy()
    for column in ordered_columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result[list(ordered_columns)].copy()


def _normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in CPK_KEY_COLUMNS:
        if column in result.columns:
            result[column] = result[column].map(_normalize_key_value)
    return result


def _normalize_key_value(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, Integral):
        return str(int(value))
    if isinstance(value, Real) and float(value).is_integer():
        return str(int(value))
    return str(value).strip()


def _parse_flag(value: object) -> bool:
    """Capability decoration is opt-in: blank or invalid values are always False."""
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {
        "true",
        "ture",  # Backward-compatible with existing manually maintained workbooks.
        "1",
        "yes",
        "y",
        "是",
        "修饰",
    }


def build_capability_detail(
    period_capability_df: pd.DataFrame,
    metric: str = CAPABILITY_METRIC_CPK,
) -> pd.DataFrame:
    """List each period's metric value computed from decorated points as the user-editable value."""
    _validate_metric(metric)
    required_columns = {*CPK_KEY_COLUMNS, metric}
    if period_capability_df.empty or not required_columns.issubset(period_capability_df.columns):
        return _empty_detail_frame(metric)

    capability_df = _normalize_key_columns(period_capability_df)
    detail = _ordered_existing_columns(
        capability_df,
        [*CPK_KEY_COLUMNS, "period_sort", "period_start", "period_end", metric],
    ).rename(columns={metric: capability_corrected_column(metric)})
    return _ordered_existing_columns(detail, capability_detail_columns(metric)).sort_values(
        ["factory", "step_id", "param_name", "period_sort"],
        kind="stable",
    ).reset_index(drop=True)


def merge_capability_detail_with_decoration_flags(
    detail_df: pd.DataFrame,
    existing_decoration_df: pd.DataFrame,
    metric: str = CAPABILITY_METRIC_CPK,
) -> pd.DataFrame:
    """Attach user-maintained corrected values and opt-in flags to current details."""
    _validate_metric(metric)
    detail_columns = capability_detail_columns(metric)
    decoration_columns = capability_decoration_columns(metric)
    corrected_column = capability_corrected_column(metric)
    if detail_df.empty:
        return _empty_decoration_frame(metric)

    detail_df = _normalize_key_columns(_ordered_existing_columns(detail_df, detail_columns))
    if existing_decoration_df.empty or "flag" not in existing_decoration_df.columns:
        result = detail_df.copy()
        result["flag"] = False
        return result[decoration_columns]

    user_values_df = _normalize_key_columns(
        _ordered_existing_columns(existing_decoration_df, decoration_columns)
    )[[*CPK_KEY_COLUMNS, corrected_column, "flag"]].copy()
    user_values_df = user_values_df.rename(
        columns={corrected_column: "_user_corrected_value"}
    )
    user_values_df["flag"] = user_values_df["flag"].apply(_parse_flag)
    user_values_df = user_values_df.drop_duplicates(CPK_KEY_COLUMNS, keep="last")
    result = detail_df.merge(user_values_df, on=CPK_KEY_COLUMNS, how="left")
    user_corrected_values = pd.to_numeric(result["_user_corrected_value"], errors="coerce")
    result.loc[user_corrected_values.notna(), corrected_column] = user_corrected_values[
        user_corrected_values.notna()
    ]
    result["flag"] = result["flag"].apply(_parse_flag)
    return result.drop(columns=["_user_corrected_value"])[decoration_columns]


def _append_missing_detail_rows(
    existing_decoration_df: pd.DataFrame,
    current_decoration_df: pd.DataFrame,
    metric: str,
) -> pd.DataFrame:
    """Append current keys absent from the user ledger without changing prior decisions."""
    decoration_columns = capability_decoration_columns(metric)
    existing_df = _normalize_key_columns(
        _ordered_existing_columns(existing_decoration_df, decoration_columns)
    )
    current_df = _normalize_key_columns(
        _ordered_existing_columns(current_decoration_df, decoration_columns)
    )
    if existing_df.empty or current_df.empty:
        return existing_df

    existing_keys = pd.MultiIndex.from_frame(existing_df[CPK_KEY_COLUMNS])
    current_keys = pd.MultiIndex.from_frame(current_df[CPK_KEY_COLUMNS])
    missing_rows = current_df.loc[~current_keys.isin(existing_keys)].drop_duplicates(
        CPK_KEY_COLUMNS,
        keep="last",
    )
    if missing_rows.empty:
        return existing_df
    return pd.concat([existing_df, missing_rows], ignore_index=True)[decoration_columns]


def apply_capability_decoration(
    period_capability_df: pd.DataFrame,
    decoration_df: pd.DataFrame | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
) -> pd.DataFrame:
    """Keep the computed metric value by default and apply only admin-enabled user corrections."""
    _validate_metric(metric)
    corrected_column = capability_corrected_column(metric)
    decorated_column = capability_decorated_column(metric)
    result = period_capability_df.copy()
    if result.empty:
        result[decorated_column] = pd.Series(dtype="bool")
        return result

    detail_df = build_capability_detail(period_capability_df, metric)
    flags_df = merge_capability_detail_with_decoration_flags(
        detail_df,
        decoration_df if decoration_df is not None else _empty_decoration_frame(metric),
        metric,
    )
    if flags_df.empty:
        result[decorated_column] = False
        return result

    selected_df = flags_df[[*CPK_KEY_COLUMNS, corrected_column, "flag"]].copy()
    selected_df = selected_df.rename(columns={"flag": decorated_column})
    result = _normalize_key_columns(result).merge(selected_df, on=CPK_KEY_COLUMNS, how="left")
    result[decorated_column] = result[decorated_column].apply(_parse_flag)
    corrected_values = pd.to_numeric(result[corrected_column], errors="coerce")
    decorated_mask = result[decorated_column] & corrected_values.notna()
    result.loc[decorated_mask, metric] = corrected_values[decorated_mask]
    return result.drop(columns=[corrected_column])


# 兼容既有调用/测试的别名（默认 metric="cpk"）。
build_cpk_detail = build_capability_detail
merge_detail_with_decoration_flags = merge_capability_detail_with_decoration_flags
apply_cpk_decoration = apply_capability_decoration
