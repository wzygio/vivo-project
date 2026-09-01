from __future__ import annotations

import logging
from dataclasses import dataclass
from numbers import Integral, Real
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.shared_kernel.utils.excel_tools import (
    _is_missing_sheet_error,
    _read_encrypted_xlsx_via_com,
    replace_workbook_sheets,
)

logger = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class CpkDecorationResult:
    """Capability values computed from decorated points, optionally overridden by user-maintained corrections."""

    period_capability_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def get_cpk_decoration_path(product_dir: Path) -> Path:
    return product_dir / CPK_DECORATION_FILE_NAME


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


def load_capability_decoration(
    product_dir: Path,
    sheet_name: str | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
) -> pd.DataFrame:
    _validate_metric(metric)
    path = get_cpk_decoration_path(product_dir)
    if not path.exists():
        return _empty_decoration_frame(metric)
    try:
        if sheet_name is None:
            loaded_df = pd.read_excel(path, engine="openpyxl")
        else:
            try:
                loaded_df = pd.read_excel(path, sheet_name=sheet_name)
            except ValueError as excel_error:
                if _is_missing_sheet_error(excel_error):
                    # 指定 sheet 缺失 —— 与文件缺失语义一致
                    return _empty_decoration_frame(metric)
                raise
    except Exception as excel_exc:
        try:
            loaded_df = _read_encrypted_xlsx_via_com(path, sheet_name)
            logger.info(
                "[SPC] loaded enterprise-encrypted %s decoration file via Excel COM: %s",
                metric.upper(),
                path,
            )
        except Exception as com_exc:
            logger.warning(
                "[SPC] failed to read %s decoration file %s with openpyxl (%s) and Excel COM (%s)",
                metric.upper(),
                path,
                excel_exc,
                com_exc,
            )
            return _empty_decoration_frame(metric)
    if loaded_df.empty:
        return _empty_decoration_frame(metric)
    return _ordered_existing_columns(
        _normalize_key_columns(loaded_df),
        capability_decoration_columns(metric),
    )


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


def _decoration_sheet_exists(decoration_path: Path, sheet_name: str | None) -> bool:
    """Return True when the target decoration sheet already exists or cannot be inspected safely."""
    if not decoration_path.exists():
        return False
    if sheet_name is None:
        return True
    try:
        import openpyxl

        workbook = openpyxl.load_workbook(decoration_path, read_only=True)
        try:
            return sheet_name in workbook.sheetnames
        finally:
            workbook.close()
    except Exception:
        # 企业加密等 openpyxl 无法打开的工作簿按“已存在”处理，避免破坏用户文件
        logger.warning(
            "[SPC] unable to inspect decoration workbook sheets, treating %s as existing",
            decoration_path,
        )
        return True


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


def persist_capability_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    sheet_name: str | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
) -> pd.DataFrame:
    """Create the user-maintained decoration sheet when it does not exist yet."""
    _validate_metric(metric)
    product_dir.mkdir(parents=True, exist_ok=True)
    decoration_path = get_cpk_decoration_path(product_dir)
    target_sheet = sheet_name or "Sheet1"
    existing_decoration_df = load_capability_decoration(product_dir, sheet_name, metric)
    decoration_to_write = merge_capability_detail_with_decoration_flags(
        detail_df,
        existing_decoration_df,
        metric,
    )
    sheet_exists = _decoration_sheet_exists(decoration_path, sheet_name)
    persisted_df = decoration_to_write
    should_write = not sheet_exists
    if sheet_exists and not existing_decoration_df.empty:
        persisted_df = _append_missing_detail_rows(
            existing_decoration_df,
            decoration_to_write,
            metric,
        )
        should_write = len(persisted_df) > len(existing_decoration_df)

    if should_write:
        write_result = replace_workbook_sheets(
            decoration_path,
            {target_sheet: persisted_df},
        )
        if not write_result.written:
            logger.warning(
                "[SPC] failed to persist %s decoration sheet %s [%s]: %s",
                metric.upper(),
                decoration_path,
                target_sheet,
                write_result.error,
            )
    return decoration_to_write


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


def prepare_capability_decoration(
    period_capability_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
    sheet_name: str | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
) -> CpkDecorationResult:
    """Build chart-ready values selected by the user-maintained decoration sheet."""
    _validate_metric(metric)
    detail_df = build_capability_detail(period_capability_df, metric)
    decoration_df = (
        persist_capability_decoration(product_dir, detail_df, sheet_name, metric)
        if persist_files
        else merge_capability_detail_with_decoration_flags(
            detail_df,
            load_capability_decoration(product_dir, sheet_name, metric),
            metric,
        )
    )
    period_capability_df = apply_capability_decoration(
        period_capability_df,
        decoration_df,
        metric,
    )
    return CpkDecorationResult(
        period_capability_df=period_capability_df,
        decoration_df=decoration_df,
        decoration_path=get_cpk_decoration_path(product_dir),
        decoration_sheet=sheet_name or "Sheet1",
    )


# 兼容既有调用/测试的别名（默认 metric="cpk"）。
build_cpk_detail = build_capability_detail
load_cpk_decoration = load_capability_decoration
merge_detail_with_decoration_flags = merge_capability_detail_with_decoration_flags
persist_cpk_decoration = persist_capability_decoration
apply_cpk_decoration = apply_capability_decoration
prepare_cpk_decoration = prepare_capability_decoration
