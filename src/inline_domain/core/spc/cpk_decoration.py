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

CPK_DECORATION_FILE_NAME = "spc_cpk_decoration.xlsx"
CPK_KEY_COLUMNS = [
    "prod_code",
    "factory",
    "step_id",
    "param_name",
    "period_type",
    "period_label",
]
CPK_DETAIL_COLUMNS = [
    *CPK_KEY_COLUMNS,
    "period_sort",
    "period_start",
    "period_end",
    "cpk_corrected",
]
CPK_DECORATION_COLUMNS = [*CPK_DETAIL_COLUMNS, "flag"]


@dataclass(frozen=True)
class CpkDecorationResult:
    """CPK values computed from decorated points, optionally overridden by user-maintained corrections."""

    period_capability_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def get_cpk_decoration_path(product_dir: Path) -> Path:
    return product_dir / CPK_DECORATION_FILE_NAME


def _empty_detail_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CPK_DETAIL_COLUMNS)


def _empty_decoration_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CPK_DECORATION_COLUMNS)


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
    """CPK decoration is opt-in: blank or invalid values are always False."""
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


def build_cpk_detail(period_capability_df: pd.DataFrame) -> pd.DataFrame:
    """List each period's CPK computed from decorated points as the user-editable value."""
    required_columns = {*CPK_KEY_COLUMNS, "cpk"}
    if period_capability_df.empty or not required_columns.issubset(period_capability_df.columns):
        return _empty_detail_frame()

    capability_df = _normalize_key_columns(period_capability_df)
    detail = _ordered_existing_columns(
        capability_df,
        [*CPK_KEY_COLUMNS, "period_sort", "period_start", "period_end", "cpk"],
    ).rename(columns={"cpk": "cpk_corrected"})
    return _ordered_existing_columns(detail, CPK_DETAIL_COLUMNS).sort_values(
        ["factory", "step_id", "param_name", "period_sort"],
        kind="stable",
    ).reset_index(drop=True)


def load_cpk_decoration(product_dir: Path, sheet_name: str | None = None) -> pd.DataFrame:
    path = get_cpk_decoration_path(product_dir)
    if not path.exists():
        return _empty_decoration_frame()
    try:
        if sheet_name is None:
            loaded_df = pd.read_excel(path, engine="openpyxl")
        else:
            try:
                loaded_df = pd.read_excel(path, sheet_name=sheet_name)
            except ValueError as excel_error:
                if _is_missing_sheet_error(excel_error):
                    # 指定 sheet 缺失 —— 与文件缺失语义一致
                    return _empty_decoration_frame()
                raise
    except Exception as excel_exc:
        try:
            loaded_df = _read_encrypted_xlsx_via_com(path, sheet_name)
            logger.info("[SPC] loaded enterprise-encrypted CPK decoration file via Excel COM: %s", path)
        except Exception as com_exc:
            logger.warning(
                "[SPC] failed to read CPK decoration file %s with openpyxl (%s) and Excel COM (%s)",
                path,
                excel_exc,
                com_exc,
            )
            return _empty_decoration_frame()
    if loaded_df.empty:
        return _empty_decoration_frame()
    return _ordered_existing_columns(_normalize_key_columns(loaded_df), CPK_DECORATION_COLUMNS)


def merge_detail_with_decoration_flags(detail_df: pd.DataFrame, existing_decoration_df: pd.DataFrame) -> pd.DataFrame:
    """Attach user-maintained corrected values and opt-in flags to current details."""
    if detail_df.empty:
        return _empty_decoration_frame()

    detail_df = _normalize_key_columns(_ordered_existing_columns(detail_df, CPK_DETAIL_COLUMNS))
    if existing_decoration_df.empty or "flag" not in existing_decoration_df.columns:
        result = detail_df.copy()
        result["flag"] = False
        return result[CPK_DECORATION_COLUMNS]

    user_values_df = _normalize_key_columns(
        _ordered_existing_columns(existing_decoration_df, CPK_DECORATION_COLUMNS)
    )[[*CPK_KEY_COLUMNS, "cpk_corrected", "flag"]].copy()
    user_values_df = user_values_df.rename(
        columns={"cpk_corrected": "_user_cpk_corrected"}
    )
    user_values_df["flag"] = user_values_df["flag"].apply(_parse_flag)
    user_values_df = user_values_df.drop_duplicates(CPK_KEY_COLUMNS, keep="last")
    result = detail_df.merge(user_values_df, on=CPK_KEY_COLUMNS, how="left")
    user_corrected_values = pd.to_numeric(result["_user_cpk_corrected"], errors="coerce")
    result.loc[user_corrected_values.notna(), "cpk_corrected"] = user_corrected_values[
        user_corrected_values.notna()
    ]
    result["flag"] = result["flag"].apply(_parse_flag)
    return result.drop(columns=["_user_cpk_corrected"])[CPK_DECORATION_COLUMNS]


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
            "[SPC] unable to inspect CPK decoration workbook sheets, treating %s as existing",
            decoration_path,
        )
        return True


def _append_missing_detail_rows(
    existing_decoration_df: pd.DataFrame,
    current_decoration_df: pd.DataFrame,
) -> pd.DataFrame:
    """Append current keys absent from the user ledger without changing prior decisions."""
    existing_df = _normalize_key_columns(
        _ordered_existing_columns(existing_decoration_df, CPK_DECORATION_COLUMNS)
    )
    current_df = _normalize_key_columns(
        _ordered_existing_columns(current_decoration_df, CPK_DECORATION_COLUMNS)
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
    return pd.concat([existing_df, missing_rows], ignore_index=True)[CPK_DECORATION_COLUMNS]


def persist_cpk_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    """Create the user-maintained decoration sheet when it does not exist yet."""
    product_dir.mkdir(parents=True, exist_ok=True)
    decoration_path = get_cpk_decoration_path(product_dir)
    target_sheet = sheet_name or "Sheet1"
    existing_decoration_df = load_cpk_decoration(product_dir, sheet_name)
    decoration_to_write = merge_detail_with_decoration_flags(
        detail_df,
        existing_decoration_df,
    )
    sheet_exists = _decoration_sheet_exists(decoration_path, sheet_name)
    persisted_df = decoration_to_write
    should_write = not sheet_exists
    if sheet_exists and not existing_decoration_df.empty:
        persisted_df = _append_missing_detail_rows(
            existing_decoration_df,
            decoration_to_write,
        )
        should_write = len(persisted_df) > len(existing_decoration_df)

    if should_write:
        write_result = replace_workbook_sheets(
            decoration_path,
            {target_sheet: persisted_df},
        )
        if not write_result.written:
            logger.warning(
                "[SPC] failed to persist CPK decoration sheet %s [%s]: %s",
                decoration_path,
                target_sheet,
                write_result.error,
            )
    return decoration_to_write


def apply_cpk_decoration(
    period_capability_df: pd.DataFrame,
    decoration_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Keep the computed CPK by default and apply only admin-enabled user corrections."""
    result = period_capability_df.copy()
    if result.empty:
        result["cpk_decorated"] = pd.Series(dtype="bool")
        return result

    detail_df = build_cpk_detail(period_capability_df)
    flags_df = merge_detail_with_decoration_flags(
        detail_df,
        decoration_df if decoration_df is not None else _empty_decoration_frame(),
    )
    if flags_df.empty:
        result["cpk_decorated"] = False
        return result

    selected_df = flags_df[[*CPK_KEY_COLUMNS, "cpk_corrected", "flag"]].copy()
    selected_df = selected_df.rename(columns={"flag": "cpk_decorated"})
    result = _normalize_key_columns(result).merge(selected_df, on=CPK_KEY_COLUMNS, how="left")
    result["cpk_decorated"] = result["cpk_decorated"].apply(_parse_flag)
    corrected_values = pd.to_numeric(result["cpk_corrected"], errors="coerce")
    result.loc[result["cpk_decorated"] & corrected_values.notna(), "cpk"] = corrected_values[
        result["cpk_decorated"] & corrected_values.notna()
    ]
    return result.drop(columns=["cpk_corrected"])


def prepare_cpk_decoration(
    period_capability_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
    sheet_name: str | None = None,
) -> CpkDecorationResult:
    """Build chart-ready values selected by the user-maintained decoration sheet."""
    detail_df = build_cpk_detail(period_capability_df)
    decoration_df = (
        persist_cpk_decoration(product_dir, detail_df, sheet_name)
        if persist_files
        else merge_detail_with_decoration_flags(detail_df, load_cpk_decoration(product_dir, sheet_name))
    )
    period_capability_df = apply_cpk_decoration(
        period_capability_df,
        decoration_df,
    )
    return CpkDecorationResult(
        period_capability_df=period_capability_df,
        decoration_df=decoration_df,
        decoration_path=get_cpk_decoration_path(product_dir),
        decoration_sheet=sheet_name or "Sheet1",
    )
