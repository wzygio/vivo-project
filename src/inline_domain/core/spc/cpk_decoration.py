from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com

logger = logging.getLogger(__name__)

CPK_DETAIL_FILE_NAME = "spc_cpk_detail.xlsx"
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
    "cpk_actual",
    "cpk_corrected",
]
CPK_DECORATION_COLUMNS = [*CPK_DETAIL_COLUMNS, "flag"]


@dataclass(frozen=True)
class CpkDecorationResult:
    """CPK values selected from real calculations or user-maintained corrections."""

    period_capability_df: pd.DataFrame
    detail_df: pd.DataFrame
    decoration_df: pd.DataFrame
    detail_path: Path
    decoration_path: Path


def get_cpk_detail_path(product_dir: Path) -> Path:
    return product_dir / CPK_DETAIL_FILE_NAME


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
            result[column] = result[column].fillna("").astype(str)
    return result


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


def build_cpk_detail(
    real_period_capability_df: pd.DataFrame,
    corrected_period_capability_df: pd.DataFrame,
) -> pd.DataFrame:
    """Pair real CPK values with the OOS-corrected alternative for each period."""
    required_columns = {*CPK_KEY_COLUMNS, "cpk"}
    if real_period_capability_df.empty or not required_columns.issubset(real_period_capability_df.columns):
        return _empty_detail_frame()

    real_df = _normalize_key_columns(real_period_capability_df)
    real_detail = _ordered_existing_columns(
        real_df,
        [*CPK_KEY_COLUMNS, "period_sort", "period_start", "period_end", "cpk"],
    ).rename(columns={"cpk": "cpk_actual"})

    if corrected_period_capability_df.empty or not required_columns.issubset(corrected_period_capability_df.columns):
        real_detail["cpk_corrected"] = pd.NA
        return _ordered_existing_columns(real_detail, CPK_DETAIL_COLUMNS)

    corrected_df = _normalize_key_columns(corrected_period_capability_df)
    corrected_detail = corrected_df[[*CPK_KEY_COLUMNS, "cpk"]].rename(columns={"cpk": "cpk_corrected"})
    corrected_detail = corrected_detail.drop_duplicates(CPK_KEY_COLUMNS, keep="last")
    result = real_detail.merge(corrected_detail, on=CPK_KEY_COLUMNS, how="left")
    return _ordered_existing_columns(result, CPK_DETAIL_COLUMNS).sort_values(
        ["factory", "step_id", "param_name", "period_sort"],
        kind="stable",
    ).reset_index(drop=True)


def load_cpk_decoration(product_dir: Path) -> pd.DataFrame:
    path = get_cpk_decoration_path(product_dir)
    if not path.exists():
        return _empty_decoration_frame()
    try:
        loaded_df = pd.read_excel(path, engine="openpyxl")
    except Exception as excel_exc:
        try:
            loaded_df = _read_encrypted_xlsx_via_com(path)
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


def persist_cpk_files(product_dir: Path, detail_df: pd.DataFrame) -> pd.DataFrame:
    product_dir.mkdir(parents=True, exist_ok=True)
    detail_path = get_cpk_detail_path(product_dir)
    decoration_path = get_cpk_decoration_path(product_dir)
    detail_to_write = _ordered_existing_columns(detail_df, CPK_DETAIL_COLUMNS)
    decoration_file_exists = decoration_path.exists()
    decoration_to_write = merge_detail_with_decoration_flags(
        detail_to_write,
        load_cpk_decoration(product_dir),
    )
    try:
        detail_to_write.to_excel(detail_path, index=False)
    except PermissionError as exc:
        logger.warning("[SPC] CPK detail file is locked, skipped writing %s: %s", detail_path, exc)
    if not decoration_file_exists:
        try:
            decoration_to_write.to_excel(decoration_path, index=False)
        except PermissionError as exc:
            logger.warning("[SPC] CPK decoration file is locked, skipped writing %s: %s", decoration_path, exc)
    return decoration_to_write


def apply_cpk_decoration(
    real_period_capability_df: pd.DataFrame,
    corrected_period_capability_df: pd.DataFrame,
    decoration_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Use real CPK by default and apply only admin-enabled user corrections."""
    result = real_period_capability_df.copy()
    if result.empty:
        result["cpk_decorated"] = pd.Series(dtype="bool")
        return result

    detail_df = build_cpk_detail(real_period_capability_df, corrected_period_capability_df)
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
    real_period_capability_df: pd.DataFrame,
    corrected_period_capability_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
) -> CpkDecorationResult:
    """Build current details and return chart-ready values selected by the user file."""
    detail_df = build_cpk_detail(real_period_capability_df, corrected_period_capability_df)
    decoration_df = (
        persist_cpk_files(product_dir, detail_df)
        if persist_files
        else merge_detail_with_decoration_flags(detail_df, load_cpk_decoration(product_dir))
    )
    period_capability_df = apply_cpk_decoration(
        real_period_capability_df,
        corrected_period_capability_df,
        decoration_df,
    )
    return CpkDecorationResult(
        period_capability_df=period_capability_df,
        detail_df=detail_df,
        decoration_df=decoration_df,
        detail_path=get_cpk_detail_path(product_dir),
        decoration_path=get_cpk_decoration_path(product_dir),
    )
