from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.shared_kernel.utils.excel_tools import (
    _read_encrypted_xlsx_via_com,
    replace_workbook_sheet,
)

logger = logging.getLogger(__name__)

OOS_DECORATION_FILE_NAME = "spc_sheet_oos_decoration.xlsx"
OOS_KEY_COLUMNS = ["prod_code", "step_id", "param_name", "sheet_id"]
OOS_DETAIL_COLUMNS = [
    "factory",
    "prod_code",
    "step_id",
    "param_name",
    "sheet_id",
    "sheet_start_time",
    "sheet_max",
    "sheet_min",
    "sheet_mean",
    "usl",
    "lsl",
    "oos_type",
]
OOS_DECORATION_COLUMNS = [*OOS_DETAIL_COLUMNS, "flag"]
DELETE_ACTION = "Delete"


class SheetOosDecorationReadError(RuntimeError):
    """Raised when an existing user-maintained decoration file cannot be read safely."""


@dataclass(frozen=True)
class SheetOosDecorationResult:
    raw_measurements_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def get_sheet_oos_decoration_path(product_dir: Path, file_name: str = OOS_DECORATION_FILE_NAME) -> Path:
    return product_dir / file_name


def _empty_detail_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OOS_DETAIL_COLUMNS)


def _empty_decoration_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OOS_DECORATION_COLUMNS)


def _ordered_existing_columns(df: pd.DataFrame, ordered_columns: Iterable[str]) -> pd.DataFrame:
    for column in ordered_columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[list(ordered_columns)].copy()


def _resolve_key_columns(key_columns: Iterable[str] | None) -> list[str]:
    """Custom key columns let non-SPC modules (e.g. aoi) reuse this machinery."""
    return list(key_columns) if key_columns else OOS_KEY_COLUMNS


def _normalize_key_columns(
    df: pd.DataFrame, key_columns: Iterable[str] | None = None
) -> pd.DataFrame:
    result = df.copy()
    for column in _resolve_key_columns(key_columns):
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _parse_flag(value: object) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"false", "0", "no", "n", "否", "不修饰", "不截断"}:
        return False
    return True


def _is_delete_action(value: object) -> bool:
    return not pd.isna(value) and str(value).strip().lower() == DELETE_ACTION.lower()


def _normalize_flag_action(value: object) -> bool | str:
    return DELETE_ACTION if _is_delete_action(value) else _parse_flag(value)


def _stable_fraction(parts: Iterable[object]) -> float:
    seed = "|".join("" if pd.isna(part) else str(part) for part in parts)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(0xFFFFFFFFFFFF)


def _clip_inside_spec(row: pd.Series, side: str) -> float:
    value = row.get("param_value")
    usl = row.get("_oos_usl")
    lsl = row.get("_oos_lsl")
    if pd.isna(value) or pd.isna(usl) or pd.isna(lsl) or float(usl) <= float(lsl):
        return value

    span = float(usl) - float(lsl)
    fraction = _stable_fraction(
        [
            row.get("prod_code"),
            row.get("step_id"),
            row.get("param_name"),
            row.get("sheet_id"),
            row.get("site_name"),
            row.get("unit_id"),
            value,
            side,
        ]
)
    margin = (0.05 + fraction * 0.1) * span
    if side == "upper":
        return float(usl) - margin
    return float(lsl) + margin


def _apply_clip_rules(
    spec_df: pd.DataFrame,
    clip_rules: Iterable[dict[str, object]] | None,
) -> pd.DataFrame:
    """Return effective clip bounds without changing the official spec columns upstream."""
    result = spec_df.copy()
    if not clip_rules or "param_name" not in result.columns:
        return result

    matched = pd.Series(False, index=result.index)
    param_names = result["param_name"].fillna("").astype(str)
    for rule in clip_rules:
        needle = str(rule.get("param_name_contains", "")).strip()
        if not needle:
            continue
        try:
            lower_offset = float(rule.get("lower_offset", 0.0))
            upper_offset = float(rule.get("upper_offset", 0.0))
        except (TypeError, ValueError):
            continue
        rule_mask = ~matched & param_names.str.contains(needle, case=False, regex=False)
        result.loc[rule_mask, "lsl"] = (
            pd.to_numeric(result.loc[rule_mask, "lsl"], errors="coerce") + lower_offset
        )
        result.loc[rule_mask, "usl"] = (
            pd.to_numeric(result.loc[rule_mask, "usl"], errors="coerce") + upper_offset
        )
        matched |= rule_mask
    return result


def build_sheet_oos_detail(sheet_features_df: pd.DataFrame) -> pd.DataFrame:
    """Return Sheet-level rows whose point max/min crosses USL/LSL."""
    required_cols = {"factory", *OOS_KEY_COLUMNS, "sheet_start_time", "sheet_max", "sheet_min", "sheet_mean", "usl", "lsl"}
    if sheet_features_df.empty or not required_cols.issubset(sheet_features_df.columns):
        return _empty_detail_frame()

    df = sheet_features_df.copy()
    for column in ["sheet_max", "sheet_min", "sheet_mean", "usl", "lsl"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    upper_mask = df["sheet_max"] > df["usl"]
    lower_mask = df["sheet_min"] < df["lsl"]
    oos_df = df[upper_mask | lower_mask].copy()
    if oos_df.empty:
        return _empty_detail_frame()

    oos_df["oos_type"] = "USL"
    oos_df.loc[lower_mask.loc[oos_df.index], "oos_type"] = "LSL"
    both_mask = upper_mask.loc[oos_df.index] & lower_mask.loc[oos_df.index]
    oos_df.loc[both_mask, "oos_type"] = "USL/LSL"
    oos_df = _normalize_key_columns(oos_df)
    return _ordered_existing_columns(oos_df, OOS_DETAIL_COLUMNS).sort_values(
        ["factory", "prod_code", "step_id", "param_name", "sheet_start_time", "sheet_id"],
        kind="stable",
    ).reset_index(drop=True)


def load_sheet_oos_decoration(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Load the user-editable decoration flags from the shared workbook sheet."""
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not decoration_path.exists():
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    try:
        if sheet_name is None:
            df = pd.read_excel(decoration_path, engine="openpyxl")
        else:
            try:
                df = pd.read_excel(decoration_path, sheet_name=sheet_name)
            except ValueError:
                # 指定 sheet 缺失 —— 与文件缺失语义一致
                return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    except Exception as excel_exc:
        try:
            df = _read_encrypted_xlsx_via_com(decoration_path, sheet_name)
            logger.info(
                "[SPC] loaded enterprise-encrypted Sheet OOS decoration file via Excel COM: %s",
                decoration_path,
            )
        except Exception as com_exc:
            logger.error(
                "[CPM] failed to read Sheet OOS decoration file %s with openpyxl (%s) and Excel COM (%s)",
                decoration_path,
                excel_exc,
                com_exc,
            )
            raise SheetOosDecorationReadError(
                f"Unable to read existing Sheet OOS decoration file: {decoration_path}"
            ) from com_exc
    if df.empty:
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    df = _normalize_key_columns(df, key_columns)
    if key_columns is None:
        return _ordered_existing_columns(df, OOS_DECORATION_COLUMNS)
    return df


def merge_detail_with_decoration_flags(
    detail_df: pd.DataFrame,
    existing_decoration_df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Attach existing user flags to current OOS details, defaulting to True."""
    if key_columns is None:
        detail_columns: list[str] = OOS_DETAIL_COLUMNS
        decoration_columns: list[str] = OOS_DECORATION_COLUMNS
    else:
        detail_columns = list(detail_df.columns)
        decoration_columns = [*detail_columns, "flag"]
    if detail_df.empty:
        return pd.DataFrame(columns=decoration_columns)

    keys = _resolve_key_columns(key_columns)
    detail_df = _normalize_key_columns(
        _ordered_existing_columns(detail_df, detail_columns), keys
    )
    if existing_decoration_df.empty or "flag" not in existing_decoration_df.columns:
        result = detail_df.copy()
        result["flag"] = True
        return result[decoration_columns]

    flags_df = _normalize_key_columns(existing_decoration_df, keys).copy()
    flags_df["flag"] = flags_df["flag"].apply(_normalize_flag_action)
    flags_df = flags_df[keys + ["flag"]].drop_duplicates(keys, keep="last")
    result = detail_df.merge(flags_df, on=keys, how="left")
    result["flag"] = result["flag"].apply(_normalize_flag_action)
    return result[decoration_columns]


def _exclude_delete_flagged_measurements(
    raw_measurements_df: pd.DataFrame,
    decoration_df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    keys = _resolve_key_columns(key_columns)
    required_columns = set(keys)
    if not required_columns.issubset(raw_measurements_df.columns):
        return raw_measurements_df.copy()

    delete_keys = decoration_df.loc[
        decoration_df["flag"].apply(_is_delete_action),
        keys,
    ].drop_duplicates(keys)
    if delete_keys.empty:
        return raw_measurements_df.copy()

    delete_keys = _normalize_key_columns(delete_keys, keys).assign(_delete_action=True)
    result = _normalize_key_columns(raw_measurements_df, keys).merge(
        delete_keys,
        on=keys,
        how="left",
        validate="many_to_one",
    )
    return result.loc[result["_delete_action"].ne(True)].drop(
        columns="_delete_action"
    )


def persist_sheet_oos_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Refresh the user-maintained Sheet OOS decoration sheet in the shared workbook."""
    product_dir.mkdir(parents=True, exist_ok=True)
    decoration_path = get_sheet_oos_decoration_path(product_dir, file_name)

    existing_decoration = load_sheet_oos_decoration(product_dir, file_name, sheet_name, key_columns)
    decoration_to_write = merge_detail_with_decoration_flags(detail_df, existing_decoration, key_columns)

    # replace_workbook_sheet 内部已处理 PermissionError（仅告警跳过）
    replace_workbook_sheet(decoration_path, sheet_name or "Sheet1", decoration_to_write)
    return decoration_to_write


def apply_sheet_oos_decoration(
    raw_measurements_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    decoration_df: pd.DataFrame | None = None,
    clip_rules: Iterable[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Apply Sheet actions: Delete excludes points, True clips OOS points, False keeps them."""
    if raw_measurements_df.empty or "param_value" not in raw_measurements_df.columns:
        return raw_measurements_df

    detail_df = build_sheet_oos_detail(sheet_features_df)
    if detail_df.empty:
        return raw_measurements_df.copy()

    if decoration_df is None or decoration_df.empty:
        decoration_df = merge_detail_with_decoration_flags(detail_df, _empty_decoration_frame())
    else:
        decoration_df = merge_detail_with_decoration_flags(detail_df, decoration_df)

    df = _exclude_delete_flagged_measurements(raw_measurements_df, decoration_df)
    active_df = decoration_df[
        ~decoration_df["flag"].apply(_is_delete_action)
        & decoration_df["flag"].apply(_parse_flag)
    ].copy()
    if active_df.empty or df.empty:
        return df

    spec_cols = [*OOS_KEY_COLUMNS, "usl", "lsl"]
    spec_df = _apply_clip_rules(
        _normalize_key_columns(active_df[spec_cols].copy()),
        clip_rules,
    ).rename(
        columns={"usl": "_oos_usl", "lsl": "_oos_lsl"}
    )
    df = _normalize_key_columns(df)
    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df = df.merge(spec_df, on=OOS_KEY_COLUMNS, how="left")

    upper_mask = df["_oos_usl"].notna() & (df["param_value"] > df["_oos_usl"])
    lower_mask = df["_oos_lsl"].notna() & (df["param_value"] < df["_oos_lsl"])
    if upper_mask.any():
        df.loc[upper_mask, "param_value"] = df.loc[upper_mask].apply(_clip_inside_spec, axis=1, side="upper")
    if lower_mask.any():
        df.loc[lower_mask, "param_value"] = df.loc[lower_mask].apply(_clip_inside_spec, axis=1, side="lower")

    return df.drop(columns=["_oos_usl", "_oos_lsl"])


def prepare_sheet_oos_decoration(
    raw_measurements_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
    clip_rules: Iterable[dict[str, object]] | None = None,
    decoration_file_name: str = OOS_DECORATION_FILE_NAME,
    decoration_sheet_name: str | None = None,
) -> SheetOosDecorationResult:
    """Return chart-ready measurements after applying tri-state Sheet actions."""
    detail_df = build_sheet_oos_detail(sheet_features_df)
    if persist_files:
        decoration_df = persist_sheet_oos_decoration(
            product_dir, detail_df, decoration_file_name, decoration_sheet_name
        )
    else:
        decoration_df = merge_detail_with_decoration_flags(
            detail_df,
            load_sheet_oos_decoration(product_dir, decoration_file_name, decoration_sheet_name),
        )

    decorated_df = apply_sheet_oos_decoration(
        raw_measurements_df,
        sheet_features_df,
        decoration_df,
        clip_rules=clip_rules,
    )
    return SheetOosDecorationResult(
        raw_measurements_df=decorated_df,
        decoration_df=decoration_df,
        decoration_path=get_sheet_oos_decoration_path(product_dir, decoration_file_name),
        decoration_sheet=decoration_sheet_name or "Sheet1",
    )
