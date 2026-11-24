"""Excel adapter for SPC CPK/CPM decoration ledgers."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from src.inline_domain.core.spc.cpk_decoration import (
    CAPABILITY_METRIC_CPK,
    CPK_KEY_COLUMNS,
    CPK_DECORATION_FILE_NAME,
    _append_missing_detail_rows,
    _empty_decoration_frame,
    _normalize_key_columns,
    _ordered_existing_columns,
    _validate_metric,
    capability_corrected_column,
    capability_decoration_columns,
    merge_capability_detail_with_decoration_flags,
    previous_week_detail,
)
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.shared_kernel.core.decoration_revision import validate_decision_update
from src.shared_kernel.utils.excel_tools import (
    _is_missing_sheet_error,
    _read_encrypted_xlsx_via_com,
    list_workbook_sheet_names,
)
from src.inline_domain.infrastructure.spc.capability_workbook_writer import (
    CapabilityRowEdits, workbook_version, write_capability_rows,
)

from src.inline_domain.application.shared.decoration_ports import (
    CapabilityDecorationReadError, CapabilityDecorationWriteError,
)

logger = logging.getLogger(__name__)


def update_capability_decisions(path: Path, sheet: str, metric: str, uploaded: pd.DataFrame, *, revision: str | None = None) -> None:
    """Edit only submitted manual cells on the current ledger version."""
    _validate_metric(metric)
    with publication_lock(path):
        version = workbook_version(path)
        saved = _read_capability_decoration(path.parent, sheet, metric, raise_on_error=True, workbook_path=path)
        keys = CPK_KEY_COLUMNS
        normalized = _normalize_key_columns(uploaded)
        if normalized.duplicated(keys).any():
            raise ValueError("重复业务键")
        replacements = f"{metric}_replacement"
        saved = _normalize_key_columns(saved)
        controls = ["flag", replacements] if replacements in saved else ["flag"]
        if replacements in saved and replacements not in normalized:
            normalized = normalized.merge(saved[[*keys, replacements]], on=keys, how="left", validate="one_to_one")
        validate_decision_update(sheet, saved, normalized, keys, revision, controls=controls)
        positions = pd.Series(range(2, len(saved) + 2), index=pd.MultiIndex.from_frame(_normalize_key_columns(saved)[keys]))
        if not positions.index.is_unique:
            raise ValueError("已有台账业务键重复")
        edits = CapabilityRowEdits()
        for key, record in normalized.set_index(keys).iterrows():
            if key not in positions:
                raise ValueError("未知业务键")
            flag = record["flag"]
            fields = {"flag": True if str(flag).strip().lower() == "ture" else parse_decoration_flag(flag)}
            if replacements in record:
                value = record[replacements]
                if pd.isna(value) or value == "":
                    value = None
                elif not 1.33 < float(value) < 1.40:
                    prior = saved.iloc[positions[key] - 2][replacements]
                    if pd.isna(prior) or float(value) != float(prior):
                        raise ValueError("替换值须在 1.33 到 1.40 之间")
                fields[replacements] = value
            edits.updates[int(positions[key])] = fields
        if edits and not write_capability_rows(path, sheet, saved.iloc[:0], edits, version).written:
            raise OSError("保存能力决策失败")


def get_cpk_decoration_path(product_dir: Path, *, workbook_path: Path | None = None) -> Path:
    return Path(workbook_path) if workbook_path is not None else product_dir / CPK_DECORATION_FILE_NAME


def get_capability_decoration_signature(product_dir: Path, *, workbook_path: Path | None = None) -> tuple[int, int]:
    """Cheap cache invalidation when Excel saves flags or replacement values."""
    try:
        stat = get_cpk_decoration_path(product_dir, workbook_path=workbook_path).stat()
    except FileNotFoundError:
        return (0, 0)
    return (stat.st_mtime_ns, stat.st_size)


def _read_capability_decoration(
    product_dir: Path,
    sheet_name: str | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
    *,
    raise_on_error: bool = False,
    workbook_path: Path | None = None,
) -> pd.DataFrame:
    _validate_metric(metric)
    path = get_cpk_decoration_path(product_dir, workbook_path=workbook_path)
    if not path.exists():
        return _empty_decoration_frame(metric)
    try:
        frame = pd.read_excel(path, sheet_name=sheet_name or 0, engine="openpyxl", dtype=object)
    except Exception as excel_exc:
        if isinstance(excel_exc, ValueError) and _is_missing_sheet_error(excel_exc):
            return _empty_decoration_frame(metric)
        try:
            frame = _read_encrypted_xlsx_via_com(path, sheet_name)
        except Exception as com_exc:
            logger.warning(
                "SPC %s decoration read failed: %s (openpyxl=%s, COM=%s)",
                metric.upper(),
                path,
                excel_exc,
                com_exc,
            )
            if raise_on_error:
                raise CapabilityDecorationReadError(
                    f"Cannot read capability decoration sheet {sheet_name!r}"
                ) from com_exc
            return _empty_decoration_frame(metric)
    return frame


def load_capability_decoration(
    product_dir: Path, sheet_name: str | None = None, metric: str = CAPABILITY_METRIC_CPK,
    *, raise_on_error: bool = False, workbook_path: Path | None = None,
) -> pd.DataFrame:
    frame = _read_capability_decoration(product_dir, sheet_name, metric,
        raise_on_error=raise_on_error, workbook_path=workbook_path)
    if frame.empty:
        return _empty_decoration_frame(metric)
    return _ordered_existing_columns(
        _normalize_key_columns(frame),
        capability_decoration_columns(metric),
    )


def persist_capability_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    sheet_name: str | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
    *, computed_detail_df: pd.DataFrame | None = None, reference_date: date | None = None,
    workbook_path: Path | None = None,
    raise_on_error: bool = False,
) -> pd.DataFrame:
    """Append new anomalies and reconcile matched calculations for the previous ISO week."""
    _validate_metric(metric)
    path = get_cpk_decoration_path(product_dir, workbook_path=workbook_path)
    with publication_lock(path):
        return _reconcile_capability_rows(product_dir, detail_df, sheet_name, metric,
            computed_detail_df=computed_detail_df, reference_date=reference_date or date.today(),
            workbook_path=workbook_path, raise_on_error=raise_on_error)


def _refresh_saved_rows(
    existing: pd.DataFrame, computed: pd.DataFrame | None, metric: str, reference_date: date,
) -> tuple[pd.DataFrame, CapabilityRowEdits]:
    edits = CapabilityRowEdits()
    if existing.empty or computed is None:
        return existing, edits
    corrected = capability_corrected_column(metric)
    weekly = previous_week_detail(_normalize_key_columns(computed), reference_date).copy()
    weekly[corrected] = pd.to_numeric(weekly[corrected], errors="coerce")
    weekly = weekly.drop_duplicates(CPK_KEY_COLUMNS, keep="last")
    if corrected not in existing:
        raise CapabilityDecorationWriteError(f"Existing capability sheet lacks {corrected!r}")
    fields = [name for name in (corrected, "period_start", "period_end") if name in weekly and name in existing]
    calculations = weekly.set_index(CPK_KEY_COLUMNS)[fields].to_dict("index")
    saved = existing.reset_index(drop=True).copy()
    keys = _normalize_key_columns(saved[CPK_KEY_COLUMNS])
    review_keys = set(previous_week_detail(keys, reference_date).itertuples(index=False, name=None))
    for index, key in enumerate(keys.itertuples(index=False, name=None)):
        fresh = calculations.get(key)
        if fresh is None:
            if key not in review_keys:
                continue
            fresh = {corrected: float("nan")}
        number = index + 2  # Header occupies Excel row 1.
        for name, value in fresh.items():
            prior = saved.at[index, name]
            if name == corrected:
                prior = pd.to_numeric(prior, errors="coerce")
                # XLSX serialization can round a float to 16 significant digits.
                equal = (pd.isna(prior) and pd.isna(value)) or format(prior, ".16g") == format(value, ".16g")
            else:
                prior = pd.to_datetime(prior, errors="coerce")
                value = pd.to_datetime(value, errors="coerce")
                if pd.isna(prior) or pd.isna(value):
                    equal = pd.isna(prior) and pd.isna(value)
                else:
                    equal = prior.round("ms") == value.round("ms")
            if not equal:
                edits.updates.setdefault(number, {})[name] = value
                saved.at[index, name] = value
    return saved.drop(index=[number - 2 for number in edits.deletes]).reset_index(drop=True), edits


def _reconcile_capability_rows(
    product_dir: Path, detail_df: pd.DataFrame, sheet_name: str | None, metric: str,
    *, computed_detail_df: pd.DataFrame | None, reference_date: date,
    workbook_path: Path | None, raise_on_error: bool,
) -> pd.DataFrame:
    path = get_cpk_decoration_path(product_dir, workbook_path=workbook_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    version = workbook_version(path)
    try:
        existing = _read_capability_decoration(
            product_dir, sheet_name, metric, raise_on_error=True, workbook_path=workbook_path,
        )
    except CapabilityDecorationReadError:
        if raise_on_error:
            raise
        # A read failure is not a successfully read empty sheet. Preserve the file.
        return merge_capability_detail_with_decoration_flags(
            detail_df, _empty_decoration_frame(metric), metric, create_replacements=False,
        )
    keys = CPK_KEY_COLUMNS
    candidates = _normalize_key_columns(detail_df).drop_duplicates(keys, keep="last")
    if not existing.empty and not candidates.empty:
        known = pd.MultiIndex.from_frame(_normalize_key_columns(existing)[keys])
        candidates = candidates.loc[~pd.MultiIndex.from_frame(candidates[keys]).isin(known)]
    current = merge_capability_detail_with_decoration_flags(candidates, _empty_decoration_frame(metric), metric)
    sheet_names = list_workbook_sheet_names(path)
    if sheet_names is None:
        if raise_on_error:
            raise CapabilityDecorationReadError("Cannot read capability workbook sheet inventory")
        return current
    target_sheet = sheet_name or (sheet_names[0] if sheet_names else "Sheet1")
    if not current.empty and target_sheet in sheet_names and set(capability_decoration_columns(metric)) - set(existing.columns):
        raise CapabilityDecorationWriteError("Existing capability headers require explicit manual maintenance")
    retained, edits = _refresh_saved_rows(existing, computed_detail_df, metric, reference_date)
    if current.empty and not edits:
        return existing.copy()
    persisted = _append_missing_detail_rows(retained, current, metric)
    new_rows = current.reindex(columns=persisted.columns)
    result = write_capability_rows(path, target_sheet, new_rows, edits, version)
    if not result.written:
        logger.warning("SPC %s decoration write failed: %s", metric.upper(), result.error)
        if raise_on_error:
            raise CapabilityDecorationWriteError(f"Cannot persist {metric.upper()} capability records")
        return existing.copy()
    return persisted


load_cpk_decoration = load_capability_decoration
persist_cpk_decoration = persist_capability_decoration
