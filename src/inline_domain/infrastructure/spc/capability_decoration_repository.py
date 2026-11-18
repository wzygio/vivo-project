"""Excel adapter for SPC CPK/CPM decoration ledgers."""

from __future__ import annotations

import logging
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
    capability_decoration_columns,
    merge_capability_detail_with_decoration_flags,
)
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import (
    _is_missing_sheet_error,
    _read_encrypted_xlsx_via_com,
    list_workbook_sheet_names,
)
from src.inline_domain.infrastructure.spc.capability_workbook_writer import create_capability_rows

from src.inline_domain.application.shared.decoration_ports import (
    CapabilityDecorationReadError, CapabilityDecorationWriteError,
)

logger = logging.getLogger(__name__)


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
    *, computed_detail_df: pd.DataFrame | None = None, workbook_path: Path | None = None,
    raise_on_error: bool = False,
) -> pd.DataFrame:
    """Create missing anomaly keys only; existing rows and columns are immutable.

    computed_detail_df is accepted for old injected ports/callers but never used
    to update saved facts. Explicit administrator upload is a separate action.
    """
    _validate_metric(metric)
    path = get_cpk_decoration_path(product_dir, workbook_path=workbook_path)
    with publication_lock(path):
        return _create_missing_capability_rows(product_dir, detail_df, sheet_name, metric,
            workbook_path=workbook_path, raise_on_error=raise_on_error)


def _create_missing_capability_rows(
    product_dir: Path, detail_df: pd.DataFrame, sheet_name: str | None, metric: str,
    *, workbook_path: Path | None, raise_on_error: bool,
) -> pd.DataFrame:
    path = get_cpk_decoration_path(product_dir, workbook_path=workbook_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    target_sheet = sheet_name or "Sheet1"
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
    sheet_exists = path.exists() and (
        sheet_name is None or sheet_names is None or sheet_name in sheet_names
    )
    if current.empty:
        return existing.copy()
    if sheet_exists and set(capability_decoration_columns(metric)) - set(existing.columns):
        raise CapabilityDecorationWriteError("Existing capability headers require explicit manual maintenance")
    persisted = _append_missing_detail_rows(existing, current, metric)
    new_rows = current.reindex(columns=persisted.columns)
    result = create_capability_rows(path, target_sheet, new_rows)
    if not result.written:
        logger.warning("SPC %s decoration write failed: %s", metric.upper(), result.error)
        if raise_on_error:
            raise CapabilityDecorationWriteError(f"Cannot persist {metric.upper()} capability records")
    return persisted


load_cpk_decoration = load_capability_decoration
persist_cpk_decoration = persist_capability_decoration
