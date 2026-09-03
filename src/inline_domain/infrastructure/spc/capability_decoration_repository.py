"""Excel adapter for SPC CPK/CPM decoration ledgers."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.inline_domain.core.spc.cpk_decoration import (
    CAPABILITY_METRIC_CPK,
    CPK_DECORATION_FILE_NAME,
    _append_missing_detail_rows,
    _empty_decoration_frame,
    _normalize_key_columns,
    _ordered_existing_columns,
    _validate_metric,
    capability_decoration_columns,
    merge_capability_detail_with_decoration_flags,
)
from src.shared_kernel.utils.excel_tools import (
    _is_missing_sheet_error,
    _read_encrypted_xlsx_via_com,
    list_workbook_sheet_names,
    replace_workbook_sheets,
)

logger = logging.getLogger(__name__)


def get_cpk_decoration_path(product_dir: Path) -> Path:
    return product_dir / CPK_DECORATION_FILE_NAME


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
        frame = pd.read_excel(path, sheet_name=sheet_name or 0, engine="openpyxl")
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
            return _empty_decoration_frame(metric)
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
) -> pd.DataFrame:
    _validate_metric(metric)
    product_dir.mkdir(parents=True, exist_ok=True)
    path = get_cpk_decoration_path(product_dir)
    target_sheet = sheet_name or "Sheet1"
    existing = load_capability_decoration(product_dir, sheet_name, metric)
    current = merge_capability_detail_with_decoration_flags(detail_df, existing, metric)
    sheet_names = list_workbook_sheet_names(path)
    sheet_exists = path.exists() and (
        sheet_name is None or sheet_names is None or sheet_name in sheet_names
    )
    persisted = current
    should_write = not sheet_exists
    if sheet_exists and not existing.empty:
        persisted = _append_missing_detail_rows(existing, current, metric)
        should_write = len(persisted) > len(existing)
    if should_write:
        result = replace_workbook_sheets(path, {target_sheet: persisted})
        if not result.written:
            logger.warning("SPC %s decoration write failed: %s", metric.upper(), result.error)
    return current


load_cpk_decoration = load_capability_decoration
persist_cpk_decoration = persist_capability_decoration
