"""Excel adapter for Inline Sheet OOS detail and decision ledgers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import (
    DECISION_FLAG_COLUMN,
    OOS_DECORATION_COLUMNS,
    OOS_DECORATION_FILE_NAME,
    REFRESH_META_SHEET_NAME,
    RefreshDecision,
    _empty_decisions_frame,
    _empty_decoration_frame,
    _normalize_key_columns,
    _ordered_existing_columns,
    _resolve_key_columns,
    _upsert_refresh_meta_row,
    build_refresh_meta_row,
    compute_decision_signature,
    get_decision_sheet_name,
    merge_detail_with_decoration_flags,
    should_regenerate_detail,
)
from src.shared_kernel.utils.excel_tools import (
    _read_encrypted_xlsx_via_com,
    list_workbook_sheet_names,
    read_workbook_sheet,
    replace_workbook_sheets,
)

logger = logging.getLogger(__name__)


class SheetOosDecorationReadError(RuntimeError):
    """Raised when an existing user-maintained workbook cannot be read safely."""


class SheetOosDecorationWriteError(RuntimeError):
    """Raised when an Inline decoration workbook cannot be written safely."""


@dataclass(frozen=True)
class SheetOosPersistOutcome:
    decoration_df: pd.DataFrame
    decisions_df: pd.DataFrame
    refresh_decision: RefreshDecision


def get_sheet_oos_decoration_path(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
) -> Path:
    return product_dir / file_name


def load_sheet_oos_decoration(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Load the generated detail sheet, with enterprise-encryption fallback."""
    path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not path.exists():
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    try:
        frame = pd.read_excel(path, sheet_name=sheet_name or 0, engine="openpyxl")
    except ValueError:
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    except Exception as excel_exc:
        try:
            frame = _read_encrypted_xlsx_via_com(path, sheet_name)
        except Exception as com_exc:
            logger.error(
                "Inline decoration workbook read failed: %s (openpyxl=%s, COM=%s)",
                path,
                excel_exc,
                com_exc,
            )
            raise SheetOosDecorationReadError(
                f"Unable to read existing Sheet OOS decoration file: {path}"
            ) from com_exc
    if frame.empty:
        return _empty_decoration_frame() if key_columns is None else pd.DataFrame()
    frame = _normalize_key_columns(frame, key_columns)
    return (
        _ordered_existing_columns(frame, OOS_DECORATION_COLUMNS)
        if key_columns is None
        else frame
    )


def load_sheet_oos_decisions(
    product_dir: Path,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Load the user-owned ``<product>__flags`` decision sheet."""
    keys = _resolve_key_columns(key_columns)
    path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not path.exists():
        return _empty_decisions_frame(keys)
    decision_sheet = get_decision_sheet_name(sheet_name)
    names = list_workbook_sheet_names(path)
    if names is None:
        raise SheetOosDecorationReadError(
            f"Unable to enumerate sheets of existing Sheet OOS decoration workbook: {path}"
        )
    if decision_sheet not in names:
        return _empty_decisions_frame(keys)
    try:
        frame = read_workbook_sheet(path, decision_sheet)
    except Exception as exc:
        raise SheetOosDecorationReadError(
            f"Unable to read existing Sheet OOS decision sheet [{decision_sheet}]: {path}"
        ) from exc
    if frame.empty:
        return _empty_decisions_frame(keys)
    return _ordered_existing_columns(
        _normalize_key_columns(frame, keys),
        [*keys, DECISION_FLAG_COLUMN],
    )


def load_refresh_meta(
    product_dir: Path,
    file_name: str,
    scope: str | None,
    prod_code: str | None,
) -> dict | None:
    path = get_sheet_oos_decoration_path(product_dir, file_name)
    if not path.exists():
        return None
    try:
        frame = read_workbook_sheet(path, REFRESH_META_SHEET_NAME)
    except Exception as exc:
        logger.warning("Inline decoration refresh metadata unavailable: %s (%s)", path, exc)
        return None
    if frame.empty or not {"scope", "prod_code"}.issubset(frame.columns):
        return None
    matched = frame.loc[
        frame["scope"].fillna("").astype(str).eq(str(scope))
        & frame["prod_code"].fillna("").astype(str).eq(str(prod_code))
    ]
    if matched.empty:
        return None
    row = matched.iloc[-1]
    generated_at = pd.to_datetime(row.get("last_generated_at"), errors="coerce")

    def text_value(column: str) -> str | None:
        value = row.get(column)
        return None if value is None or pd.isna(value) else str(value)

    return {
        "scope": str(row.get("scope")),
        "prod_code": str(row.get("prod_code")),
        "last_generated_at": None
        if pd.isna(generated_at)
        else generated_at.to_pydatetime(),
        "product_revision": text_value("product_revision"),
        "decision_signature": text_value("decision_signature"),
        "detail_row_count": row.get("detail_row_count"),
    }


def persist_sheet_oos_decoration_outcome(
    product_dir: Path,
    detail_df: pd.DataFrame,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
    *,
    scope: str | None = None,
    prod_code: str | None = None,
    product_revision: str | None = None,
    decision_signature: str | None = None,
    now: datetime | None = None,
    force: bool = False,
) -> SheetOosPersistOutcome:
    """Merge decisions and atomically persist generated detail/metadata sheets."""
    product_dir.mkdir(parents=True, exist_ok=True)
    path = get_sheet_oos_decoration_path(product_dir, file_name)
    sheet = sheet_name or "Sheet1"
    keys = _resolve_key_columns(key_columns)
    decision_sheet = get_decision_sheet_name(sheet)
    sheet_names = list_workbook_sheet_names(path)
    decisions = load_sheet_oos_decisions(product_dir, file_name, sheet_name, keys)
    merged = merge_detail_with_decoration_flags(detail_df, decisions, keys)
    effective_now = now or datetime.now()
    effective_revision = "" if product_revision is None else str(product_revision)
    effective_signature = decision_signature or compute_decision_signature(decisions, keys)
    meta = load_refresh_meta(product_dir, file_name, scope, prod_code or sheet) if scope else None
    refresh = should_regenerate_detail(
        current_sheet_exists=sheet_names is not None and sheet in sheet_names,
        last_generated_at=meta["last_generated_at"] if meta else None,
        stored_product_revision=meta["product_revision"] if meta else None,
        current_product_revision=effective_revision,
        stored_decision_signature=meta["decision_signature"] if meta else None,
        current_decision_signature=effective_signature,
        now=effective_now,
    )
    if force or scope is None:
        refresh = RefreshDecision(True, refresh.reason)
    if refresh.should_write:
        sheets: dict[str, pd.DataFrame] = {sheet: merged}
        if sheet_names is None or decision_sheet not in sheet_names:
            sheets[decision_sheet] = decisions
        if scope is not None:
            meta_row = build_refresh_meta_row(
                scope,
                prod_code or sheet,
                effective_now,
                effective_revision,
                effective_signature,
                len(merged),
            )
            existing_meta = read_workbook_sheet(path, REFRESH_META_SHEET_NAME)
            sheets[REFRESH_META_SHEET_NAME] = _upsert_refresh_meta_row(existing_meta, meta_row)
        result = replace_workbook_sheets(path, sheets)
        if not result.written:
            raise SheetOosDecorationWriteError(
                f"Failed to persist Sheet OOS decoration workbook {path}: {result.error}"
            )
    logger.info(
        "Inline decoration refresh: product=%s scope=%s reason=%s write=%s rows=%s",
        prod_code or sheet,
        scope,
        refresh.reason,
        refresh.should_write,
        len(merged),
    )
    return SheetOosPersistOutcome(merged, decisions, refresh)


def persist_sheet_oos_decoration(
    product_dir: Path,
    detail_df: pd.DataFrame,
    file_name: str = OOS_DECORATION_FILE_NAME,
    sheet_name: str | None = None,
    key_columns: Iterable[str] | None = None,
    **kwargs: object,
) -> pd.DataFrame:
    """Backward-compatible adapter entry returning only the merged detail frame."""
    return persist_sheet_oos_decoration_outcome(
        product_dir,
        detail_df,
        file_name,
        sheet_name,
        key_columns,
        **kwargs,
    ).decoration_df
