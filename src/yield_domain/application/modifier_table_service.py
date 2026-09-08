"""Application orchestration for the Yield modifier ledger."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.yield_domain.core.mwd_trend.modifier_table import (
    COL_DEFECT,
    COL_MONTH,
    COL_SCALE_FACTOR,
    COL_SPECIFIED_LOSS,
    ModifierTableValidationError,
    _apply_current_month_loss,
    compute_current_month_losses,
    compute_scale_factors,
    specified_signature,
)
from src.yield_domain.infrastructure.modifier_table_repository import (
    load_modifier_signatures,
    read_modifier_table,
    store_modifier_signatures,
    write_modifier_sheet,
)

logger = logging.getLogger(__name__)


def _same_excel_cell(left: object, right: object) -> bool:
    left_missing = pd.isna(left)
    right_missing = pd.isna(right)
    if left_missing or right_missing:
        return bool(left_missing and right_missing)
    return left == right


def _assert_specified_loss_unchanged(
    original: pd.DataFrame,
    updated: pd.DataFrame,
    *,
    sheet_name: str,
) -> None:
    """Enforce that automatic refreshes never create or alter manual target cells."""
    if len(updated) < len(original):
        raise ModifierTableValidationError(
            f"禁止自动修改指定良损：Sheet={sheet_name}，刷新过程删除了原有行"
        )
    original_values = original[COL_SPECIFIED_LOSS].tolist()
    updated_values = updated[COL_SPECIFIED_LOSS].iloc[: len(original)].tolist()
    existing_changed = any(
        not _same_excel_cell(before, after)
        for before, after in zip(original_values, updated_values)
    )
    appended_has_specified = any(
        not pd.isna(value)
        for value in updated[COL_SPECIFIED_LOSS].iloc[len(original) :]
    )
    if existing_changed or appended_has_specified:
        raise ModifierTableValidationError(
            f"禁止自动修改指定良损：Sheet={sheet_name}；该列仅允许人工维护"
        )


def sync_modifier_table(
    xlsx_path: Path,
    product_code: str,
    panel_details_df: pd.DataFrame,
    current_month: str,
    signature_path: Path | None = None,
    read_only: bool = False,
) -> dict[str, pd.DataFrame]:
    """Synchronize monthly losses, optionally without persisting ledger state."""
    path = Path(xlsx_path)
    signature_path = Path(signature_path or path.with_suffix(".sig.json"))
    stored = load_modifier_signatures(signature_path)
    committed = stored.copy()
    table = read_modifier_table(path, product_code)
    losses = compute_current_month_losses(panel_details_df, current_month)
    for level in ("group", "code"):
        suffix = "Group级" if level == "group" else "Code级"
        sheet_name = f"{product_code}_{suffix}"
        original = table[level]
        updated, loss_changed = _apply_current_month_loss(
            original, losses[level], current_month
        )
        _assert_specified_loss_unchanged(original, updated, sheet_name=sheet_name)
        signature = specified_signature(updated)
        signature_key = f"{product_code}:{level}"
        factors = compute_scale_factors(updated)
        if not updated.empty:
            updated[COL_SCALE_FACTOR] = [
                factors.get((str(defect).strip(), str(month).strip()), 1.0)
                for defect, month in zip(updated[COL_DEFECT], updated[COL_MONTH])
            ]
        needs_write = (
            not read_only
            and not updated.empty
            and (loss_changed or stored.get(signature_key) != signature)
        )
        if needs_write:
            try:
                written = write_modifier_sheet(path, sheet_name, updated)
            except Exception as exc:
                written = False
                logger.error("Yield modifier write failed for %s: %s", sheet_name, exc)
            if written:
                committed[signature_key] = signature
        table[level] = updated
    if not read_only:
        store_modifier_signatures(signature_path, committed)
    return table
