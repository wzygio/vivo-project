"""Application orchestration for the Yield modifier ledger."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.yield_domain.core.mwd_trend.modifier_table import (
    COL_DEFECT,
    COL_MONTH,
    COL_SCALE_FACTOR,
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
        updated, loss_changed = _apply_current_month_loss(
            table[level], losses[level], current_month
        )
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
