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
    """Refresh current losses and backfill missing historical ledger rows."""
    path = Path(xlsx_path)
    signature_path = Path(signature_path or path.with_suffix(".sig.json"))
    stored = load_modifier_signatures(signature_path)
    committed = stored.copy()
    table = read_modifier_table(path, product_code)
    losses = compute_current_month_losses(panel_details_df, current_month)
    historical_losses = {}
    if not panel_details_df.empty:
        dates = pd.to_datetime(
            panel_details_df["warehousing_time"], format="%Y%m%d", errors="coerce"
        )
        months = dates.dropna().dt.to_period("M").astype(str).unique()
        historical_losses = {
            month: compute_current_month_losses(panel_details_df, month)
            for month in sorted(months)
            if month < current_month
        }
    for level in ("group", "code"):
        suffix = "Group级" if level == "group" else "Code级"
        sheet_name = f"{product_code}_{suffix}"
        updated, loss_changed = _apply_current_month_loss(
            table[level], losses[level], current_month
        )
        # Historical snapshots may be partial: only append missing rows, never
        # recalculate existing historical references or copy specified values.
        for month, monthly_losses in historical_losses.items():
            existing = set(
                updated.loc[
                    updated[COL_MONTH].astype(str).str.strip().eq(month), COL_DEFECT
                ].astype(str).str.strip()
            )
            missing = monthly_losses[level]
            missing = missing[~missing.index.isin(existing)]
            updated, appended = _apply_current_month_loss(updated, missing, month)
            loss_changed = loss_changed or appended
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
