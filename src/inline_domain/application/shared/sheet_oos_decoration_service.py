"""Application orchestration for Inline Sheet OOS decoration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DECORATION_FILE_NAME,
    apply_sheet_oos_decoration,
    build_sheet_oos_detail,
    get_decision_sheet_name,
    merge_detail_with_decoration_flags,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    get_sheet_oos_decoration_path,
    load_sheet_oos_decisions,
    persist_sheet_oos_decoration_outcome,
)


@dataclass(frozen=True)
class SheetOosDecorationResult:
    raw_measurements_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str
    decision_sheet: str = ""
    decision_df: pd.DataFrame | None = None
    refresh_reason: str = ""


def prepare_sheet_oos_decoration(
    raw_measurements_df: pd.DataFrame,
    sheet_features_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
    decoration_file_name: str = OOS_DECORATION_FILE_NAME,
    decoration_sheet_name: str | None = None,
    *,
    scope: str | None = None,
    prod_code: str | None = None,
    product_revision: str | None = None,
    decision_signature: str | None = None,
    now: datetime | None = None,
    force: bool = False,
) -> SheetOosDecorationResult:
    """Load decisions, invoke pure rules, and persist generated audit detail."""
    detail = build_sheet_oos_detail(sheet_features_df)
    sheet = decoration_sheet_name or "Sheet1"
    if persist_files:
        outcome = persist_sheet_oos_decoration_outcome(
            product_dir,
            detail,
            decoration_file_name,
            decoration_sheet_name,
            scope=scope,
            prod_code=prod_code,
            product_revision=product_revision,
            decision_signature=decision_signature,
            now=now,
            force=force,
        )
        decoration = outcome.decoration_df
        decisions = outcome.decisions_df
        refresh_reason = outcome.refresh_decision.reason
    else:
        decisions = load_sheet_oos_decisions(
            product_dir,
            decoration_file_name,
            decoration_sheet_name,
        )
        decoration = merge_detail_with_decoration_flags(detail, decisions)
        refresh_reason = ""
    return SheetOosDecorationResult(
        raw_measurements_df=apply_sheet_oos_decoration(
            raw_measurements_df,
            sheet_features_df,
            decoration,
        ),
        decoration_df=decoration,
        decoration_path=get_sheet_oos_decoration_path(product_dir, decoration_file_name),
        decoration_sheet=sheet,
        decision_sheet=get_decision_sheet_name(sheet),
        decision_df=decisions,
        refresh_reason=refresh_reason,
    )
