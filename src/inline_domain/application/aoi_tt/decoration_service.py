"""Application orchestration for AOI-TT decoration."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.core.aoi_tt.aoi_tt_decoration import (
    AOI_TT_OOS_DECORATION_FILE_NAME,
    AOI_TT_OOS_KEY_COLUMNS,
    apply_aoi_tt_decoration,
    build_aoi_tt_oos_detail,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    merge_detail_with_decoration_flags,
)
from src.inline_domain.application.shared.decoration_defaults import (
    load_sheet_oos_decisions,
    persist_sheet_oos_decoration_outcome,
)
from src.inline_domain.application.shared.decoration_ports import SheetDecorationPort


@dataclass(frozen=True)
class AoiTtDecorationResult:
    tt_details_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def prepare_aoi_tt_decoration(
    tt_details_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    product_dir: Path,
    prod_code: str,
    persist: bool = True,
    exempt_param_name_contains: Iterable[str] | None = None,
    *,
    scope: str | None = None,
    product_revision: str = "",
    decision_signature: str = "",
    now: datetime | None = None,
    decoration_port: SheetDecorationPort | None = None,
) -> AoiTtDecorationResult:
    detail = build_aoi_tt_oos_detail(tt_details_df, spec_df)
    if persist:
        persist_outcome = (
            decoration_port.persist_sheet_oos_decoration_outcome
            if decoration_port is not None
            else persist_sheet_oos_decoration_outcome
        )
        outcome = persist_outcome(
            product_dir,
            detail,
            AOI_TT_OOS_DECORATION_FILE_NAME,
            prod_code,
            key_columns=AOI_TT_OOS_KEY_COLUMNS,
            scope=scope,
            prod_code=prod_code,
            product_revision=product_revision,
            decision_signature=decision_signature,
            now=now,
        )
        decoration = outcome.decoration_df
    else:
        load_decisions = (
            decoration_port.load_sheet_oos_decisions
            if decoration_port is not None
            else load_sheet_oos_decisions
        )
        decisions = load_decisions(
            product_dir,
            AOI_TT_OOS_DECORATION_FILE_NAME,
            prod_code,
            AOI_TT_OOS_KEY_COLUMNS,
        )
        decoration = merge_detail_with_decoration_flags(
            detail,
            decisions,
            AOI_TT_OOS_KEY_COLUMNS,
        )
    return AoiTtDecorationResult(
        tt_details_df=apply_aoi_tt_decoration(
            tt_details_df,
            spec_df,
            decoration,
            exempt_param_name_contains,
        ),
        decoration_df=decoration,
        decoration_path=product_dir / AOI_TT_OOS_DECORATION_FILE_NAME,
        decoration_sheet=prod_code,
    )
