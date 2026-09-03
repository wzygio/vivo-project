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
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    load_sheet_oos_decisions,
    persist_sheet_oos_decoration_outcome,
)


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
) -> AoiTtDecorationResult:
    detail = build_aoi_tt_oos_detail(tt_details_df, spec_df)
    if persist:
        outcome = persist_sheet_oos_decoration_outcome(
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
        decisions = load_sheet_oos_decisions(
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
