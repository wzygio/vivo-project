"""Application orchestration for AOI-RS decoration."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_DECORATION_FILE_NAME,
    AOI_RS_OOS_KEY_COLUMNS,
    apply_aoi_rs_decoration,
    build_aoi_rs_oos_detail,
)
from src.inline_domain.core.shared.sheet_oos_decoration import (
    merge_detail_with_decoration_flags,
)
from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import (
    load_sheet_oos_decisions,
    persist_sheet_oos_decoration_outcome,
)


@dataclass(frozen=True)
class AoiRsDecorationResult:
    lot_points_df: pd.DataFrame
    sheet_points_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def prepare_aoi_rs_decoration(
    lot_points_df: pd.DataFrame,
    sheet_points_df: pd.DataFrame,
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
) -> AoiRsDecorationResult:
    detail = build_aoi_rs_oos_detail(lot_points_df, sheet_points_df, spec_df, prod_code)
    if persist:
        outcome = persist_sheet_oos_decoration_outcome(
            product_dir,
            detail,
            AOI_RS_OOS_DECORATION_FILE_NAME,
            prod_code,
            key_columns=AOI_RS_OOS_KEY_COLUMNS,
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
            AOI_RS_OOS_DECORATION_FILE_NAME,
            prod_code,
            AOI_RS_OOS_KEY_COLUMNS,
        )
        decoration = merge_detail_with_decoration_flags(
            detail,
            decisions,
            AOI_RS_OOS_KEY_COLUMNS,
        )
    lot_decorated, sheet_decorated = apply_aoi_rs_decoration(
        lot_points_df,
        sheet_points_df,
        spec_df,
        prod_code,
        decoration,
        exempt_param_name_contains,
    )
    return AoiRsDecorationResult(
        lot_decorated,
        sheet_decorated,
        decoration,
        product_dir / AOI_RS_OOS_DECORATION_FILE_NAME,
        prod_code,
    )
