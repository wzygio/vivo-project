"""Application orchestration for AOI-RS decoration."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.core.aoi_rs.aoi_rs_decoration import (
    AOI_RS_OOS_DECORATION_FILE_NAME,
    AOI_RS_OOS_KEY_COLUMNS,
    build_aoi_rs_oos_detail,
)
from src.inline_domain.core.aoi_rs.aoi_rs_special_decoration import (
    apply_factory_scoped_decoration,
    build_decorated_period_trend_df,
)
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.core.shared.decoration_window import DecorationWindow
from src.inline_domain.core.shared.sheet_oos_decoration import (
    merge_detail_with_decoration_flags,
)
from src.inline_domain.core.shared.auto_decoration import release_exempt_parameter_flags
from src.inline_domain.application.shared.decoration_defaults import (
    load_sheet_oos_decisions,
    persist_sheet_oos_decoration_outcome,
)
from src.inline_domain.application.shared.decoration_ports import SheetDecorationPort


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
    decoration_port: SheetDecorationPort | None = None,
    rs_details_df: pd.DataFrame | None = None,
    special_factories: Iterable[str] = (),
    pass_through_df: pd.DataFrame | None = None,
    workbook_path: Path | None = None,
    decoration_window: DecorationWindow | None = None,
) -> AoiRsDecorationResult:
    exempt_param_name_contains = tuple(exempt_param_name_contains or ())
    detail = build_aoi_rs_oos_detail(lot_points_df, sheet_points_df, spec_df, prod_code)
    if workbook_path is not None:
        product_dir = Path(workbook_path).parent
    file_name = Path(workbook_path).name if workbook_path is not None else AOI_RS_OOS_DECORATION_FILE_NAME
    if persist:
        persist_outcome = (
            decoration_port.persist_sheet_oos_decoration_outcome
            if decoration_port is not None
            else persist_sheet_oos_decoration_outcome
        )
        outcome = persist_outcome(
            product_dir,
            detail,
            file_name,
            prod_code,
            key_columns=AOI_RS_OOS_KEY_COLUMNS,
            scope=scope,
            prod_code=prod_code,
            product_revision=product_revision,
            decision_signature=decision_signature,
            now=now,
            parameter_column="rs_code",
            exempt_param_name_contains=exempt_param_name_contains,
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
            file_name,
            prod_code,
            AOI_RS_OOS_KEY_COLUMNS,
        )
        decoration = merge_detail_with_decoration_flags(
            detail,
            decisions,
            AOI_RS_OOS_KEY_COLUMNS,
        )
    decoration = release_exempt_parameter_flags(
        decoration, "rs_code", exempt_param_name_contains,
    )
    lot_decorated, sheet_decorated = apply_factory_scoped_decoration(
        lot_points_df,
        sheet_points_df,
        spec_df,
        prod_code,
        decoration,
        exempt_param_name_contains,
        rs_details_df=rs_details_df,
        special_factories=special_factories,
        decoration_window=decoration_window,
    )
    return AoiRsDecorationResult(
        lot_decorated,
        sheet_decorated,
        decoration,
        product_dir / file_name,
        prod_code,
    )


def build_aoi_rs_period_trend(
    rs_details_df: pd.DataFrame, pass_through_df: pd.DataFrame, end_date: date,
) -> pd.DataFrame:
    """Build page/PDF trends with the configured factory scope."""
    return build_decorated_period_trend_df(
        rs_details_df, pass_through_df, end_date,
        special_factories=ConfigLoader.get_aoi_rs_special_decoration_factories(),
        decoration_window=ConfigLoader.get_aoi_rs_special_decoration_window(),
    )
