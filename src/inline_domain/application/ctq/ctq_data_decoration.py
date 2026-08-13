from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features
from src.inline_domain.core.spc.spc_sheet_oos_decoration import (
    SheetOosDecorationResult,
    prepare_sheet_oos_decoration,
)
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecoratedCtqData:
    """CTQ measurement data after isolated Sheet OOS decoration."""

    raw_measurements_df: pd.DataFrame
    sheet_features_df: pd.DataFrame
    sheet_oos_decoration_result: SheetOosDecorationResult


def resolve_ctq_product_resource_dir(prod_code: str, product_dir: Path | None = None) -> Path:
    """Resolve the CTQ-specific product resource directory."""
    if product_dir is not None:
        return product_dir
    return ConfigLoader.get_project_root() / "resources" / str(prod_code) / "ctq"


def _preprocess_sheet_features_by_type(
    measure_df: pd.DataFrame,
    spec_df: pd.DataFrame,
) -> pd.DataFrame:
    if measure_df.empty:
        return pd.DataFrame()
    if "data_type" not in measure_df.columns:
        return preprocess_sheet_features(measure_df=measure_df, spec_df=spec_df)

    frames: list[pd.DataFrame] = []
    for _, typed_measure_df in measure_df.groupby("data_type", dropna=False):
        features = preprocess_sheet_features(measure_df=typed_measure_df, spec_df=spec_df)
        if not features.empty:
            frames.append(features)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def prepare_decorated_ctq_data(
    raw_measurements_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
    product_dir: Path | None = None,
    persist_decoration: bool = True,
) -> DecoratedCtqData:
    """Apply CTQ-scoped OOS flags without exporting a separate detail workbook.

    Current OOS rows remain an internal matching input for the user-maintained
    decoration workbook; they are not returned or persisted as detail data.
    """
    original_features_df = _preprocess_sheet_features_by_type(raw_measurements_df, spec_df)
    decoration_result = prepare_sheet_oos_decoration(
        raw_measurements_df=raw_measurements_df,
        sheet_features_df=original_features_df,
        product_dir=resolve_ctq_product_resource_dir(prod_code, product_dir),
        persist_files=persist_decoration,
        clip_rules=ConfigLoader.get_spc_sheet_oos_clip_rules(),
    )
    decorated_features_df = _preprocess_sheet_features_by_type(
        decoration_result.raw_measurements_df,
        spec_df,
    )
    logger.info(
        "[CTQ] Sheet OOS decoration prepared for %s: features=%s",
        prod_code,
        len(decorated_features_df),
    )
    return DecoratedCtqData(
        raw_measurements_df=decoration_result.raw_measurements_df,
        sheet_features_df=decorated_features_df,
        sheet_oos_decoration_result=decoration_result,
    )
