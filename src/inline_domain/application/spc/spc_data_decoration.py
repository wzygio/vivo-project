from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.shared_kernel.config import ConfigLoader
from src.inline_domain.core.spc.spc_sheet_oos_decoration import (
    SheetOosDecorationResult,
    prepare_sheet_oos_decoration,
)
from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecoratedSpcData:
    """SPC measurement data after product-scoped Sheet OOS decoration."""

    raw_measurements_df: pd.DataFrame
    sheet_features_df: pd.DataFrame
    original_sheet_features_df: pd.DataFrame
    sheet_oos_decoration_result: SheetOosDecorationResult


def resolve_product_resource_dir(prod_code: str, product_dir: Path | None = None) -> Path:
    """Resolve the product-scoped resources directory used by decoration files."""
    if product_dir is not None:
        return product_dir
    return ConfigLoader.get_project_root() / "resources" / str(prod_code)


def _preprocess_sheet_features_by_type(measure_df: pd.DataFrame, spec_df: pd.DataFrame) -> pd.DataFrame:
    """Preserve the existing data_type isolation used by the auto-warning service."""
    if measure_df.empty:
        return pd.DataFrame()

    if "data_type" not in measure_df.columns:
        return preprocess_sheet_features(measure_df=measure_df, spec_df=spec_df)

    frames: list[pd.DataFrame] = []
    for _, typed_measure_df in measure_df.groupby("data_type", dropna=False):
        features = preprocess_sheet_features(measure_df=typed_measure_df, spec_df=spec_df)
        if not features.empty:
            frames.append(features)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def prepare_decorated_spc_data(
    raw_measurements_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
    product_dir: Path | None = None,
    persist_files: bool = True,
) -> DecoratedSpcData:
    """
    Build product OOS files, apply flag-controlled point clipping, and recompute Sheet features.

    The detail/flag files are generated from the original raw measurements so users can audit
    the true out-of-spec Sheets. Downstream reports receive recomputed features from the
    decorated point data, making CPM/CPK and auto-warning views share the same backend contract.
    """
    resolved_product_dir = resolve_product_resource_dir(prod_code, product_dir)
    original_features_df = _preprocess_sheet_features_by_type(raw_measurements_df, spec_df)

    decoration_result = prepare_sheet_oos_decoration(
        raw_measurements_df=raw_measurements_df,
        sheet_features_df=original_features_df,
        product_dir=resolved_product_dir,
        persist_files=persist_files,
        clip_rules=ConfigLoader.get_spc_sheet_oos_clip_rules(),
    )

    decorated_features_df = _preprocess_sheet_features_by_type(
        decoration_result.raw_measurements_df,
        spec_df,
    )

    logger.info(
        "[SPC] Sheet OOS decoration prepared for %s: raw_features=%s, decorated_features=%s, detail=%s",
        prod_code,
        len(original_features_df),
        len(decorated_features_df),
        len(decoration_result.detail_df),
    )
    return DecoratedSpcData(
        raw_measurements_df=decoration_result.raw_measurements_df,
        sheet_features_df=decorated_features_df,
        original_sheet_features_df=original_features_df,
        sheet_oos_decoration_result=decoration_result,
    )
