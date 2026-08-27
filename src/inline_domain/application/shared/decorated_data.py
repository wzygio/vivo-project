"""Unified scope-driven Sheet OOS decoration entry for the inline domain.

Replaces the former per-module wrappers (``spc_data_decoration.py`` /
``ctq_data_decoration.py``): the only difference between the SPC and CTQ
decoration calibres was the workbook file name, so a single scope-parameterized
entry now serves both (see ``docs/dev_docs/generated/Inline_domain/``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features
from src.inline_domain.core.shared.sheet_oos_decoration import (
    OOS_DECORATION_FILE_NAME,
    SheetOosDecorationResult,
    prepare_sheet_oos_decoration,
)
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)

CTQ_OOS_DECORATION_FILE_NAME = "ctq_sheet_oos_decoration.xlsx"

# scope -> 用户维护的修饰工作簿文件名（resources/ 根目录，每产品一个 sheet）
SCOPE_DECORATION_FILE_NAME = {
    "spc": OOS_DECORATION_FILE_NAME,
    "ctq": CTQ_OOS_DECORATION_FILE_NAME,
}


@dataclass(frozen=True)
class DecoratedData:
    """Measurement data after scope-scoped Sheet OOS decoration."""

    raw_measurements_df: pd.DataFrame
    sheet_features_df: pd.DataFrame
    sheet_oos_decoration_result: SheetOosDecorationResult


def resolve_product_resource_dir(prod_code: str, product_dir: Path | None = None) -> Path:
    """Resolve the shared resources directory used by the per-sheet decoration workbooks.

    Decoration workbooks live at ``resources/`` root with one sheet per product;
    ``product_dir`` stays an explicit override for tests.
    """
    if product_dir is not None:
        return product_dir
    return ConfigLoader.get_project_root() / "resources"


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


def prepare_decorated_data(
    raw_measurements_df: pd.DataFrame,
    spec_df: pd.DataFrame,
    prod_code: str,
    scope: str,
    product_dir: Path | None = None,
    persist: bool = True,
    *,
    product_revision: str = "",
    decision_signature: str = "",
) -> DecoratedData:
    """Apply the scope's tri-state Sheet actions and recompute Sheet features.

    The user-maintained decoration sheet (named after the product) is matched
    against the original out-of-spec Sheets: ``flag=Delete`` removes the matching
    product/station/parameter/Sheet points, ``True`` clips OOS points and
    ``False`` preserves their real values. ``scope`` only selects the workbook
    (``SCOPE_DECORATION_FILE_NAME``); the engine and flag semantics are shared.

    ``product_revision``/``decision_signature`` 透传到 core 刷新门控：
    相同 revision + 相同决策签名 + 距成功写入不足 4h 时不重写工作簿；
    缺省为空字符串时 core 自行从决策台账计算签名，门控仍然生效。
    """
    normalized_scope = (scope or "").strip().lower()
    if normalized_scope not in SCOPE_DECORATION_FILE_NAME:
        raise ValueError(f"unknown decoration scope: {scope!r}")

    original_features_df = _preprocess_sheet_features_by_type(raw_measurements_df, spec_df)
    decoration_result = prepare_sheet_oos_decoration(
        raw_measurements_df=raw_measurements_df,
        sheet_features_df=original_features_df,
        product_dir=resolve_product_resource_dir(prod_code, product_dir),
        persist_files=persist,
        clip_rules=ConfigLoader.get_spc_sheet_oos_clip_rules(),
        decoration_file_name=SCOPE_DECORATION_FILE_NAME[normalized_scope],
        decoration_sheet_name=prod_code,
        scope=normalized_scope,
        prod_code=prod_code,
        product_revision=product_revision,
        decision_signature=decision_signature,
    )
    decorated_features_df = _preprocess_sheet_features_by_type(
        decoration_result.raw_measurements_df,
        spec_df,
    )
    logger.info(
        "[shared] Sheet OOS decoration prepared for %s (scope=%s): decorated_features=%s",
        prod_code,
        normalized_scope,
        len(decorated_features_df),
    )
    return DecoratedData(
        raw_measurements_df=decoration_result.raw_measurements_df,
        sheet_features_df=decorated_features_df,
        sheet_oos_decoration_result=decoration_result,
    )
