"""Application orchestration for SPC CPK/CPM decoration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.inline_domain.core.spc.cpk_decoration import (
    CAPABILITY_METRIC_CPK,
    apply_capability_decoration,
    build_capability_detail,
    merge_capability_detail_with_decoration_flags,
)
from src.inline_domain.infrastructure.spc.capability_decoration_repository import (
    get_cpk_decoration_path,
    load_capability_decoration,
    persist_capability_decoration,
)


@dataclass(frozen=True)
class CpkDecorationResult:
    period_capability_df: pd.DataFrame
    decoration_df: pd.DataFrame
    decoration_path: Path
    decoration_sheet: str


def prepare_capability_decoration(
    period_capability_df: pd.DataFrame,
    product_dir: Path,
    persist_files: bool = True,
    sheet_name: str | None = None,
    metric: str = CAPABILITY_METRIC_CPK,
) -> CpkDecorationResult:
    detail = build_capability_detail(period_capability_df, metric)
    decoration = (
        persist_capability_decoration(product_dir, detail, sheet_name, metric)
        if persist_files
        else merge_capability_detail_with_decoration_flags(
            detail,
            load_capability_decoration(product_dir, sheet_name, metric),
            metric,
        )
    )
    return CpkDecorationResult(
        apply_capability_decoration(period_capability_df, decoration, metric),
        decoration,
        get_cpk_decoration_path(product_dir),
        sheet_name or "Sheet1",
    )


prepare_cpk_decoration = prepare_capability_decoration
