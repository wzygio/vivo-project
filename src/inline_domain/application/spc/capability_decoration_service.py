"""Application orchestration for SPC CPK/CPM decoration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from src.inline_domain.core.spc.cpk_decoration import (
    CAPABILITY_METRIC_CPK,
    CPK_DECORATION_FILE_NAME,
    apply_capability_decoration,
    build_capability_anomaly_detail,
    merge_capability_detail_with_decoration_flags,
)
from src.inline_domain.application.shared.decoration_defaults import (
    load_capability_decoration,
    persist_capability_decoration,
)
from src.inline_domain.application.shared.decoration_ports import CapabilityDecorationPort


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
    reference_date: date | None = None,
    *,
    decoration_port: CapabilityDecorationPort | None = None,
) -> CpkDecorationResult:
    detail = build_capability_anomaly_detail(
        period_capability_df, metric, reference_date or date.today(),
    )
    persist_decoration = (
        decoration_port.persist_capability_decoration
        if decoration_port is not None
        else persist_capability_decoration
    )
    load_decoration = (
        decoration_port.load_capability_decoration
        if decoration_port is not None
        else load_capability_decoration
    )
    decoration = (
        persist_decoration(product_dir, detail, sheet_name, metric)
        if persist_files
        else merge_capability_detail_with_decoration_flags(
            detail,
            load_decoration(product_dir, sheet_name, metric),
            metric,
        )
    )
    return CpkDecorationResult(
        apply_capability_decoration(period_capability_df, decoration, metric),
        decoration,
        product_dir / CPK_DECORATION_FILE_NAME,
        sheet_name or "Sheet1",
    )


prepare_cpk_decoration = prepare_capability_decoration
