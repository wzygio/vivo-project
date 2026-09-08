"""Live CPK computation with cache-only intermediates and an Excel read model."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandas as pd
import streamlit as st

from src.inline_domain.application.monitor.summary_workbook_service import (
    ALL_FACTORIES, _selection_key,
)
from src.inline_domain.application.shared.decorated_data import resolve_product_resource_dir
from src.inline_domain.application.spc.capability_decoration_service import prepare_capability_decoration
from src.inline_domain.application.spc.spc_service import exclude_cpm_cpk_parameters
from src.inline_domain.core.monitor.cpk_summary import (
    CPK_DETAIL_COLUMNS, CPK_SUMMARY_COLUMNS, CPK_THRESHOLD,
    build_cpk_summary_from_records, build_current_cpk_detail, build_current_cpk_records,
)
from src.inline_domain.core.monitor.period_summary import current_period_windows
from src.shared_kernel.config import ConfigLoader


class CpkInputSource(Protocol):
    def source_signature(self, products: Iterable[str], scopes: Iterable[str]) -> object: ...

    def read_capability_inputs(
        self, prod_code: str, start_date: object, end_date: object
    ) -> dict[str, object]: ...


class CpkWorkbookStore(Protocol):
    def source_signature(self) -> str: ...

    def upsert_current_periods(
        self, rows: pd.DataFrame, *, as_of: pd.Timestamp
    ) -> pd.DataFrame: ...


@dataclass(frozen=True)
class CpkMonitorViewModel:
    summary_df: pd.DataFrame
    detail_df: pd.DataFrame


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=32, show_spinner=False)
def _cached_cpk_inputs(
    source_signature: object, products: tuple[str, ...], factories: tuple[str, ...],
    end_date: str, exemptions: tuple[str, ...], resource_dir: str | None,
    *, _source: CpkInputSource,
) -> dict[str, pd.DataFrame]:
    """Cache native frames only; configuration signatures invalidate computations."""
    as_of = pd.Timestamp(end_date)
    start = min(window.start for window in current_period_windows(as_of))
    frames = []
    for product in products:
        payload = _source.read_capability_inputs(
            product, start.date().isoformat(), as_of.date().isoformat(),
        )
        if payload.get("spec_empty", False):
            raise ValueError(f"{product} 的 SPC 规格为空，无法生成 CPK 汇总")
        features = payload.get("sheet_features_df")
        if not isinstance(features, pd.DataFrame):
            raise ValueError(f"{product} 的 CPK 输入缺少 Sheet 特征")
        features = exclude_cpm_cpk_parameters(features, exemptions)
        if not features.empty:
            features = features[features["factory"].isin(factories)].copy()
        detail = build_current_cpk_detail(features, end_date=as_of)
        decoration = prepare_capability_decoration(
            detail, resolve_product_resource_dir(
                product, Path(resource_dir) if resource_dir else None,
            ), persist_files=False, sheet_name=product, metric="cpk",
        )
        if not decoration.period_capability_df.empty:
            frames.append(decoration.period_capability_df)
    detail = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=CPK_DETAIL_COLUMNS)
    detail["status"] = detail["cpk"].map(
        lambda value: "无法计算" if pd.isna(value) else ("达标" if value >= CPK_THRESHOLD else "预警")
    )
    return {"detail_df": detail}


class CpkMonitorService:
    def __init__(
        self, source: CpkInputSource, store: CpkWorkbookStore,
        *, product_resource_root: Path | None = None,
    ) -> None:
        self._source = source
        self._store = store
        self._resource_dir = str(product_resource_root) if product_resource_root else None

    def build_dashboard(
        self, *, products: Iterable[str], factories: Iterable[str],
        end_date: object | None = None,
    ) -> CpkMonitorViewModel:
        selected_products = tuple(sorted(set(products)))
        selected_factories = tuple(sorted(set(factories)))
        as_of = pd.Timestamp(end_date if end_date is not None else pd.Timestamp.today()).normalize()
        if not selected_products or not selected_factories:
            return CpkMonitorViewModel(
                build_cpk_summary_from_records(pd.DataFrame(columns=CPK_SUMMARY_COLUMNS), end_date=as_of),
                pd.DataFrame(columns=[*CPK_DETAIL_COLUMNS, "status"]),
            )
        payload = _cached_cpk_inputs(
            self._source.source_signature(selected_products, ("spc",)),
            selected_products, selected_factories, as_of.date().isoformat(),
            tuple(ConfigLoader.get_spc_capability_param_exemptions()), self._resource_dir,
            _source=self._source,
        )
        factory_key = _selection_key(selected_factories, ALL_FACTORIES)
        rows = build_current_cpk_records(
            payload["detail_df"], products=selected_products,
            factory_key=factory_key, end_date=as_of,
        )
        persisted = self._store.upsert_current_periods(rows, as_of=as_of)
        selected = persisted[persisted["产品"].isin(selected_products)
                             & persisted["监控类型"].eq("SPC")
                             & persisted["厂别"].eq(factory_key)]
        return CpkMonitorViewModel(
            build_cpk_summary_from_records(selected, end_date=as_of, expected_products=selected_products),
            payload["detail_df"].copy(),
        )
