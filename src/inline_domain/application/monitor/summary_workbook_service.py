"""Application orchestration for the automatic-warning Excel read model."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

import pandas as pd

from src.inline_domain.core.monitor.period_summary import (
    build_current_period_records,
    build_period_summary_from_records,
)

ALL_SCOPES = {"SPC", "CTQ", "AOI_TT", "AOI_RS"}
ALL_FACTORIES = {"ARRAY", "OLED", "TP"}


class SummaryWorkbookStore(Protocol):
    def source_signature(self) -> str: ...

    def upsert_current_periods(
        self, rows: pd.DataFrame, *, as_of: pd.Timestamp
    ) -> pd.DataFrame: ...


def _selection_key(values: Iterable[str], all_values: set[str]) -> str:
    normalized = {str(value).strip().upper() for value in values if str(value).strip()}
    if normalized == all_values:
        return "ALL"
    return ",".join(sorted(normalized)) if normalized else "NONE"


class MonitorSummaryWorkbookService:
    def __init__(self, store: SummaryWorkbookStore) -> None:
        self._store = store

    def source_signature(self) -> str:
        return self._store.source_signature()

    def refresh_summary(
        self,
        *,
        products: Iterable[str],
        scopes: Iterable[str],
        factories: Iterable[str],
        alerts_df: pd.DataFrame,
        throughput_df: pd.DataFrame,
        end_date: pd.Timestamp,
    ) -> pd.DataFrame:
        selected_products = tuple(dict.fromkeys(str(value) for value in products))
        scope_key = _selection_key(scopes, ALL_SCOPES)
        factory_key = _selection_key(factories, ALL_FACTORIES)
        if not selected_products or scope_key == "NONE" or factory_key == "NONE":
            return build_period_summary_from_records(
                pd.DataFrame(),
                end_date=end_date,
                expected_products=selected_products,
            )
        current_rows = build_current_period_records(
            alerts_df,
            throughput_df,
            products=selected_products,
            scope_key=scope_key,
            factory_key=factory_key,
            end_date=end_date,
        )
        persisted = self._store.upsert_current_periods(current_rows, as_of=end_date)
        selected = persisted[
            persisted["产品"].astype(str).isin(selected_products)
            & persisted["监控类型"].astype(str).eq(scope_key)
            & persisted["厂别"].astype(str).eq(factory_key)
        ]
        return build_period_summary_from_records(
            selected,
            end_date=end_date,
            expected_products=selected_products,
        )


__all__ = ["MonitorSummaryWorkbookService"]
