"""Application orchestration for the automatic-warning Excel read model."""

from __future__ import annotations

from collections.abc import Iterable
from contextvars import ContextVar
from typing import Protocol

import pandas as pd

from src.inline_domain.core.monitor.period_summary import (
    build_period_summary_from_records,
)

ALL_SCOPES = {"SPC", "CTQ", "AOI_TT", "AOI_RS"}
ALL_FACTORIES = {"ARRAY", "OLED", "TP"}


class SummaryWorkbookStore(Protocol):
    def source_signature(self) -> str: ...

    def refresh_current_week(
        self, alerts: pd.DataFrame, *, products: Iterable[str], scope_key: str,
        factory_key: str, as_of: pd.Timestamp, available_types: dict[str, set[str]],
    ) -> tuple[pd.DataFrame, list[str]]: ...


def _selection_key(values: Iterable[str], all_values: set[str]) -> str:
    normalized = {str(value).strip().upper() for value in values if str(value).strip()}
    if normalized == all_values:
        return "ALL"
    return ",".join(sorted(normalized)) if normalized else "NONE"


class MonitorSummaryWorkbookService:
    supports_weekly_replacement = True

    def __init__(self, store: SummaryWorkbookStore) -> None:
        self._store = store
        self._warnings: ContextVar[tuple[str, ...]] = ContextVar(
            f"monitor_summary_warnings_{id(self)}", default=()
        )

    @property
    def last_warnings(self) -> tuple[str, ...]:
        """Warnings belong to this query context, not another Streamlit session."""
        return self._warnings.get()

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
        source_status_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        self._warnings.set(())
        selected_products = tuple(dict.fromkeys(str(value) for value in products))
        scopes = tuple(scopes)
        scope_key = _selection_key(scopes, ALL_SCOPES)
        factory_key = _selection_key(factories, ALL_FACTORIES)
        if not selected_products or scope_key == "NONE" or factory_key == "NONE":
            return build_period_summary_from_records(
                pd.DataFrame(),
                end_date=end_date,
                expected_products=selected_products,
            )
        available_types = {product: set() for product in selected_products}
        required_scopes = {str(scope).lower() for scope in scopes}
        if source_status_df is not None and not source_status_df.empty:
            for product in selected_products:
                for alarm_type in ("OOS", "OOC"):
                    matching = source_status_df[
                        source_status_df["prod_code"].eq(product)
                        & source_status_df["alarm_type"].eq(alarm_type)
                        & source_status_df["source"].eq("excel")
                    ]
                    if required_scopes.issubset(set(matching["scope"].str.lower())):
                        available_types[product].add(alarm_type)
        persisted, warnings = self._store.refresh_current_week(
            alerts_df,
            products=selected_products,
            scope_key=scope_key,
            factory_key=factory_key,
            as_of=end_date,
            available_types=available_types,
        )
        self._warnings.set(tuple(warnings))
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
