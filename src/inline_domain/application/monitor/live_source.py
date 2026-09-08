"""Configuration-aware, memory-only intermediate results shared by warning boards."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path

import pandas as pd
import streamlit as st

from src.inline_domain.application.shared.decorated_data import prepare_decorated_data
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.inline_domain.application.aoi_rs.dtos import AoiRsQueryConfig
from src.inline_domain.core.aoi_tt.aoi_tt_decoration import build_aoi_tt_oos_detail
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    build_lot_point_df, build_sheet_point_df,
)
from src.inline_domain.core.aoi_rs.aoi_rs_decoration import build_aoi_rs_oos_detail
from src.inline_domain.core.shared.sheet_ooc_decoration import (
    build_sheet_ooc_detail, build_aoi_tt_ooc_detail, build_aoi_rs_ooc_detail,
)
from src.inline_domain.core.shared.throughput_facts import build_daily_throughput_facts
from src.shared_kernel.config import ConfigLoader


def monitor_date_window() -> tuple[str, str]:
    today = pd.Timestamp.today().normalize()
    start = min(today.replace(month=1, day=1), today - pd.Timedelta(days=today.weekday()))
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


@st.cache_data(show_spinner=False, max_entries=32, ttl=ConfigLoader.get_cache_ttl_seconds())
def _cached_live_payload(
    _source: "LiveMonitorSource", product: str, scope: str,
    start_date: str, end_date: str, input_signature: str,
) -> dict[str, object]:
    del input_signature
    payload = _source.compute(product, scope, start_date, end_date)
    return {**payload, "computed_at": pd.Timestamp.now().isoformat()}


class LiveMonitorSource:
    """Read raw snapshots through existing ports and reapply current configurations."""

    def __init__(
        self, *, port_factory: Callable, signature_provider: Callable,
        resource_dir_provider: Callable,
    ) -> None:
        self._port_factory = port_factory
        self._signature_provider = signature_provider
        self._resource_dir_provider = resource_dir_provider

    def source_signature(self, products: Iterable[str], scopes: Iterable[str]) -> str:
        return f"live-v1:{','.join(sorted(scopes))}:{self._signature_provider(products)}"

    def read_payload(
        self, product: str, scope: str, start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, object]:
        default_start, default_end = monitor_date_window()
        return _cached_live_payload(
            self, product, scope,
            pd.Timestamp(start_date or default_start).date().isoformat(),
            pd.Timestamp(end_date or default_end).date().isoformat(),
            self.source_signature([product], [scope]),
        )

    def read_capability_inputs(
        self, prod_code: str, start_date: str, end_date: str,
    ) -> dict[str, object]:
        return self.read_payload(prod_code, "spc", start_date, end_date)

    def compute(
        self, product: str, scope: str, start_date: str, end_date: str,
    ) -> dict[str, object]:
        port = self._port_factory(scope, product)
        query_args = dict(prod_code=product, start_date=start_date, end_date=end_date)
        if scope in {"spc", "ctq"}:
            query = SpcQueryConfig(**query_args, data_type_filter=scope.upper())
            raw = port.get_spc_measurements(query)
            specs = port.get_spc_spec_limits(product)
            if not raw.empty and specs.empty:
                raise ValueError(f"{product}/{scope}: specifications are unavailable")
            decorated = prepare_decorated_data(
                raw, specs, product, scope,
                product_dir=Path(self._resource_dir_provider(scope)), persist=False,
            )
            original = decorated.original_sheet_features_df
            original = original if original is not None else pd.DataFrame()
            return {
                "oos": decorated.sheet_oos_decoration_result.decoration_df,
                "ooc": build_sheet_ooc_detail(original),
                "throughput": self._throughput(original, scope, product, "sheet_start_time"),
                "sheet_features_df": decorated.sheet_features_df,
                "raw_measurements_df": decorated.raw_measurements_df,
                "spec_empty": specs.empty,
            }
        if scope == "aoi_tt":
            details = port.get_tt_details(AoiTtQueryConfig(**query_args))
            specs = port.get_tt_spec_limits(product)
            if not details.empty and specs.empty:
                raise ValueError(f"{product}/{scope}: specifications are unavailable")
            return {
                "oos": build_aoi_tt_oos_detail(details, specs),
                "ooc": build_aoi_tt_ooc_detail(details, specs),
                "throughput": self._throughput(details, scope, product, "start_time"),
            }
        if scope == "aoi_rs":
            query = AoiRsQueryConfig(**query_args)
            details, passed = port.get_rs_details(query), port.get_pass_through(query)
            specs = port.get_rs_spec_limits(product)
            if not details.empty and specs.empty:
                raise ValueError(f"{product}/{scope}: specifications are unavailable")
            return {
                "oos": build_aoi_rs_oos_detail(
                    build_lot_point_df(details, passed), build_sheet_point_df(details), specs, product,
                ),
                "ooc": build_aoi_rs_ooc_detail(),
                "throughput": self._throughput(passed, scope, product, "start_time"),
            }
        raise ValueError(f"Unsupported Inline scope: {scope}")

    @staticmethod
    def _throughput(frame, scope, product, time_column) -> pd.DataFrame:
        return build_daily_throughput_facts(
            frame, scope=scope, prod_code=product,
            event_time_column=time_column, item_id_column="sheet_id",
        )


class LiveThroughputReader:
    def __init__(self, source: LiveMonitorSource) -> None:
        self._source = source

    def read_product(self, scope: str, prod_code: str) -> pd.DataFrame:
        return self._source.read_payload(prod_code, scope)["throughput"]

    def source_signature(self, products, scopes) -> str:
        return self._source.source_signature(products, scopes)


def get_monitor_computation_caches() -> list:
    """Expose both intermediate caches to the page's explicit refresh action."""
    from src.inline_domain.application.monitor.cpk_monitor_service import _cached_cpk_inputs

    return [_cached_live_payload, _cached_cpk_inputs]
