"""Monitor data_type -> decoration scope routing tests (D2/D3, 段2).

The monitor fetches ALL prepared measurements per product, groups them by
``data_type`` and routes each group to the shared decorated-feature pipeline:
SPC -> scope 'spc', CTQ -> scope 'ctq', AOI -> scope 'none'.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.inline_domain.application.monitor import monitor_service
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.shared.decorated_features import fetch_decorated_features
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from src.inline_domain.core.monitor.monitor_calculator import preprocess_sheet_features

PROD = "M999"


@pytest.fixture(autouse=True)
def _clear_caches():
    MonitorAnalysisService.fetch_dashboard_data_dict.clear()
    fetch_decorated_features.clear()
    yield
    MonitorAnalysisService.fetch_dashboard_data_dict.clear()
    fetch_decorated_features.clear()


class _MixedTypeRepository:
    """One OOC sheet per data_type (SPC/CTQ/AOI): mean 55 > ucl 54, < usl 60."""

    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_spc_measurements(
        self, config: SpcQueryConfig, force_refresh: bool = False
    ) -> pd.DataFrame:
        rows = []
        for param_name, data_type in (
            ("SPC_PARAM", "SPC"),
            ("CTQ_PARAM", "CTQ"),
            ("AOI_PARAM", "AOI"),
        ):
            for site_name in ("P1", "P2"):
                rows.append(
                    {
                        "factory": "ARRAY",
                        "prod_code": config.prod_code,
                        "sheet_start_time": "2026-08-08 09:00:00",
                        "sheet_id": f"S_{data_type}",
                        "step_id": "100",
                        "param_name": param_name,
                        "site_name": site_name,
                        "param_value": 55.0,
                        "data_type": data_type,
                    }
                )
        return pd.DataFrame(rows)

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
                    "step_id": "100",
                    "param_name": param_name,
                    "usl": 60.0,
                    "lsl": 40.0,
                    "ucl": 54.0,
                    "lcl": 46.0,
                    "target": 50.0,
                }
                for param_name in ("SPC_PARAM", "CTQ_PARAM", "AOI_PARAM")
            ]
        )


def _spy_fetch(recorded: list[tuple[str, str]]):
    """Passthrough spy: records the routed scope, computes undecorated features."""

    def spy(
        _features_source,
        prod_code: str,
        scope: str,
        start_date: str,
        end_date: str,
        snapshot_signature: str = "",
    ) -> dict:
        recorded.append((prod_code, scope))
        measurements_df = _features_source.get_spc_measurements(None)
        spec_df = _features_source.get_spc_spec_limits(prod_code)
        features_df = preprocess_sheet_features(
            measure_df=measurements_df, spec_df=spec_df
        )
        return {
            "sheet_features_df": features_df,
            "original_sheet_features_df": features_df,
            "raw_measurements_df": measurements_df,
            "original_raw_measurements_df": measurements_df,
            "spec_empty": spec_df.empty,
            "sheet_oos_decoration": None,
        }

    return spy


def _query() -> SpcQueryConfig:
    return SpcQueryConfig(
        prod_code=PROD,
        start_date="2026-06-01",
        end_date="2026-08-10",
        data_type_filter="SPC",
    )


def test_dashboard_routes_each_data_type_to_its_decoration_scope(monkeypatch) -> None:
    recorded: list[tuple[str, str]] = []
    monkeypatch.setattr(monitor_service, "fetch_decorated_features", _spy_fetch(recorded))
    MonitorAnalysisService.set_analysis_end_date(datetime(2026, 8, 10))
    try:
        result = MonitorAnalysisService.fetch_dashboard_data_dict(
            _repository_factory=lambda _prod: _MixedTypeRepository(),
            query_config_json=_query().model_dump_json(),
            time_type="MIXED",
            data_type_filter="SPC",
            snapshot_signature="monitor-scope-routing",
        )
    finally:
        MonitorAnalysisService.set_analysis_end_date(None)

    # D2/D3: SPC 组 -> 'spc'，CTQ 组 -> 'ctq'，AOI 组 -> 'none'。
    assert sorted(recorded) == [(PROD, "ctq"), (PROD, "none"), (PROD, "spc")]

    # 各组特征 concat 后进入既有聚合逻辑，三种 data_type 都在站点明细中。
    station_detail_df = result["station_detail_df"]
    assert set(station_detail_df["data_type"]) == {"SPC", "CTQ", "AOI"}


def test_defect_details_uses_the_same_scope_routing(monkeypatch) -> None:
    recorded: list[tuple[str, str]] = []
    monkeypatch.setattr(monitor_service, "fetch_decorated_features", _spy_fetch(recorded))
    MonitorAnalysisService.set_analysis_end_date(datetime(2026, 8, 10))
    try:
        MonitorAnalysisService.get_monitor_defect_details(
            _repository_factory=lambda _prod: _MixedTypeRepository(),
            query_config_json=_query().model_dump_json(),
            time_group="ALL",
            defect_type="OOS",
            time_type="MIXED",
            data_type_filter="SPC",
        )
    finally:
        MonitorAnalysisService.set_analysis_end_date(None)

    # 下钻路径与大盘共享同一路由（原内联取数/修饰副本已删除）。
    assert sorted(recorded) == [(PROD, "ctq"), (PROD, "none"), (PROD, "spc")]
