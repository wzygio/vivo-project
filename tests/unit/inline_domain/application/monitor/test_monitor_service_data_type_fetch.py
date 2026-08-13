from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.inline_domain.application.spc import spc_service
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.inline_domain.application.spc.dtos import SpcQueryConfig
from inline_domain.infrastructure.spc.spc_repository import SpcRepository


class _FakeRepository:
    requested_data_types: list[str] = []

    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def get_spc_measurements(self, config: SpcQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        self.requested_data_types.append(config.data_type_filter or "")
        return pd.DataFrame()

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame()


class _SpcOnlyFakeRepository(_FakeRepository):
    requested_data_types: list[str] = []


def test_monitor_service_fetches_all_parameter_types_for_a_ctq_view() -> None:
    MonitorAnalysisService.fetch_dashboard_data_dict.clear()
    _FakeRepository.requested_data_types = []
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="CTQ",
    )

    MonitorAnalysisService.fetch_dashboard_data_dict(
        _repository_factory=lambda _prod: _FakeRepository(),
        query_config_json=query.model_dump_json(),
        data_type_filter="CTQ",
        snapshot_signature="monitor-fetches-all-types",
    )

    assert _FakeRepository.requested_data_types == ["ALL"]


def test_spc_service_fetches_only_spc_parameters() -> None:
    SpcReportService.fetch_spc_report_payload.clear()
    _SpcOnlyFakeRepository.requested_data_types = []
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="CTQ",
    )

    SpcReportService.fetch_spc_report_payload(
        _data_port=_SpcOnlyFakeRepository(Path("data"), True, object()),
        query_config_json=query.model_dump_json(),
        snapshot_signature="spc-fetches-spc-only",
    )

    assert _SpcOnlyFakeRepository.requested_data_types == ["SPC"]


def test_repository_filters_by_whitelist_parameter_data_type() -> None:
    measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "start_time": "2026-06-01",
                "sheet_id": "S1",
                "step_id": "ANY_STEP",
                "param_name": "SPC_PARAM",
                "site_name": "P1",
                "unit_id": "EQ-1",
                "param_value": 1.0,
            },
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "start_time": "2026-06-01",
                "sheet_id": "S2",
                "step_id": "ANY_STEP",
                "param_name": "CTQ_PARAM",
                "site_name": "P1",
                "unit_id": "EQ-1",
                "param_value": 1.0,
            },
        ]
    )
    whitelist = pd.DataFrame(
        {
            "ref_param_name": ["SPC_PARAM", "CTQ_PARAM"],
            "data_type": ["SPC", "CTQ"],
        }
    )
    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-01",
        data_type_filter="SPC",
    )

    class Raw:
        def get_measurements(self, prod_code, end_date, force_refresh=False):
            return measurements

    class Metadata:
        def get_parameter_catalog(self, prod_code):
            return whitelist

        def get_parameter_specs(self, prod_code):
            return pd.DataFrame()

    class History:
        def get_main_process_history(self, routed, history_start, history_end):
            return pd.DataFrame()

    with patch.object(
        SpcRepository,
        "_apply_outlier_filters",
        lambda self, df, prod_code: df,
    ):
        result = SpcRepository(Raw(), Metadata(), History()).get_spc_measurements(query)

    assert result["param_name"].tolist() == ["SPC_PARAM"]
