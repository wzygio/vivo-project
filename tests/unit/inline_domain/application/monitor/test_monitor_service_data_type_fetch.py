from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.inline_domain.application.monitor import monitor_service
from src.inline_domain.application.spc import spc_service
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.inline_domain.infrastructure.spc.data_loader import SpcQueryConfig
import src.inline_domain.infrastructure.spc.repositories.spc_repository as spc_repository
from src.inline_domain.infrastructure.spc.repositories.spc_repository import SpcRepository


class _FakeRepository:
    requested_data_types: list[str] = []

    def __init__(self, snapshot_dir: Path, use_snapshot: bool, db_manager: object) -> None:
        self.snapshot_dir = snapshot_dir

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

    with patch.object(monitor_service, "SpcRepository", _FakeRepository):
        MonitorAnalysisService.fetch_dashboard_data_dict(
            _db_manager=object(),
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

    with patch.object(spc_service, "SpcRepository", _SpcOnlyFakeRepository):
        SpcReportService.fetch_spc_report_payload(
            _db_manager=object(),
            query_config_json=query.model_dump_json(),
            snapshot_signature="spc-fetches-spc-only",
        )

    assert _SpcOnlyFakeRepository.requested_data_types == ["SPC"]


def test_repository_filters_by_whitelist_parameter_data_type(tmp_path: Path) -> None:
    measurements = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": "2026-06-01",
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
                "sheet_start_time": "2026-06-01",
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

    with (
        patch.object(spc_repository, "load_spc_measurements", return_value=measurements),
        patch.object(spc_repository, "load_param_whitelist", return_value=whitelist),
        patch.object(SpcRepository, "_apply_outlier_filters", lambda self, df, prod_code: df),
        patch.object(spc_repository, "export_probed_details"),
    ):
        result = SpcRepository(snapshot_dir=tmp_path, use_snapshot=False, db_manager=object()).get_spc_measurements(
            query
        )

    assert result["param_name"].tolist() == ["SPC_PARAM"]
