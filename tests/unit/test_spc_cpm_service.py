from pathlib import Path

import pandas as pd

from src.spc_domain.application import cpm_service
from src.spc_domain.application.cpm_service import CpmReportService
from src.spc_domain.infrastructure.data_loader import SpcQueryConfig


class FakeSpcRepository:
    seen_data_type_filters: list[str] = []

    def __init__(self, snapshot_dir: Path, use_snapshot: bool, db_manager: object) -> None:
        self.snapshot_dir = snapshot_dir
        self.use_snapshot = use_snapshot
        self.db_manager = db_manager

    def get_spc_measurements(self, config: SpcQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        self.seen_data_type_filters.append(config.data_type_filter or "")
        return pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-06-01",
                    "sheet_id": "LOT00000101",
                    "step_id": "S1",
                    "param_name": "THK",
                    "site_name": "P1",
                    "param_value": 49.0,
                    "data_type": "SPC",
                },
                {
                    "factory": "ARRAY",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-06-02",
                    "sheet_id": "LOT00000102",
                    "step_id": "S1",
                    "param_name": "THK",
                    "site_name": "P1",
                    "param_value": 50.0,
                    "data_type": "SPC",
                },
            ]
        )

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
                    "step_id": "S1",
                    "param_name": "THK",
                    "usl": 55.0,
                    "lsl": 45.0,
                    "ucl": 54.0,
                    "lcl": 46.0,
                    "target": 50.0,
                }
            ]
        )


def test_cpm_service_requests_spc_only_and_returns_lot_report(monkeypatch) -> None:
    FakeSpcRepository.seen_data_type_filters = []
    monkeypatch.setattr(cpm_service, "SpcRepository", FakeSpcRepository)

    query = SpcQueryConfig(
        prod_code="M626",
        start_date="2026-06-01",
        end_date="2026-06-07",
        data_type_filter="CTQ",
    )

    report = CpmReportService.get_cpm_report_data(
        _db_manager=object(),
        query_config_json=query.model_dump_json(),
        snapshot_signature="unit-test",
    )

    assert FakeSpcRepository.seen_data_type_filters == ["SPC"]
    assert len(report.lot_cpm_df) == 1
    assert report.lot_cpm_df.loc[0, "lot_id"] == "LOT000001"
    assert "cpk" in report.lot_cpm_df.columns
    assert set(report.sheet_measurements_df["data_type"]) == {"SPC"}
