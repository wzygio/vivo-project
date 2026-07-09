from datetime import datetime
from pathlib import Path

import pandas as pd

from src.spc_domain.application import spc_data_decoration, spc_service
from src.spc_domain.application.spc_service import SpcAnalysisService
from src.spc_domain.core.cpm_sheet_oos_decoration import OOS_DETAIL_FILE_NAME
from src.spc_domain.infrastructure.data_loader import SpcQueryConfig


class FakeAutoWarningRepository:
    def __init__(self, snapshot_dir: Path, use_snapshot: bool, db_manager: object) -> None:
        self.snapshot_dir = snapshot_dir
        self.use_snapshot = use_snapshot
        self.db_manager = db_manager

    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame()

    def get_spc_measurements(self, config: SpcQueryConfig, force_refresh: bool = False) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "factory": "OLED",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-07-01 08:00:00",
                    "sheet_id": "S1",
                    "step_id": "21200",
                    "param_name": "PPA_B_X",
                    "site_name": "P1",
                    "unit_id": "3CEE01-PPA",
                    "param_value": 8.0,
                    "data_type": "SPC",
                },
                {
                    "factory": "OLED",
                    "prod_code": config.prod_code,
                    "sheet_start_time": "2026-07-01 08:00:00",
                    "sheet_id": "S1",
                    "step_id": "21200",
                    "param_name": "PPA_B_X",
                    "site_name": "P2",
                    "unit_id": "3CEE01-PPA",
                    "param_value": 0.0,
                    "data_type": "SPC",
                },
            ]
        )

    def get_spc_spec_limits(self, prod_code: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "prod_code": prod_code,
                    "step_id": "21200",
                    "param_name": "PPA_B_X",
                    "usl": 6.0,
                    "lsl": -6.0,
                    "ucl": 3.0,
                    "lcl": -3.0,
                    "target": 0.0,
                }
            ]
        )


def test_auto_warning_dashboard_uses_decorated_spc_features(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(spc_service, "SpcRepository", FakeAutoWarningRepository)
    monkeypatch.setattr(
        spc_data_decoration.ConfigLoader,
        "get_project_root",
        staticmethod(lambda: tmp_path),
    )
    monkeypatch.setattr(
        SpcAnalysisService,
        "get_time_window",
        classmethod(lambda cls: (datetime(2026, 7, 1), datetime(2026, 7, 7))),
    )
    monkeypatch.setattr(spc_service, "sanitize_to_compliant", lambda df, *args, **kwargs: df)
    SpcAnalysisService.fetch_dashboard_data_dict.clear()

    query = SpcQueryConfig(
        prod_code="Z571",
        start_date="2026-07-01",
        end_date="2026-07-07",
        data_type_filter="ALL",
    )

    result = SpcAnalysisService.fetch_dashboard_data_dict(
        _db_manager=object(),
        query_config_json=query.model_dump_json(),
        time_type="MIXED",
        force_compliant=False,
        data_type_filter="ALL",
        snapshot_signature="auto-warning-decoration-test",
    )

    detail_df = result["detail_df"]
    station_detail_df = result["station_detail_df"]

    assert (tmp_path / "resources" / "Z571" / OOS_DETAIL_FILE_NAME).exists()
    assert station_detail_df.empty
    assert detail_df[["OOS片数", "OOC片数", "SOOS片数"]].fillna(0).sum().sum() == 0
