from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.application.spc.dtos import SpcQueryConfig
import src.inline_domain.infrastructure.spc.repositories.spc_repository as spc_repository
from src.inline_domain.infrastructure.spc.repositories.spc_repository import SpcRepository


class DummyDbManager:
    engine = object()


def _snapshot_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": datetime(2026, 6, 30),
                "sheet_id": "S1",
                "step_id": "10140",
                "param_name": "PPA_B_X",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 1.0,
            },
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": datetime(2026, 6, 30),
                "sheet_id": "S2",
                "step_id": "10140",
                "param_name": "TOTAL_LOSS_RATE",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 2.0,
            },
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "sheet_start_time": datetime(2026, 6, 30),
                "sheet_id": "S3",
                "step_id": "10140",
                "param_name": "MT_CH_PRESS_A",
                "site_name": "P1",
                "unit_id": "3CEE01-PPA",
                "param_value": 3.0,
            },
        ]
    )


def test_repository_filters_excluded_params_from_fresh_snapshot(monkeypatch, tmp_path: Path) -> None:
    snapshot_path = tmp_path / "spc_snapshot_M626.parquet"
    _snapshot_rows().to_parquet(snapshot_path, index=False)

    monkeypatch.setattr(
        spc_repository,
        "load_param_whitelist",
        lambda db, prod: pd.DataFrame(
            [
                {"ref_param_name": "PPA_B_X", "data_type": "SPC"},
                {"ref_param_name": "TOTAL_LOSS_RATE", "data_type": "SPC"},
                {"ref_param_name": "MT_CH_PRESS_A", "data_type": "SPC"},
            ]
        ),
    )
    monkeypatch.setattr(SpcRepository, "_apply_outlier_filters", lambda self, df, prod: df)

    repo = SpcRepository(snapshot_dir=tmp_path, use_snapshot=True, db_manager=DummyDbManager())
    result = repo.get_spc_measurements(
        SpcQueryConfig(
            prod_code="M626",
            start_date="2026-06-01",
            end_date="2026-06-30",
        )
    )

    assert result["param_name"].tolist() == ["PPA_B_X"]
