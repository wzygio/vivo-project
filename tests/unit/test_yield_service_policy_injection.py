from types import SimpleNamespace

import pandas as pd

from src.shared_kernel.config import ConfigLoader
from src.yield_domain.application.dtos import YieldDataPolicy
from src.yield_domain.application.yield_service import YieldAnalysisService
from src.yield_domain.infrastructure import data_loader
from src.yield_domain.infrastructure.repositories.yield_repository import (
    build_yield_snapshot_path,
)


def test_safe_refresh_injects_loaded_config_into_bottom_data_provider(
    tmp_path, monkeypatch
) -> None:
    def fake_read_sql(sql_query, engine) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "batch_no": "2026/07/01",
                    "lot_id": "LOT-1",
                    "sheet_id": "SHEET-1",
                    "panel_id": "PANEL-1",
                    "warehousing_time": "2026-07-01",
                    "prod_code": "M678",
                    "defect_code": "LINE",
                    "defect_desc": "亮线",
                    "defect_group": "Array_Line",
                }
            ]
        )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(data_loader.pd, "read_sql", fake_read_sql)
    config = ConfigLoader.load_config("M678")
    policy = YieldDataPolicy.from_app_config(config)

    refreshed = YieldAnalysisService.safe_refresh_snapshots(
        SimpleNamespace(engine=object()),
        config,
    )

    assert refreshed is True
    expected_snapshot = build_yield_snapshot_path(
        tmp_path / "data",
        "M678",
        policy,
    )
    assert expected_snapshot.exists()
