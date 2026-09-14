from types import SimpleNamespace
from datetime import datetime

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.data_health import attach_data_health, make_data_health
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


@pytest.mark.parametrize("end,start", [
    (datetime(2026, 9, 14), "2026-06-01"),
    (datetime(2026, 1, 1), "2025-10-01"),
])
def test_yield_button_refresh_uses_full_calendar_window_and_selected_product(monkeypatch, end, start):
    calls = []

    def read_panel(query, policy, force_refresh=False):
        calls.append((query, force_refresh))
        return attach_data_health(pd.DataFrame(), make_data_health("fresh"))

    monkeypatch.setattr(YieldAnalysisService, "_custom_end_date", end)
    config = ConfigLoader.load_config("M678")
    assert YieldAnalysisService.safe_refresh_snapshots(
        None, config, _data_port=SimpleNamespace(read_panel=read_panel),
    )
    query, forced = calls[0]
    assert len(calls) == 1
    assert forced is True
    assert (query.start_date, query.end_date, query.product_code) == (
        start, end.strftime("%Y-%m-%d"), "M678",
    )
