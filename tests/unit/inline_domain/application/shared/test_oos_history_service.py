from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.inline_domain.application.shared.oos_history_service import OosHistoryService
from src.inline_domain.infrastructure.shared.oos_history_store import OosHistoryStore
from src.inline_domain.infrastructure.shared.oos_decision_repository import (
    OosDecisionWorkbookRepository,
)
from src.inline_domain.infrastructure.shared import oos_decision_repository


def _spc_detail(flag: object = False) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "step_id": "1100",
                "param_name": "PPA",
                "sheet_id": "S1",
                "sheet_start_time": "2026-08-10 08:00:00",
                "sheet_max": 3.0,
                "sheet_min": 1.0,
                "sheet_mean": 2.0,
                "usl": 2.5,
                "lsl": 1.5,
                "oos_type": "USL",
                "flag": flag,
            }
        ]
    )


def test_history_read_applies_current_decision_and_projects_common_columns(
    tmp_path: Path,
) -> None:
    resources = tmp_path / "resources"
    resources.mkdir()
    store = OosHistoryStore(tmp_path / "history")
    store.update(
        "spc",
        "M626",
        _spc_detail(),
        coverage_start=pd.Timestamp("2026-08-01"),
        coverage_end=pd.Timestamp("2026-09-01"),
        refreshed_at=datetime(2026, 9, 2, 8),
    )
    pd.DataFrame(
        [{"prod_code": "M626", "step_id": "1100", "param_name": "PPA", "sheet_id": "S1", "flag": False}]
    ).to_excel(
        resources / "spc_sheet_oos_decoration.xlsx",
        sheet_name="M626__flags",
        index=False,
    )

    result = OosHistoryService(store, OosDecisionWorkbookRepository(resources)).read_product("spc", "M626")

    assert result.source == "history"
    assert result.alerts_df["metric_name"].tolist() == ["PPA"]
    assert result.alerts_df["item_id"].tolist() == ["S1"]
    assert result.alerts_df["scope"].tolist() == ["spc"]
    assert result.refreshed_at == pd.Timestamp("2026-09-02 08:00:00")


def test_missing_history_falls_back_to_current_workbook_without_writing_history(
    tmp_path: Path,
) -> None:
    resources = tmp_path / "resources"
    resources.mkdir()
    _spc_detail(False).to_excel(
        resources / "spc_sheet_oos_decoration.xlsx",
        sheet_name="M626",
        index=False,
    )
    store = OosHistoryStore(tmp_path / "history")

    result = OosHistoryService(store, OosDecisionWorkbookRepository(resources)).read_product("spc", "M626")

    assert result.source == "workbook_fallback"
    assert len(result.alerts_df) == 1
    assert not store.snapshot_path("spc", "M626").exists()


def test_default_true_decision_is_not_an_external_alert(tmp_path: Path) -> None:
    resources = tmp_path / "resources"
    resources.mkdir()
    _spc_detail(True).to_excel(
        resources / "spc_sheet_oos_decoration.xlsx",
        sheet_name="M626",
        index=False,
    )

    result = OosHistoryService(
        OosHistoryStore(tmp_path / "history"), OosDecisionWorkbookRepository(resources)
    ).read_product("spc", "M626")

    assert result.alerts_df.empty


def test_decision_workbook_is_loaded_once_for_multiple_products(
    tmp_path: Path, monkeypatch
) -> None:
    resources = tmp_path / "resources"
    resources.mkdir()
    workbook = resources / "spc_sheet_oos_decoration.xlsx"
    workbook.write_bytes(b"signature")
    calls: list[Path] = []

    def fake_read(path, **kwargs):
        calls.append(Path(path))
        return {
            "M626__flags": pd.DataFrame({"prod_code": ["M626"], "flag": [False]}),
            "M678__flags": pd.DataFrame({"prod_code": ["M678"], "flag": [True]}),
        }

    monkeypatch.setattr(oos_decision_repository.pd, "read_excel", fake_read)
    repository = OosDecisionWorkbookRepository(resources)

    repository.load_decisions("spc_sheet_oos_decoration.xlsx", "M626", ["prod_code"])
    repository.load_decisions("spc_sheet_oos_decoration.xlsx", "M678", ["prod_code"])

    assert calls == [workbook]
