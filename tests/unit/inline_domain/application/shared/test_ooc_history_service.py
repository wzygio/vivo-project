from __future__ import annotations

import pandas as pd

from src.inline_domain.application.shared.ooc_history_service import OocHistoryService
from src.inline_domain.infrastructure.shared.ooc_history_store import OocHistoryStore
from src.inline_domain.infrastructure.shared.oos_decision_repository import (
    OosDecisionWorkbookRepository,
)


def test_ooc_history_is_durable_and_false_decision_releases_alert(tmp_path) -> None:
    store = OocHistoryStore(tmp_path / "history")
    resources = tmp_path / "resources"
    resources.mkdir()
    facts = pd.DataFrame(
        [{
            "factory": "ARRAY", "prod_code": "M626", "step_id": "100",
            "param_name": "CD", "sheet_id": "S1",
            "sheet_start_time": "2026-09-01", "sheet_max": 56.0,
            "sheet_min": 55.0, "sheet_mean": 55.5, "ucl": 54.0,
            "lcl": 46.0, "ooc_type": "UCL",
        }]
    )
    service = OocHistoryService(store, OosDecisionWorkbookRepository(resources))
    store.update(
        "spc", "M626", facts,
        coverage_start=pd.Timestamp("2026-09-01"),
        coverage_end=pd.Timestamp("2026-09-03"),
    )
    decision = facts[["prod_code", "step_id", "param_name", "sheet_id"]].assign(flag=False)
    with pd.ExcelWriter(resources / "spc_sheet_ooc_decoration.xlsx") as writer:
        decision.to_excel(writer, sheet_name="M626__flags", index=False)

    result = service.read_product("spc", "M626")

    assert len(result.alerts_df) == 1
    assert result.alerts_df.iloc[0]["alarm_type"] == "OOC"
    assert result.alerts_df.iloc[0]["usl"] == 54.0
    assert result.alerts_df.iloc[0]["upper_limit"] == 54.0
    assert result.alerts_df.iloc[0]["limit_type"] == "UCL"

    admin = service.build_decision_admin_view("spc", "M626")
    assert admin.key_columns == ("prod_code", "step_id", "param_name", "sheet_id")
    assert admin.decision_sheet == "M626__flags"
    assert admin.decision_df["flag"].tolist() == [False]


def test_ooc_admin_prefills_new_facts_with_default_true_decision(tmp_path) -> None:
    store = OocHistoryStore(tmp_path / "history")
    resources = tmp_path / "resources"
    resources.mkdir()
    facts = pd.DataFrame(
        [
            {
                "factory": "ARRAY", "prod_code": "M626", "step_id": "100",
                "param_name": "CD", "sheet_id": "NEW", "sheet_start_time": "2026-09-01",
                "sheet_max": 53.0, "sheet_min": 52.0, "sheet_mean": 52.5,
                "ucl": 52.0, "lcl": 48.0, "ooc_type": "UCL",
            }
        ]
    )
    service = OocHistoryService(store, OosDecisionWorkbookRepository(resources))
    store.update(
        "spc", "M626", facts,
        coverage_start=pd.Timestamp("2026-09-01"),
        coverage_end=pd.Timestamp("2026-09-02"),
    )

    admin = service.build_decision_admin_view("spc", "M626")

    assert admin.decision_df.to_dict("records") == [
        {
            "prod_code": "M626",
            "step_id": "100",
            "param_name": "CD",
            "sheet_id": "NEW",
            "flag": True,
        }
    ]
