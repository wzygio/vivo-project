"""Missing product sheets mean no Yield alerts in the display consumer."""
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from app.sections.inline_domain.monitor.alert_matrix_service import AlertMatrixContext, MATRIX_ROW_MAP
from app.sections.shared.alert_results import alert_result_signature, read_alert_result
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

DAY = date(2026, 11, 17)
NOW = datetime(2026, 11, 17, 10, tzinfo=timezone(timedelta(hours=8)))


@pytest.fixture
def saved_yield(tmp_path, monkeypatch):
    from src.yield_domain import composition
    from src.yield_domain.application.yield_service import YieldAnalysisService

    store = YieldAlertWorkbookStore({kind: tmp_path / f"{kind}.xlsx" for kind in ("lot", "trend")})
    service = YieldAlertResults(store, policy_signature=lambda _: "test", ttl_seconds=43200, clock=lambda: NOW)
    monkeypatch.setattr(composition, "build_yield_alert_results", lambda: service)
    for method in ("get_data_health", "get_lot_defect_rates", "get_mwd_trend_data", "get_code_level_trend_data"):
        monkeypatch.setattr(YieldAnalysisService, method, lambda *a, **k: pytest.fail("display ran analysis"))
    monkeypatch.setattr(pd, "read_sql", lambda *a, **k: pytest.fail("display queried SQL"))
    return service


@pytest.mark.parametrize("kind,row_key", [("lot", "yield_lot_oos"), ("trend", "yield_trend_fluctuation")])
def test_readable_workbook_without_product_is_ok_and_changes_invalidate_cache(saved_yield, kind, row_key):
    path = saved_yield.store.paths[kind]
    pd.DataFrame({"other_product": [1]}).to_excel(path, sheet_name="M678", index=False)
    before = path.read_bytes(), path.stat().st_mtime_ns
    context = AlertMatrixContext(DAY, alert_result_loader=read_alert_result)

    result = read_alert_result(row_key, "Z622", DAY)
    assert result["state"] == "ok"
    assert result["records"].empty and result["alerts"].empty
    assert MATRIX_ROW_MAP[row_key].evaluator("Z622", context)["state"] == "ok"
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    # This is a display convention, not evidence of a successful publication.
    assert saved_yield.read("Z622", kind, DAY).status == "missing"
    signature = alert_result_signature(row_key, "Z622", DAY)
    saved_yield.mark_failed("Z622", kind, DAY, "YIELD_ANALYSIS_FAILED")
    assert alert_result_signature(row_key, "Z622", DAY) != signature
    assert read_alert_result(row_key, "Z622", DAY)["state"] == "error"


@pytest.mark.parametrize("kind,row_key", [("lot", "yield_lot_oos"), ("trend", "yield_trend_fluctuation")])
@pytest.mark.parametrize("condition", ["unreadable", "records_only", "metadata_only", "flags_only", "stale"])
def test_missing_sheet_rule_preserves_real_publication_errors(saved_yield, kind, row_key, condition):
    path = saved_yield.store.paths[kind]
    if condition == "unreadable":
        path.write_bytes(b"unreadable workbook")
        def fail_read(*args):
            raise OSError("unreadable workbook")
        saved_yield.store.workbook_reader = fail_read
    elif condition in ("records_only", "metadata_only", "flags_only"):
        sheet = {"records_only": "Z622", "metadata_only": "Z622__meta", "flags_only": "Z622__flags"}[condition]
        pd.DataFrame({"invalid": [1]}).to_excel(path, sheet_name=sheet, index=False)
    elif condition == "stale":
        saved_yield.publish("Z622", kind, [], reference_date=DAY - timedelta(days=1),
            coverage_start="2026-10-01", coverage_end=str(DAY), has_data=True,
            health={"status": "fresh", "refreshed_at": NOW.isoformat()}, calculation_signature="test")
    assert read_alert_result(row_key, "Z622", DAY)["state"] == "error"


@pytest.mark.parametrize("kind,row_key", [("lot", "yield_lot_oos"), ("trend", "yield_trend_fluctuation")])
def test_missing_workbook_is_passing_without_creating_a_file(saved_yield, kind, row_key):
    assert read_alert_result(row_key, "Z622", DAY)["state"] == "ok"
    assert not saved_yield.store.paths[kind].exists()


def _render_yield_cells():
    import streamlit as st
    from app.sections.inline_domain.monitor.alert_matrix import _render_matrix_rows

    _render_matrix_rows(st.session_state["rows"], ["Z622"], [2, 1], st.session_state["cells"], set())


def test_missing_product_renders_both_yield_cells_as_green(saved_yield):
    from streamlit.testing.v1 import AppTest

    context = AlertMatrixContext(DAY, alert_result_loader=read_alert_result)
    keys = ("yield_lot_oos", "yield_trend_fluctuation")
    for path in saved_yield.store.paths.values():
        pd.DataFrame().to_excel(path, sheet_name="M678", index=False)
    app = AppTest.from_function(_render_yield_cells)
    app.session_state["rows"] = [{"row_key": key} for key in keys]
    app.session_state["cells"] = {(key, "Z622"): MATRIX_ROW_MAP[key].evaluator("Z622", context) for key in keys}
    app.run()
    assert not app.exception
    assert len(app.button) == 2
    assert all(button.label == "🟢" and "达标" in button.help for button in app.button)
