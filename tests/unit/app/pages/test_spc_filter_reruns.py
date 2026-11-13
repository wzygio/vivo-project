"""Exercise the actual SPC page without database, workbook or chart side effects."""
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest


@pytest.fixture
def page(monkeypatch):
    from app.components import indicator_cache, page_header
    from app.manager.session_manager import SessionManager
    from app.sections.inline_domain.spc import scroll_layout, spc_dashboard, spc_export
    from app.utils import step_labels
    from app.utils.app_setup import AppSetup
    from src.inline_domain import composition
    from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
    from src.inline_domain.application.shared import decision_signature
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from src.shared_kernel.infrastructure import db_handler

    state = {"loads": 0, "charts": 0, "alerts": 0}
    indicators = pd.DataFrame([
        {"prod_code": "Z553", "factory": "ARRAY", "step_id": "1B990", "param_name": "TFT_7_SS_S_V2"},
        {"prod_code": "Z553", "factory": "ARRAY", "step_id": "1B990", "param_name": "TFT_7_VTH_L_V2"},
        {"prod_code": "Z553", "factory": "OLED", "step_id": "2120A", "param_name": "UNI"},
    ])
    report = SimpleNamespace(
        indicators_df=indicators, raw_measurements_df=indicators.copy(),
        sheet_features_df=indicators.copy(), period_capability_df=pd.DataFrame(),
    )

    def load(**kwargs):
        state["loads"] += 1
        if state.get("mode") == "failure":
            raise RuntimeError("private producer diagnostic")
        if state.get("mode") == "empty":
            return SimpleNamespace(**{
                name: pd.DataFrame() for name in (
                    "indicators_df", "raw_measurements_df", "sheet_features_df", "period_capability_df",
                )
            })
        return report

    def charts(**kwargs):
        state["charts"] += 1

    def alerts(*args, **kwargs):
        state["alerts"] += 1

    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(SessionManager, "get_active_config", lambda: SimpleNamespace(
        data_source=SimpleNamespace(product_code="Z553")))
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(step_labels, "get_cached_step_description_map", lambda _: {})
    monkeypatch.setattr(scroll_layout, "render_spc_page_scroll_layout", lambda: None)
    monkeypatch.setattr(page_header, "render_page_header", lambda **kwargs: None)
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *args: [])
    monkeypatch.setattr(indicator_cache, "build_indicator_product_cache_signature", lambda *args: "test-revision")
    monkeypatch.setattr(spc_export, "render_spc_export", lambda **kwargs: None)
    monkeypatch.setattr(MonitorAnalysisService, "get_time_window", lambda: (
        pd.Timestamp("2026-08-01"), pd.Timestamp("2026-11-13")))
    monkeypatch.setattr(decision_signature, "get_scope_decision_signature", lambda *args: "test-decisions")
    monkeypatch.setattr(composition, "build_spc_repository", lambda *args: object())
    monkeypatch.setattr(composition, "build_spc_input_signature", lambda *args: "test-snapshot")
    monkeypatch.setattr(composition, "build_spc_sheet_membership_store", lambda: object())
    monkeypatch.setattr(composition, "build_spc_sheet_membership_policy_signature", lambda: "test-policy")
    monkeypatch.setattr(composition, "build_spc_sheet_membership_coverage", lambda *args: None)
    monkeypatch.setattr(SpcReportService, "get_spc_report_data", staticmethod(load))
    monkeypatch.setattr(spc_dashboard, "render_spc_indicator_sections", charts)
    monkeypatch.setattr(spc_dashboard, "render_saved_spc_alerts", alerts)
    app = AppTest.from_file(str(Path(__file__).parents[4] / "app/pages/SPC监控报表.py"))
    return app, state


def test_spc_cascade_filters_use_one_fragment_and_require_query(page):
    app, state = page
    app.run()
    assert not app.exception
    assert state["loads"] == 1
    # AppTest itself always requests a full script run. Widget metadata proves
    # browser events route to the fragment; browser verification checks counts.
    metadata = app.session_state._state._new_widget_state.widget_metadata
    filter_ids = [widget.id for widget in [
        app.selectbox(key="spc_factory_filter"),
        app.multiselect(key="spc_step_filter"),
        app.multiselect(key="spc_param_filter"),
        next(button for button in app.button if button.label == "查询"),
    ]]
    fragment_ids = {metadata[widget_id].fragment_id for widget_id in filter_ids}
    assert None not in fragment_ids, "Filter events currently rerun the full page and report loader"
    assert len(fragment_ids) == 1
    app.multiselect(key="spc_step_filter").select("1B990").run()
    assert not app.exception
    app.multiselect(key="spc_param_filter").unselect("TFT_7_VTH_L_V2").run()
    assert state["charts"] == 0
    next(button for button in app.button if button.label == "查询").click().run()
    assert not app.exception
    assert state["charts"] == 1
    app.selectbox(key="spc_factory_filter").select("OLED").run()
    assert not app.exception
    assert app.multiselect(key="spc_step_filter").value == []
    assert state["charts"] == 1


@pytest.mark.parametrize("mode", ["empty", "failure"])
def test_spc_unavailable_report_still_displays_saved_alerts(page, mode):
    app, state = page
    state["mode"] = mode
    app.run()
    assert not app.exception
    assert state["alerts"] == 1
    assert state["charts"] == 0
    assert not app.multiselect
    assert all("private producer diagnostic" not in error.value for error in app.error)
