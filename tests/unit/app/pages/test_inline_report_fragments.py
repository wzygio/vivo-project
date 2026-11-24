"""CTQ/AOI page interaction boundaries with no database or maintained-file I/O."""
from datetime import date
import importlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest


def install_inline_page_fakes(scope, monkeypatch):
    from app.components import indicator_cache, page_header
    from app.utils import step_labels
    from app.utils.app_setup import AppSetup
    from src.inline_domain import composition
    from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
    from src.inline_domain.application.shared import decision_signature
    from src.shared_kernel.infrastructure import db_handler

    dashboard = importlib.import_module(f"app.sections.inline_domain.{scope}.{scope}_dashboard")
    services = importlib.import_module(f"src.inline_domain.application.{scope}.{scope}_service")
    service = getattr(services, {"ctq": "CtqReportService", "aoi_rs": "AoiRsReportService", "aoi_tt": "AoiTtReportService"}[scope])
    column = {"ctq": "param_name", "aoi_rs": "rs_code", "aoi_tt": "tt_name"}[scope]
    frame = pd.DataFrame([
        dict(prod_code="M678", factory="ARRAY", step_id="S1", **{column: name})
        for name in ("P1", "P2")
    ])
    state = {"loads": 0, "charts": [], "alert_reads": 0, "publications": 0}
    report = SimpleNamespace(
        indicators_df=frame.copy(), sheet_features_df=frame.copy(), raw_measurements_df=frame.copy(),
        rs_details_df=frame.copy(), tt_details_df=frame.assign(particle_size="Total"),
        spec_df=pd.DataFrame(), pass_through_df=frame.copy(), lot_points_df=frame.copy(),
        sheet_points_df=frame.copy(), trend_evaluation_df=pd.DataFrame(), sheet_oos_decoration_result=None,
    )

    def load(**kwargs):
        state["loads"] += 1
        return report

    def alerts(*args, **kwargs):
        state["alert_reads"] += 1
        if scope != "ctq":
            assert kwargs["publish_report"] is False
        st.caption(f"Fixture loads: {state['loads']}; publications: {state['publications']}")

    def charts(**kwargs):
        state["charts"].append(kwargs)
        st.success("Fixture queried chart")

    def publish(*args):
        state["publications"] += 1

    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(step_labels, "get_cached_step_description_map", lambda _: {})
    monkeypatch.setattr(page_header, "render_page_header", lambda **kwargs: "M678")
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *args: [])
    monkeypatch.setattr(indicator_cache, "build_indicator_product_cache_signature", lambda *args: "test-revision")
    monkeypatch.setattr(indicator_cache, "get_indicator_product_revision", lambda *args: "test-revision")
    monkeypatch.setattr(MonitorAnalysisService, "get_time_window", lambda: (pd.Timestamp("2026-08-01"), pd.Timestamp("2026-11-13")))
    monkeypatch.setattr(decision_signature, "get_scope_decision_signature", lambda *args: "test-decisions")
    monkeypatch.setattr(composition, f"build_{scope}_repository", lambda *args: object())
    monkeypatch.setattr(composition, "build_trend_alert_results", lambda *args: SimpleNamespace(publish=publish))
    monkeypatch.setattr(service, f"get_{scope}_report_data", staticmethod(load))
    monkeypatch.setattr(dashboard, f"render_{scope}_indicator_sections", charts)
    monkeypatch.setattr(dashboard, f"render_saved_{scope}_alerts", alerts)
    monkeypatch.setattr(dashboard, "render_ctq_decoration_admin" if scope == "ctq" else "get_default_" + scope + "_start_date",
                        (lambda *args: None) if scope == "ctq" else (lambda end: date(2026, 10, 1)))
    for suffix in ("export", "weekly_export"):
        if scope == "ctq" or (scope == "aoi_tt" and suffix == "export"):
            continue
        module = importlib.import_module(f"app.sections.inline_domain.{scope}.{scope}_{suffix}")
        monkeypatch.setattr(module, f"render_{scope}_{suffix}", lambda **kwargs: None)
    return state


@pytest.mark.parametrize("scope", ["ctq", "aoi_rs", "aoi_tt"])
def test_preloaded_inline_filters_are_in_one_fragment_and_require_submission(monkeypatch, scope):
    state = install_inline_page_fakes(scope, monkeypatch)
    page = next((Path(__file__).parents[4] / "app/pages").glob(scope.upper() + "*.py"))
    app = AppTest.from_file(str(page)).run()
    assert not app.exception
    assert state["loads"] == 1
    metadata = app.session_state._state._new_widget_state.widget_metadata
    controls = [app.selectbox(key=f"{scope}_factory_filter"), app.multiselect(key=f"{scope}_step_filter")]
    if scope == "aoi_tt":
        controls.append(app.multiselect(key="aoi_tt_particle_size_filter"))
    fragments = {metadata[control.id].fragment_id for control in controls}
    assert None not in fragments and len(fragments) == 1
    app.multiselect(key=f"{scope}_step_filter").select("S1").run()
    assert not state["charts"]
    next(button for button in app.button if button.label == "查询").click().run()
    assert not app.exception
    assert len(state["charts"]) == 1
    if scope == "aoi_tt":
        app.multiselect(key="aoi_tt_particle_size_filter").unselect("S").run()
        assert not app.exception
        assert len(state["charts"]) == 1, "Particle change must require a new submitted query"


@pytest.mark.parametrize("scope,suffix", [
    ("spc", "export"), ("aoi_rs", "export"),
    ("aoi_rs", "weekly_export"), ("aoi_tt", "weekly_export"),
])
def test_export_interaction_wrappers_own_independent_widgets(monkeypatch, scope, suffix):
    module = importlib.import_module(f"app.sections.inline_domain.{scope}.{scope}_{suffix}")
    def controls(**kwargs):
        st.multiselect("Independent export", ["M678"], key="fixture_export_scope")
    monkeypatch.setattr(module, f"render_{scope}_{suffix}", controls)
    extra = "" if scope == "spc" else ', current_product="M678"'
    app = AppTest.from_string(f'''
from datetime import date
from app.sections.inline_domain.{scope}.{scope}_{suffix} import render_{scope}_{suffix}_interaction
render_{scope}_{suffix}_interaction(start_date=date(2026,10,1), end_date=date(2026,11,13), db_manager=None{extra})
''').run()
    assert not app.exception
    widget = app.multiselect(key="fixture_export_scope")
    assert app.session_state._state._new_widget_state.widget_metadata[widget.id].fragment_id


@pytest.mark.parametrize("scope", ["aoi_rs", "aoi_tt"])
def test_saved_alert_display_can_read_without_republishing_report(monkeypatch, scope):
    from app.sections.shared import alert_results
    from src.inline_domain import composition
    dashboard = importlib.import_module(f"app.sections.inline_domain.{scope}.{scope}_dashboard")
    trend = importlib.import_module(f"app.sections.inline_domain.{scope}.trend_alerts")
    monkeypatch.setattr(composition, "build_trend_alert_results", lambda *args: pytest.fail("Display rerun attempted publication"))
    monkeypatch.setattr(alert_results, "read_alert_result", lambda *args: {
        "state": "no_data", "alerts": pd.DataFrame(), "records": pd.DataFrame(),
    })
    monkeypatch.setattr(dashboard, "render_sheet_oos_alert_center", lambda *args, **kwargs: None)
    monkeypatch.setattr(dashboard, f"render_{scope}_sheet_oos_alert_indicator_sections", lambda *args, **kwargs: None)
    monkeypatch.setattr(trend, f"render_{scope}_trend_alerts", lambda *args, **kwargs: None)
    report = SimpleNamespace(**{name: pd.DataFrame() for name in (
        "trend_evaluation_df", "rs_details_df", "pass_through_df", "spec_df", "indicators_df",
        "lot_points_df", "sheet_points_df", "tt_details_df",
    )})
    getattr(dashboard, f"render_saved_{scope}_alerts")(
        "M678", reference_date=date(2026, 11, 13), report=report, publish_report=False,
    )
