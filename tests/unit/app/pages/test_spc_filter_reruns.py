"""Exercise the actual SPC page without database, workbook or chart side effects."""
import json
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
    from app.utils import reloader, step_labels
    from app.utils.app_setup import AppSetup
    from src.inline_domain import composition
    from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
    from src.inline_domain.application.shared import decision_signature
    from src.inline_domain.application.spc.spc_service import SpcReportService
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.infrastructure import db_handler

    state = {
        "loads": 0, "products": [], "charts": 0, "alerts": 0, "alert_products": [],
        "refreshes": [], "revisions": [], "db_inits": 0,
    }
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
        product = json.loads(kwargs["query_config_json"])["prod_code"]
        state["products"].append(product)
        if state.get("mode") == "failure":
            raise RuntimeError("private producer diagnostic")
        if state.get("mode") == "empty":
            return SimpleNamespace(**{
                name: pd.DataFrame() for name in (
                    "indicators_df", "raw_measurements_df", "sheet_features_df", "period_capability_df",
                )
            })
        frames = {name: frame.assign(prod_code=product) if not frame.empty else frame
                  for name, frame in vars(report).items()}
        return SimpleNamespace(**frames)

    def charts(**kwargs):
        state["charts"] += 1

    def alerts(product, **kwargs):
        state["alerts"] += 1
        state["alert_products"].append(product)

    def db_manager():
        state["db_inits"] += 1
        return object()

    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(SessionManager, "get_active_config", lambda: SimpleNamespace(
        data_source=SimpleNamespace(product_code="M626")))
    monkeypatch.setattr(SessionManager, "AVAILABLE_PRODUCTS", ["M626", "Z553", "Z622"])
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda: ["M626", "Z553", "Z622"])
    monkeypatch.setattr(db_handler, "DatabaseManager", db_manager)
    monkeypatch.setattr(step_labels, "get_cached_step_description_map", lambda _: {})
    monkeypatch.setattr(scroll_layout, "render_spc_page_scroll_layout", lambda: None)
    monkeypatch.setattr(page_header, "detect_project_changes", lambda: False)
    monkeypatch.setattr(page_header, "bump_indicator_product_revision",
                        lambda indicator, product: state["revisions"].append((indicator, product)))
    monkeypatch.setattr(reloader, "deep_reload_modules", lambda: None)
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *args: [])
    monkeypatch.setattr(indicator_cache, "build_indicator_product_cache_signature", lambda *args: "test-revision")
    monkeypatch.setattr(composition, "refresh_raw_measurements",
                        lambda db, product, date, *, refresh_days: state["refreshes"].append(product) or True)
    monkeypatch.setattr(MonitorAnalysisService, "get_time_window", lambda: (
        pd.Timestamp("2026-08-01"), pd.Timestamp("2026-11-13")))
    monkeypatch.setattr(decision_signature, "get_scope_decision_signature", lambda *args: "test-decisions")
    monkeypatch.setattr(composition, "build_spc_repository", lambda *args: object())
    monkeypatch.setattr(spc_export, "get_scope_decision_signature", lambda *args: "test-decisions")
    monkeypatch.setattr(spc_export, "build_spc_repository", lambda *args: object())
    monkeypatch.setattr(composition, "build_spc_input_signature", lambda *args: "test-snapshot")
    monkeypatch.setattr(composition, "build_spc_sheet_membership_store", lambda: object())
    monkeypatch.setattr(composition, "build_spc_sheet_membership_policy_signature", lambda _: "test-policy")
    monkeypatch.setattr(composition, "build_spc_sheet_membership_coverage", lambda *args: None)
    monkeypatch.setattr(SpcReportService, "get_spc_report_data", staticmethod(load))
    monkeypatch.setattr(spc_dashboard, "render_spc_indicator_sections", charts)
    monkeypatch.setattr(spc_dashboard, "render_saved_spc_alerts", alerts)
    app = AppTest.from_file(str(Path(__file__).parents[4] / "app/pages/SPC监控报表.py"))
    return app, state


def submit_product(app, product):
    app.selectbox(key="header_prod_sel_SPC监控报表").select(product)
    return app.button(key="header_product_query_SPC监控报表_submit").click().run()


def test_spc_product_query_and_refresh_use_only_the_confirmed_product(page):
    app, state = page
    app.query_params["admin"] = "true"
    app.session_state["selected_product_code"] = "M626"
    app.run()
    assert not app.exception
    assert state["products"] == state["alert_products"] == []
    assert state["db_inits"] == 0
    assert app.button(key="btn_refresh_SPC监控报表").disabled
    assert app.button(key="btn_clear_SPC监控报表").disabled
    assert not any(section.label == "SPC 报告导出" for section in app.expander)

    submit_product(app, "Z622")
    assert not app.exception
    assert state["products"] == state["alert_products"] == ["Z622"]
    assert any(caption.value.startswith("当前报表产品：Z622") for caption in app.caption)
    assert not any(control.label == "导出产品" for control in app.multiselect)

    app.selectbox(key="header_prod_sel_SPC监控报表").select("M626").run()
    assert any(caption.value.startswith("当前报表产品：Z622") for caption in app.caption)
    app.button(key="btn_refresh_SPC监控报表").click().run()
    assert not app.exception
    assert state["refreshes"] == ["Z622"]
    assert set(state["products"]) == set(state["alert_products"]) == {"Z622"}
    assert state["revisions"] and {product for _, product in state["revisions"]} == {"Z622"}
    assert app.session_state["selected_product_code"] == "M626"

    submit_product(app, "M626")
    assert not app.exception
    assert state["products"][-1] == state["alert_products"][-1] == "M626"
    assert state["refreshes"] == ["Z622"]


def test_spc_product_switch_invalidates_chart_query_and_export(page, monkeypatch):
    from app.sections.inline_domain.spc import spc_export

    app, state = page
    app.query_params["admin"] = "true"
    app.run()
    submit_product(app, "Z622")
    app.multiselect(key="spc_step_filter").select("1B990").run()
    next(button for button in app.button if button.label == "查询" and not button.key).click().run()
    assert state["charts"] == 1

    exported = []

    def build_pdf(products, *, load_report, **kwargs):
        for product in products:
            load_report(product)
            exported.append(product)
        return b"fixture PDF"

    monkeypatch.setattr(spc_export, "build_spc_pdf", build_pdf)
    app.button(key="spc_pdf_generate").click().run()
    assert not app.exception
    assert exported == ["Z622"]
    assert app.session_state[spc_export.EXPORT_STATUS_KEY]["data"] == b"fixture PDF"

    charts_before_switch = state["charts"]
    submit_product(app, "M626")
    assert not app.exception
    assert state["charts"] == charts_before_switch
    assert app.session_state[spc_export.EXPORT_STATUS_KEY]["data"] is None
    app.button(key="spc_pdf_generate").click().run()
    assert exported == ["Z622", "M626"]
    submit_product(app, "Z622")
    assert not app.exception
    assert state["charts"] == charts_before_switch


def test_spc_cascade_filters_use_one_fragment_and_require_query(page):
    app, state = page
    app.run()
    submit_product(app, "Z553")
    assert not app.exception
    assert state["loads"] == 1
    # AppTest itself always requests a full script run. Widget metadata proves
    # browser events route to the fragment; browser verification checks counts.
    metadata = app.session_state._state._new_widget_state.widget_metadata
    filter_ids = [widget.id for widget in [
        app.selectbox(key="spc_factory_filter"),
        app.multiselect(key="spc_step_filter"),
        app.multiselect(key="spc_param_filter"),
        next(button for button in app.button if button.label == "查询" and not button.key),
    ]]
    fragment_ids = {metadata[widget_id].fragment_id for widget_id in filter_ids}
    assert None not in fragment_ids, "Filter events currently rerun the full page and report loader"
    assert len(fragment_ids) == 1
    app.multiselect(key="spc_step_filter").select("1B990").run()
    assert not app.exception
    app.multiselect(key="spc_param_filter").unselect("TFT_7_VTH_L_V2").run()
    assert state["charts"] == 0
    next(button for button in app.button if button.label == "查询" and not button.key).click().run()
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
    submit_product(app, "Z553")
    assert not app.exception
    assert state["alerts"] == 1
    assert state["charts"] == 0
    assert not app.multiselect
    assert all("private producer diagnostic" not in error.value for error in app.error)
