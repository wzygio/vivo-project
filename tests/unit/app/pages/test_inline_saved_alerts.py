"""Saved alert consumption survives independent measurement producer failures."""
from contextlib import nullcontext
from datetime import date
import importlib
from pathlib import Path
import runpy
from types import SimpleNamespace

import pandas as pd
import pytest


@pytest.mark.parametrize("scope,label,service,method,keys", [
    ("spc", "SPC", "SpcReportService", "get_spc_report_data",
     ["spc_cpk_trend", "spc_cpm_trend", "spc_sheet_oos"]),
    ("ctq", "CTQ", "CtqReportService", "get_ctq_report_data", ["ctq_sheet_oos"]),
    ("aoi_rs", "AOI_RS", "AoiRsReportService", "get_aoi_rs_report_data",
     ["aoi_rs_sheet_oos", "aoi_rs_trend"]),
    ("aoi_tt", "AOI_TT", "AoiTtReportService", "get_aoi_tt_report_data",
     ["aoi_tt_sheet_oos", "aoi_tt_trend"]),
])
@pytest.mark.parametrize("failure", ["report", "setup"])
def test_saved_alerts_remain_visible_when_producer_fails(
    monkeypatch, scope, label, service, method, keys, failure,
):
    from app.components import page_header, indicator_cache
    from app.manager.session_manager import SessionManager
    from app.sections.shared import alert_results
    from app.utils.app_setup import AppSetup
    from app.utils import step_labels
    from src.inline_domain import composition
    from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
    from src.inline_domain.application.shared import decision_signature
    from src.shared_kernel.infrastructure import db_handler
    import streamlit as st

    errors, tables, reads = [], [], []
    parameter_label = {"spc": "参数名称", "ctq": "参数名称", "aoi_rs": "RS Code", "aoi_tt": "TT名称"}[scope]
    alert = pd.DataFrame([{
        "厂别": "ARRAY", "站点": "S", parameter_label: "TEST", "Sheet ID": "A",
        "超规时间": "2026-10-20 12:00:00", "超规类型": "USL",
    }])

    def read(key, product, reference):
        reads.append(key)
        assert product == "M678" and reference == date(2026, 10, 27)
        return {
            "state": "alert" if key.endswith("sheet_oos") else "no_data",
            "alerts": alert.copy() if key.endswith("sheet_oos") else pd.DataFrame(),
            "records": pd.DataFrame(), "evaluation": pd.DataFrame(),
        }

    class PageStopped(Exception):
        pass

    def fail(*args, **kwargs):
        raise RuntimeError("private producer diagnostic")

    monkeypatch.setattr(alert_results, "read_alert_result", read)
    monkeypatch.setattr(st, "set_page_config", lambda **kw: None)
    monkeypatch.setattr(st, "spinner", lambda *a, **kw: nullcontext())
    monkeypatch.setattr(st, "expander", lambda *a, **kw: nullcontext())
    monkeypatch.setattr(st, "dataframe", lambda frame, **kw: tables.append(frame.copy()))
    monkeypatch.setattr(st, "error", errors.append)
    monkeypatch.setattr(st, "info", lambda *a, **kw: None)
    monkeypatch.setattr(st, "success", lambda *a, **kw: None)
    monkeypatch.setattr(st, "stop", lambda: (_ for _ in ()).throw(PageStopped()))
    monkeypatch.setattr(st, "query_params", {})
    monkeypatch.setattr(AppSetup, "initialize_app", staticmethod(lambda: None))
    monkeypatch.setattr(SessionManager, "get_active_config", staticmethod(
        lambda: SimpleNamespace(data_source=SimpleNamespace(product_code="M678")),
    ))
    monkeypatch.setattr(page_header, "render_page_header", lambda **kw: None)
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *a: [])
    monkeypatch.setattr(indicator_cache, "build_indicator_product_cache_signature", lambda *a: "test")
    monkeypatch.setattr(indicator_cache, "get_indicator_product_revision", lambda *a: "test")
    monkeypatch.setattr(db_handler, "DatabaseManager", fail if failure == "setup" else lambda: object())
    monkeypatch.setattr(step_labels, "get_cached_step_description_map", lambda *a: {})
    monkeypatch.setattr(MonitorAnalysisService, "get_time_window", staticmethod(
        lambda: (pd.Timestamp("2026-09-01"), pd.Timestamp("2026-10-27")),
    ))
    monkeypatch.setattr(decision_signature, "get_scope_decision_signature", lambda *a: "test")
    monkeypatch.setattr(composition, f"build_{scope}_repository", lambda *a: object())
    service_module = importlib.import_module(f"src.inline_domain.application.{scope}.{scope}_service")
    monkeypatch.setattr(getattr(service_module, service), method, staticmethod(fail))
    package = importlib.import_module(f"src.inline_domain.application.{scope}")
    monkeypatch.setattr(package, f"{scope}_service", service_module)
    for module_name, function in {
        "spc.spc_export": "render_spc_export",
        "aoi_rs.aoi_rs_export": "render_aoi_rs_export",
        "aoi_rs.aoi_rs_weekly_export": "render_aoi_rs_weekly_export",
        "aoi_tt.aoi_tt_weekly_export": "render_aoi_tt_weekly_export",
    }.items():
        module = importlib.import_module(f"app.sections.inline_domain.{module_name}")
        monkeypatch.setattr(module, function, lambda **kw: None)
    page = next((Path(__file__).parents[4] / "app/pages").glob(f"{label}*.py"))
    with pytest.raises(PageStopped):
        runpy.run_path(str(page), run_name="__main__")
    assert reads == keys
    assert len(tables) == 1
    pd.testing.assert_frame_equal(tables[0], alert)
    assert not any("private producer diagnostic" in message for message in errors)


def test_spc_metric_read_error_does_not_hide_other_saved_alerts(monkeypatch):
    from app.sections.shared import alert_results
    from app.sections.inline_domain.spc import spc_dashboard as dashboard

    reads, rendered, errors = [], [], []

    def read(key, *_):
        reads.append(key)
        return {"state": "error" if key == "spc_cpk_trend" else "ok", "alerts": pd.DataFrame()}

    monkeypatch.setattr(alert_results, "read_alert_result", read)
    monkeypatch.setattr(dashboard.st, "error", errors.append)
    monkeypatch.setattr(dashboard, "render_cpm_alert_section", lambda *a, **kw: rendered.append("cpm"))
    monkeypatch.setattr(dashboard, "render_sheet_oos_alert_center", lambda *a, **kw: rendered.append("sheet"))
    dashboard.render_saved_spc_alerts("M678", reference_date=date(2026, 10, 27))
    assert reads == ["spc_cpk_trend", "spc_cpm_trend", "spc_sheet_oos"]
    assert rendered == ["cpm", "sheet"]
    assert errors == ["CPK 预警数据暂不可用，请稍后重试。"]


@pytest.mark.parametrize("scope", ["aoi_rs", "aoi_tt"])
@pytest.mark.parametrize("publication_fails", [False, True])
def test_trend_summary_reads_saved_evaluation_after_publication(
    monkeypatch, scope, publication_fails,
):
    from app.sections.shared import alert_results
    from src.inline_domain import composition

    dashboard = importlib.import_module(f"app.sections.inline_domain.{scope}.{scope}_dashboard")
    trend_ui = importlib.import_module(f"app.sections.inline_domain.{scope}.trend_alerts")
    current = pd.DataFrame([{"source": "current analysis"}])
    saved = pd.DataFrame([{"source": "committed publication"}])
    report = SimpleNamespace(**{
        name: pd.DataFrame()
        for name in ("rs_details_df", "pass_through_df", "spec_df", "indicators_df",
                     "lot_points_df", "sheet_points_df", "tt_details_df")
    }, trend_evaluation_df=current)
    events, shown, warnings = [], [], []

    def publish(product, reference, evaluation):
        assert evaluation is current
        events.append("publish")
        if publication_fails:
            raise OSError("private publication diagnostic")

    def read(key, *_):
        events.append(key)
        return {"state": "ok", "alerts": pd.DataFrame(), "records": pd.DataFrame(), "evaluation": saved}

    monkeypatch.setattr(composition, "build_trend_alert_results", lambda _: SimpleNamespace(publish=publish))
    monkeypatch.setattr(alert_results, "read_alert_result", read)
    monkeypatch.setattr(dashboard, "render_sheet_oos_alert_center", lambda *a, **kw: None)
    monkeypatch.setattr(dashboard, f"render_{scope}_sheet_oos_alert_indicator_sections", lambda *a, **kw: None)
    monkeypatch.setattr(dashboard.st, "warning", warnings.append)
    monkeypatch.setattr(trend_ui, f"render_{scope}_trend_alerts", lambda *a, **kw: shown.append(kw["evaluation_df"]))
    getattr(dashboard, f"render_saved_{scope}_alerts")("M678", reference_date=date(2026, 10, 27), report=report)
    assert events == ["publish", f"{scope}_sheet_oos", f"{scope}_trend"]
    assert shown[0] is saved
    assert warnings == (["趋势预警更新失败，请稍后重试。"] if publication_fails else [])


def test_spc_chart_failure_does_not_hide_later_saved_sections(monkeypatch):
    from app.sections.shared import alert_results
    from app.sections.inline_domain.spc import spc_dashboard as dashboard

    reads, tables, warnings, charts = [], [], [], []
    capability_alert = pd.DataFrame([{
        "厂别": "ARRAY", "站点": "100", "参数名称": "CD", "超规周次": "2026-W43", "CPK值": 0.8,
    }])
    frame = pd.DataFrame([{"factory": "ARRAY", "step_id": "100", "param_name": "CD"}])
    report = SimpleNamespace(period_capability_df=frame, sheet_features_df=frame, raw_measurements_df=frame)

    def read(key, *_):
        reads.append(key)
        return {"state": "alert" if key == "spc_cpk_trend" else "ok",
                "alerts": capability_alert if key == "spc_cpk_trend" else pd.DataFrame()}

    def draw(**kwargs):
        charts.append("cpk")
        raise RuntimeError("private chart diagnostic")

    monkeypatch.setattr(alert_results, "read_alert_result", read)
    monkeypatch.setattr(dashboard.st, "expander", lambda *a, **kw: nullcontext())
    monkeypatch.setattr(dashboard.st, "error", lambda *a: None)
    monkeypatch.setattr(dashboard.st, "success", lambda *a: None)
    monkeypatch.setattr(dashboard.st, "warning", warnings.append)
    monkeypatch.setattr(dashboard.st, "dataframe", lambda value, **kw: tables.append(value.copy()))
    monkeypatch.setattr(dashboard.st, "markdown", lambda *a: None)
    monkeypatch.setattr(dashboard.st, "caption", lambda *a: None)
    monkeypatch.setattr(dashboard, "_alert_charts_signature", lambda *a, **kw: "test")
    monkeypatch.setattr(dashboard, "render_spc_indicator_sections", draw)
    monkeypatch.setattr(dashboard, "render_sheet_oos_alert_center", lambda *a, **kw: None)
    monkeypatch.setattr(dashboard, "render_sheet_oos_alert_indicator_sections", lambda *a, **kw: None)
    dashboard.render_saved_spc_alerts("M678", reference_date=date(2026, 10, 27), report=report)
    assert reads == ["spc_cpk_trend", "spc_cpm_trend", "spc_sheet_oos"]
    assert charts == ["cpk"]
    pd.testing.assert_frame_equal(tables[0], capability_alert)
    assert warnings == ["CPK 预警图像暂不可用。"]


@pytest.mark.parametrize("scope", ["ctq", "aoi_rs", "aoi_tt"])
def test_sheet_chart_failure_keeps_saved_summary_and_later_trend(monkeypatch, scope):
    from app.sections.shared import alert_results

    dashboard = importlib.import_module(f"app.sections.inline_domain.{scope}.{scope}_dashboard")
    report = SimpleNamespace(**{name: pd.DataFrame() for name in (
        "sheet_features_df", "raw_measurements_df", "rs_details_df", "pass_through_df",
        "spec_df", "indicators_df", "lot_points_df", "sheet_points_df", "tt_details_df",
    )})
    events, warnings = [], []

    def read(key, *_):
        events.append(key)
        return {"state": "ok", "alerts": pd.DataFrame(), "records": pd.DataFrame(), "evaluation": pd.DataFrame()}

    def draw(*args, **kwargs):
        events.append("chart")
        raise RuntimeError("private chart diagnostic")

    monkeypatch.setattr(alert_results, "read_alert_result", read)
    monkeypatch.setattr(dashboard, "render_sheet_oos_alert_center", lambda *a, **kw: events.append("sheet table"))
    monkeypatch.setattr(dashboard, f"render_{scope}_sheet_oos_alert_indicator_sections", draw)
    monkeypatch.setattr(dashboard.st, "warning", warnings.append)
    if scope != "ctq":
        trend_ui = importlib.import_module(f"app.sections.inline_domain.{scope}.trend_alerts")
        monkeypatch.setattr(trend_ui, f"render_{scope}_trend_alerts", lambda *a, **kw: events.append("trend table"))
    getattr(dashboard, f"render_saved_{scope}_alerts")("M678", reference_date=date(2026, 10, 27), report=report)
    assert events == [f"{scope}_sheet_oos", "sheet table", "chart"] + (
        [] if scope == "ctq" else [f"{scope}_trend", "trend table"]
    )
    assert warnings == ["单片异常预警图像暂不可用。"]
