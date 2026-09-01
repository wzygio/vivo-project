from contextlib import nullcontext
import importlib
from pathlib import Path
import runpy
from types import SimpleNamespace

import pandas as pd

from app.components import page_header
from app.sections.inline_domain.spc import spc_dashboard
from app.utils.app_setup import AppSetup
from app.manager.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService


def test_spc_page_renders_filters_below_header_and_before_auto_warning(monkeypatch) -> None:
    current_spc_service = importlib.import_module(
        "src.inline_domain.application.spc.spc_service"
    )
    current_spc_package = importlib.import_module("src.inline_domain.application.spc")
    monkeypatch.setattr(current_spc_package, "spc_service", current_spc_service)
    monkeypatch.delattr(current_spc_service, "SpcDecorationFileError")
    events: list[str] = []
    load_count = 0
    loaded_signatures: list[str] = []
    header_kwargs: dict[str, object] = {}
    rendered_alerts: list[pd.DataFrame] = []
    rendered_cpm_alerts: list[pd.DataFrame] = []
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "TP",
                "step_id": "41260",
                "param_name": "4PP_Rs",
                "period_type": "month",
                "period_label": "2026-07",
                "cpk": 1.536,
            },
            {
                "factory": "TP",
                "step_id": "41260",
                "param_name": "4PP_Rs",
                "period_type": "week",
                "period_label": "2026-W30",
                "cpk": 1.278,
            },
        ]
    )
    indicator_df = pd.DataFrame(
        [{"prod_code": "M673", "factory": "TP", "step_id": "41260", "param_name": "4PP_Rs"}]
    )
    sheet_features_df = pd.DataFrame(
        [{"factory": "TP", "step_id": "41260", "param_name": "4PP_Rs", "sheet_id": "S1"}]
    )
    report = SimpleNamespace(
        period_capability_df=period_capability_df,
        indicators_df=indicator_df,
        sheet_features_df=sheet_features_df,
        raw_measurements_df=pd.DataFrame(),
        sheet_oos_decoration_result=None,
    )

    monkeypatch.setattr(spc_dashboard.st, "set_page_config", lambda **_kwargs: None)
    monkeypatch.setattr(spc_dashboard.st, "spinner", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(spc_dashboard.st, "query_params", {})
    monkeypatch.setattr(AppSetup, "initialize_app", staticmethod(lambda: None))
    monkeypatch.setattr(
        SessionManager,
        "get_active_config",
        staticmethod(lambda: SimpleNamespace(data_source=SimpleNamespace(product_code="M673"))),
    )
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(
        MonitorAnalysisService,
        "get_time_window",
        staticmethod(lambda: (pd.Timestamp("2026-05-01"), pd.Timestamp("2026-07-28"))),
    )
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        page_header,
        "build_product_cache_signature",
        lambda base_signature, product_code: f"{base_signature}|scoped={product_code}",
    )
    monkeypatch.setattr(
        page_header,
        "render_page_header",
        lambda *_args, **kwargs: (
            header_kwargs.update(kwargs),
            events.append("header"),
        ),
    )
    monkeypatch.setattr(ConfigLoader, "get_spc_period_sigma_source", staticmethod(lambda: "point_value"))
    monkeypatch.setattr(ConfigLoader, "get_spc_period_box_source", staticmethod(lambda: "point_value"))

    def fake_load_report(**kwargs):
        nonlocal load_count
        load_count += 1
        loaded_signatures.append(kwargs["snapshot_signature"])
        return report

    monkeypatch.setattr(
        current_spc_service.SpcReportService,
        "get_spc_report_data",
        staticmethod(fake_load_report),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "render_cpk_alert_section",
        lambda alerts_df, **_kwargs: (
            rendered_alerts.append(alerts_df.copy()),
            events.append("alerts"),
        ),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "render_cpm_alert_section",
        lambda alerts_df, **_kwargs: (
            rendered_cpm_alerts.append(alerts_df.copy()),
            events.append("cpm_alerts"),
        ),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_filters",
        lambda **_kwargs: events.append("filters") or ("TP", ["4PP_Rs"], ["41260"], True),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **_kwargs: events.append("charts"),
    )

    page_path = Path(__file__).parents[4] / "app" / "pages" / "SPC监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert load_count == 1
    assert loaded_signatures == [
        "spc_capability_distribution_report_v1|scoped=M673"
    ]
    assert header_kwargs["product_cache_scope"] == "M673"
    assert rendered_alerts[0].to_dict("records") == [
        {
            "厂别": "TP",
            "站点": "41260",
            "参数名称": "4PP_Rs",
            "超规周次": "2026-W30",
            "CPK值": 1.278,
        }
    ]
    assert events == ["header", "filters", "alerts", "cpm_alerts", "charts"]
    assert rendered_cpm_alerts[0].empty
