from contextlib import nullcontext
from pathlib import Path
import runpy
from types import SimpleNamespace

import pandas as pd

from app.components import page_header
from app.sections.spc import spc_dashboard
from app.utils.app_setup import AppSetup
from app.utils.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler
from src.inline_domain.application.spc.spc_service import SpcReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService


def test_cpm_page_preloads_once_then_renders_alerts_before_filters(monkeypatch) -> None:
    events: list[str] = []
    load_count = 0
    period_capability_df = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "step_id": "15260",
                "param_name": "4PP_Rs",
                "period_type": "day",
                "period_label": "2026-07-14",
                "cpk": 1.20,
            }
        ]
    )
    indicator_df = pd.DataFrame(
        [{"prod_code": "M626", "factory": "ARRAY", "step_id": "15260", "param_name": "4PP_Rs"}]
    )
    sheet_features_df = pd.DataFrame(
        [{"factory": "ARRAY", "step_id": "15260", "param_name": "4PP_Rs", "sheet_id": "S1"}]
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
        staticmethod(lambda: SimpleNamespace(data_source=SimpleNamespace(product_code="M626"))),
    )
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(
        MonitorAnalysisService,
        "get_time_window",
        staticmethod(lambda: (pd.Timestamp("2026-05-01"), pd.Timestamp("2026-07-14"))),
    )
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(page_header, "render_page_header", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ConfigLoader, "get_spc_period_sigma_source", staticmethod(lambda: "point_value"))
    monkeypatch.setattr(ConfigLoader, "get_spc_period_box_source", staticmethod(lambda: "point_value"))

    def fake_load_report(**_kwargs):
        nonlocal load_count
        load_count += 1
        return report

    monkeypatch.setattr(SpcReportService, "get_spc_report_data", staticmethod(fake_load_report))
    monkeypatch.setattr(
        spc_dashboard,
        "render_cpk_alert_center",
        lambda *_args, **_kwargs: events.append("alerts"),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_filters",
        lambda **_kwargs: events.append("filters") or ("ARRAY", ["4PP_Rs"], ["15260"], True),
    )
    monkeypatch.setattr(
        spc_dashboard,
        "render_spc_indicator_sections",
        lambda **_kwargs: events.append("charts"),
    )

    page_path = Path(__file__).parents[4] / "app" / "pages" / "SPC监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert load_count == 1
    assert events == ["alerts", "filters", "charts"]
