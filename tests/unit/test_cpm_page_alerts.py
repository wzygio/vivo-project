from contextlib import nullcontext
from pathlib import Path
import runpy
from types import SimpleNamespace

import pandas as pd

from app.components import page_header
from app.sections import spc_cpm_dashboard
from app.utils.app_setup import AppSetup
from app.utils.session_manager import SessionManager
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler
from src.spc_domain.application.cpm_service import CpmReportService
from src.spc_domain.application.spc_service import SpcAnalysisService


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

    monkeypatch.setattr(spc_cpm_dashboard.st, "set_page_config", lambda **_kwargs: None)
    monkeypatch.setattr(spc_cpm_dashboard.st, "spinner", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(spc_cpm_dashboard.st, "query_params", {})
    monkeypatch.setattr(AppSetup, "initialize_app", staticmethod(lambda: None))
    monkeypatch.setattr(
        SessionManager,
        "get_active_config",
        staticmethod(lambda: SimpleNamespace(data_source=SimpleNamespace(product_code="M626"))),
    )
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(
        SpcAnalysisService,
        "get_time_window",
        staticmethod(lambda: (pd.Timestamp("2026-05-01"), pd.Timestamp("2026-07-14"))),
    )
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(page_header, "render_page_header", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ConfigLoader, "get_cpm_period_sigma_source", staticmethod(lambda: "point_value"))
    monkeypatch.setattr(ConfigLoader, "get_cpm_period_box_source", staticmethod(lambda: "point_value"))

    def fake_load_report(**_kwargs):
        nonlocal load_count
        load_count += 1
        return report

    monkeypatch.setattr(CpmReportService, "get_cpm_report_data", staticmethod(fake_load_report))
    monkeypatch.setattr(
        spc_cpm_dashboard,
        "render_cpk_alert_center",
        lambda *_args, **_kwargs: events.append("alerts"),
    )
    monkeypatch.setattr(
        spc_cpm_dashboard,
        "render_cpm_filters",
        lambda **_kwargs: events.append("filters") or ("ARRAY", ["4PP_Rs"], ["15260"], True),
    )
    monkeypatch.setattr(
        spc_cpm_dashboard,
        "render_cpm_indicator_sections",
        lambda **_kwargs: events.append("charts"),
    )

    page_path = Path(__file__).parents[2] / "app" / "pages" / "CPM监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert load_count == 1
    assert events == ["alerts", "filters", "charts"]
