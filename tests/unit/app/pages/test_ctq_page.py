from contextlib import nullcontext
from pathlib import Path
import runpy
from types import SimpleNamespace

import pandas as pd

from app.components import page_header
from app.sections.ctq import ctq_dashboard
from app.utils.app_setup import AppSetup
from app.manager.session_manager import SessionManager
from src.inline_domain.application.ctq.ctq_service import CtqReportService
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.infrastructure.spc.data_loader import SpcQueryConfig
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler


def test_ctq_page_loads_once_and_renders_only_filters_and_distributions(monkeypatch) -> None:
    events: list[str] = []
    loaded_queries: list[SpcQueryConfig] = []
    loaded_signatures: list[str] = []
    header_kwargs: dict[str, object] = {}
    report = SimpleNamespace(
        indicators_df=pd.DataFrame(
            [
                {
                    "prod_code": "M678",
                    "factory": "ARRAY",
                    "step_id": "12140",
                    "param_name": "SE_L1T_UNI",
                    "chart_type": "line",
                }
            ]
        ),
        sheet_features_df=pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "step_id": "12140",
                    "param_name": "SE_L1T_UNI",
                    "sheet_id": "S1",
                }
            ]
        ),
        raw_measurements_df=pd.DataFrame(),
        sheet_oos_decoration_result=None,
    )

    monkeypatch.setattr(ctq_dashboard.st, "set_page_config", lambda **_kwargs: None)
    monkeypatch.setattr(ctq_dashboard.st, "spinner", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(ctq_dashboard.st, "query_params", {})
    monkeypatch.setattr(AppSetup, "initialize_app", staticmethod(lambda: None))
    monkeypatch.setattr(
        SessionManager,
        "get_active_config",
        staticmethod(lambda: SimpleNamespace(data_source=SimpleNamespace(product_code="M678"))),
    )
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: object())
    monkeypatch.setattr(
        MonitorAnalysisService,
        "get_time_window",
        staticmethod(lambda: (pd.Timestamp("2026-05-01"), pd.Timestamp("2026-07-21"))),
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
        lambda *_args, **kwargs: header_kwargs.update(kwargs),
    )
    monkeypatch.setattr(ConfigLoader, "get_spc_period_box_source", staticmethod(lambda: "point_value"))

    def fake_load_report(**kwargs):
        loaded_queries.append(SpcQueryConfig.model_validate_json(kwargs["query_config_json"]))
        loaded_signatures.append(kwargs["snapshot_signature"])
        return report

    monkeypatch.setattr(CtqReportService, "get_ctq_report_data", staticmethod(fake_load_report))
    monkeypatch.setattr(
        ctq_dashboard,
        "render_ctq_filters",
        lambda **_kwargs: events.append("filters") or ("ARRAY", ["SE_L1T_UNI"], ["12140"], True),
    )
    monkeypatch.setattr(
        ctq_dashboard,
        "render_ctq_indicator_sections",
        lambda **_kwargs: events.append("charts"),
    )

    page_path = Path(__file__).parents[4] / "app" / "pages" / "CTQ监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert [query.data_type_filter for query in loaded_queries] == ["CTQ"]
    assert loaded_signatures == ["ctq_distribution_report_v1|scoped=M678"]
    assert header_kwargs["product_cache_scope"] == "M678"
    assert events == ["filters", "charts"]
    assert not hasattr(report, "period_capability_df")


def test_portal_navigation_points_ctq_to_the_streamlit_page() -> None:
    config_path = Path(__file__).parents[4] / "app" / "static" / "config.js"
    config_text = config_path.read_text(encoding="utf-8")

    assert 'CTQ_REPORT: "http://10.72.26.31:8503/CTQ监控报表"' in config_text
    assert '{ name: "CTQ", url: LINKS.CTQ_REPORT }' in config_text
    assert "{l:'', v:'CTQ', url: LINKS.CTQ_REPORT }" in config_text
