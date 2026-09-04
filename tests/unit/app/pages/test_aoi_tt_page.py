"""AOI_TT 页面测试：加载链路、固定时间窗、渲染顺序、门户注册。"""

from contextlib import nullcontext
from pathlib import Path
import runpy
from types import SimpleNamespace

import pandas as pd

from app.components import page_header
from app.sections.inline_domain.aoi_tt import aoi_tt_dashboard
from app.utils.app_setup import AppSetup
from app.manager.session_manager import SessionManager
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
from src.inline_domain.application.shared import decision_signature
from src.inline_domain.application.monitor.monitor_service import MonitorAnalysisService
from src.inline_domain.application.aoi_tt.dtos import AoiTtQueryConfig
from src.shared_kernel.infrastructure import db_handler


def test_aoi_tt_page_loads_with_fixed_window_and_renders_filters_then_charts(monkeypatch) -> None:
    events: list[str] = []
    loaded_queries: list[AoiTtQueryConfig] = []
    loaded_signatures: list[str] = []
    header_kwargs: dict[str, object] = {}
    rendered_particle_sizes: list[str] = []
    report = SimpleNamespace(
        indicators_df=pd.DataFrame(
            [
                {
                    "prod_code": "M678",
                    "factory": "ARRAY",
                    "step_id": "11620",
                    "tt_name": "TDSUM",
                }
            ]
        ),
        tt_details_df=pd.DataFrame(
            [
                {
                    "factory": "ARRAY",
                    "prod_code": "M678",
                    "start_time": pd.Timestamp("2026-08-09 08:00"),
                    "sheet_id": "S1",
                    "lot_id": "L1",
                    "step_id": "11620",
                    "tt_name": "TDSUM",
                    "particle_size": "S",
                    "tt_qty": 3,
                }
            ]
        ),
        spec_df=pd.DataFrame(),
    )

    monkeypatch.setattr(aoi_tt_dashboard.st, "set_page_config", lambda **_kwargs: None)
    monkeypatch.setattr(aoi_tt_dashboard.st, "spinner", lambda *_args, **_kwargs: nullcontext())
    monkeypatch.setattr(
        aoi_tt_dashboard.st,
        "date_input",
        lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError("AOI_TT 页面不得提供时间筛选")),
        raising=False,
    )
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
        staticmethod(lambda: (pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-10"))),
    )
    monkeypatch.setattr(page_header, "extract_cached_funcs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        decision_signature,
        "get_scope_decision_signature",
        lambda *_args, **_kwargs: "test-decision",
    )
    monkeypatch.setattr(
        aoi_tt_dashboard,
        "load_aoi_tt_oos_decoration",
        lambda *_args, **_kwargs: None,
    )
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

    def fake_load_report(**kwargs):
        loaded_queries.append(AoiTtQueryConfig.model_validate_json(kwargs["query_config_json"]))
        loaded_signatures.append(kwargs["snapshot_signature"])
        return report

    monkeypatch.setattr(AoiTtReportService, "get_aoi_tt_report_data", staticmethod(fake_load_report))
    monkeypatch.setattr(
        aoi_tt_dashboard,
        "render_aoi_tt_filters",
        lambda **_kwargs: events.append("filters")
        or ("ARRAY", ["TDSUM"], ["11620"], ["S"], True),
    )

    def fake_render_sections(**kwargs):
        events.append("charts")
        rendered_particle_sizes.extend(kwargs["tt_details_df"]["particle_size"].tolist())

    monkeypatch.setattr(
        aoi_tt_dashboard,
        "render_aoi_tt_indicator_sections",
        fake_render_sections,
    )

    page_path = Path(__file__).parents[4] / "app" / "pages" / "AOI_TT监控报表.py"
    runpy.run_path(str(page_path), run_name="__main__")

    assert [q.prod_code for q in loaded_queries] == ["M678"]
    # 固定窗口：上一自然月 1 日 ~ 当前日期（含当天）
    assert loaded_queries[0].start_date == "2026-07-01"
    assert loaded_queries[0].end_date == "2026-08-10"
    assert loaded_signatures == ["aoi_tt_report_v3_particle_size_generation|scoped=M678"]
    assert header_kwargs["product_cache_scope"] == "M678"
    assert events == ["filters", "charts"]
    assert rendered_particle_sizes == ["S"]


def test_portal_navigation_points_aoi_tt_to_the_streamlit_page() -> None:
    config_path = Path(__file__).parents[4] / "app" / "static" / "config.js"
    config_text = config_path.read_text(encoding="utf-8")

    assert 'AOI_TT_REPORT: "http://10.72.26.31:8503/AOI_TT监控报表"' in config_text
    assert '{ name: "AOI_TT", url: LINKS.AOI_TT_REPORT }' in config_text
    assert "{l:'', v:'AOI_TT', url: LINKS.AOI_TT_REPORT }" in config_text
