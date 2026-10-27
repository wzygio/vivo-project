"""Real page publishes its alerts before unrelated Sheet/Mapping failures."""
from datetime import datetime
import importlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.components import page_header
from app.manager.session_manager import SessionManager
from app.sections.yield_domain import resource_config
from app.utils.app_setup import AppSetup
from src.shared_kernel.infrastructure import db_handler
from src.yield_domain import composition
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore


def test_page_publishes_both_results_before_sheet_failure(monkeypatch, tmp_path):
    analysis_module = importlib.import_module("yield_domain.application.yield_service")
    analysis = analysis_module.YieldAnalysisService
    today = datetime.now().astimezone()
    day = today.date()
    health = {"status": "fresh", "refreshed_at": today.isoformat()}
    publisher = YieldAlertResults(YieldAlertWorkbookStore({"lot": tmp_path / "lots.xlsx",
        "trend": tmp_path / "trends.xlsx"}), policy_signature=lambda product: "fixture",
        ttl_seconds=43200)
    config = SimpleNamespace(data_source=SimpleNamespace(product_code="M678"), processing={})
    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(SessionManager, "get_active_config", lambda: config)
    monkeypatch.setattr(SessionManager, "get_product_dir", lambda: tmp_path)
    monkeypatch.setattr(page_header, "render_page_header", lambda **kwargs: None)
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: None)
    monkeypatch.setattr(composition, "build_yield_alert_results", lambda: publisher)
    monkeypatch.setattr(resource_config, "get_dashboard_resource_config", lambda config, revision: config)
    monkeypatch.setattr(resource_config, "get_code_monthly_rate_threshold", lambda: 1.0)
    context = {"analysis_start_date": str(day.replace(day=1)), "analysis_end_date": str(day),
               "modifier_signature": "m", "warning_signature": "w", "rate_override_signature": "r"}
    monkeypatch.setattr(analysis, "build_cache_context", lambda *args, **kwargs: context)
    monkeypatch.setattr(analysis, "get_data_health", lambda *args, **kwargs: health)
    for name in ("get_mwd_trend_data", "get_code_level_trend_data"):
        monkeypatch.setattr(analysis, name, lambda *args, **kwargs: {"weekly": pd.DataFrame()})
    monkeypatch.setattr(analysis, "get_lot_defect_rates", lambda *args, **kwargs: None)
    monkeypatch.setattr(analysis, "load_static_warning_lines", lambda *args, **kwargs: {})
    def sheet_failure(*args, **kwargs):
        raise analysis_module.YieldDataLoadError("internal source failure")
    monkeypatch.setattr(analysis, "get_sheet_defect_rates", sheet_failure)
    page = Path(__file__).resolve().parents[4] / "app/pages/入库不良率分析看板.py"
    app = AppTest.from_file(str(page)).run(timeout=20)
    assert not app.exception
    assert (tmp_path / "lots.xlsx").exists()
    assert (tmp_path / "trends.xlsx").exists()
    assert publisher.read("M678", "lot", day).status in {"no_data", "uncovered"}
    assert publisher.read("M678", "trend", day).status == "no_data"
    assert app.error
    assert [item.label for item in app.expander[:2]] == [
        'Lot 超规预警中心（最近过货日向前30天）', '趋势预警中心（月／周）',
    ]
    assert all("internal source failure" not in item.value for item in app.error)
    versions = {path.name: path.stat().st_mtime_ns for path in tmp_path.glob("*.xlsx")}
    app.run()
    assert not app.exception
    assert {path.name: path.stat().st_mtime_ns for path in tmp_path.glob("*.xlsx")} == versions
