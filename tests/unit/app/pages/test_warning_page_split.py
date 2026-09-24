"""Independent report routes must not accidentally execute the full matrix."""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from app.manager.session_manager import SessionManager
from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_snapshot
from app.utils.app_setup import AppSetup
from src.inline_domain import composition
from src.inline_domain.application.monitor.oos_monitor_service import OosMonitorService


PAGES = Path(__file__).parents[4] / "app/pages"


@pytest.fixture
def isolated_pages(monkeypatch):
    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(SessionManager, "AVAILABLE_PRODUCTS", ["M626"])
    monkeypatch.setattr(alert_matrix_snapshot, "has_daily_matrix_snapshot", lambda: False)


def test_summary_route_shows_both_boards_without_loading_matrix(isolated_pages, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Summary route must not load the matrix or query before clicking")

    monkeypatch.setattr(alert_matrix, "render_alert_matrix_board", forbidden)
    monkeypatch.setattr(alert_matrix_snapshot, "has_daily_matrix_snapshot", forbidden)
    monkeypatch.setattr(OosMonitorService, "build_dashboard", forbidden)
    app = AppTest.from_file(str(PAGES / "超规与CPK预警看板.py")).run()
    assert not app.exception
    assert [item.value for item in app.subheader] == ["⚠️ 超规片预警看板", "📊 CPK预警看板"]
    assert app.button(key="btn_monitor_query_submit")
    assert app.button(key="cpk_monitor_query_submit")
    assert not any(button.key == "btn_load_alert_matrix" for button in app.button)


def test_matrix_route_has_no_summary_service_dependency(isolated_pages, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Matrix route must not construct summary services")

    monkeypatch.setattr(composition, "build_cpk_monitor_service", forbidden)
    monkeypatch.setattr(composition, "build_oos_history_service", forbidden)
    app = AppTest.from_file(str(PAGES / "自动预警看板.py")).run()
    assert not app.exception
    assert [item.value for item in app.subheader] == ["🚦 全指标预警看板", "全产品良率看板"]
    assert app.button(key="btn_load_alert_matrix")
    assert not any(button.key in {"btn_monitor_query_submit", "cpk_monitor_query_submit"} for button in app.button)


def test_summary_failure_preserves_cpk_entry_without_exposing_exception(isolated_pages, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("private-source-debug-message")

    monkeypatch.setattr(composition, "build_oos_history_service", fail)
    app = AppTest.from_file(str(PAGES / "超规与CPK预警看板.py")).run()
    app.button(key="btn_monitor_query_submit").click().run()
    assert not app.exception
    assert len(app.error) == 1
    assert "private-source-debug-message" not in app.error[0].value
    assert app.button(key="cpk_monitor_query_submit")
