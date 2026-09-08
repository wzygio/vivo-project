"""Actual-page regression with local workbook fixtures and forbidden raw paths."""
from pathlib import Path

from streamlit.testing.v1 import AppTest


def test_inline_excel_query_refresh_and_closed_periods(monkeypatch):
    # The runpy fixture patches process-global seams; restore them for other tests.
    from app.components import page_header
    from app.manager.session_manager import SessionManager
    from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_cache
    from app.utils import app_setup, reloader
    from src.inline_domain.application.monitor.live_source import LiveMonitorSource
    import src.inline_domain.composition as composition
    import src.shared_kernel.infrastructure.db_handler as db_handler

    targets = {
        composition: ("build_oos_history_service", "build_ooc_history_service",
                      "build_monitor_summary_workbook_service", "build_cpk_monitor_service",
                      "build_live_throughput_reader", "build_live_monitor_source",
                      "build_raw_measurement_repository"),
        LiveMonitorSource: ("compute",), db_handler: ("DatabaseManager",),
        page_header: ("render_page_header",),
        app_setup.AppSetup: ("initialize_app",), reloader: ("deep_reload_modules",),
        alert_matrix: ("render_alert_matrix_filter_bar", "render_alert_matrix_board"),
        alert_matrix_cache: ("get_alert_matrix_cached_funcs",),
        SessionManager: ("AVAILABLE_PRODUCTS", "get_active_config"),
    }
    for target, names in targets.items():
        for name in names:
            monkeypatch.setattr(target, name, getattr(target, name))
    fixture = Path(__file__).parent / "fixtures" / "oos_monitor_app.py"
    app = AppTest.from_file(str(fixture)).run(timeout=60)

    def check(*captions):
        assert not app.exception
        assert not app.error
        rendered = {item.value for item in app.caption}
        assert set(captions).issubset(rendered)

    def headings():
        return {item.value for item in app.markdown}

    check("Fixture week OOS: 0", "Fixture year OOS: 100")
    assert "#### 超规趋势" not in headings()
    assert "#### CPK 汇总表" not in headings()

    app.button(key="btn_monitor_query_submit").click().run(timeout=60)
    check("Fixture week OOS: 1", "Fixture year OOS: 101", "Fixture closed preserved: True")
    assert "#### 超规趋势" in headings()
    assert "#### CPK 汇总表" not in headings()
    assert all("数据最后更新时间" not in value for value in headings())

    app.button(key="btn_monitor_query_submit").click().run(timeout=60)
    check("Fixture year OOS: 101")
    app.button(key="cpk_monitor_query_submit").click().run(timeout=60)
    check("Fixture CPK warning total: 8")
    assert "#### CPK 汇总表" in headings()

    app.button(key="fixture_edit_excel").click().run(timeout=60)
    check("Fixture Excel decorated: True")
    app.button(key="fixture_refresh_cache").click().run(timeout=60)
    assert "#### 超规趋势" not in headings()
    assert "#### CPK 汇总表" not in headings()
    app.button(key="btn_monitor_query_submit").click().run(timeout=60)
    check("Fixture week OOS: 0", "Fixture year OOS: 100", "Fixture closed preserved: True")

    app.query_params["admin"] = "true"
    app.run(timeout=60)
    check("Fixture closed preserved: True")
    assert "#### 数据最后更新时间（管理员）" in headings()
    app.button(key="fixture_edit_cpk").click().run(timeout=60)
    app.button(key="fixture_refresh_cache").click().run(timeout=60)
    app.button(key="cpk_monitor_query_submit").click().run(timeout=60)
    check("Fixture CPK warning total: 6", "Fixture CPK history preserved: True")
    assert "#### CPK 预警趋势" in headings()
