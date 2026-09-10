"""Real page sessions share matrix calculations, including targeted admin refresh."""

from collections import Counter
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from app.components import indicator_cache, page_header
from app.manager.session_manager import SessionManager
from app.sections.inline_domain.monitor import alert_matrix, alert_matrix_cache
from app.sections.inline_domain.monitor import alert_matrix_service, cpk_monitor_dashboard
from app.utils import step_labels
from app.utils.app_setup import AppSetup
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure import db_handler


PAGE_PATH = Path(__file__).parents[4] / "app/pages/自动预警看板.py"
PRODUCTS = ("M626", "M678")
INDICATORS = ("spc_sheet_oos", "spc_cpk_trend")


@pytest.fixture
def matrix_sessions(monkeypatch, tmp_path):
    calls = Counter()
    rows = []
    for key in INDICATORS:
        def evaluate(product, context, key=key):
            calls[key, product] += 1
            return {"state": "ok", "detail_key": f"{key}|{product}"}

        rows.append(alert_matrix_service.AlertMatrixRow(key, key, "spc", "weekly", evaluate))

    monkeypatch.setattr(alert_matrix_service, "MATRIX_ROWS", tuple(rows))
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda: list(PRODUCTS))
    monkeypatch.setattr(SessionManager, "AVAILABLE_PRODUCTS", list(PRODUCTS))
    monkeypatch.setattr(SessionManager, "get_active_config", lambda: None)
    monkeypatch.setattr(AppSetup, "initialize_app", lambda: None)
    monkeypatch.setattr(page_header, "render_page_header", lambda **kwargs: None)
    monkeypatch.setattr(db_handler, "DatabaseManager", lambda: None)
    monkeypatch.setattr(step_labels, "get_cached_step_description_map", lambda db: {})
    monkeypatch.setattr(cpk_monitor_dashboard, "render_cpk_monitor_section", lambda *args: None)
    monkeypatch.setattr(alert_matrix_cache, "build_default_matrix_context",
                        lambda products, reference_date: alert_matrix_service.AlertMatrixContext(reference_date))
    monkeypatch.setattr(alert_matrix_cache, "build_cell_source_signature", lambda *args: "stable")
    monkeypatch.setattr(alert_matrix_cache, "get_indicator_product_revision",
                        lambda key, product: indicator_cache.get_indicator_product_revision(
                            key, product, revision_dir=tmp_path))
    monkeypatch.setattr(alert_matrix, "bump_indicator_product_revision",
                        lambda key, product: indicator_cache.bump_indicator_product_revision(
                            key, product, revision_dir=tmp_path))
    from app.manager import compliance_manager

    monkeypatch.setattr(compliance_manager, "load_matrix_config", lambda: None)
    monkeypatch.setattr(compliance_manager, "apply_matrix_compliance", lambda payload, config: payload)
    alert_matrix_cache._cached_alert_matrix_cell.clear()

    def new_session(admin=False):
        app = AppTest.from_file(str(PAGE_PATH), default_timeout=15)
        if admin:
            app.query_params["admin"] = "true"
        app.run()
        assert not app.exception
        return app

    yield calls, new_session
    alert_matrix_cache._cached_alert_matrix_cell.clear()


@pytest.mark.parametrize("warm_cache", [False, True])
def test_admin_can_refresh_before_query_without_computing_board(matrix_sessions, warm_cache):
    calls, new_session = matrix_sessions
    if warm_cache:
        normal = new_session()
        normal.button(key="btn_load_alert_matrix").click().run()
        assert not normal.exception
    before_refresh = calls.copy()
    admin = new_session(admin=True)

    admin.button(key="matrix_refresh_cell").click().run()

    assert not admin.exception
    assert calls == before_refresh
    assert admin.button(key="btn_load_alert_matrix")


def test_normal_and_admin_sessions_reuse_cells_and_targeted_refresh(matrix_sessions):
    calls, new_session = matrix_sessions
    normal = new_session()
    assert not any(button.key == "matrix_refresh_cell" for button in normal.button)
    normal.button(key="btn_load_alert_matrix").click().run()
    assert not normal.exception
    expected = Counter({(key, product): 1 for key in INDICATORS for product in PRODUCTS})
    assert calls == expected

    admin = new_session(admin=True)
    assert calls == expected
    admin.button(key="btn_load_alert_matrix").click().run()
    assert not admin.exception
    assert calls == expected

    admin.button(key="matrix_refresh_cell").click().run()
    assert not admin.exception
    assert calls == expected
    admin.button(key="btn_load_alert_matrix").click().run()
    assert not admin.exception
    expected[INDICATORS[0], PRODUCTS[0]] += 1
    assert calls == expected

    normal.run()
    assert not normal.exception
    assert calls == expected


def test_matrix_data_refresh_reloads_target_and_preserves_other_boards(matrix_sessions, monkeypatch):
    from app.sections.inline_domain.monitor import refresh_controls

    cleared = []
    monkeypatch.setattr(refresh_controls, "clear_cpk_source_cache", lambda: cleared.append("cpk"))
    calls, new_session = matrix_sessions
    admin = new_session(admin=True)
    admin.button(key="btn_load_alert_matrix").click().run()
    before = calls.copy()
    admin.session_state["monitor_query_signature"] = "oos-selection"
    admin.session_state["cpk_monitor_query_signature"] = "cpk-selection"
    admin.selectbox(key="matrix_refresh_indicator").select("spc_cpk_trend").run()

    admin.button(key="matrix_refresh_data").click().run()

    assert not admin.exception
    before["spc_cpk_trend", PRODUCTS[0]] += 1
    assert calls == before
    assert cleared == ["cpk"]
    assert admin.session_state["monitor_query_signature"] == "oos-selection"
    assert admin.session_state["cpk_monitor_query_signature"] == "cpk-selection"
