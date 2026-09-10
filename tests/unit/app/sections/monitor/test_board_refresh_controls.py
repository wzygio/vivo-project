"""Refresh actions preserve other boards and invalidate only selected workbooks."""

from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.monitor import cpk_monitor_dashboard, refresh_controls
from src.inline_domain.infrastructure.monitor import excel_alarm_store


def _cpk_app():
    import streamlit as st
    from app.sections.inline_domain.monitor.cpk_monitor_dashboard import render_cpk_monitor_section

    class Service:
        def build_dashboard(self, **kwargs):
            st.session_state["cpk_loads"] = st.session_state.get("cpk_loads", 0) + 1
            return object()

    st.session_state.setdefault("monitor_query_signature", "oos-query")
    st.session_state.setdefault("alert_matrix_board_loaded", True)
    render_cpk_monitor_section(Service(), ["M626"], ["ARRAY"])


@pytest.mark.parametrize("action", ["cache", "data"])
def test_cpk_refresh_is_independent(monkeypatch, action):
    clear = Mock()
    monkeypatch.setattr(cpk_monitor_dashboard, "clear_cpk_source_cache", clear)
    monkeypatch.setattr(cpk_monitor_dashboard, "render_cpk_monitor_results", lambda view: None)
    app = AppTest.from_function(_cpk_app)
    app.query_params["admin"] = "true"
    app.run()
    app.button(key="cpk_monitor_query_submit").click().run()
    assert not app.exception
    assert app.session_state["cpk_loads"] == 1

    app.button(key=f"cpk_monitor_refresh_{action}").click().run()

    assert not app.exception
    clear.assert_called_once_with()
    assert app.session_state["cpk_loads"] == (2 if action == "data" else 1)
    assert app.session_state["monitor_query_signature"] == "oos-query"
    assert app.session_state["alert_matrix_board_loaded"] is True


def test_refresh_failure_is_visible_and_does_not_submit_query(monkeypatch):
    monkeypatch.setattr(cpk_monitor_dashboard, "clear_cpk_source_cache", Mock(side_effect=OSError("locked")))
    app = AppTest.from_function(_cpk_app)
    app.query_params["admin"] = "true"
    app.run()
    app.button(key="cpk_monitor_refresh_data").click().run()
    assert not app.exception
    assert len(app.error) == 1
    assert "cpk_loads" not in app.session_state
    assert app.session_state["monitor_query_signature"] == "oos-query"


def test_workbook_refresh_does_not_clear_other_workbooks(monkeypatch, tmp_path):
    path = tmp_path / "cpk.xlsx"
    path.write_bytes(b"cache-key-only")
    cached = Mock()
    monkeypatch.setattr(excel_alarm_store, "read_cached_alarm_workbook", cached)
    excel_alarm_store.clear_alarm_workbook_cache(path)
    stat = path.stat()
    cached.clear.assert_called_once_with(str(path.resolve()), stat.st_mtime_ns, stat.st_size)


def test_oos_refresh_only_invalidates_selected_scopes(monkeypatch, tmp_path):
    clear = Mock()
    monkeypatch.setattr(refresh_controls, "clear_alarm_workbook_cache", clear)
    monkeypatch.setattr(refresh_controls, "scope_decoration_path", lambda scope, alarm: tmp_path / f"{scope}_{alarm}.xlsx")
    refresh_controls.clear_oos_source_cache(("ctq",))
    assert [call.args[0].name for call in clear.call_args_list] == ["ctq_oos.xlsx", "ctq_ooc.xlsx"]
