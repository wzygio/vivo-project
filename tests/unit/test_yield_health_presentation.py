import pytest
from streamlit.testing.v1 import AppTest


@pytest.mark.parametrize("admin", [None, "false", "True", "true"])
@pytest.mark.parametrize("status,kind", [("stale", "warning"), ("unknown", "warning"), ("partial", "warning"), ("unavailable", "error"), ("fresh", "caption")])
def test_yield_health_is_visible_only_for_admin(status, kind, admin):
    app = AppTest.from_string(
        "import streamlit as st\n"
        "from app.sections.yield_domain.data_health import render_yield_data_health\n"
        f"render_yield_data_health({{'status': '{status}', 'refreshed_at': '2026-09-01', 'source_start': '2026-06-01', 'source_end': '2026-09-01'}})\n"
        "st.text('results')"
    )
    if admin is not None:
        app.query_params["admin"] = admin
    app.run()
    assert not app.exception
    if admin == "true":
        assert len(app.get(kind)) == 1
    else:
        assert not app.warning
        assert not app.error
        assert not app.caption
    assert len(app.text) == (0 if status == "unavailable" else 1)


def test_yield_health_can_opt_out_of_admin_only_visibility():
    app = AppTest.from_string(
        "from app.sections.yield_domain.data_health import render_yield_data_health\n"
        "render_yield_data_health({'status': 'stale'}, admin_only=False)"
    ).run()
    assert not app.exception
    assert len(app.warning) == 1
