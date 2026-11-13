"""Real SPC page with isolated producers and visible browser rerun counters."""
import runpy
import sys
from pathlib import Path

import pytest
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


@st.cache_resource
def install_fixture():
    from tests.unit.app.pages.test_spc_filter_reruns import page
    patches = pytest.MonkeyPatch()
    _app, state = page.__wrapped__(patches)
    from app.sections.inline_domain.spc import spc_dashboard

    def alerts(*args, **kwargs):
        state["alerts"] += 1
        st.caption(f"Fixture load calls: {state['loads']}")
        st.caption("Fixture published alerts remain visible")

    def charts(**kwargs):
        state["charts"] += 1
        st.success(f"Fixture query result: {state['charts']}")

    patches.setattr(spc_dashboard, "render_saved_spc_alerts", alerts)
    patches.setattr(spc_dashboard, "render_spc_indicator_sections", charts)
    return state, patches


state, _patches = install_fixture()
st.button("Fixture full page reload")
runpy.run_path(str(ROOT / "app/pages/SPC监控报表.py"), run_name="__main__")
