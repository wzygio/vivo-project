from streamlit.testing.v1 import AppTest


def test_cpk_section_computes_only_after_own_query_and_filter_changes_hide_results():
    app = AppTest.from_string('''
import pandas as pd
import streamlit as st
from app.sections.inline_domain.monitor.cpk_monitor_dashboard import render_cpk_monitor_section
from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel
class Service:
    def build_dashboard(self, **kwargs):
        st.session_state["calls"] = st.session_state.get("calls", 0) + 1
        return CpkMonitorViewModel(
            pd.DataFrame({"指标": ["CPK总项目数"], "Y26": [2]}),
            pd.DataFrame({"period_type": ["month"], "cpk": [2.0], "status": ["达标"]}),
        )
render_cpk_monitor_section(Service(), ["M626", "M673"], ["ARRAY", "OLED"])
''')
    app.run()
    assert not app.exception
    assert len(app.dataframe) == 0
    app.button(key="cpk_monitor_query_submit").click().run()
    assert not app.exception
    assert app.session_state["calls"] == 1
    assert len(app.dataframe) == 2  # Summary and latest project details.
    app.multiselect(key="cpk_monitor_products").set_value(["M626"]).run()
    assert not app.exception
    assert app.session_state["calls"] == 1
    assert len(app.dataframe) == 0
