from streamlit.testing.v1 import AppTest


def test_cpk_cpm_queries_are_independent_and_public_cpm_details_hide_internal_columns():
    app = AppTest.from_string('''
import pandas as pd
import streamlit as st
from app.sections.inline_domain.monitor.cpk_monitor_dashboard import render_cpk_monitor_section
from app.sections.inline_domain.monitor.cpm_monitor_dashboard import render_cpm_monitor_section
from src.inline_domain.application.monitor.cpk_monitor_service import CpkMonitorViewModel
class Service:
    def __init__(self, metric):
        self.metric = metric
    def build_dashboard(self, **kwargs):
        key = self.metric + "_calls"
        st.session_state[key] = st.session_state.get(key, 0) + 1
        return CpkMonitorViewModel(
            pd.DataFrame({"指标": [self.metric.upper() + "总项目数"], "W40": [10]}),
            pd.DataFrame({"prod_code": ["M626"], "factory": ["ARRAY"],
                          self.metric + "_corrected": [1.2], "flag": [False], "status": ["预警"],
                          self.metric + "_replacement": [1.39], "private": ["internal-only"]}),
            refresh_status_df=pd.DataFrame({"internal_path": ["private-source"]}),
        )
render_cpk_monitor_section(Service("cpk"), ["M626"], ["ARRAY"])
render_cpm_monitor_section(Service("cpm"), ["M626"], ["ARRAY"])
''').run()
    assert not app.exception
    assert not app.dataframe
    app.button(key="cpm_monitor_query_submit").click().run()
    assert not app.exception
    assert app.session_state["cpm_calls"] == 1
    assert "cpk_calls" not in app.session_state
    assert len(app.dataframe) == 2
    detail = app.dataframe[1].value
    assert "CPM" in detail and "CPK" not in detail
    assert set(detail.columns) == {"产品", "厂别", "CPM", "已达标", "判定状态"}
    assert not any("管理员" in item.value for item in app.markdown)
    app.button(key="cpk_monitor_query_submit").click().run()
    assert not app.exception
    assert len(app.dataframe) == 4


def test_cpm_failure_uses_safe_message():
    app = AppTest.from_string('''
from app.sections.inline_domain.monitor.cpm_monitor_dashboard import render_cpm_monitor_section
class Service:
    def build_dashboard(self, **kwargs):
        raise RuntimeError("private-source-path")
render_cpm_monitor_section(Service(), ["M626"], ["ARRAY"])
''').run()
    app.button(key="cpm_monitor_query_submit").click().run()
    assert not app.exception
    assert app.error[0].value == "CPM 数据读取失败，请稍后重试。"
    assert not app.dataframe
