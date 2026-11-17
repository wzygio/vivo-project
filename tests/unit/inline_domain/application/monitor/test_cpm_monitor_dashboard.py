from streamlit.testing.v1 import AppTest


def test_cpk_cpm_queries_show_separate_summaries_and_no_latest_project_details():
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
    assert len(app.dataframe) == 1
    assert app.dataframe[0].value["指标"].tolist() == ["CPM总项目数"]
    assert len(app.get("plotly_chart")) == 1
    public_output = app.dataframe[0].value.to_json() + app.get("plotly_chart")[0].proto.spec
    assert all(token not in public_output for token in ("flag", "replacement", "private", "internal-only"))
    assert not any("最新项目明细" in item.value for item in app.markdown)
    assert not any("管理员" in item.value for item in app.markdown)
    app.button(key="cpk_monitor_query_submit").click().run()
    assert not app.exception
    assert len(app.dataframe) == 2
    assert len(app.get("plotly_chart")) == 2
    assert {frame.value["指标"].iloc[0] for frame in app.dataframe} == {"CPK总项目数", "CPM总项目数"}


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
