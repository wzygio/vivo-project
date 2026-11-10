"""Exercise production presentation through its public service seam."""

import json

from streamlit.testing.v1 import AppTest

APP = '''
import pandas as pd
import streamlit as st
from tests.unit.indicator_domain.point_test.test_report import MemorySource, summary_rows
from src.indicator_domain.application.point_test.service import PointTestReportService
from app.sections.indicator_domain.point_test.dashboard import render_point_test_dashboard
from src.shared_kernel.config import ConfigLoader
from unittest.mock import patch
class Source(MemorySource):
    def read_sheets(self):
        mode = st.session_state.get('mode', 'normal')
        if mode == 'failure':
            raise RuntimeError('private source path admin=true credentials')
        if mode == 'empty':
            return {'Z576-detail': pd.DataFrame(), 'Z576-summary': pd.DataFrame()}
        result = dict(self.sheets)
        result['M626-detail'] = result['Z576-detail'].copy()
        result['M626-detail'].iloc[1, 3] = 7.5
        result['M626-summary'] = pd.DataFrame(summary_rows('M'))
        result['DISABLED-detail'] = result['Z576-detail'].copy()
        result['DISABLED-summary'] = pd.DataFrame(summary_rows('DISABLED'))
        return result
with patch.object(ConfigLoader, 'get_enabled_products',
                  side_effect=RuntimeError('private config admin=true') if st.session_state.get('mode') == 'config_failure' else None,
                  return_value=st.session_state.get('enabled', ['Z576', 'M626'])), \\
     patch.object(ConfigLoader, 'get_product_annotations', return_value={'M626': '测试注释'}):
    render_point_test_dashboard(PointTestReportService(Source()))
'''


def test_user_can_switch_products_and_see_summary_and_detail_boxes():
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert app.selectbox[0].options == ["Z576", "M626（测试注释）"]
    assert [tab.label for tab in app.tabs] == ["膜位汇总", "明细箱线图", "明细数据"]
    assert len(app.dataframe) == 2
    assert "阶段" not in app.dataframe[0].value.columns
    assert len(app.dataframe[0].value) == 12
    assert len(app.get("plotly_chart")) == 1
    assert [expander.label for expander in app.expander] == ["Z576 / 1B455 / CD1"]
    link = json.loads(app.dataframe[1].proto.columns)["IMAGE_DATA"]["type_config"]
    assert link["type"] == "link"
    assert link["display_text"] == "查看图片"
    assert app.dataframe[1].value.loc[0, "IMAGE_DATA"] == "https://example.invalid/point-test.png"
    app.selectbox[0].select("M626").run()
    assert not app.exception
    assert set(app.dataframe[0].value["产品"]) == {"M626"}
    assert app.dataframe[1].value.loc[0, "PARAM_VALUE"] == 7.5
    assert [expander.label for expander in app.expander] == ["M626 / 1B455 / CD1"]


def test_disabled_products_are_removed_and_obsolete_selection_is_reset():
    app = AppTest.from_string(APP).run()
    app.session_state["enabled"] = ["M626", "Z576"]
    app.run()
    assert app.selectbox[0].options == ["M626（测试注释）", "Z576"]
    app.selectbox[0].select("M626").run()
    app.session_state["enabled"] = ["Z576"]
    app.run()
    assert not app.exception
    assert app.selectbox[0].options == ["Z576"]
    assert set(app.dataframe[0].value["产品"]) == {"Z576"}
    assert app.dataframe[1].value.loc[0, "PARAM_VALUE"] == 12.543
    app.session_state["enabled"] = []
    app.run()
    assert not app.exception
    assert not app.selectbox and not app.dataframe


def test_empty_failure_and_recovery_do_not_retain_previous_tables_or_leak_errors():
    app = AppTest.from_string(APP).run()
    for mode in ("empty", "failure", "config_failure"):
        app.session_state["mode"] = mode
        app.run()
        assert not app.exception
        assert len(app.dataframe) == 0
        assert not app.get("plotly_chart")
        if mode == "empty":
            assert "暂无" in app.info[0].value
        else:
            assert "暂时无法读取" in app.error[0].value
            assert "private" not in app.error[0].value
            assert "admin=true" not in app.error[0].value
    app.session_state["mode"] = "normal"
    app.run()
    assert not app.exception
    assert len(app.dataframe) == 2
