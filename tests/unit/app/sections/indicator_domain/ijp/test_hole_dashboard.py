from streamlit.testing.v1 import AppTest


def test_filter_failure_is_safe_and_clears_previous_result():
    app = AppTest.from_string('''
from datetime import datetime
import streamlit as st
from app.sections.indicator_domain.ijp_hole.dashboard import render_ijp_hole_dashboard, RESULT_KEY
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
class Service:
    def get_reporting_window(self):
        return datetime(2026, 8, 1), datetime(2026, 9, 10)
    def get_filter_options(self, products):
        raise IjpDataAccessError('SELECT secret_table; admin=true')
st.session_state[RESULT_KEY] = {'previous': 'result'}
render_ijp_hole_dashboard(Service())
''').run()
    assert not app.exception
    assert app.error[0].value == 'IJP 孔区筛选项暂时无法读取，请稍后重试。'
    assert 'ijp_hole_result' not in app.session_state
    assert not app.get('plotly_chart')
