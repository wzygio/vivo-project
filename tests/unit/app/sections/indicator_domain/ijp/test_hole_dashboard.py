from streamlit.testing.v1 import AppTest
from pathlib import Path
import json

from src.shared_kernel.config import ConfigLoader


def test_actual_hole_page_periods_filters_empty_and_failure(monkeypatch):
    # The browser fixture replaces composition roots while executing the real page.
    # Register originals so its replacements cannot leak into subsequent AppTests.
    from app.components import page_header
    from app.manager.session_manager import SessionManager
    from app.sections.indicator_domain.ijp import dashboard
    from app.utils.app_setup import AppSetup
    from src.indicator_domain import composition
    from src.shared_kernel.infrastructure import db_handler

    for module, name in (
        (page_header, 'render_page_header'), (dashboard, 'render_ijp_dashboard'),
        (composition, 'build_ijp_service'), (composition, 'build_ijp_hole_service'),
        (db_handler, 'DatabaseManager'),
    ):
        monkeypatch.setattr(module, name, getattr(module, name))
    for owner, name in (
        (AppSetup, 'initialize_app'), (SessionManager, 'get_active_config'),
        (ConfigLoader, 'get_data_forward_policy'),
    ):
        monkeypatch.setattr(owner, name, owner.__dict__[name])
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: None)
    fixture = Path(__file__).resolve().parents[6] / 'tests/e2e/fixtures/ijp_hole_app.py'
    app = AppTest.from_file(str(fixture)).run()
    app.selectbox(key='ijp_region').select('孔区').run()
    assert not app.exception and not app.get('plotly_chart')
    assert app.multiselect(key='ijp_hole_codes').options == ['C3RA1', 'C3RA2', 'C3RA3']
    app.session_state['ijp_hole_view'] = 'Total'  # Older sessions cannot restore removed modes.
    app.button(key='ijp_hole_search').click().run()
    assert not app.exception and not app.radio
    charts = app.get('plotly_chart')
    assert len(charts) == 2
    for chart in charts:
        spec = json.loads(chart.proto.spec)
        assert [trace['name'] for trace in spec['data']] == ['C3RA1', 'C3RA2', 'C3RA3']
        assert len(spec['data'][0]['x']) == 5
        assert spec['data'][0]['x'][:2] == ['2026-08（月）', '2026-09（月）']
        assert spec['layout']['barmode'] == 'stack'
        assert not any(word in chart.proto.spec for word in ('raw_ratio', 'code_num', 'in_window'))
    app.multiselect(key='ijp_hole_codes').select('C3RA3').run()
    app.button(key='ijp_hole_search').click().run()
    assert not app.exception and len(app.get('plotly_chart')) == 2
    assert all([t['name'] for t in json.loads(c.proto.spec)['data']] == ['C3RA3']
               for c in app.get('plotly_chart'))
    app.multiselect(key='ijp_hole_codes').unselect('C3RA3').run()
    app.multiselect(key='ijp_hole_codes').select('C3RA2').run()
    assert not app.get('plotly_chart')
    app.button(key='ijp_hole_search').click().run()
    assert all([t['name'] for t in json.loads(c.proto.spec)['data']] == ['C3RA2']
               for c in app.get('plotly_chart'))
    app.multiselect(key='ijp_hole_lines').select('3CEE02').run()
    app.button(key='ijp_hole_search').click().run()
    assert not app.exception and not app.get('plotly_chart')
    assert app.info[0].value == '当前筛选条件下暂无 IJP 溢流数据。'
    app.multiselect(key='ijp_hole_lines').unselect('3CEE02').run()
    app.multiselect(key='ijp_hole_products').select('M678').run()
    app.button(key='ijp_hole_search').click().run()
    assert not app.exception and not app.get('plotly_chart')
    assert app.error[0].value == 'IJP 数据暂时无法读取，请稍后重试。'
    assert 'ijp_hole_result' not in app.session_state


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
    assert app.error[0].value == 'IJP 筛选项暂时无法读取，请稍后重试。'
    assert not app.subheader
    assert not app.caption
    assert 'ijp_hole_result' not in app.session_state
    assert not app.get('plotly_chart')
