from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.shared_kernel.config import ConfigLoader
from src.indicator_domain.application.qtime.chamber_cache import clear_chamber_cache

ROOT = Path(__file__).resolve().parents[5]


@pytest.mark.parametrize('fixture,button', [
    ('qtime_app.py', 'qtime_search'),
    ('chamber_qtime_app.py', 'chamber_search'),
    ('ijp_app.py', 'ijp_search'),
])
def test_policy_change_hides_submitted_charts_until_requery(monkeypatch, fixture, button):
    state = {'exclusion': None}
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: state['exclusion'])
    clear_chamber_cache()
    app = AppTest.from_file(str(ROOT / 'tests/e2e/fixtures' / fixture)).run()
    app.button(key=button).click().run()
    assert not app.exception and app.get('plotly_chart')
    state['exclusion'] = ('2026-10-01', '2026-10-06')
    app.run()
    assert not app.exception and not app.get('plotly_chart')
    assert any('点击“查询' in item.value for item in app.info)
    app.button(key=button).click().run()
    assert not app.exception and app.get('plotly_chart')


def test_hole_policy_change_invalidates_submitted_session(monkeypatch):
    state = {'exclusion': None}
    monkeypatch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: state['exclusion'])
    app = AppTest.from_string('''
from datetime import datetime
import pandas as pd
from app.sections.indicator_domain.ijp_hole.dashboard import render_ijp_hole_dashboard

class FixtureService:
    enabled_products = ('M626',)
    work_order_types = ()
    def get_reporting_window(self):
        return datetime(2026, 9, 1), datetime(2026, 9, 10)
    def get_filter_options(self, products):
        return {'product_codes': ('M626',), 'lines': ('3CEE01',),
                'codes': ('C3RA1',), 'picis': ()}
    def get_report(self, query):
        frame = pd.DataFrame({'productcode': ['M626'], 'line': ['3CEE01'],
                              'printer': ['3CEE01-IK2-PR1'], 'glass_id': ['G1'],
                              'rs_code': ['C3RA1'], 'code_num': [1], 'ratio': [1.0]})
        return {'glass': frame, 'total': frame, 'day': frame.assign(day='2026-09-01')}

render_ijp_hole_dashboard(FixtureService())
''').run()
    app.button(key='ijp_hole_search').click().run()
    assert not app.exception and app.get('plotly_chart')
    state['exclusion'] = ('2026-10-01', '2026-10-06')
    app.run()
    assert not app.exception and not app.get('plotly_chart')
