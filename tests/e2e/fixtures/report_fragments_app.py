"""Offline browser counters around real lifetime, point-test and IJP fragments.

Select ?report=lifetime|point|ijp. No database or workbook adapter is used.
The full-page button is the positive control; the sibling fragment provides
an independently submitted result whose DOM must survive report interactions.
"""

from datetime import datetime
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for path in (ROOT, ROOT / 'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def increment(key):
    st.session_state[key] = st.session_state.get(key, 0) + 1
    return st.session_state[key]


class LifetimeSource:
    def read_report(self):
        from tests.unit.test_iqc_lifetime import source_frame

        increment('fixture_source_loads')
        frame = source_frame()
        frame = frame.loc[frame.bu.eq('W') & frame.panel_id.eq('private-a')].copy()
        other = frame.copy()
        other['product_group'] = 'M626'
        other['date_key'] = 'other-batch'
        return pd.concat([frame, other], ignore_index=True)


class PointSource:
    def read_sheets(self):
        from tests.unit.indicator_domain.point_test.test_report import MemorySource, summary_rows

        increment('fixture_source_loads')
        sheets = dict(MemorySource().sheets)
        sheets['M626-detail'] = sheets['Z576-detail'].copy()
        sheets['M626-detail'].iloc[1, 3] = 7.5
        sheets['M626-summary'] = pd.DataFrame(summary_rows('M'))
        return sheets


class IjpService:
    enabled_products = ('M626', 'M678')
    work_order_types = ()

    def __init__(self, region):
        from src.indicator_domain.application.ijp.settings import IjpSettings

        self.region = region
        self.settings = IjpSettings()

    def get_reporting_window(self):
        return datetime(2026, 9, 1), datetime(2026, 9, 10)

    def get_filter_options(self, products=(), **kwargs):
        return {'product_codes': self.enabled_products, 'lines': ('3CEE01',),
                'codes': ('C3RA1',) if self.region == '孔区' else ('C3DM1',),
                'picis': ('LOT1',)}

    def _report(self, query):
        counter = 'fixture_hole_queries' if self.region == '孔区' else 'fixture_aa_queries'
        increment(counter)
        return pd.DataFrame({
            'productcode': [next(iter(query.product_codes), 'M626')],
            'line': ['3CEE01'], 'printer': ['3CEE01-IK2-PR1'],
            'rs_code': ['C3RA1' if self.region == '孔区' else 'C3DM1'],
            'period': ['2026-09（月）'], 'period_start': ['2026-09-01'],
            'period_end': ['2026-09-10'], 'has_data': [True],
            'display_ratio': [0.91], 'ratio': [0.91],
        })

    def get_period_ratios(self, query):
        return self._report(query)

    def get_report(self, query):
        return {'day': self._report(query)}


@st.cache_resource
def install_fixture():
    from app.components import page_header
    from app.manager.session_manager import SessionManager
    from app.sections.indicator_domain.ijp import dashboard as aa_ui
    from app.sections.indicator_domain.ijp_hole import dashboard as hole_ui
    from app.sections.iqc_domain.lifetime import lifetime_dashboard
    from app.utils.app_setup import AppSetup
    from src.indicator_domain import composition
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.data_forward import DataForwardPolicy
    from src.shared_kernel.infrastructure import db_handler

    patch = pytest.MonkeyPatch()
    patch.setattr(lifetime_dashboard, 'load_lifetime_specs', dict)
    patch.setattr(AppSetup, 'initialize_app', lambda: None)
    patch.setattr(SessionManager, 'get_active_config', lambda: {})
    patch.setattr(page_header, 'render_page_header', lambda *a, **kw: None)
    patch.setattr(db_handler, 'DatabaseManager', lambda: SimpleNamespace(engine=None))
    patch.setattr(ConfigLoader, 'get_enabled_products', lambda **kw: ['M678', 'M626', 'Z576'])
    patch.setattr(ConfigLoader, 'get_product_annotations', lambda: {})
    patch.setattr(ConfigLoader, 'get_indicator_data_exclusion', lambda: None)
    patch.setattr(ConfigLoader, 'get_data_forward_policy', lambda: DataForwardPolicy(enabled=False))

    def build(region):
        increment('fixture_source_loads')
        return IjpService(region)

    def saved_alerts(service, region, **kwargs):
        st.caption(f'Fixture region: {region}')

    render_aa = aa_ui._render_grouped_glass_charts
    render_hole = hole_ui.render_hole_charts

    def aa_charts(frame, **kwargs):
        render_aa(frame, **kwargs)
        st.caption(f"AA queries: {st.session_state.get('fixture_aa_queries', 0)}")
        st.caption('Fixture result service: AA区')

    def hole_charts(frame):
        render_hole(frame)
        st.caption(f"Hole queries: {st.session_state.get('fixture_hole_queries', 0)}")
        st.caption('Fixture result service: 孔区')

    patch.setattr(composition, 'build_ijp_service', lambda _db: build('AA区'))
    patch.setattr(composition, 'build_ijp_hole_service', lambda _db: build('孔区'))
    # Alerts are an independent persisted-result adapter; no automatic publish
    # or alert store is exercised by this report-fragment fixture.
    patch.setattr(aa_ui, 'render_ijp_alert_center', saved_alerts)
    patch.setattr(hole_ui, 'render_ijp_alert_center', saved_alerts)
    patch.setattr(aa_ui, '_render_grouped_glass_charts', aa_charts)
    patch.setattr(hole_ui, 'render_hole_charts', hole_charts)
    return patch


@st.fragment
def render_sibling():
    runs = increment('fixture_sibling_runs')
    if st.button('Fixture sibling query', key='fixture_sibling_query'):
        increment('fixture_sibling_queries')
    st.caption(f'Sibling renders: {runs}')
    st.success(f"Sibling submitted result: {st.session_state.get('fixture_sibling_queries', 0)}")


_patch = install_fixture()
mode = st.query_params.get('report', 'lifetime')
increment('fixture_page_runs')
st.button('Fixture full page reload', key='fixture_full_reload')
if mode == 'lifetime':
    from app.sections.iqc_domain.lifetime.lifetime_dashboard import render_lifetime_dashboard
    from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService

    from tests.e2e.fixtures.lifetime_forecast import fixture_alert_result
    render_lifetime_dashboard(LifetimeReportService(LifetimeSource()),
                              saved_alert_result=fixture_alert_result('normal'))
elif mode == 'point':
    from app.sections.indicator_domain.point_test.dashboard import render_point_test_dashboard
    from src.indicator_domain.application.point_test.service import PointTestReportService

    render_point_test_dashboard(PointTestReportService(PointSource()))
elif mode == 'ijp':
    runpy.run_path(str(ROOT / 'app/pages/IJP溢流监控报表.py'), run_name='__main__')
else:
    raise ValueError(f'Unsupported fixture report: {mode}')
st.caption(f"Fixture page runs: {st.session_state['fixture_page_runs']}")
st.caption(f"Fixture source loads: {st.session_state.get('fixture_source_loads', 0)}")
render_sibling()
