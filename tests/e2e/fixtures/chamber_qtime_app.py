"""Deterministic source adapter; exercise the real service, cache and section."""

import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

root = Path(__file__).resolve().parents[3]
for path in (root, root / 'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from qtime_app import FixtureQTimeService
from app.sections.indicator_domain.qtime.dashboard import render_qtime_dashboard
from app.sections.indicator_domain.qtime.chamber_dashboard import render_chamber_dashboard, clear_chamber_result
from src.indicator_domain.application.qtime.chamber_cache import clear_chamber_cache
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.decoration_service import parse_qtime_decision_upload
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.core.qtime.alerts import build_qtime_alerts
from src.indicator_domain.core.qtime.decoration import apply_qtime_decoration
from src.indicator_domain.core.qtime.chamber import CHAMBERS


class FixtureSource:
    def cache_signature(self):
        return ('chamber-browser-fixture-v2', st.query_params.get('scenario', 'normal'))

    def read(self, *, as_of):
        scenario = st.query_params.get('scenario', 'normal')
        if scenario == 'failure':
            raise QTimeDataAccessError('INTERNAL_SECRET_SQL_MUST_NOT_LEAK')
        records = [
            {'line': line, 'glass_id': glass, 'entry_time': pd.Timestamp(date.today()) - pd.Timedelta(days=1),
             **dict.fromkeys(CHAMBERS, value)}
            for line, glass, value in [('3CEE001', 'GLASS_A', 1000), ('3CEE002', 'GLASS_B', 120),
                                      ('3CEE002', 'GLASS_C', None), ('3CEE001', 'DISABLED_GLASS', 10),
                                      ('3CEE002', 'OUTLIER_GLASS', 1000.01)]
        ]
        measurements = pd.DataFrame(records)
        if scenario == 'empty':
            measurements = measurements.iloc[:0]
        return {
            'measurements': measurements,
            'products': pd.DataFrame({'glass_id': ['GLASS_A', 'GLASS_B', 'GLASS_C', 'DISABLED_GLASS', 'OUTLIER_GLASS'],
                                      'prod_code': ['M626', 'M678', 'M678', 'DISABLED_PRODUCT', 'M678']}),
            'targets': pd.DataFrame([{'line': line, 'chamber': chamber, 'target_seconds': 6000}
                                    for line in ('3CEE001', '3CEE002') for chamber in CHAMBERS]),
            'invalid_rows': 0,
        }


class CountingStationService(FixtureQTimeService):
    def cache_signature(self, shop):
        return (*super().cache_signature(shop), st.session_state.get('fixture_decision_revision', 0))

    def get_filter_options(self, shop):
        count = st.session_state.get('fixture_station_runs', 0) + 1
        st.session_state['fixture_station_runs'] = count
        st.caption(f'station-executions: {count}')
        st.caption(f"station-decision-revision: {st.session_state.get('fixture_decision_revision', 0)}")
        monitoring = st.session_state.get('qtime_report_result')
        if monitoring is not None:
            st.caption(f"fixture-result-flags: {monitoring.decisions['flag'].tolist()}")
        return super().get_filter_options(shop)

    def get_current_monitoring(self, **kwargs):
        result = super().get_current_monitoring(**kwargs)
        decisions = st.session_state.get('fixture_decisions')
        if decisions is None:
            return result
        decorated = apply_qtime_decoration(result.details, decisions)
        return replace(result, details=decorated.details, decoration=decorated.decoration,
                       alerts=build_qtime_alerts(decorated.decoration), decisions=decisions)

    def update_decisions(self, file_bytes):
        outcome = parse_qtime_decision_upload(file_bytes)
        if outcome.status == 'success':
            st.session_state['fixture_decisions'] = outcome.decisions
            st.session_state['fixture_decision_revision'] = st.session_state.get('fixture_decision_revision', 0) + 1
        return outcome


class CountingChamberService(ChamberQTimeService):
    def cache_signature(self):
        count = st.session_state.get('fixture_chamber_runs', 0) + 1
        st.session_state['fixture_chamber_runs'] = count
        st.caption(f'chamber-executions: {count}')
        return super().cache_signature()


st.set_page_config(layout='wide')
st.session_state['fixture_page_runs'] = st.session_state.get('fixture_page_runs', 0) + 1
st.caption(f"page-executions: {st.session_state['fixture_page_runs']}")
if st.button('刷新单腔缓存', key='fixture_refresh'):
    clear_chamber_cache()
    clear_chamber_result()
render_qtime_dashboard(CountingStationService())
render_chamber_dashboard(CountingChamberService(
    FixtureSource(), enabled_products=('M678',) if st.query_params.get('restricted') else ('M626', 'M678'),
    cache_namespace='isolated-browser-fixture',
))
