"""Deterministic source adapter; exercise the real service, cache and section."""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

root = Path(__file__).resolve().parents[3]
for path in (root, root / 'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.sections.indicator_domain.qtime.chamber_dashboard import render_chamber_dashboard, clear_chamber_result
from src.indicator_domain.application.qtime.chamber_cache import clear_chamber_cache
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError
from src.indicator_domain.core.qtime.chamber import CHAMBERS


class FixtureSource:
    def cache_signature(self):
        return ('chamber-browser-fixture-v1', st.query_params.get('scenario', 'normal'))

    def read(self, *, as_of):
        scenario = st.query_params.get('scenario', 'normal')
        if scenario == 'failure':
            raise QTimeDataAccessError('INTERNAL_SECRET_SQL_MUST_NOT_LEAK')
        records = [
            {'line': line, 'glass_id': glass, 'entry_time': pd.Timestamp(date.today()) - pd.Timedelta(days=1),
             **dict.fromkeys(CHAMBERS, value)}
            for line, glass, value in [('3CEE001', 'GLASS_A', 6100), ('3CEE002', 'GLASS_B', 6000),
                                      ('3CEE002', 'GLASS_C', None), ('3CEE001', 'DISABLED_GLASS', 10)]
        ]
        measurements = pd.DataFrame(records)
        if scenario == 'empty':
            measurements = measurements.iloc[:0]
        return {
            'measurements': measurements,
            'products': pd.DataFrame({'glass_id': ['GLASS_A', 'GLASS_B', 'GLASS_C', 'DISABLED_GLASS'],
                                      'prod_code': ['M626', 'M678', 'M678', 'DISABLED_PRODUCT']}),
            'targets': pd.DataFrame([{'line': line, 'chamber': chamber, 'target_seconds': 6000}
                                    for line in ('3CEE001', '3CEE002') for chamber in CHAMBERS]),
            'invalid_rows': 0,
        }


st.set_page_config(layout='wide')
st.subheader('北极星QTime监控')
st.caption('既有站间监控区域')
if st.button('刷新单腔缓存'):
    clear_chamber_cache()
    clear_chamber_result()
render_chamber_dashboard(ChamberQTimeService(
    FixtureSource(), enabled_products=('M678',) if st.query_params.get('restricted') else ('M626', 'M678'),
    cache_namespace='isolated-browser-fixture',
))
