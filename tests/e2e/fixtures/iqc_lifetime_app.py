"""Controlled scenarios around the actual page; not a production route."""

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for path in (ROOT, ROOT / 'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.components.page_header import render_page_header
from app.sections.iqc_domain.lifetime.lifetime_dashboard import (
    fetch_report_payload, render_lifetime_dashboard,
)
from app.utils.app_setup import AppSetup
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from tests.unit.test_iqc_lifetime import source_frame


class ScenarioSource:
    def read_report(self):
        scenario = st.query_params.get('scenario', 'normal')
        if scenario == 'failure':
            raise RuntimeError('private SQL connection credentials')
        frame = source_frame()
        if scenario == 'empty':
            return frame.iloc[:0]
        if scenario == 'missing':
            frame.loc[frame.test_time.eq('20'), 'eff_decay'] = None
            frame.loc[frame.test_time.eq('20'), 'lumi_decay'] = None
        # 4 screens x 9 samples x 3 times = 108, plus a second product/batch.
        frames = []
        for number in range(9):
            sample = frame.loc[frame.panel_id.eq('private-a')].copy()
            sample['panel_id'] = f'private-{number}'
            frames.append(sample)
        other = frame.iloc[[0]].copy()
        other['product_group'] = 'M626'
        other['product_id'] = '模组'
        other['date_key'] = 'other-batch'
        frames.append(other)
        disabled = other.copy()
        disabled['product_group'] = 'M999'
        frames.append(disabled)
        return pd.concat(frames, ignore_index=True)


# Keep injection local to this session; mutating a module-level renderer races
# with Streamlit reruns and concurrent browser sessions.
st.set_page_config(page_title='IQC寿命测试报表', layout='wide', initial_sidebar_state='collapsed')
AppSetup.initialize_app()
render_page_header(
    title='IQC寿命测试报表', show_product_filter=False,
    show_data_refresh=False, cached_funcs=[fetch_report_payload],
)
render_lifetime_dashboard(LifetimeReportService(ScenarioSource()))
