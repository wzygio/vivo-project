"""Controlled scenarios around the actual page; not a production route."""

from pathlib import Path
import runpy
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
for path in (ROOT, ROOT / 'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.sections.iqc_domain.lifetime import lifetime_dashboard
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


original_render = lifetime_dashboard.render_lifetime_dashboard
try:
    lifetime_dashboard.render_lifetime_dashboard = lambda: original_render(
        LifetimeReportService(ScenarioSource())
    )
    runpy.run_path(str(ROOT / 'app/pages/IQC寿命测试报表.py'), run_name='__main__')
finally:
    lifetime_dashboard.render_lifetime_dashboard = original_render
