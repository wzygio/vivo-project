"""Production section exercised through its service injection boundary."""

from streamlit.testing.v1 import AppTest

APP = '''
import streamlit as st
import pandas as pd
from unittest.mock import patch
from src.shared_kernel.config import ConfigLoader
from tests.unit.test_iqc_lifetime import MemorySource, source_frame
from src.iqc_domain.application.lifetime.lifetime import LifetimeReportService
from app.sections.iqc_domain.lifetime.lifetime_dashboard import render_lifetime_dashboard
class Source(MemorySource):
    def read_report(self):
        mode = st.session_state.get('mode', 'normal')
        if mode == 'failure':
            raise RuntimeError('private SQL credentials')
        if mode == 'empty':
            return self.frame.iloc[:0]
        if mode == 'missing_luminance':
            return self.frame.assign(lumi_decay=None)
        if mode == 'many':
            frame = pd.concat([self.frame] * 6, ignore_index=True)
            frame['test_time'] = range(len(frame))
            return frame
        if st.session_state.get('extended'):
            other = self.frame.copy()
            other['product_group'] = 'M626'
            other['product_id'] = '模组'
            disabled = self.frame.copy()
            disabled['product_group'] = 'DISABLED'
            return pd.concat([self.frame, other, disabled], ignore_index=True)
        return self.frame.copy()
with patch.object(ConfigLoader, 'get_enabled_products', return_value=st.session_state.get('enabled', ['M678', 'M626'])):
    render_lifetime_dashboard(LifetimeReportService(Source(source_frame())))
'''


def test_filter_empty_failure_and_recovery():
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert len(app.multiselect) == 3
    assert not app.button
    assert [expander.label for expander in app.expander] == [
        'M678 · 2026/3/10 · 效率衰减', 'M678 · 2026/3/10 · 亮度衰减',
    ]
    assert all(len(expander.get('plotly_chart')) == 4 for expander in app.expander)
    assert len(app.get('plotly_chart')) == 8
    assert 'private' not in app.dataframe[0].value.to_json(force_ascii=False)
    assert list(app.dataframe[0].value.columns)[4] == '样品编号'
    assert len(app.dataframe[0].value.columns) == 15
    app.multiselect(key='iqc_lifetime_products').set_value(['M678']).run()
    assert not app.exception and len(app.get('plotly_chart')) == 8
    app.session_state['mode'] = 'failure'
    app.run()
    assert not app.exception and len(app.error) == 1
    assert 'private' not in app.error[0].value
    assert not app.get('plotly_chart') and not app.dataframe
    app.session_state['mode'] = 'empty'
    app.run()
    assert not app.exception and app.info
    assert not app.dataframe
    app.session_state['mode'] = 'normal'
    app.run()
    assert not app.exception and len(app.get('plotly_chart')) == 8


def test_enabled_product_scope_status_filter_and_configuration_change():
    app = AppTest.from_string(APP).run()
    app.session_state['extended'] = True
    app.run()
    assert not app.exception
    assert app.multiselect(key='iqc_lifetime_products').options == ['M678', 'M626']
    assert 'DISABLED' not in app.dataframe[0].value.to_json(force_ascii=False)
    assert len(app.expander) == 4
    app.multiselect(key='iqc_lifetime_statuses').set_value(['模组']).run()
    assert not app.exception and len(app.expander) == 2
    assert app.expander[0].label.startswith('M626')
    assert '屏体' not in app.dataframe[0].value.to_json(force_ascii=False)
    app.multiselect(key='iqc_lifetime_products').set_value(['M626']).run()
    app.session_state['enabled'] = ['M678']
    app.run()
    assert not app.exception
    assert app.multiselect(key='iqc_lifetime_products').value == []
    assert app.multiselect(key='iqc_lifetime_statuses').value == []
    assert len(app.expander) == 2 and app.expander[0].label.startswith('M678')
    assert 'M626' not in app.dataframe[0].value.to_json(force_ascii=False)
    app.session_state['enabled'] = []
    app.run()
    assert not app.exception and app.info
    assert not app.dataframe and not app.get('plotly_chart')


def test_native_table_receives_all_filtered_rows_without_pagination():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'many'
    app.run()
    assert not app.exception
    assert not app.number_input
    assert len(app.dataframe[0].value) == 144
    assert app.dataframe[0].value.index.tolist() == list(range(144))
    assert 'panel_id' not in app.dataframe[0].value.columns
    assert not app.get('html')


def test_missing_luminance_does_not_hide_efficiency_charts():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'missing_luminance'
    app.run()
    assert not app.exception
    assert len(app.expander[0].get('plotly_chart')) == 4
    assert not app.expander[1].get('plotly_chart')
    assert len(app.expander[1].info) == 4
    assert all(info.value == '该组暂无有效亮度衰减测点。' for info in app.expander[1].info)
    assert app.warning and len(app.dataframe[0].value) == 24
