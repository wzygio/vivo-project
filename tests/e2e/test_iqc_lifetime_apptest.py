"""Production section exercised through its service injection boundary."""

import json

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
        if mode == 'missing_jncd':
            return self.frame.assign(jncd=None)
        if mode == 'no_white_screen':
            return self.frame.loc[self.frame.bu.ne('W')]
        if mode == 'jncd_screen_values':
            frame = self.frame.copy()
            frame['jncd'] = frame.bu.map({'W': '0.25', 'R': '9.75', 'G': '9.75', 'B': '9.75'})
            return frame
        if mode == 'missing_batch':
            missing = self.frame.assign(date_key=None)
            return pd.concat([self.frame, missing], ignore_index=True)
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
    assert app.button(key='iqc_lifetime_query').label == '查询'
    assert all(not selector.value for selector in app.multiselect)
    assert not app.expander and not app.get('plotly_chart') and not app.dataframe
    app.multiselect(key='iqc_lifetime_products').set_value(['M678']).run()
    assert not app.expander and not app.get('plotly_chart') and not app.dataframe
    app.button(key='iqc_lifetime_query').click().run()
    assert [expander.label for expander in app.expander] == [
        'M678 · 2026/3/10', '效率衰减', '亮度衰减', 'JNCD',
    ]
    assert len(app.expander[0].get('plotly_chart')) == 9
    assert [len(expander.get('plotly_chart')) for expander in app.expander[1:]] == [4, 4, 1]
    assert len(app.get('plotly_chart')) == 9
    assert 'private' not in app.dataframe[0].value.to_json(force_ascii=False)
    assert list(app.dataframe[0].value.columns)[4] == '样品编号'
    assert len(app.dataframe[0].value.columns) == 16
    assert app.dataframe[0].value['JNCD'].tolist()[:3] == [0.15, 0.75, 1.25]
    app.multiselect(key='iqc_lifetime_products').set_value(['M678']).run()
    assert not app.exception and len(app.get('plotly_chart')) == 9
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
    assert not app.exception and len(app.get('plotly_chart')) == 9


def test_enabled_product_scope_status_filter_and_configuration_change():
    app = AppTest.from_string(APP).run()
    app.session_state['extended'] = True
    app.run()
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception
    assert app.multiselect(key='iqc_lifetime_products').options == [
        'M678（CPD2455）', 'M626（CPD2515）',
    ]
    assert 'DISABLED' not in app.dataframe[0].value.to_json(force_ascii=False)
    assert len(app.expander) == 8
    app.multiselect(key='iqc_lifetime_statuses').set_value(['模组']).run()
    assert len(app.dataframe[0].value) == 48
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception and len(app.expander) == 4
    assert app.expander[0].label.startswith('M626')
    assert '屏体' not in app.dataframe[0].value.to_json(force_ascii=False)
    app.multiselect(key='iqc_lifetime_products').set_value(['M626']).run()
    app.button(key='iqc_lifetime_query').click().run()
    app.session_state['enabled'] = ['M678']
    app.run()
    assert not app.exception
    assert app.multiselect(key='iqc_lifetime_products').value == []
    assert app.multiselect(key='iqc_lifetime_statuses').value == []
    assert len(app.expander) == 4 and app.expander[0].label.startswith('M678')
    assert 'M626' not in app.dataframe[0].value.to_json(force_ascii=False)
    app.session_state['enabled'] = []
    app.run()
    assert not app.exception and app.info
    assert not app.dataframe and not app.get('plotly_chart')


def test_native_table_receives_all_filtered_rows_without_pagination():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'many'
    app.run()
    app.button(key='iqc_lifetime_query').click().run()
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
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception
    assert len(app.expander[1].get('plotly_chart')) == 4
    assert not app.expander[2].get('plotly_chart')
    assert len(app.expander[2].info) == 4
    assert all(info.value == '该组暂无有效亮度衰减测点。' for info in app.expander[2].info)
    assert not app.warning and len(app.dataframe[0].value) == 24


def test_missing_batch_filters_and_renders_all_metrics_without_warning():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'missing_batch'
    app.run()
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception and not app.error and not app.warning and not app.info
    assert len(app.dataframe[0].value) == 48
    assert '未填写批次' in app.multiselect(key='iqc_lifetime_batches').options
    app.multiselect(key='iqc_lifetime_batches').set_value(['未填写批次']).run()
    assert len(app.dataframe[0].value) == 48
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception and not app.error and not app.warning and not app.info
    assert len(app.dataframe[0].value) == 24
    assert app.dataframe[0].value['批次号'].unique().tolist() == ['未填写批次']
    assert [expander.label for expander in app.expander] == [
        'M678 · 未填写批次', '效率衰减', '亮度衰减', 'JNCD',
    ]
    assert len(app.get('plotly_chart')) == 9
    app.multiselect(key='iqc_lifetime_batches').set_value([]).run()
    assert len(app.dataframe[0].value) == 24
    app.button(key='iqc_lifetime_query').click().run()
    assert len(app.dataframe[0].value) == 48


def test_missing_jncd_does_not_hide_decay_charts():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'missing_jncd'
    app.run()
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception
    assert len(app.expander[1].get('plotly_chart')) == 4
    assert len(app.expander[2].get('plotly_chart')) == 4
    assert app.expander[3].label == 'JNCD'
    assert not app.expander[3].get('plotly_chart')
    assert len(app.expander[3].info) == 1
    assert all(info.value == '该组暂无有效JNCD测点。' for info in app.expander[3].info)
    assert not app.warning and len(app.dataframe[0].value) == 24
    assert app.dataframe[0].value['JNCD'].isna().all()


def test_jncd_uses_only_white_screen_while_details_keep_all_screens():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'jncd_screen_values'
    app.run()
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception
    jncd = app.expander[3]
    assert [element.value for element in jncd.text] == ['W']
    charts = jncd.get('plotly_chart')
    assert len(charts) == 1
    figure = json.loads(charts[0].proto.spec)
    assert len(figure['data']) == 2
    assert all(trace['y'] == [0.25, 0.25, 0.25] for trace in figure['data'])
    assert len(app.expander[1].get('plotly_chart')) == 4
    assert len(app.expander[2].get('plotly_chart')) == 4
    assert set(app.dataframe[0].value['测试画面']) == {'W', 'R', 'G', 'B'}
    assert len(app.dataframe[0].value) == 24
    assert app.dataframe[0].value.loc[app.dataframe[0].value['测试画面'].ne('W'), 'JNCD'].eq(9.75).all()


def test_without_white_screen_jncd_has_no_chart_and_decay_charts_remain():
    app = AppTest.from_string(APP).run()
    app.session_state['mode'] = 'no_white_screen'
    app.run()
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception and not app.error
    assert len(app.expander[1].get('plotly_chart')) == 3
    assert len(app.expander[2].get('plotly_chart')) == 3
    assert not app.expander[3].get('plotly_chart')
    assert [info.value for info in app.expander[3].info] == ['该组暂无 W 画面测点。']
    assert len(app.dataframe[0].value) == 18


def test_legacy_automatic_filters_do_not_display_results_before_query():
    app = AppTest.from_string(APP)
    app.session_state['iqc_lifetime_applied_filters'] = ([], [], [])
    app.run()
    assert not app.exception and not app.dataframe and not app.get('plotly_chart')
    app.run()
    assert not app.dataframe and not app.get('plotly_chart')
    app.button(key='iqc_lifetime_query').click().run()
    assert not app.exception and len(app.dataframe[0].value) == 24


def test_unfiltered_query_returns_all_products_and_changes_wait_for_query():
    app = AppTest.from_string(APP)
    app.session_state['extended'] = True
    app.run()
    assert not app.exception and not app.dataframe and not app.get('plotly_chart')
    app.button(key='iqc_lifetime_query').click().run()
    assert set(app.dataframe[0].value['产品型号']) == {'M678', 'M626'}
    assert len(app.dataframe[0].value) == 48
    app.multiselect(key='iqc_lifetime_products').set_value(['M626']).run()
    assert len(app.dataframe[0].value) == 48
    app.button(key='iqc_lifetime_query').click().run()
    assert set(app.dataframe[0].value['产品型号']) == {'M626'}
    assert len(app.dataframe[0].value) == 24
