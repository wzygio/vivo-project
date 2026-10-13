"""Real Streamlit section, with the data port as the only controlled boundary."""

import pytest
from streamlit.testing.v1 import AppTest

from src.shared_kernel.config import ConfigLoader
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS


APP = '''
import streamlit as st
import pandas as pd
from tests.unit.test_iqc_evaporation import MemorySource, source_frame
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService, IqcSourceUnavailable
from app.sections.iqc_domain.eva_materials.evaporation_dashboard import render_evaporation_dashboard

class Source(MemorySource):
    def read_report(self, start, end, products):
        mode = st.session_state.get("test_mode", "normal")
        if mode == "failure":
            raise IqcSourceUnavailable("secret database details")
        if mode == "empty":
            return self.frame.iloc[:0]
        if 'M678' not in products:
            return self.frame.iloc[:0]
        return super().read_report(start, end, products)

frame = source_frame()
render_evaporation_dashboard(EvaporationReportService(Source(frame), enabled_products=('M678', 'M626')))
'''



def test_date_query_empty_failure_and_retry(monkeypatch):
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls, **kwargs: ['M678', 'M626']))
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert '查询' in app.info[0].value
    assert len(app.date_input) == 1
    assert app.multiselect[0].options == ['M678（CPD2455）', 'M626（CPD2515）']
    assert not app.selectbox
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    frame = app.dataframe[0].value
    assert list(frame.columns) == list(REPORT_COLUMNS)
    assert frame.columns[:3].tolist() == ['序号', '产品型号', '量产/非量产']
    assert frame['产品型号'].tolist() == ['M678']
    assert '蒸镀批次' not in frame.to_csv(index=False)
    assert 'batch_no' not in frame.to_csv(index=False)
    assert '通用' not in frame.to_csv(index=False)
    assert frame['IQC-1'].tolist() == [63.081]
    assert frame['IQC结果'].tolist() == ['OK']
    assert not app.number_input
    for mode in ['failure', 'empty', 'normal']:
        app.session_state['test_mode'] = mode
        app.button(key='iqc_evap_query').click().run()
        assert not app.exception
        if mode == 'failure':
            assert len(app.error) == 1 and 'secret' not in app.error[0].value
            assert not app.dataframe
        elif mode == 'empty':
            assert '没有符合' in app.info[0].value
            assert not app.dataframe
        else:
            assert len(app.dataframe) == 1


def test_product_query_gate_empty_selection_and_registry_change(monkeypatch):
    enabled = ['M678', 'M626']
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls, **kwargs: enabled.copy()))
    app = AppTest.from_string(APP).run()
    app.button(key='iqc_evap_query').click().run()
    assert app.dataframe
    app.multiselect[0].set_value(['M626']).run()
    assert not app.exception and not app.dataframe
    app.button(key='iqc_evap_query').click().run()
    assert not app.dataframe and '没有符合' in app.info[0].value
    app.multiselect[0].set_value([]).run()
    app.button(key='iqc_evap_query').click().run()
    assert len(app.dataframe[0].value) == 1
    app.multiselect[0].set_value(['M678']).run()
    app.button(key='iqc_evap_query').click().run()
    enabled.remove('M678')
    app.run()
    assert not app.exception and not app.dataframe
    assert app.multiselect[0].value == []
    assert app.multiselect[0].options == ['M626（CPD2515）']


def test_work_order_configuration_change_hides_old_result(monkeypatch):
    orders = ['P']
    monkeypatch.setattr(ConfigLoader, 'get_work_order_types', classmethod(lambda cls: orders.copy()))
    app = AppTest.from_string(APP).run()
    app.button(key='iqc_evap_query').click().run()
    assert app.dataframe
    orders.append('ESLC')
    app.run()
    assert not app.exception and not app.dataframe


@pytest.mark.parametrize('today,expected_start', [
    ('2026-10-13', '2026-09-01'),
    ('2026-01-01', '2025-12-01'),
    ('2024-03-01', '2024-02-01'),
])
def test_date_change_hides_old_result_until_query(monkeypatch, today, expected_start):
    from datetime import date, timedelta
    from app.sections.iqc_domain.eva_materials import evaporation_dashboard

    class FixedDate(date):
        @classmethod
        def today(cls):
            return date.fromisoformat(today)

    monkeypatch.setattr(evaporation_dashboard, 'date', FixedDate)
    app = AppTest.from_string(APP).run()
    assert app.date_input[0].value == (date.fromisoformat(expected_start), date.fromisoformat(today))
    app.button(key='iqc_evap_query').click().run()
    assert app.dataframe
    start, end = app.date_input[0].value
    app.date_input[0].set_value((start + timedelta(days=1), end)).run()
    assert not app.exception and not app.dataframe
    app.button(key='iqc_evap_query').click().run()
    assert app.dataframe


def test_full_result_is_available_to_native_table():
    extended = APP.replace('frame = source_frame()', 'frame = pd.concat([source_frame()] * 205, ignore_index=True)')
    app = AppTest.from_string(extended).run()
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    frame = app.dataframe[0].value
    assert len(frame) == 205
    assert list(frame.columns) == list(REPORT_COLUMNS)
    assert frame['序号'].tolist() == list(range(1, 206))
    assert not app.number_input


@pytest.mark.parametrize('method', ['get_enabled_products','get_work_order_types','get_product_annotations'])
def test_config_failure_removes_stale_report_and_keeps_error_private(monkeypatch, caplog, method):
    app = AppTest.from_string(APP).run()
    app.button(key='iqc_evap_query').click().run()
    assert app.dataframe
    def fail(cls, **kwargs):
        raise ValueError('private configuration marker')
    monkeypatch.setattr(ConfigLoader, method, classmethod(fail))
    app.run()
    assert not app.exception and not app.dataframe
    assert app.error[0].value == '蒸镀材料检验筛选条件暂时无法加载，请稍后重试。'
    assert 'private configuration marker' not in caplog.text


def test_no_enabled_products_does_not_read_source(monkeypatch):
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls, **kwargs: []))
    app = AppTest.from_string(APP).run()
    assert not app.exception and not app.dataframe and not app.button
    assert app.info[0].value == '暂无可查询的产品型号。'
