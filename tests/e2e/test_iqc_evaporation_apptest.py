"""Real Streamlit section, with the data port as the only controlled boundary."""

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
    def read_report(self, start, end):
        mode = st.session_state.get("test_mode", "normal")
        if mode == "failure":
            raise IqcSourceUnavailable("secret database details")
        if mode == "empty":
            return self.frame.iloc[:0]
        return super().read_report(start, end)

frame = source_frame()
render_evaporation_dashboard(EvaporationReportService(Source(frame)))
'''



def test_date_query_empty_failure_and_retry(monkeypatch):
    def forbidden_products(cls):
        raise AssertionError('Organic report must not consult enabled_products')
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(forbidden_products))
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert '查询' in app.info[0].value
    assert len(app.date_input) == 1
    assert not app.multiselect and not app.selectbox
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    frame = app.dataframe[0].value
    assert list(frame.columns) == list(REPORT_COLUMNS)
    assert frame['产品型号'].tolist() == ['通用']
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


def test_date_change_hides_old_result_until_query():
    from datetime import timedelta
    app = AppTest.from_string(APP).run()
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
