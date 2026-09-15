"""Real Streamlit section, with the data port as the only controlled boundary."""

from streamlit.testing.v1 import AppTest


APP = '''
import streamlit as st
from tests.unit.test_iqc_evaporation import MemorySource, source_frame
from src.iqc_domain.application.evaporation import EvaporationReportService, IqcSourceUnavailable
from app.sections.iqc_domain.evaporation_dashboard import render_evaporation_dashboard

class Source(MemorySource):
    def read_report(self, start, end):
        mode = st.session_state.get("test_mode", "normal")
        if mode == "failure":
            raise IqcSourceUnavailable("secret database details")
        if mode == "empty":
            return self.frame.iloc[:0]
        return super().read_report(start, end)

render_evaporation_dashboard(EvaporationReportService(Source(source_frame())))
'''


def test_query_filter_empty_failure_and_retry():
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert '查询' in app.info[0].value
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.get('html')) == 1
    html = app.get('html')[0].body
    assert 'IQC结果' in html and 'COA结果' in html
    assert '63.081' in html and '<td>NG</td>' in html
    app.multiselect(key='iqc_evap_IQC结果').set_value(['NG']).run()
    assert len(app.get('html')) == 1
    app.session_state['test_mode'] = 'failure'
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.error) == 1
    assert 'secret' not in app.error[0].value
    assert not app.get('html')
    app.session_state['test_mode'] = 'empty'
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert '没有符合' in app.info[0].value
    assert not app.get('html')
    app.session_state['test_mode'] = 'normal'
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.get('html')) == 1
