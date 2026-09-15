"""Production section exercised through its service injection boundary."""

from streamlit.testing.v1 import AppTest

APP = '''
import streamlit as st
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
        return self.frame.copy()
render_lifetime_dashboard(LifetimeReportService(Source(source_frame())))
'''


def test_filter_empty_failure_and_recovery():
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert len(app.multiselect) == 2
    assert len(app.get('plotly_chart')) == 4
    assert 'private' not in app.get('html')[0].body
    app.multiselect(key='iqc_lifetime_products').set_value(['M678']).run()
    assert not app.exception and len(app.get('plotly_chart')) == 4
    app.session_state['mode'] = 'failure'
    app.button(key='iqc_lifetime_refresh').click().run()
    assert not app.exception and len(app.error) == 1
    assert 'private' not in app.error[0].value
    assert not app.get('plotly_chart') and not app.get('html')
    app.session_state['mode'] = 'empty'
    app.button(key='iqc_lifetime_refresh').click().run()
    assert not app.exception and app.info
    assert not app.get('html')
    app.session_state['mode'] = 'normal'
    app.button(key='iqc_lifetime_refresh').click().run()
    assert not app.exception and len(app.get('plotly_chart')) == 4
