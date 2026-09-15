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
    assert len(app.expander) == 1
    assert app.expander[0].label == 'M678 · 2026/3/10'
    assert len(app.get('plotly_chart')) == 4
    assert 'private' not in app.get('html')[0].body
    app.multiselect(key='iqc_lifetime_products').set_value(['M678']).run()
    assert not app.exception and len(app.get('plotly_chart')) == 4
    app.session_state['mode'] = 'failure'
    app.run()
    assert not app.exception and len(app.error) == 1
    assert 'private' not in app.error[0].value
    assert not app.get('plotly_chart') and not app.get('html')
    app.session_state['mode'] = 'empty'
    app.run()
    assert not app.exception and app.info
    assert not app.get('html')
    app.session_state['mode'] = 'normal'
    app.run()
    assert not app.exception and len(app.get('plotly_chart')) == 4


def test_enabled_product_scope_status_filter_and_configuration_change():
    app = AppTest.from_string(APP).run()
    app.session_state['extended'] = True
    app.run()
    assert not app.exception
    assert app.multiselect(key='iqc_lifetime_products').options == ['M678', 'M626']
    assert 'DISABLED' not in app.get('html')[0].body
    assert len(app.expander) == 2
    app.multiselect(key='iqc_lifetime_statuses').set_value(['模组']).run()
    assert not app.exception and len(app.expander) == 1
    assert app.expander[0].label.startswith('M626')
    assert '屏体' not in app.get('html')[0].body
    app.multiselect(key='iqc_lifetime_products').set_value(['M626']).run()
    app.session_state['enabled'] = ['M678']
    app.run()
    assert not app.exception
    assert app.multiselect(key='iqc_lifetime_products').value == []
    assert app.multiselect(key='iqc_lifetime_statuses').value == []
    assert len(app.expander) == 1 and app.expander[0].label.startswith('M678')
    assert 'M626' not in app.get('html')[0].body
    app.session_state['enabled'] = []
    app.run()
    assert not app.exception and app.info
    assert not app.get('html') and not app.get('plotly_chart')
