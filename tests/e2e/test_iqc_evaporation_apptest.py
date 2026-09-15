"""Real Streamlit section, with the data port as the only controlled boundary."""

from streamlit.testing.v1 import AppTest
from src.shared_kernel.config import ConfigLoader


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

frame = pd.concat([
    source_frame().assign(prod_code='M626'),
    source_frame().assign(prod_code='M678', iqc_1=12.345),
    source_frame().assign(iqc_1=999.999),
], ignore_index=True)
render_evaporation_dashboard(EvaporationReportService(Source(frame)))
'''


def test_query_filter_empty_failure_and_retry(monkeypatch):
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls: ['M626', 'M678']))
    app = AppTest.from_string(APP).run()
    assert not app.exception
    assert '查询' in app.info[0].value
    assert len(app.date_input) == 1 and len(app.selectbox) == 1
    assert not app.multiselect
    assert app.selectbox[0].label == '产品型号'
    assert app.selectbox[0].options == ['M626', 'M678', '通用']
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.get('html')) == 1
    html = app.get('html')[0].body
    assert 'IQC结果' in html and 'COA结果' in html
    assert '63.081' in html and '<td>NG</td>' in html
    assert '999.999' not in html and '12.345' not in html
    assert '产品型号' in html and '项目名' not in html
    app.selectbox(key='iqc_evap_product').set_value('M678').run()
    assert not app.get('html')
    assert '点击' in app.info[0].value
    app.button(key='iqc_evap_query').click().run()
    assert len(app.get('html')) == 1
    assert '12.345' in app.get('html')[0].body
    assert '63.081' not in app.get('html')[0].body
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


def test_removed_product_resets_selection_and_excludes_common_records(monkeypatch):
    products = ['M626', 'M678']
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls: products))
    app = AppTest.from_string(APP).run()
    app.button(key='iqc_evap_query').click().run()
    assert app.get('html')
    products[:] = ['Z571']
    app.run()
    assert not app.exception
    assert app.selectbox[0].options == ['Z571', '通用']
    assert app.selectbox[0].value == 'Z571'
    assert not app.get('html')
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert not app.get('html')
    assert '没有符合' in app.info[0].value


def test_common_is_an_explicit_deduplicated_option(monkeypatch):
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls: ['M626', '通用']))
    app = AppTest.from_string(APP).run()
    assert app.selectbox[0].options == ['M626', '通用']
    app.selectbox(key='iqc_evap_product').set_value('通用').run()
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    html = app.get('html')[0].body
    assert '999.999' in html
    assert '63.081' not in html and '12.345' not in html
