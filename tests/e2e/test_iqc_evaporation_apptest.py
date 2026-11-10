"""Real manual-report UI with controlled data and saved-alert read ports."""

import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from app.sections.iqc_domain.eva_materials import evaporation_dashboard as dashboard
from src.shared_kernel.config import ConfigLoader
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS


@pytest.fixture(autouse=True)
def isolated_saved_alert_result(monkeypatch):
    """Keep manual UI tests independent of maintained automatic-alert workbooks."""
    def read_saved(row, product, reference):
        if st.session_state.get("test_mode", "normal") == "failure":
            return {"state": "error", "alerts": pd.DataFrame(), "message": "预警结果暂不可用。"}
        return {"state": "ok", "alerts": pd.DataFrame(), "message": ""}

    monkeypatch.setattr(dashboard, "read_alert_result", read_saved)


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
class MemoryAlertStore:
    def save(self, alerts, **scope):
        pass
render_evaporation_dashboard(EvaporationReportService(Source(frame), enabled_products=('M678', 'M626')),
                             alert_store=MemoryAlertStore())
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
            assert len(app.error) == 2 and all('secret' not in error.value for error in app.error)
            assert not app.dataframe
            assert not app.get('plotly_chart')
        elif mode == 'empty':
            assert any('没有符合' in info.value for info in app.info)
            assert not app.dataframe
            assert not app.get('plotly_chart')
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
    assert not app.dataframe and any('没有符合' in info.value for info in app.info)
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


def test_material_expanders_show_only_fixed_measured_characteristics_and_load_on_open():
    import json

    data = '''
frame = pd.concat([source_frame()] * 6, ignore_index=True)
frame.loc[1, 'cha_item'] = '未测项目'
frame.loc[1, ['iqc_1', 'coa_1']] = None
frame.loc[2, 'cha_item'] = 'DSC-Tg中点'
frame.loc[2, ['iqc_1', 'low_spec', 'upp_spec']] = None
frame.loc[3, 'mitemname'] = 'SECOND'
frame.loc[4, 'mitemname'] = 'UNMEASURED'
frame.loc[4, ['iqc_1', 'coa_1']] = None
frame.loc[5, 'mitemname'] = 'EXCLUDED'
frame.loc[5, 'cha_item'] = 'DSC-Tg起始点'
'''
    app = AppTest.from_string(APP.replace('frame = source_frame()', data)).run()
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.expander) == 3  # Automatic alerts plus two measured materials.
    assert not any('UNMEASURED' in exp.label or 'EXCLUDED' in exp.label for exp in app.expander)
    figures = [json.loads(chart.proto.spec) for chart in app.get('plotly_chart')]
    assert [fig['layout']['title']['text'] for fig in figures] == ['DSC-Tg中点', 'HPLC-A']
    assert [trace['name'] for trace in figures[0]['data']] == ['COA-1']
    assert any('未提供规格上限、规格下限' in caption.value for caption in app.caption)
    assert len(app.dataframe[0].value) == 6  # All original rows remain in details.
    assert not any('INTERNAL' in json.dumps(fig) or 'batch_no' in json.dumps(fig) for fig in figures)

    app.session_state['iqc_evap_measured_material_SECOND'] = True
    app.run()
    assert not app.exception and len(app.get('plotly_chart')) == 3


def test_empty_first_material_is_omitted_and_dsc_chart_is_visible_immediately():
    import json

    data = '''
frame = pd.concat([source_frame()] * 2, ignore_index=True)
frame.loc[0, 'mitemname'] = '100000016'
frame.loc[0, ['iqc_1', 'coa_1']] = None
frame.loc[1, 'cha_item'] = 'DSC-Tg中点'
frame.loc[1, ['iqc_1', 'coa_1', 'low_spec', 'upp_spec']] = [113, 113.4, 110, 118]
'''
    app = AppTest.from_string(APP.replace('frame = source_frame()', data)).run()
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.get('plotly_chart')) == 1
    assert len(app.expander) == 2
    assert app.expander[1].label.startswith('100024720')
    figure = json.loads(app.get('plotly_chart')[0].proto.spec)
    assert figure['layout']['title']['text'] == 'DSC-Tg中点'
    assert [(trace['name'], trace['y']) for trace in figure['data']] == [
        ('IQC-1', [113]), ('COA-1', [113.4]),
    ]
    assert {shape['y0'] for shape in figure['layout']['shapes']} == {110, 118}
    assert not any('该物料暂无可绘制' in info.value for info in app.info)


def test_no_eligible_points_shows_no_material_expander_and_keeps_details():
    data = '''
frame = pd.concat([source_frame()] * 2, ignore_index=True)
frame.loc[0, ['iqc_1', 'coa_1']] = None
frame.loc[1, 'cha_item'] = 'DSC-Tg起始点'
'''
    app = AppTest.from_string(APP.replace('frame = source_frame()', data)).run()
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    assert len(app.expander) == 1  # Only the separate automatic-alert section.
    assert not app.get('plotly_chart')
    assert len(app.dataframe[0].value) == 2
    assert any('所选范围内暂无上述特性项目' in info.value for info in app.info)


def test_displayed_table_export_and_chart_share_the_inner_band_and_keep_ng():
    import json

    script = APP.replace('frame = source_frame()', 'frame = source_frame().assign(iqc_1=65, coa_1=61)')
    app = AppTest.from_string(script).run()
    app.button(key='iqc_evap_query').click().run()
    assert not app.exception
    row = app.dataframe[0].value.iloc[0]
    assert row['IQC-1'] == 63.8 and row['COA-1'] == 62.2
    assert row[['IQC结果', 'COA结果']].tolist() == ['NG', 'NG']
    chart = json.loads(app.get('plotly_chart')[0].proto.spec)
    assert chart['data'][0]['y'] == [row['IQC-1']]
    assert chart['data'][1]['y'] == [row['COA-1']]
    # Native table downloads consume exactly this public DataFrame.
    csv = app.dataframe[0].value.to_csv(index=False)
    assert '63.8' in csv and '62.2' in csv
    assert not any(field in csv for field in ['ticketno', 'flag', 'batch_no'])
