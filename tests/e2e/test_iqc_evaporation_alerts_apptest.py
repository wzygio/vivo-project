from datetime import date

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.sections.iqc_domain.eva_materials import evaporation_dashboard as dashboard
from src.shared_kernel.config import ConfigLoader


APP = '''
import streamlit as st
import pandas as pd
from tests.unit.test_iqc_evaporation import source_frame
from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.infrastructure.eva_materials.alert_workbook import EvaporationAlertWorkbook
from app.sections.iqc_domain.eva_materials.evaporation_dashboard import render_evaporation_dashboard
from pathlib import Path

class Source:
    def read_report(self, start, end, products):
        st.session_state['source_scope'] = (start, end, products)
        if MODE == 'source_failure':
            raise RuntimeError('private SQL admin=true')
        records = []
        for material, product, iqc, coa, checked in [
            ('A', 'M678', 65, 63, '2026-10-20'),
            ('B', 'M626', 63, 65, '2026-10-20'),
            ('C', 'M678', 65, 65, '2026-10-20'),
            ('D', 'M678', 63, 63, '2026-10-20'),
            ('E', 'M678', 65, 65, '2026-10-26'),
            ('DISABLED', 'DISABLED', 65, 65, '2026-10-20'),
        ]:
            record = source_frame().iloc[0].to_dict()
            record.update(mitemname=material, prod_code=product, iqc_1=iqc, coa_1=coa,
                          checkeddate=pd.Timestamp(checked))
            records.append(record)
        frame = pd.DataFrame(records)
        if MODE == 'normal':
            frame[['iqc_1', 'coa_1']] = 63
        if MODE == 'empty':
            return frame.iloc[:0]
        return frame.loc[frame.checkeddate.dt.date.between(start, end) & frame.prod_code.isin(products)]

class Store(EvaporationAlertWorkbook):
    def save(self, *args, **kwargs):
        if MODE == 'write_failure':
            raise PermissionError('private workbook admin=true')
        return super().save(*args, **kwargs)
render_evaporation_dashboard(
    EvaporationReportService(Source(), enabled_products=('M678', 'M626')),
    alert_store=Store(Path(SNAPSHOT)),
)
'''


@pytest.mark.parametrize('mode', ['alert', 'normal', 'empty', 'source_failure', 'write_failure'])
def test_automatic_ng_scope_snapshot_and_public_failures(monkeypatch, tmp_path, caplog, mode):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return date(2026, 10, 26)
    monkeypatch.setattr(dashboard, 'date', FixedDate)
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', classmethod(lambda cls, **kwargs: ['M678', 'M626']))
    path = tmp_path / 'alerts.xlsx'
    # Pre-existing bytes must remain untouched when source or publication fails.
    if 'failure' in mode:
        path.write_bytes(b'previous snapshot')
    app = AppTest.from_string(f'MODE={mode!r}\nSNAPSHOT={str(path)!r}\n' + APP).run()
    assert not app.exception
    assert '2026-W43' in app.expander[0].label
    assert app.session_state['source_scope'] == (date(2026, 10, 19), date(2026, 10, 25), ('M678', 'M626'))
    if 'failure' in mode:
        assert app.error[0].value == '蒸镀材料预警数据暂时无法更新，请稍后重试。'
        assert path.read_bytes() == b'previous snapshot'
        assert not app.dataframe
        assert 'private' not in caplog.text and 'admin=true' not in caplog.text
        if mode == 'write_failure':
            app.button(key='iqc_evap_query').click().run()
            assert not app.exception and len(app.dataframe) == 1
            assert path.read_bytes() == b'previous snapshot'
    elif mode == 'alert':
        assert app.expander[0].proto.expanded
        alerts = app.dataframe[0].value
        assert set(alerts['物料号']) == {'A', 'B', 'C'} and len(alerts) == 3
        assert not {'batch_no', '蒸镀批次', 'ticketno', 'flag'} & set(alerts)
        saved = pd.read_excel(path, sheet_name='超规材料')
        pd.testing.assert_frame_equal(saved.fillna(''), alerts.fillna(''), check_dtype=False)
        # Changing manual products/dates never narrows the automatic alert scope.
        app.multiselect[0].set_value(['M678']).run()
        app.date_input[0].set_value((date(2026, 10, 26), date(2026, 10, 26))).run()
        assert set(app.dataframe[0].value['物料号']) == {'A', 'B', 'C'}
        app.button(key='iqc_evap_query').click().run()
        assert not app.exception
        assert app.dataframe[1].value['物料号'].tolist() == ['E']
        assert set(pd.read_excel(path, sheet_name='超规材料')['物料号']) == {'A', 'B', 'C'}
    elif mode == 'normal':
        assert app.success[0].value == '上一周未发现 IQC 或 COA 结果为 NG 的记录。'
        assert pd.read_excel(path, sheet_name='超规材料').empty
    else:
        assert app.info[0].value == '上一周暂无蒸镀材料检验数据。'
        assert pd.read_excel(path, sheet_name='超规材料').empty
