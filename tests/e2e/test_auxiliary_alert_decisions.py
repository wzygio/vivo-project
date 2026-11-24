from datetime import date, datetime
from io import BytesIO

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from api.inspection.producers_auxiliary import _verify_saved
from app.sections.shared.alert_results import read_alert_result
from src.equipment_domain.core.parts_alerts import build_parts_alerts
from src.equipment_domain.infrastructure.parts_alert_workbook import PARTS_DECISIONS, PartsAlertWorkbook
from src.indicator_domain.core.ijp.alerts import previous_week_range
from src.indicator_domain.infrastructure.ijp.alert_workbook import IjpAlertWorkbook, ijp_decisions
from src.iqc_domain.core.eva_materials.alerts import build_evaporation_alerts
from src.iqc_domain.infrastructure.eva_materials.alert_workbook import EVAPORATION_DECISIONS, EvaporationAlertWorkbook
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.core.decoration_revision import VERSION_SHEET
from src.shared_kernel.infrastructure.alert_workbook import SnapshotChangedError


def scenario(module, path, today):
    start, end = previous_week_range(today)
    if module.startswith('ijp_'):
        region = '孔区' if module == 'ijp_hole' else 'AA区'
        layout = ijp_decisions(region)
        frame = pd.DataFrame([dict(region=region, productcode='M678', line='L', printer='P',
                                   rs_code='C3RA1' if module == 'ijp_hole' else 'C3DM1',
                                   period_start=str(start), period_end=str(end), ratio=.82, minimum=.9)])
        def save(rows):
            return IjpAlertWorkbook(path).save(region, rows, today)
        script = f'''
from datetime import datetime
from app.sections.indicator_domain.ijp.alert_center import render_ijp_alert_center
class Service:
    def get_reporting_window(self):
        return datetime.now(), datetime.now()
render_ijp_alert_center(Service(), "{region}", produce=False)
'''
        return layout, f'{module}_oos', 'ijp_alerts', frame, save, script
    if module == 'parts':
        frame = build_parts_alerts(pd.DataFrame([{'厂别':'ARRAY', '备件类型':'备件', '站点':'0001', '使用进度':125.}]))
        def save(rows):
            return PartsAlertWorkbook(path).save(rows, as_of_date=str(today), last_update=str(today))
        return PARTS_DECISIONS, 'parts_oos', 'parts_alerts', frame, save, '''
from app.sections.equipment_domain.parts_alerts import render_saved_parts_alerts
render_saved_parts_alerts()
'''
    frame = build_evaporation_alerts(pd.DataFrame([{
        '产品型号':'M678', '物料号':'0012', '工厂':'V3', 'IQC结果':'NG', 'COA结果':'OK',
        '报检日期':pd.Timestamp(start), '检验时间':pd.Timestamp(end) + pd.Timedelta(hours=4),
    }]))
    def save(rows):
        return EvaporationAlertWorkbook(path).save(rows, start=start, end=end, products=('M678',))
    return EVAPORATION_DECISIONS, 'iqc_evaporation_ng', 'evaporation_alerts', frame, save, '''
from datetime import date
from app.sections.iqc_domain.eva_materials.evaporation_dashboard import _render_saved_automatic_alerts
from src.iqc_domain.core.eva_materials.alerts import previous_week_window
today = date.today()
_render_saved_automatic_alerts(today, "蒸镀材料预警中心", *previous_week_window(today))
'''


@pytest.mark.parametrize('module', ['ijp_aa', 'ijp_hole', 'parts', 'iqc_evaporation'])
def test_upload_republication_local_edit_and_release(module, tmp_path, monkeypatch):
    today = date.today()
    path = tmp_path / 'private_fixture.xlsx'
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', lambda *args: path)
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda **kwargs: ['M678'])
    layout, row_key, resource, frame, save, script = scenario(module, path, today)
    assert len(save(frame)) == 1
    app = AppTest.from_string(script, default_timeout=20).run()
    assert not app.exception and not app.file_uploader
    assert len(app.dataframe[0].value) == 1 and 'Flag' not in app.dataframe[0].value
    app.query_params['admin'] = 'ture'
    app.run()
    assert app.get('download_button')[0].label == '下载预警决定'

    def upload(flag):
        payload, _ = layout.download(path)
        keys = pd.read_excel(BytesIO(payload), sheet_name=layout.decisions, dtype=object,
                             keep_default_na=False)
        buf = BytesIO()
        with pd.ExcelWriter(buf) as writer:
            keys.assign(Flag=flag).to_excel(writer, sheet_name=layout.decisions, index=False)
            pd.read_excel(BytesIO(payload), sheet_name=VERSION_SHEET).to_excel(
                writer, sheet_name=VERSION_SHEET, index=False)
        app.file_uploader[0].set_value(('edited.xlsx', buf.getvalue(), 'application/octet-stream')).run()
        app.button(key=f'{resource}_{layout.sheet}_apply').click().run()
        assert not app.exception

    first_download, _ = layout.download(path)
    upload(True)
    assert read_alert_result(row_key, 'M678', today)['state'] == 'ok'
    assert not app.dataframe
    _, fresh_version = layout.download(path)
    with pytest.raises(SnapshotChangedError):
        layout.apply_upload(path, first_download, fresh_version)
    upload('Delete')
    assert save(frame.iloc[:0]).empty
    started = datetime.now()
    assert save(frame).empty
    _verify_saved(module, ('M678',), today, 0, started)
    sheets = pd.read_excel(path, sheet_name=None, dtype=object, keep_default_na=False)
    assert sheets[layout.decisions]['Flag'].tolist() == ['Delete']
    assert len(sheets[layout.candidates]) == 1 and sheets[layout.sheet].empty

    # A direct Excel edit of the maintained sheet invalidates already-read public results.
    with pd.ExcelWriter(path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        sheets[layout.decisions].assign(Flag=False).to_excel(writer, sheet_name=layout.decisions, index=False)
    result = read_alert_result(row_key, 'M678', today)
    assert result['state'] == 'alert'
    assert len(result['alerts']) == 1 and 'Flag' not in result['alerts']
    assert len(save(frame)) == 1
    del app.query_params['admin']
    app.run()
    assert not app.exception and not app.file_uploader and not app.get('download_button')
    assert len(app.dataframe[0].value) == 1 and 'Flag' not in app.dataframe[0].value
