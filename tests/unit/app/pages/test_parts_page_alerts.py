from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from app.components import page_header
from app.manager.session_manager import SessionManager
from src.equipment_domain import composition
from src.equipment_domain.application.parts_service import PartsReportService, PartsReportViewModel
from src.equipment_domain.infrastructure.parts_alert_workbook import PartsAlertWorkbook
from src.shared_kernel.infrastructure import db_handler

PAGE = Path(__file__).resolve().parents[4] / 'app/pages/关键备件报表.py'


@pytest.mark.parametrize('mode', ['alert', 'normal', 'empty', 'write_failure', 'config_failure', 'load_failure'])
def test_parts_page_automatic_alert_scope_snapshot_and_safe_failure(monkeypatch, tmp_path, mode):
    frame = pd.DataFrame({
        '厂别': ['Array', 'OLED'], '设备类型': ['PVD', 'CVD'],
        '备件类型': ['Target', 'Mask'], '使用进度': [110, 100],
        '预警状态': ['超规', '预警'], '寿命规格': [100, '2年'],
        '机台号-腔室': ['P1', 'P2'], '测量值': [110, '2026-01-01'],
        '测量时间': ['2026-10-26'] * 2,
        '是否超规': [True] * 2, '原始测量值': [999] * 2,
        '参数名称': ['secret'] * 2, '数据修饰': ['internal'] * 2,
    })
    if mode == 'normal':
        frame['使用进度'] = [100, 99]
    if mode == 'empty':
        frame = frame.iloc[:0]
    path = tmp_path / 'parts_alerts.xlsx'
    store = PartsAlertWorkbook(path)
    if mode in ('write_failure', 'config_failure', 'load_failure'):
        store.save(frame.iloc[:0], as_of_date='2026-10-25', last_update='')
        previous = path.read_bytes()
    if mode == 'write_failure':
        def fail(*args, **kwargs):
            raise PermissionError('admin=true secret internal-path')
        monkeypatch.setattr(store, 'save', fail)
    def report(**kwargs):
        if mode == 'load_failure':
            raise ValueError('admin=true secret SQL')
        return PartsReportViewModel(frame.copy(), len(frame), 1, 1, 0, '2026-10-26')
    monkeypatch.setattr(PartsReportService, 'get_report_data', staticmethod(report))
    monkeypatch.setattr(SessionManager, 'get_active_config', staticmethod(lambda: {}))
    monkeypatch.setattr(db_handler, 'DatabaseManager', lambda: object())
    monkeypatch.setattr(page_header, 'render_page_header', lambda *args, **kwargs: None)
    def store_factory():
        if mode == 'config_failure':
            raise KeyError('internal-path admin=true')
        return store
    monkeypatch.setattr(composition, 'create_parts_alert_store', store_factory)
    app = AppTest.from_file(str(PAGE)).run()
    assert not app.exception
    if mode == 'load_failure':
        assert app.error[0].value == '备件寿命数据暂时无法加载，请稍后重试。'
        assert path.read_bytes() == previous and not app.dataframe
        return
    if mode in ('write_failure', 'config_failure'):
        assert app.error[0].value == '备件预警数据暂时无法更新，请稍后重试。'
        assert path.read_bytes() == previous
        assert len(app.dataframe) == 1  # Ordinary report remains available.
    elif mode == 'alert':
        assert app.expander[0].proto.expanded
        assert app.error[0].value == '检测到 1 项超规备件，请关注。'
        assert app.dataframe[0].value['备件类型'].tolist() == ['Target']
        # Excluding the alert's factory only filters the ordinary report.
        app.multiselect(key='parts_filter_factories').select('OLED').run()
        assert not app.exception
        assert app.dataframe[0].value['备件类型'].tolist() == ['Target']
        assert app.dataframe[1].value['备件类型'].tolist() == ['Mask']
        saved = pd.read_excel(path, sheet_name='超规备件')
        assert saved['备件类型'].tolist() == ['Target']
        assert saved['使用进度'].item() == 110
    elif mode == 'normal':
        assert app.success[0].value == '当前未发现使用进度超过 100% 的备件。'
        assert pd.read_excel(path, sheet_name='超规备件').empty
    else:
        assert '当前暂无备件监控数据。' in [item.value for item in app.info]
        assert pd.read_excel(path, sheet_name='超规备件').empty
    for table in app.dataframe:
        assert not {'是否超规', '原始测量值', '参数名称', '数据修饰'} & set(table.value)
    if mode not in ('write_failure', 'config_failure', 'load_failure'):
        assert not {'是否超规', '原始测量值', '参数名称', '数据修饰'} & set(pd.read_excel(path, sheet_name='超规备件'))
