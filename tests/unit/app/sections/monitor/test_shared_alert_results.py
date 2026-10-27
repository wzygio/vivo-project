from datetime import date

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader


@pytest.fixture
def saved_spc(tmp_path, monkeypatch):
    path = tmp_path / 'spc.xlsx'
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', lambda *args, **kwargs: path)
    monkeypatch.setattr(ConfigLoader, 'get_inline_data_exclusion', lambda: None)
    monkeypatch.setattr(ConfigLoader, 'get_spc_sheet_oos_alert_param_exclusions', lambda: ['PPA'])
    frame = pd.DataFrame([
        dict(factory='ARRAY', prod_code='M678', step_id='100', param_name=param,
             sheet_id=sheet, sheet_start_time='2026-10-20 10:00:00', sheet_max=11.,
             sheet_min=5., sheet_mean=8., usl=10., lsl=2., oos_type='USL', flag=False)
        for sheet, param in [('S1', 'pPa_X'), ('S2', 'CD')]
    ])
    frame.to_excel(path, sheet_name='M678', index=False)
    return path


def test_saved_spc_policy_and_public_table_share_one_consumer(saved_spc):
    from app.sections.shared.alert_results import read_alert_result
    result = read_alert_result('spc_sheet_oos', 'M678', date(2026, 10, 27))
    assert result['state'] == 'alert'
    assert result['alerts']['Sheet ID'].tolist() == ['S2']
    assert result['alert_factories'] == ['ARRAY']
    assert 'flag' not in result['alerts']
    assert result['records']['sheet_id'].tolist() == ['S2']


def test_file_and_policy_changes_invalidate_result(saved_spc, monkeypatch):
    from app.sections.shared.alert_results import read_alert_result, alert_result_signature
    day = date(2026, 10, 27)
    before = alert_result_signature('spc_sheet_oos', 'M678', day)
    monkeypatch.setattr(ConfigLoader, 'get_spc_sheet_oos_alert_param_exclusions', lambda: [])
    assert before != alert_result_signature('spc_sheet_oos', 'M678', day)
    assert len(read_alert_result('spc_sheet_oos', 'M678', day)['alerts']) == 2
    frame = pd.read_excel(saved_spc, sheet_name='M678').iloc[:0]
    frame.to_excel(saved_spc, sheet_name='M678', index=False)
    assert read_alert_result('spc_sheet_oos', 'M678', day)['state'] == 'ok'
    saved_spc.unlink()
    assert read_alert_result('spc_sheet_oos', 'M678', day)['state'] == 'no_data'


def test_two_callers_share_selection_cache_and_receive_safe_copies(saved_spc, monkeypatch):
    from app.sections.shared import alert_results as saved
    saved._cached_alert_result.clear()
    calls = []
    actual = saved._read_inline
    def read(*args):
        calls.append(args)
        return actual(*args)
    monkeypatch.setattr(saved, '_read_inline', read)
    day = date(2026, 10, 27)
    first = saved.read_alert_result('spc_sheet_oos', 'M678', day)
    first['alerts'].loc[:, '参数名称'] = 'changed'
    second = saved.read_alert_result('spc_sheet_oos', 'M678', day)
    assert calls and len(calls) == 1
    assert second['alerts']['参数名称'].tolist() == ['CD']


def test_matrix_state_and_details_use_common_saved_selection(saved_spc, monkeypatch):
    from app.sections.shared.alert_results import read_alert_result
    from app.sections.inline_domain.monitor.alert_matrix_service import AlertMatrixContext, MATRIX_ROWS
    from app.sections.inline_domain.monitor import alert_matrix_detail as details
    day = date(2026, 10, 27)
    context = AlertMatrixContext(day, alert_result_loader=read_alert_result)
    row = next(item for item in MATRIX_ROWS if item.row_key == 'spc_sheet_oos')
    assert row.evaluator('M678', context)['state'] == 'alert'
    def unavailable(*args):
        raise RuntimeError('charts unavailable')
    monkeypatch.setattr(details, '_load_spc_view', unavailable)
    monkeypatch.setattr(details, '_resolve_db_manager', lambda _: object())
    monkeypatch.setattr(details, '_report_end_date', lambda: day)
    bundle = details.build_default_detail_loaders()['spc_sheet_oos']('M678', day)
    assert bundle['alerts_df']['Sheet ID'].tolist() == ['S2']
    assert bundle['chart_unavailable'] is True


def test_changed_detail_is_committed_before_ttl(tmp_path):
    from datetime import datetime, timedelta
    from src.inline_domain.infrastructure.shared.sheet_oos_decoration_repository import persist_sheet_oos_decoration_outcome
    frame = pd.DataFrame([dict(factory='ARRAY', prod_code='M678', step_id='100', param_name='CD',
        sheet_id='S1', sheet_start_time='2026-10-20', sheet_max=11, sheet_min=5,
        sheet_mean=8, usl=10, lsl=2, oos_type='USL')])
    args = dict(scope='spc', prod_code='M678', product_revision='same', decision_signature='same')
    now = datetime(2026, 10, 27, 10)
    first = persist_sheet_oos_decoration_outcome(tmp_path, frame, sheet_name='M678', now=now, **args)
    changed = frame.assign(sheet_max=20)
    second = persist_sheet_oos_decoration_outcome(tmp_path, changed, sheet_name='M678', now=now + timedelta(minutes=1), **args)
    assert first.refresh_decision.should_write and second.refresh_decision.should_write
    assert second.refresh_decision.reason == 'detail_changed'
    assert pd.read_excel(tmp_path / 'spc_sheet_oos_decoration.xlsx', sheet_name='M678')['sheet_max'].tolist() == [20]


def test_qtime_saved_table_precedes_optional_chart_failure(monkeypatch):
    from app.sections.inline_domain.monitor import alert_matrix_detail as detail
    from app.sections.indicator_domain.qtime import alert_center
    calls = []
    rows = pd.DataFrame([dict(step_desc='PATH', lot_id='L1')])
    bundle = dict(alerts_df=rows, details_df=pd.DataFrame(), total_lots=1,
                  reference_date='2026-10-27')
    monkeypatch.setattr(alert_center, 'render_qtime_alert_center', lambda *a, **k: calls.append('table'))
    monkeypatch.setattr(detail, '_resolve_db_manager', lambda _: object())
    def unavailable(_db, day):
        assert str(day) == '2026-10-27'
        calls.append('chart')
        raise RuntimeError('unavailable')
    monkeypatch.setattr(detail, 'load_all_product_qtime_monitoring', unavailable)
    monkeypatch.setattr(detail.st, 'button', lambda *a, **k: False)
    monkeypatch.setattr(detail.st, 'info', lambda *a, **k: calls.append('info'))
    detail._render_qtime_detail(bundle, row_key='qtime_sheet_oos', prod_code='M678', memo_base='v1')
    assert calls == ['table']
    calls.clear()
    monkeypatch.setattr(detail.st, 'button', lambda *a, **k: True)
    detail._render_qtime_detail(bundle, row_key='qtime_sheet_oos', prod_code='M678', memo_base='v1')
    assert calls == ['table', 'chart', 'info']
