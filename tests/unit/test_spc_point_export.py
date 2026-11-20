from datetime import date
import io
import json
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from tools.spc_point_export import ExportError
from tools.spc_point_export.__main__ import main, make_plan
from tools.spc_point_export.workbook import build_workbook


@pytest.fixture(autouse=True)
def enabled_products(monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda: ['P1', 'P2'])


def test_calendar_defaults_and_selection():
    plan = make_plan([], today=date(2027, 1, 4))
    assert plan.start == date(2026, 12, 1)
    assert plan.end == date(2027, 1, 4)
    assert plan.products == ('P1', 'P2')
    assert make_plan(['--product', 'P2', '--product', 'P2'], today=date(2027,1,4)).products == ('P2',)


@pytest.mark.parametrize('args', [
    ['--product','../P1'], ['--product','disabled'],
    ['--start-date','2027-02-01'], ['--end-date','2027-01-05'],
])
def test_invalid_selection_fails_before_loading(args):
    with pytest.raises(ExportError):
        make_plan(args, today=date(2027,1,4))


def test_workbook_preserves_values_and_splits_rows_within_one_product(monkeypatch,tmp_path):
    import tools.spc_point_export.workbook as module
    monkeypatch.setattr(module,'MAX_DATA_ROWS',2)
    frame = pd.DataFrame({'prod_code':['P1']*3, 'id':['001','=1+1','中文'],
        'value':[1.25,float('nan'),-3.0],
        'when':[pd.Timestamp('2026-11-10 09:08:07'),pd.NaT,pd.Timestamp('2026-11-10 12:00:00')]})
    payload, sheets = build_workbook(frame,'P1',{'start_date':'2026-10-01','end_date':'2026-11-10'},tmp_path)
    book = openpyxl.load_workbook(io.BytesIO(payload),read_only=True,data_only=False)
    assert sheets == ['P1_1','P1_2']
    assert list(book['P1_1'].values) == [tuple(frame.columns),
        ('P1','001',1.25,pd.Timestamp('2026-11-10 09:08:07').to_pydatetime()),
        ('P1','=1+1',None,None)]
    assert book['P1_1']['B3'].data_type == 's'
    assert list(book['P1_2'].values)[1][:3] == ('P1','中文',-3)
    book.close()


def test_empty_product_still_gets_a_workbook(tmp_path):
    payload,sheets = build_workbook(pd.DataFrame(columns=['prod_code','param_value']),'P2',{},tmp_path)
    book = openpyxl.load_workbook(io.BytesIO(payload),read_only=True)
    assert sheets == ['P2']
    assert list(book['P2'].values) == [('prod_code','param_value')]
    book.close()


def test_check_does_not_load_or_create_output(tmp_path):
    output = tmp_path/'must_not_exist'
    def forbidden(*args):
        pytest.fail('check mode must not load points')
    assert main(['--check','--output-dir',str(output)],loader=forbidden,today=date(2026,11,11)) == 0
    assert not output.exists()


def test_written_points_include_both_requested_times_and_exclude_other_rows(tmp_path):
    start = '2026-11-11 12:00:00'
    end = '2026-11-20 12:00:00'

    def loader(product, _start, _end):
        return pd.DataFrame({
            'prod_code': [product] * 5,
            'sheet_start_time': pd.to_datetime([
                '2026-10-01 00:00:00', '2026-11-11 11:59:59', start, end, '2026-11-20 12:00:01',
            ]),
            'param_value': [1, 2, 3, 4, 5],
        })

    output = tmp_path / 'result'
    assert main(['--product', 'P1', '--start-time', start, '--end-time', end,
                 '--output-dir', str(output)], loader=loader, today=date(2026, 11, 20)) == 0
    points = pd.read_parquet(output / 'points' / 'P1.parquet')
    assert points['sheet_start_time'].tolist() == [pd.Timestamp(start), pd.Timestamp(end)]
    assert points['param_value'].tolist() == [3, 4]
    manifest = json.loads((output / 'manifest.json').read_text(encoding='utf-8'))
    assert manifest['rows'][0]['row_count'] == 2


def test_pds_read_guard_requires_the_bounded_adapter_filters():
    from datetime import datetime
    from tools.spc_point_export.source import TemporalReadGuard

    guard = TemporalReadGuard('SELECT 1', datetime(2026, 11, 20, 12))
    query = ('SELECT p.glass_start_time FROM eda.array_pds_result_t p '
             'WHERE p.glass_start_time >= :start_time AND p.glass_start_time < :end_time '
             'AND p.glass_id LIKE :lot_prefix AND (p.step_id = :step AND p.param_name = :param)')
    assert guard(None, None, query, {}, None, False) == (query, {})
    for predicate in (' AND p.glass_start_time < :end_time', ' AND p.glass_id LIKE :lot_prefix',
                      ' AND p.param_name = :param'):
        with pytest.raises(ExportError, match='PDS query is missing'):
            guard(None, None, query.replace(predicate, ''), {}, None, False)


def test_from_export_inherits_dates_and_cannot_relabel(tmp_path):
    (tmp_path/'manifest.json').write_text(json.dumps({'start_date':'2026-10-01','end_date':'2026-11-10',
        'products':['P1'],'rows':[{'product':'P1','row_count':0,'columns':['prod_code']}]}))
    plan = make_plan(['--from-export',str(tmp_path)],today=date(2026,11,11))
    assert plan.products == ('P1',) and plan.end == date(2026,11,10)
    with pytest.raises(ExportError):
        make_plan(['--from-export',str(tmp_path),'--end-date','2026-11-11'],today=date(2026,11,11))


def test_product_failure_is_reported_without_success_or_overwrite(tmp_path):
    def loader(product,start,end):
        if product == 'P2':
            raise RuntimeError('secret connection details must not escape')
        return pd.DataFrame({'prod_code':[product],'sheet_start_time':[pd.Timestamp('2026-11-10')],
                             'param_value':[1.0],'data_type':['SPC']})
    output = tmp_path/'result'
    assert main(['--output-dir',str(output)],loader=loader,today=date(2026,11,11)) == 2
    manifest = json.loads((output/'manifest.json').read_text())
    assert manifest['status'] == 'error' and len(manifest['rows']) == 1
    assert 'secret' not in json.dumps(manifest)
    with pytest.raises(ExportError):
        main(['--output-dir',str(output)],loader=loader,today=date(2026,11,11))
