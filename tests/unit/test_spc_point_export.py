from datetime import date, timedelta
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


def test_forwarded_export_splits_files_and_replays_by_product(tmp_path, monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.data_forward import DataForwardPolicy
    from tools.spc_point_export import workbook

    monkeypatch.setattr(ConfigLoader, 'get_data_forward_policy', lambda: DataForwardPolicy(True, 4))
    monkeypatch.setattr(workbook, 'MAX_FILE_ROWS', 2)
    today = date(2026, 11, 26)
    args = ['--product', 'P1', '--start-time', '2026-11-26 12:00:00',
            '--end-time', '2026-11-27 12:00:00']
    with pytest.raises(ExportError, match='include-forwarded-future'):
        make_plan(args, today=today)

    def loader(product, _start, _end):
        return pd.DataFrame({
            'prod_code': [product] * 4,
            'sheet_start_time': pd.to_datetime(['2026-11-26 12:00:00', '2026-11-26 12:01:00',
                                               '2026-11-27 12:00:00', '2026-11-27 12:00:01']),
            'param_value': [1, 2, 3, 4],
        })

    output = tmp_path / 'export'
    assert main([*args, '--include-forwarded-future', '--output-dir', str(output)],
                loader=loader, today=today) == 0
    replay = tmp_path / 'replay'
    assert main(['--from-export', str(output), '--output-dir', str(replay)], today=today) == 0
    for result in (output, replay):
        manifest = json.loads((result / 'manifest.json').read_text(encoding='utf-8'))
        assert manifest['forwarded_future'] is True
        row, = manifest['rows']
        assert row['row_count'] == 3
        assert [file['row_count'] for file in row['files']] == [2, 1]
        assert [file['file'].split('_')[-1] for file in row['files']] == ['part01.xlsx', 'part02.xlsx']
        values = []
        for file in row['files']:
            book = openpyxl.load_workbook(result / file['file'], read_only=True)
            values.extend(record[-1] for record in list(book['P1'].values)[1:])
            book.close()
        assert values == [1, 2, 3]


def test_forwarded_source_uses_verified_raw_coverage_and_exact_cutoff(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import InlineMeasurementSnapshotRepository
    from src.inline_domain.infrastructure.shared.rolling_snapshot import publish_snapshots
    from src.shared_kernel.config import ConfigLoader
    from src.shared_kernel.data_forward import DataForwardPolicy
    from tools.spc_point_export.source import MeasurementSource

    monkeypatch.setattr(ConfigLoader, 'get_data_forward_policy', lambda: DataForwardPolicy(True, 4))
    monkeypatch.setattr(ConfigLoader, 'get_inline_data_exclusion', lambda: None)

    def forbidden(*args):
        pytest.fail('forwarded measurement export must use the verified snapshot')

    adapter = InlineMeasurementSnapshotRepository(tmp_path, None, measurement_loader=forbidden)
    source_day = pd.Timestamp.now(tz='Asia/Shanghai').normalize().tz_localize(None) - pd.Timedelta(days=1)
    raw_end = source_day + pd.Timedelta(hours=12)
    display_end = raw_end + pd.Timedelta(days=4)
    frame = pd.DataFrame({'start_time': [raw_end, raw_end + pd.Timedelta(seconds=1)], 'param_value': [1, 2]})
    snapshot = tmp_path / 'inline_measurements_P1.parquet'
    publish_snapshots([(snapshot, frame, adapter.SNAPSHOT_POLICY_VERSION, source_day, True)],
                      window_start=source_day)

    def source(end):
        return MeasurementSource(adapter, display_end.date(), end.date(), query_manager=None,
                                 display_end=end.to_pydatetime())

    projected = source(display_end).get_measurements('P1', display_end.date().isoformat())
    assert projected['start_time'].tolist() == [display_end]
    later = display_end + timedelta(days=1)
    with pytest.raises(ExportError, match='snapshot does not cover'):
        source(later).get_measurements('P1', later.date().isoformat())


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
