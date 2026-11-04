"""Local files → real HTTP → portable caller/export, with owned listener cleanup."""
from __future__ import annotations

from contextlib import contextmanager
import csv
import hashlib
import json
import os
from pathlib import Path
import socket
import threading
import time
from uuid import uuid4

from fastapi import FastAPI
import pandas as pd
import psutil
import pytest
import uvicorn

from api.main import create_app
from api.settings import ApiClient
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from tests.integration.test_anomaly_api_capability import (
    _capability_rows, capability_files,
)
from tests.integration.test_anomaly_api_files import (
    FIXTURE_KEY, NOW, _write, published_files,
)
from tools import fetch_sheet_anomalies as caller


ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def _live_server(app: FastAPI, record_path: Path):
    """Own one Uvicorn thread/socket, never the containing pytest process."""
    process = psutil.Process(os.getpid())
    child_ids_before = {(child.pid, child.create_time()) for child in process.children(recursive=True)}
    record = {
        'runner_pid': process.pid, 'runner_created': process.create_time(),
        'owned_processes': [], 'owned_children': [], 'cleaned': False,
    }
    record_path.parent.mkdir(parents=True, exist_ok=True)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    assert port not in (8890, 8503)
    record['port'] = port
    record['ownership'] = 'Uvicorn server instance, thread and pre-bound socket only'
    failures = []
    server = uvicorn.Server(uvicorn.Config(
        app, host='127.0.0.1', port=port, loop='asyncio', lifespan='on',
        access_log=False, log_config=None, log_level='critical',
    ))

    def run():
        try:
            server.run(sockets=[listener])
        except BaseException as exc:
            failures.append(type(exc).__name__)

    thread = threading.Thread(target=run, name=f'anomaly-api-e2e-{port}', daemon=True)
    record['thread_name'] = thread.name
    record['server_handle'] = id(server)
    record_path.write_text(json.dumps(record, indent=2), encoding='utf-8')
    started = False
    try:
        thread.start()
        started = True
        record['thread_ident'] = thread.ident
        record_path.write_text(json.dumps(record, indent=2), encoding='utf-8')
        deadline = time.monotonic() + 10
        opener = caller.direct_opener()
        while time.monotonic() < deadline:
            if not thread.is_alive():
                raise AssertionError(f'Temporary API thread exited: {failures}')
            if server.started:
                try:
                    with opener.open(f'http://127.0.0.1:{port}/health/live', timeout=.5) as response:
                        if response.status == 200 and json.load(response) == {'status': 'ok'}:
                            break
                except OSError:
                    pass
            time.sleep(.02)
        else:
            raise AssertionError('Temporary API did not become healthy')
        yield f'http://127.0.0.1:{port}'
        record['outcome'] = 'success'
    except BaseException as exc:
        record['outcome'] = type(exc).__name__
        raise
    finally:
        server.should_exit = True
        if started:
            thread.join(timeout=10)
            if thread.is_alive():
                server.force_exit = True
                thread.join(timeout=2)
        listener.close()
        with socket.socket() as probe:
            probe.settimeout(.2)
            listening = probe.connect_ex(('127.0.0.1', port)) == 0
        children_after = {(child.pid, child.create_time()) for child in process.children(recursive=True)}
        unexpected_children = sorted(children_after - child_ids_before)
        record.update(thread_alive=thread.is_alive(), listening=listening,
                      unexpected_children=unexpected_children, server_failures=failures)
        record['cleaned'] = not thread.is_alive() and not listening and not unexpected_children
        record_path.write_text(json.dumps(record, indent=2), encoding='utf-8')
        assert record['cleaned'], f'Temporary service cleanup failed: {record_path}'


@pytest.fixture(autouse=True)
def forbid_external_sources(monkeypatch):
    """Fail the test even if a source catches a forbidden DB/Office attempt."""
    attempted = []

    def forbidden(*args, **kwargs):
        attempted.append('database-or-office')
        raise RuntimeError('E2E must only use its temporary published files')

    monkeypatch.setattr(DatabaseManager, '__new__', staticmethod(forbidden))
    monkeypatch.setattr(pd, 'read_sql', forbidden)
    import win32com.client
    monkeypatch.setattr(win32com.client, 'Dispatch', forbidden)
    monkeypatch.setattr(win32com.client, 'DispatchEx', forbidden)
    yield
    assert attempted == [], 'A local-file API flow attempted a database or Office fallback'


@pytest.mark.parametrize('exit_kind', ['normal', 'failure', 'interrupt'])
def test_owned_network_fixture_cleans_normal_failed_and_interrupted_runs(exit_kind):
    """Lifecycle verification uses a minimal page, never report data or a DB."""
    app = FastAPI()

    @app.get('/health/live')
    def health():
        return {'status': 'ok'}

    record_path = ROOT / 'output/test-results/api-sheet-membership-e2e' / f'lifecycle-{exit_kind}-{uuid4().hex}.json'
    if exit_kind == 'normal':
        with _live_server(app, record_path):
            pass
    else:
        error = RuntimeError if exit_kind == 'failure' else KeyboardInterrupt
        with pytest.raises(error):
            with _live_server(app, record_path):
                raise error('simulated fixture-body exit')
    record = json.loads(record_path.read_text(encoding='utf-8'))
    assert record['cleaned'] is True
    assert record['thread_alive'] is False and record['listening'] is False
    assert record['owned_processes'] == record['owned_children'] == []
    assert record['outcome'] == {'normal': 'success', 'failure': 'RuntimeError', 'interrupt': 'KeyboardInterrupt'}[exit_kind]


def _prepared_members():
    records = []
    expected = {}
    for parameter, ids in (
        ('PPA_X', [f'{index:08d}' for index in range(607)]),
        ('SECOND', [f'B{index:08d}' for index in range(503)]),
        ('OLD_WEEK', ['OLD0001', 'OLD0002']),
    ):
        expected[parameter] = sorted(ids)
        start, end = ('2026-10-20 10:00:00', '2026-10-22 10:00:00') if parameter == 'OLD_WEEK' else (
            '2026-10-27 10:00:00', '2026-10-29 10:00:00')
        # Reverse input and repeat points: the published list must be unique and stable.
        for index, sheet_id in enumerate(reversed(ids)):
            records.append(dict(prod_code='P1', factory='ARRAY', step_id='0100',
                                param_name=parameter, sheet_id=sheet_id,
                                sheet_start_time=start if index % 2 else end, param_value=8.0))
        records.append({**records[-1], 'sheet_start_time': end})
    # Same parameter in another factory/station, and outside the target ISO week.
    for overrides in (
        dict(factory='CELL', sheet_id='OTHER_FACTORY'),
        dict(step_id='0200', sheet_id='OTHER_STEP'),
        dict(param_name='UNRELATED', sheet_id='OTHER_PARAMETER'),
        dict(sheet_start_time='2026-10-25 23:59:59', sheet_id='BEFORE_WEEK'),
        dict(sheet_start_time='2026-11-02 00:00:00', sheet_id='AFTER_WEEK'),
    ):
        records.append({**records[0], **overrides})
    return pd.DataFrame(records), expected


@pytest.mark.parametrize('event_type', ['CPK', 'CPM'])
def test_published_member_files_reach_real_http_caller_and_complete_exports(capability_files, monkeypatch, event_type):
    files = capability_files
    points, expected = _prepared_members()
    workbook = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    sheets = {}
    for metric, name in (('cpk', 'P1'), ('cpm', 'P1_cpm')):
        rows = _capability_rows(metric)
        sheets[name] = pd.concat([rows, rows.iloc[[0]].assign(param_name='SECOND')], ignore_index=True)
    _write(workbook, sheets)
    files.publish_membership(points)
    oos_path = files.paths[('inline_domain', 'spc_sheet_oos_decoration')]
    oos_rows = pd.read_excel(oos_path, sheet_name='P1', dtype=object)
    _write(oos_path, {'P1': pd.concat([
        oos_rows,
        oos_rows.iloc[[0]].assign(sheet_id='NULL_BOUNDARY', oos_type=None),
        oos_rows.iloc[[0]].assign(sheet_id='LOWER_BOUNDARY', oos_type='lower'),
    ], ignore_index=True)})
    _write(files.policy_path, {'全指标预警看板': files.policy.assign(P2=False)})
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda **_: ['P1', 'P2'])
    principal = ApiClient('e2e-fixture', hashlib.sha256(FIXTURE_KEY.encode()).hexdigest(),
                          ('P1', 'P2'), ('ARRAY', 'CELL'))
    app = create_app(clients_provider=lambda: (principal,), clock=lambda: NOW)
    artifacts = ROOT / 'output/test-results/api-sheet-membership-e2e' / f'{event_type}-{uuid4().hex}'
    before = {path: path.read_bytes() for path in files.metadata_path.parent.rglob('*') if path.is_file()}
    with _live_server(app, artifacts / 'lifecycle.json') as base_url:
        params = {'product': 'P1', 'sub_domain': 'SPC', 'event_type': event_type}
        full = caller.fetch_anomalies(base_url, FIXTURE_KEY, params)
        assert full['meta']['complete'] is True
        assert full['meta']['period_start'] == '2026-10-26T00:00:00+08:00'
        assert full['meta']['period_end_exclusive'] == '2026-11-02T00:00:00+08:00'
        assert full['meta']['returned_count'] == 2
        by_parameter = {row['监控参数']: row for row in full['data']}
        assert set(by_parameter) == {'PPA_X', 'SECOND'}
        for parameter, row in by_parameter.items():
            assert row['对象编号'] == expected[parameter]
            assert row['对象类型'] == 'capability'
            assert row['统计周期'] == '2026-W44'
            assert row['命中边界'] == 'spec'
            assert row['部门'] == 'ARRAY' and row['发生站点'] == '0100'
        paged = caller.fetch_anomalies(base_url, FIXTURE_KEY, params, page_size=1)
        assert paged['data'] == full['data']
        assert paged['meta']['returned_count'] == 2 and paged['client']['pages_fetched'] == 2
        selected = caller.fetch_anomalies(base_url, FIXTURE_KEY,
                                         {**params, 'entity_id': '00000001'})
        assert selected['data'] == [by_parameter['PPA_X']]
        for extra in ({'entity_id': 'OTHER_FACTORY'}, {'entity_id': 'OTHER_STEP'},
                      {'entity_id': 'OTHER_PARAMETER'}, {'entity_id': 'BEFORE_WEEK'},
                      {'entity_id': 'AFTER_WEEK'}, {'step_id': '0200'}, {'factory': 'CELL'}):
            unmatched = caller.fetch_anomalies(base_url, FIXTURE_KEY, {**params, **extra})
            assert unmatched['meta']['complete'] is True and unmatched['data'] == []
        old = caller.fetch_anomalies(base_url, FIXTURE_KEY,
                                    {**params, 'start_date': '2026-10-19', 'end_date': '2026-10-25'})
        assert [row['对象编号'] for row in old['data']] == [expected['OLD_WEEK']]
        original = caller.fetch_anomalies(base_url, FIXTURE_KEY, {'product': 'P1', 'sub_domain': 'SPC', 'event_type': 'OOS'})
        assert all(isinstance(row['对象编号'], str) for row in original['data'])
        assert {row['对象编号']: row['命中边界'] for row in original['data']} == {
            '00000000001': 'USL', 'NULL_BOUNDARY': 'spec', 'LOWER_BOUNDARY': 'spec'}
        partial = caller.fetch_anomalies(base_url, FIXTURE_KEY,
                                        {'sub_domain': 'SPC', 'event_type': event_type}, page_size=1)
        assert partial['data'] == full['data']
        assert partial['meta']['complete'] is False
        assert partial['meta']['failed_sources'] == [dict(
            product='P2', sub_domain='SPC', event_type=event_type, code='result_unavailable')]
        for name, result in (('complete', paged), ('partial', partial)):
            json_path, csv_path = artifacts / f'{name}.json', artifacts / f'{name}.csv'
            caller.save_result(result, json_path)
            caller.save_records_csv(result, csv_path)
            assert json.loads(json_path.read_text(encoding='utf-8')) == result
            with csv_path.open(encoding='utf-8-sig', newline='') as stream:
                exported = list(csv.DictReader(stream))
            assert len(exported) == 2
            assert {row['监控参数']: json.loads(row['对象编号']) for row in exported} == {
                parameter: expected[parameter] for parameter in ('PPA_X', 'SECOND')}
            receipt = json.loads(csv_path.with_name(csv_path.name + '.meta.json').read_text(encoding='utf-8'))
            assert receipt['export']['row_count'] == 2
            assert receipt['meta']['complete'] is (name == 'complete')
        assert all(path.read_bytes() == contents for path, contents in before.items())
    lifecycle = json.loads((artifacts / 'lifecycle.json').read_text(encoding='utf-8'))
    assert lifecycle['cleaned'] is True
