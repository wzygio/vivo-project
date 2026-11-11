"""Portable client checks with no listening server or production credentials."""
from io import BytesIO
import csv
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest

from tools import fetch_sheet_anomalies as client


FAKE_KEY = 'offline-client-fixture-key'
CAPABILITY_FIELDS = ('指标值', '达标下限', '统计周期类型', '统计周期', '统计开始日期', '统计截止日期')


@pytest.fixture(autouse=True)
def isolated_credential_environment(monkeypatch):
    monkeypatch.delenv('API_KEY', raising=False)
    monkeypatch.delenv('VIVO_API_KEY', raising=False)


def test_environment_key_can_be_filled_without_loading_or_saving_a_file(monkeypatch):
    assert client.configured_api_key() == ''
    monkeypatch.setenv('VIVO_API_KEY', 'synthetic-legacy')
    assert client.configured_api_key() == 'synthetic-legacy'
    monkeypatch.setenv('API_KEY', FAKE_KEY)
    assert client.configured_api_key() == FAKE_KEY


def capability_record(index=0, event_type='CPK'):
    return {'分类': 'SPC', '发生站点': '00100', '部门': 'ARRAY', '层别': 'PI2',
            '监控参数': '中文参数', '异常事件': event_type, '对象类型': 'capability',
            '对象编号': [f'{index:08d}-SHEET-01', f'{index:08d}-SHEET-02'], '产品型号': 'M626',
            '发生时间': '2026-11-01', '命中边界': 'spec',
            '指标值': 1.12, '达标下限': 1.33, '统计周期类型': 'week',
            '统计周期': '2026-W44', '统计开始日期': '2026-10-26', '统计截止日期': '2026-11-01'}


def response(rows=None, *, more=False, cursor=None, complete=True, failed=None):
    rows = [] if rows is None else rows
    return {'data': rows, 'meta': {
        'status': 'alert' if rows or more else 'ok', 'returned_count': len(rows),
        'has_more': more, 'next_cursor': cursor,
        'period_start': '2026-10-26T00:00:00+08:00',
        'period_end_exclusive': '2026-11-02T00:00:00+08:00',
        'timezone': 'Asia/Shanghai', 'read_at': '2026-11-03T10:00:00+08:00',
        'limitations': [], 'complete': complete, 'failed_sources': failed or [],
    }}


class Reply(BytesIO):
    status = 200
    headers = {'X-Request-ID': 'a' * 32}


class Opener:
    def __init__(self, *results):
        self.results = list(results)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return Reply(json.dumps(result).encode('utf-8'))


def test_default_request_uses_header_and_no_pagination_parameters():
    opener = Opener(response([{'对象编号': '0001'}]))
    result = client.fetch_anomalies('http://127.0.0.1:8890', FAKE_KEY,
                                    {'product': 'M626'}, opener=opener)
    request, timeout = opener.requests[0]
    assert request.method == 'GET' and timeout == 120
    assert request.get_header('X-api-key') == FAKE_KEY
    assert parse_qs(urlsplit(request.full_url).query) == {'product': ['M626']}
    assert FAKE_KEY not in request.full_url
    assert result['meta']['returned_count'] == 1
    assert result['client']['pages_fetched'] == 1


def test_pagination_fetches_all_and_preserves_filters():
    opener = Opener(response([{'对象编号': '1'}], more=True, cursor='next+token/='),
                    response([{'对象编号': '2'}]))
    result = client.fetch_anomalies('http://host:8890', FAKE_KEY,
                                    {'product': 'M626', 'parameter_name': '中文 空格'},
                                    page_size=1, opener=opener)
    assert [r['对象编号'] for r in result['data']] == ['1', '2']
    assert result['meta']['returned_count'] == 2
    assert result['meta']['has_more'] is False and result['meta']['next_cursor'] is None
    assert result['client']['pages_fetched'] == 2
    first, second = [parse_qs(urlsplit(r.full_url).query) for r, _ in opener.requests]
    assert first == {'product': ['M626'], 'parameter_name': ['中文 空格'], 'limit': ['1']}
    assert second == dict(first, cursor=['next+token/='])


@pytest.mark.parametrize('event_type', ['CPK', 'CPM'])
def test_capability_query_keeps_date_source_and_pagination_defaults_on_server(event_type):
    opener = Opener(response([capability_record(event_type=event_type)]))
    client.fetch_anomalies('http://host:8890', FAKE_KEY, {'event_type': event_type}, opener=opener)
    assert parse_qs(urlsplit(opener.requests[0][0].full_url).query) == {'event_type': [event_type]}


@pytest.mark.parametrize('event_type', ['CPK', 'CPM'])
@pytest.mark.parametrize('complete', [True, False])
def test_capability_pages_and_exports_keep_every_row_and_public_field(tmp_path: Path, event_type, complete):
    records = [capability_record(index, event_type) for index in range(1205)]
    failed = [] if complete else [
        {'product': 'M678', 'sub_domain': 'SPC', 'event_type': event_type, 'code': 'result_unavailable'}]
    pages = [response(records[start:start + 500], more=start < 1000,
                      cursor=f'page-{start + 500}' if start < 1000 else None,
                      complete=complete, failed=failed) for start in range(0, 1205, 500)]
    opener = Opener(*pages)
    filters = {'sub_domain': 'SPC', 'event_type': event_type, 'entity_type': 'capability',
               'start_date': '2026-10-26', 'end_date': '2026-11-01'}
    result = client.fetch_anomalies('http://host:8890', FAKE_KEY, filters, page_size=500, opener=opener)
    assert result['data'] == records
    assert result['meta']['returned_count'] == 1205 and result['meta']['failed_sources'] == failed
    assert result['client']['pages_fetched'] == 3
    for request, _ in opener.requests:
        query = parse_qs(urlsplit(request.full_url).query)
        assert all(query[name] == [value] for name, value in filters.items())
        assert query['limit'] == ['500']
    json_path, csv_path = tmp_path / 'capability.json', tmp_path / 'capability.csv'
    client.save_result(result, json_path)
    assert json.loads(json_path.read_text(encoding='utf-8')) == result
    client.save_records_csv(result, csv_path)
    with csv_path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames[-6:] == list(CAPABILITY_FIELDS)
        exported = list(reader)
    assert len(exported) == 1205
    assert json.loads(exported[-1]['对象编号']) == records[-1]['对象编号']
    assert exported[-1]['对象类型'] == 'capability'
    assert exported[-1]['异常事件'] == event_type
    assert [exported[-1][name] for name in CAPABILITY_FIELDS] == [str(records[-1][name]) for name in CAPABILITY_FIELDS]
    receipt = json.loads(csv_path.with_name(csv_path.name + '.meta.json').read_text(encoding='utf-8'))
    assert receipt['meta']['complete'] is complete and receipt['meta']['failed_sources'] == failed
    assert receipt['export']['row_count'] == 1205


@pytest.mark.parametrize('event_type', ['CPK', 'CPM'])
def test_capability_sheet_filter_and_exports_preserve_the_complete_id_array(tmp_path: Path, event_type):
    record = capability_record(event_type=event_type)
    # A single capability project can contain more members than one page's row limit.
    sheet_ids = [f'{index:08d}' for index in range(1205)] + ['中文片号', 'S,"quoted"', '=1+2', '\t=1']
    record['对象编号'] = sheet_ids
    opener = Opener(response([record]))
    result = client.fetch_anomalies('http://host:8890', FAKE_KEY,
                                    {'event_type': event_type, 'entity_id': '00000001'},
                                    page_size=1, opener=opener)
    query = parse_qs(urlsplit(opener.requests[0][0].full_url).query)
    assert query['entity_id'] == ['00000001']
    assert result['meta']['returned_count'] == 1
    assert result['data'][0]['对象编号'] == sheet_ids
    json_path, csv_path = tmp_path / 'members.json', tmp_path / 'members.csv'
    client.save_result(result, json_path)
    assert json.loads(json_path.read_text(encoding='utf-8'))['data'][0]['对象编号'] == sheet_ids
    client.save_records_csv(result, csv_path)
    with csv_path.open(encoding='utf-8-sig', newline='') as stream:
        exported = list(csv.DictReader(stream))
    assert len(exported) == 1
    assert json.loads(exported[0]['对象编号']) == sheet_ids
    assert exported[0]['命中边界'] == 'spec'
    receipt = json.loads(csv_path.with_name(csv_path.name + '.meta.json').read_text(encoding='utf-8'))
    assert receipt['export']['row_count'] == 1


@pytest.mark.parametrize('invalid_member', [float('nan'), {'not-a-json-value'}])
def test_csv_rejects_non_json_id_lists_before_creating_files(tmp_path: Path, invalid_member):
    record = capability_record()
    record['对象编号'] = ['00001', invalid_member]
    output = tmp_path / 'invalid-list.csv'
    with pytest.raises(client.ApiCallError, match='JSON'):
        client.save_records_csv(response([record]), output)
    assert not output.exists()
    assert not output.with_name(output.name + '.meta.json').exists()


def test_empty_success_and_partial_success_are_distinguished():
    empty = client.fetch_anomalies('http://localhost', FAKE_KEY, opener=Opener(response()))
    assert '没有可提供' in client.result_summary(empty)
    failed = [{'product': 'M626', 'sub_domain': 'Yield', 'event_type': 'OOS', 'code': 'result_unavailable'}]
    partial = client.fetch_anomalies('http://localhost', FAKE_KEY,
                                     opener=Opener(response(complete=False, failed=failed)))
    assert '部分来源' in client.result_summary(partial)
    assert partial['meta']['failed_sources'] == failed


@pytest.mark.parametrize('status', [401, 403, 410, 422, 429, 503, 500, 302])
def test_http_errors_are_actionable_without_echoing_server_secret(status):
    error = HTTPError('http://host', status, FAKE_KEY, {'X-Request-ID': 'b' * 32},
                      BytesIO(json.dumps({'error': {'message': FAKE_KEY, 'code': FAKE_KEY}}).encode()))
    with pytest.raises(client.ApiCallError) as caught:
        client.fetch_anomalies('http://host', FAKE_KEY, opener=Opener(error))
    assert caught.value.status == status
    assert FAKE_KEY not in str(caught.value)
    assert 'b' * 32 in str(caught.value)


def test_transport_error_hides_exception_reason():
    with pytest.raises(client.ApiCallError) as caught:
        client.fetch_anomalies('http://host', FAKE_KEY, opener=Opener(URLError(FAKE_KEY)))
    assert FAKE_KEY not in str(caught.value)


def test_read_timeout_explains_service_is_reachable_without_echoing_server_message():
    error = HTTPError('http://host', 503, FAKE_KEY, {'X-Request-ID': 'c' * 32},
                      BytesIO(json.dumps({'error': {'code': 'read_timeout', 'message': FAKE_KEY}}).encode()))
    with pytest.raises(client.ApiCallError) as caught:
        client.fetch_anomalies('http://host', FAKE_KEY, opener=Opener(error))
    message = str(caught.value)
    assert '已连接' in message and '读取超时' in message
    assert FAKE_KEY not in message and 'c' * 32 in message


@pytest.mark.parametrize('body', [b'not-json', b'[]', b'{"error":null}', b'{"error":[]}', b'x' * 4097])
def test_malformed_error_body_keeps_safe_http_error(body):
    error = HTTPError('http://host', 503, FAKE_KEY, {}, BytesIO(body))
    with pytest.raises(client.ApiCallError) as caught:
        client.fetch_anomalies('http://host', FAKE_KEY, opener=Opener(error))
    assert caught.value.status == 503 and FAKE_KEY not in str(caught.value)


@pytest.mark.parametrize('base', [
    'file:///tmp/secret', 'http://user:password@host', 'http://host?key=abc',
    'http://host/#secret', 'http://', 'http://host:abc',
])
def test_invalid_base_url_is_rejected_without_call(base):
    opener = Opener()
    with pytest.raises(client.ApiCallError):
        client.fetch_anomalies(base, FAKE_KEY, opener=opener)
    assert not opener.requests


@pytest.mark.parametrize('page_size', [0, 501, True])
def test_invalid_page_size_is_rejected(page_size):
    with pytest.raises(client.ApiCallError):
        client.fetch_anomalies('http://localhost', FAKE_KEY, page_size=page_size, opener=Opener())


def test_repeated_cursor_and_changed_scope_fail_without_partial_result():
    page = response([{'对象编号': '1'}], more=True, cursor='same')
    with pytest.raises(client.ApiCallError):
        client.fetch_anomalies('http://localhost', FAKE_KEY, page_size=1, opener=Opener(page, page))
    second = response([{'对象编号': '2'}])
    second['meta']['period_start'] = '2026-10-01T00:00:00+08:00'
    with pytest.raises(client.ApiCallError):
        client.fetch_anomalies('http://localhost', FAKE_KEY, page_size=1, opener=Opener(page, second))


def test_expired_second_page_does_not_return_first_page():
    error = HTTPError('http://host', 410, 'expired', {}, BytesIO(b'{}'))
    with pytest.raises(client.ApiCallError, match='重新查询'):
        client.fetch_anomalies('http://host', FAKE_KEY, page_size=1,
                                opener=Opener(response([{'对象编号': '1'}], more=True, cursor='next'), error))


@pytest.mark.parametrize('change', ['count', 'data', 'meta', 'cursor', 'complete'])
def test_malformed_response_is_never_exported_as_success(change):
    payload = response([{'对象编号': '1'}])
    if change == 'count':
        payload['meta']['returned_count'] = 9
    elif change == 'data':
        payload['data'] = 'wrong'
    elif change == 'meta':
        payload['meta'] = None
    elif change == 'cursor':
        payload['meta']['has_more'] = True
    else:
        payload['meta']['complete'] = 'true'
    with pytest.raises(client.ApiCallError):
        client.fetch_anomalies('http://host', FAKE_KEY, opener=Opener(payload))


def test_json_export_is_utf8_and_does_not_overwrite_existing(tmp_path: Path):
    payload = response([{'对象编号': '中文编号'}])
    path = tmp_path / 'data/result.json'
    client.save_result(payload, path)
    assert json.loads(path.read_text(encoding='utf-8')) == payload
    assert b'\\u' not in path.read_bytes()
    with pytest.raises(client.ApiCallError):
        client.save_result(response(), path)
    assert json.loads(path.read_text(encoding='utf-8')) == payload


def test_csv_download_writes_all_1205_records_and_only_public_columns(tmp_path: Path):
    rows = [{'分类': 'SPC', '对象编号': f'{i:08d}', '产品型号': 'M626',
             '发生站点': '00100', '层别': 'PI2', '命中边界': None,
             '监控参数': '中文,带逗号', 'private_flag': 'must never be exported'}
            for i in range(1205)]
    payload = response(rows)
    output = tmp_path / 'all-records.csv'
    assert client.save_records_csv(payload, output) == output
    assert output.read_bytes().startswith(b'\xef\xbb\xbf')
    with output.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == list(client.PUBLIC_FIELDS)
        saved = list(reader)
    assert len(saved) == 1205
    assert saved[0]['对象编号'] == '00000000' and saved[-1]['对象编号'] == '00001204'
    assert saved[-1]['发生站点'] == '00100'
    assert saved[-1]['监控参数'] == '中文,带逗号'
    assert saved[-1]['命中边界'] == ''
    assert list(client.PUBLIC_FIELDS[:11]) == ['分类', '发生站点', '部门', '层别', '监控参数',
                                             '异常事件', '对象类型', '对象编号', '产品型号', '发生时间', '命中边界']
    assert all(saved[-1][field] == '' for field in CAPABILITY_FIELDS)
    assert 'must never be exported' not in output.read_text(encoding='utf-8-sig')
    receipt = json.loads(output.with_name(output.name + '.meta.json').read_text(encoding='utf-8'))
    assert receipt['meta']['complete'] is True
    assert receipt['export']['row_count'] == 1205


@pytest.mark.parametrize('value', [
    '=SUM(1,2)', '+cmd', '-1+2', '@SUM(A1)', '\t=1', '\r=1', '\n=1', '   =1', '\ufeff=1',
    '\v=1', '\u00a0=1',
])
def test_csv_download_neutralizes_excel_formulas(tmp_path: Path, value):
    output = tmp_path / 'safe.csv'
    client.save_records_csv(response([{'监控参数': value}]), output)
    with output.open(encoding='utf-8-sig', newline='') as stream:
        row = next(csv.DictReader(stream))
    assert row['监控参数'] == "'" + value


def test_partial_csv_download_keeps_failure_metadata_and_does_not_overwrite(tmp_path: Path):
    failed = [{'product': 'M626', 'sub_domain': 'CTQ', 'event_type': 'OOS', 'code': 'result_unavailable'}]
    payload = response([{'对象编号': '00001'}], complete=False, failed=failed)
    output = tmp_path / 'partial.csv'
    client.save_records_csv(payload, output)
    receipt_path = output.with_name(output.name + '.meta.json')
    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    assert receipt['meta']['complete'] is False and receipt['meta']['failed_sources'] == failed
    before = (output.read_bytes(), receipt_path.read_bytes())
    with pytest.raises(client.ApiCallError):
        client.save_records_csv(response(), output)
    assert before == (output.read_bytes(), receipt_path.read_bytes())


def test_empty_partial_csv_has_headers_and_explicit_incomplete_receipt(tmp_path: Path):
    output = tmp_path / 'empty-partial.csv'
    payload = response(complete=False, failed=[{'code': 'result_unavailable'}])
    client.save_records_csv(payload, output)
    with output.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == list(client.PUBLIC_FIELDS)
        assert list(reader) == []
    receipt = json.loads(output.with_name(output.name + '.meta.json').read_text(encoding='utf-8'))
    assert receipt['export'] == {'format': 'csv', 'data_file': output.name, 'row_count': 0, 'complete': False}


@pytest.mark.parametrize('format', ['csv', 'json'])
def test_incomplete_pagination_cannot_be_downloaded(tmp_path: Path, format):
    payload = response([{'对象编号': '1'}], more=True, cursor='next')
    output = tmp_path / ('unfinished.' + format)
    with pytest.raises(client.ApiCallError, match='分页'):
        (client.save_records_csv if format == 'csv' else client.save_result)(payload, output)
    assert not output.exists()


def test_csv_receipt_collision_prevents_creating_data_file(tmp_path: Path):
    output = tmp_path / 'collision.csv'
    receipt = output.with_name(output.name + '.meta.json')
    receipt.write_text('existing user file', encoding='utf-8')
    with pytest.raises(client.ApiCallError):
        client.save_records_csv(response(), output)
    assert not output.exists()
    assert receipt.read_text(encoding='utf-8') == 'existing user file'


def test_csv_download_rolls_back_its_new_data_file_if_receipt_cannot_be_saved(tmp_path: Path, monkeypatch):
    real_write = client._write_new_text
    output = tmp_path / 'receipt-failure.csv'
    def fail_receipt(path, text, **kwargs):
        if str(path).endswith('.meta.json'):
            raise client.ApiCallError('offline simulated receipt failure')
        return real_write(path, text, **kwargs)
    monkeypatch.setattr(client, '_write_new_text', fail_receipt)
    with pytest.raises(client.ApiCallError):
        client.save_records_csv(response(), output)
    assert not output.exists()
    assert not output.with_name(output.name + '.meta.json').exists()


@pytest.mark.parametrize('variable', ['API_KEY', 'VIVO_API_KEY'])
def test_cli_can_download_csv_and_companion_metadata(tmp_path: Path, monkeypatch, capsys, variable):
    monkeypatch.setenv(variable, FAKE_KEY)
    monkeypatch.setattr(client.getpass, 'getpass', lambda *args: pytest.fail('Configured key should not prompt'))
    monkeypatch.setattr(client, 'fetch_anomalies', lambda *a, **k: response([{'对象编号': '0001'}]))
    output = tmp_path / 'cli.csv'
    assert client.main(['--cli', '--format', 'csv', '--output', str(output)]) == 0
    with output.open(encoding='utf-8-sig', newline='') as stream:
        assert next(csv.DictReader(stream))['对象编号'] == '0001'
    assert output.with_name(output.name + '.meta.json').exists()
    assert FAKE_KEY not in capsys.readouterr().out


def test_default_opener_ignores_environment_proxy_and_refuses_redirect():
    opener = client.direct_opener()
    assert not any(type(h).__name__ == 'ProxyHandler' for h in opener.handlers)
    redirect = next(h for h in opener.handlers if isinstance(h, client.NoCredentialRedirect))
    assert redirect.redirect_request(None, None, 302, '', {}, 'http://other') is None


def test_cli_uses_environment_key_and_saves_result_without_printing_key(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv('VIVO_API_KEY', FAKE_KEY)
    seen = []
    monkeypatch.setattr(client, 'fetch_anomalies', lambda base, key, params, **kwargs:
                        seen.append((base, key, params, kwargs)) or response())
    path = tmp_path / 'result.json'
    assert client.main(['--cli', '--product', 'M626', '--output', str(path)]) == 0
    assert seen[0][1] == FAKE_KEY
    assert FAKE_KEY not in capsys.readouterr().out
    assert path.exists()


@pytest.mark.parametrize('event_type', ['CPK', 'CPM'])
def test_cli_passes_capability_filters_and_keeps_json_fields(tmp_path: Path, monkeypatch, event_type):
    monkeypatch.setenv('VIVO_API_KEY', FAKE_KEY)
    seen = []
    payload = response([capability_record(event_type=event_type)])
    monkeypatch.setattr(client, 'fetch_anomalies', lambda base, key, params, **kwargs:
                        seen.append(params) or payload)
    output = tmp_path / f'{event_type}.json'
    assert client.main(['--cli', '--sub-domain', 'SPC', '--event-type', event_type,
                        '--entity-type', 'capability', '--output', str(output)]) == 0
    assert seen == [{'sub_domain': 'SPC', 'event_type': event_type, 'entity_type': 'capability'}]
    assert json.loads(output.read_text(encoding='utf-8')) == payload


@pytest.mark.parametrize('key', ['', '中文密钥', 'key\nheader', 'key\x00header', 'key with space'])
def test_invalid_key_never_reaches_http_transport(key):
    opener = Opener()
    with pytest.raises(client.ApiCallError):
        client.fetch_anomalies('http://localhost', key, opener=opener)
    assert not opener.requests


def test_response_size_limit_and_max_pages_do_not_return_prefix(monkeypatch):
    monkeypatch.setattr(client, 'MAX_RESPONSE_BYTES', 10)
    with pytest.raises(client.ApiCallError, match='上限'):
        client.fetch_anomalies('http://localhost', FAKE_KEY, opener=Opener(response()))
    monkeypatch.setattr(client, 'MAX_RESPONSE_BYTES', 10000)
    with pytest.raises(client.ApiCallError, match='次数上限'):
        client.fetch_anomalies('http://localhost', FAKE_KEY, page_size=1, max_pages=1,
                                opener=Opener(response([{'对象编号': '1'}], more=True, cursor='next')))


def test_cli_partial_export_has_distinct_exit_status(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv('VIVO_API_KEY', FAKE_KEY)
    payload = response(complete=False, failed=[{'code': 'result_unavailable'}])
    monkeypatch.setattr(client, 'fetch_anomalies', lambda *a, **k: payload)
    path = tmp_path / 'partial.json'
    assert client.main(['--cli', '--output', str(path)]) == 3
    assert '部分来源' in capsys.readouterr().out
    assert path.exists()


def test_cli_errors_do_not_write_or_echo_private_exception(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv('VIVO_API_KEY', FAKE_KEY)
    def failing(*args, **kwargs):
        raise ValueError(FAKE_KEY)
    monkeypatch.setattr(client, 'fetch_anomalies', failing)
    path = tmp_path / 'must-not-exist.json'
    assert client.main(['--cli', '--output', str(path)]) == 2
    output = capsys.readouterr()
    assert FAKE_KEY not in output.out + output.err
    assert not path.exists()


def test_no_arguments_open_gui_and_help_does_not(monkeypatch):
    opened = []
    monkeypatch.setattr(client, 'launch_gui', lambda args: opened.append(args) or 0)
    assert client.main([]) == 0
    assert len(opened) == 1
    with pytest.raises(SystemExit) as done:
        client.main(['--help'])
    assert done.value.code == 0 and len(opened) == 1


@pytest.mark.parametrize('field', [
    'factory', 'step-id', 'layer', 'parameter-name', 'entity-type', 'entity-id', 'output',
])
def test_gui_rejects_unsupported_filters_instead_of_silently_widening_scope(field, monkeypatch, capsys):
    opened = []
    monkeypatch.setattr(client, 'launch_gui', lambda args: opened.append(args) or 0)
    with pytest.raises(SystemExit) as failure:
        client.main(['--gui', '--' + field, 'PRIVATE_INPUT'])
    assert failure.value.code == 2 and not opened
    text = capsys.readouterr().err
    assert '--cli' in text
    assert 'PRIVATE_INPUT' not in text


@pytest.mark.parametrize('arguments', [
    ['--api-key', 'TOP_SECRET'], ['TOP_SECRET'], ['--timeout', 'TOP_SECRET'],
])
def test_argument_errors_do_not_echo_misplaced_secret(arguments, capsys):
    with pytest.raises(SystemExit) as failure:
        client.main(arguments)
    assert failure.value.code == 2
    output = capsys.readouterr()
    assert 'TOP_SECRET' not in output.out + output.err
    assert '--help' in output.err


def test_gui_timeout_is_forwarded_by_main(monkeypatch):
    opened = []
    monkeypatch.setattr(client, 'launch_gui', lambda args: opened.append(args) or 0)
    assert client.main(['--gui', '--timeout', '12.5']) == 0
    assert opened[0].timeout == 12.5


@pytest.mark.parametrize('complete', [True, False])
@pytest.mark.parametrize('event_type', ['OOS', 'CPK', 'CPM', None])
def test_hidden_gui_queries_previews_and_exports_without_retaining_key(tmp_path: Path, monkeypatch, complete, event_type):
    """Exercise real Tk widgets hidden; the HTTP function is an in-memory fixture."""
    tk = pytest.importorskip('tkinter')
    from tkinter import filedialog
    from types import SimpleNamespace

    real_tk = tk.Tk
    failures, calls, completed, dialogs = [], [], [], []
    output = tmp_path / 'gui-result.json'
    capability = event_type in ('CPK', 'CPM')
    if event_type is None:
        monkeypatch.setenv('API_KEY', FAKE_KEY)
    records = ([{'对象编号': f'{i:08d}'} for i in range(1205)] if not capability else
               [capability_record(i, event_type) for i in range(1205)])
    payload = response(records, complete=complete,
                       failed=[] if complete else [{'code': 'result_unavailable'}])
    payload['client'] = {'pages_fetched': 1, 'pagination_enabled': False, 'request_ids': []}

    def fake_fetch(base, key, params, **kwargs):
        calls.append((base, key, params, kwargs))
        return payload

    monkeypatch.setattr(client, 'fetch_anomalies', fake_fetch)
    monkeypatch.setattr(client, 'default_output', lambda: output)
    monkeypatch.setattr(filedialog, 'asksaveasfilename',
                        lambda **kwargs: dialogs.append(kwargs) or str(output.with_suffix(kwargs['defaultextension'])))

    def root_factory():
        root = real_tk()
        root.withdraw()

        def check():
            try:
                frame = root.winfo_children()[0]
                inputs = {int(w.grid_info()['row']): w for w in frame.winfo_children()
                          if w.winfo_class() in ('TEntry', 'TCombobox')}
                buttons = next(w for w in frame.winfo_children() if w.winfo_class() == 'TFrame')
                query_button, csv_button, save_button = buttons.winfo_children()
                assert root.title() == '异常明细查询'
                assert tuple(inputs[4]['values']) == ('全部', 'OOS', 'OOC', 'CPK', 'CPM')
                assert inputs[4].get() == '全部'
                labels = ' '.join(str(w['text']) for w in frame.winfo_children() if w.winfo_class() == 'TLabel')
                assert 'CPK/CPM' in labels and 'SPC' in labels and '周统计项' in labels
                assert csv_button['text'] == '下载全部明细(CSV)'
                assert save_button['text'] == '保存完整响应(JSON)'
                if event_type is None:
                    assert inputs[1].get() == FAKE_KEY and inputs[1]['show'] == '*'
                else:
                    inputs[1].insert(0, FAKE_KEY)
                inputs[2].insert(0, 'M626')
                if capability:
                    inputs[3].set('SPC')
                if event_type is not None:
                    inputs[4].set(event_type)
                query_button.invoke()

                def finish():
                    try:
                        if str(save_button['state']) != 'normal':
                            root.after(50, finish)
                            return
                        assert inputs[1].get() == ''
                        assert calls[0][1] == FAKE_KEY
                        expected = {'product': 'M626'}
                        if event_type is not None:
                            expected['event_type'] = event_type
                        if capability:
                            expected['sub_domain'] = 'SPC'
                        assert calls[0][2] == expected
                        assert calls[0][3]['timeout'] == 12.5
                        view = next(w for w in frame.winfo_children() if w.winfo_class() == 'Text')
                        first_id, last_id = records[0]['对象编号'], records[-1]['对象编号']
                        if capability:
                            first_id, last_id = first_id[0], last_id[0]
                        assert first_id in view.get('1.0', 'end')
                        assert last_id not in view.get('1.0', 'end')
                        assert FAKE_KEY not in view.get('1.0', 'end')
                        assert csv_button['text'] == ('下载全部明细(CSV)' if complete else '下载可用部分(CSV)')
                        csv_button.invoke()
                        with output.with_suffix('.csv').open(encoding='utf-8-sig', newline='') as stream:
                            exported = list(csv.DictReader(stream))
                        assert len(exported) == 1205
                        if capability:
                            assert exported[-1]['指标值'] == '1.12'
                            assert exported[-1]['统计周期'] == '2026-W44'
                            assert json.loads(exported[-1]['对象编号']) == records[-1]['对象编号']
                        save_button.invoke()
                        assert json.loads(output.read_text(encoding='utf-8')) == payload
                        assert all(('-partial' in call['initialfile']) == (not complete) for call in dialogs)
                        completed.append(True)
                        root.destroy()
                    except BaseException as exc:
                        failures.append(exc)
                        root.destroy()
                root.after(150, finish)
            except BaseException as exc:
                failures.append(exc)
                root.destroy()

        def deadline():
            failures.append(AssertionError('GUI did not finish within 5 seconds'))
            root.destroy()
        root.after(20, check)
        root.after(5000, deadline)
        return root

    monkeypatch.setattr(tk, 'Tk', root_factory)
    assert client.launch_gui(SimpleNamespace(timeout=12.5)) == 0
    assert completed and not failures, failures


@pytest.mark.parametrize('failure_kind', ['request', 'invalid_page_size', 'unsupported_capability_source'])
def test_hidden_gui_failed_new_query_disables_old_downloads(failure_kind, monkeypatch):
    tk = pytest.importorskip('tkinter')
    from tkinter import messagebox

    real_tk = tk.Tk
    failures, completed, calls = [], [], []
    payload = response([{'对象编号': 'old-query-record'}])
    payload['client'] = {'pages_fetched': 1, 'pagination_enabled': False, 'request_ids': []}

    def fake_fetch(*args, **kwargs):
        calls.append(args[2])
        if len(calls) > 1:
            raise client.ApiCallError('offline failed query', status=422)
        return payload

    monkeypatch.setattr(client, 'fetch_anomalies', fake_fetch)
    monkeypatch.setattr(messagebox, 'showerror', lambda *a, **k: None)

    def forbidden_export(*args, **kwargs):
        raise AssertionError('Old query must not be exported after a new failed query')

    monkeypatch.setattr(client, 'save_result', forbidden_export)
    monkeypatch.setattr(client, 'save_records_csv', forbidden_export)

    def factory():
        root = real_tk()
        root.withdraw()
        phase = []

        def poll_test():
            try:
                frame = root.winfo_children()[0]
                inputs = {int(w.grid_info()['row']): w for w in frame.winfo_children()
                          if w.winfo_class() in ('TEntry', 'TCombobox')}
                buttons = next(w for w in frame.winfo_children() if w.winfo_class() == 'TFrame')
                query, csv_download, json_download = buttons.winfo_children()
                if not phase:
                    inputs[1].insert(0, FAKE_KEY)
                    query.invoke()
                    phase.append('first')
                elif phase[0] == 'first' and str(csv_download['state']) == 'normal':
                    inputs[1].insert(0, FAKE_KEY)
                    if failure_kind == 'invalid_page_size':
                        inputs[7].insert(0, 'not-an-integer')
                    elif failure_kind == 'unsupported_capability_source':
                        inputs[3].set('Yield')
                        inputs[4].set('CPK')
                    query.invoke()
                    phase[0] = 'second'
                elif phase[0] == 'second' and str(query['state']) == 'normal':
                    assert str(csv_download['state']) == 'disabled'
                    assert str(json_download['state']) == 'disabled'
                    view = next(w for w in frame.winfo_children() if w.winfo_class() == 'Text')
                    assert view.get('1.0', 'end').strip() == ''
                    csv_download.invoke()
                    json_download.invoke()
                    assert len(calls) == (1 if failure_kind == 'invalid_page_size' else 2)
                    if failure_kind == 'unsupported_capability_source':
                        assert calls[1] == {'sub_domain': 'Yield', 'event_type': 'CPK'}
                    completed.append(True)
                    root.destroy()
                    return
                root.after(50, poll_test)
            except BaseException as exc:
                failures.append(exc)
                root.destroy()

        def deadline():
            failures.append(AssertionError('GUI did not finish'))
            root.destroy()
        root.after(20, poll_test)
        root.after(5000, deadline)
        return root

    monkeypatch.setattr(tk, 'Tk', factory)
    assert client.launch_gui() == 0
    assert completed and not failures, failures
