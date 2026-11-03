"""Portable caller through real HTTP handlers and temporary published workbooks.

Transport stays inside ASGI: no listener, database or maintained workbook use.
"""
from io import BytesIO
from urllib.error import HTTPError

from fastapi.testclient import TestClient
import pandas as pd
import pytest

from tests.integration.test_anomaly_api_files import FIXTURE_KEY, published_files, _inline_rows, _write
from tools.fetch_sheet_anomalies import ApiCallError, fetch_anomalies, result_summary


class WireResponse:
    def __init__(self, response):
        self.status, self.headers = response.status_code, response.headers
        self.stream = BytesIO(response.content)

    def read(self, size):
        return self.stream.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stream.close()


class AsgiOpener:
    def __init__(self, client, after_first=None):
        self.client, self.after_first = client, after_first
        self.calls = 0

    def open(self, request, timeout):
        response = self.client.get(request.full_url, headers=dict(request.header_items()), follow_redirects=False)
        self.calls += 1
        if self.calls == 1 and self.after_first:
            self.after_first()
        if response.status_code >= 400:
            raise HTTPError(request.full_url, response.status_code, 'fixture response',
                            response.headers, BytesIO(response.content))
        return WireResponse(response)


def _two_spc_rows(files):
    path = files.paths[('inline_domain', 'spc_sheet_oos_decoration')]
    frame = _inline_rows('spc')
    another = frame.iloc[[0]].assign(sheet_id='00000000002')
    _write(path, {'P1': pd.concat([frame, another], ignore_index=True)})
    return path


def test_client_gets_all_real_records_with_or_without_pagination(published_files):
    path = _two_spc_rows(published_files)
    before = path.read_bytes()
    with TestClient(published_files.app()) as http:
        full = fetch_anomalies('http://api.test', FIXTURE_KEY,
                               {'product': 'P1', 'sub_domain': 'SPC'}, opener=AsgiOpener(http))
        paged = fetch_anomalies('http://api.test', FIXTURE_KEY,
                                {'product': 'P1', 'sub_domain': 'SPC'}, page_size=1,
                                opener=AsgiOpener(http))
    assert full['data'] == paged['data']
    assert len(full['data']) == 2
    assert full['data'][0]['发生站点'] == '0100'
    assert full['data'][0]['层别'] == 'PI2'
    assert full['client']['pages_fetched'] == 1
    assert paged['client']['pages_fetched'] == 2
    assert full['meta']['complete'] is True
    assert path.read_bytes() == before


def test_client_reports_complete_empty_query_without_calling_it_a_failure(published_files):
    with TestClient(published_files.app()) as http:
        result = fetch_anomalies('http://api.test', FIXTURE_KEY,
            {'product': 'P1', 'sub_domain': 'SPC', 'start_date': '2020-01-01', 'end_date': '2020-01-07'},
            opener=AsgiOpener(http))
    assert result['data'] == [] and result['meta']['complete'] is True
    assert '调用成功' in result_summary(result)
    assert '没有可提供' in result_summary(result)


def test_client_marks_partial_source_failure_while_retaining_available_data(published_files):
    published_files.paths[('inline_domain', 'ctq_sheet_oos_decoration')].unlink()
    with TestClient(published_files.app()) as http:
        result = fetch_anomalies('http://api.test', FIXTURE_KEY, {'product': 'P1'}, opener=AsgiOpener(http))
    assert result['data']
    assert result['meta']['complete'] is False
    assert any(s['sub_domain'] == 'CTQ' for s in result['meta']['failed_sources'])
    assert '部分来源不可用' in result_summary(result)


def test_client_rejects_invalid_authentication_with_safe_error(published_files):
    with TestClient(published_files.app()) as http:
        with pytest.raises(ApiCallError) as error:
            fetch_anomalies('http://api.test', 'wrong-placeholder', opener=AsgiOpener(http))
    assert error.value.status == 401
    assert 'wrong-placeholder' not in str(error.value)


def test_client_does_not_return_a_page_prefix_when_published_result_changes(published_files):
    path = _two_spc_rows(published_files)

    def publish_different_identity():
        frame = pd.read_excel(path, sheet_name='P1', dtype=object)
        frame.loc[0, 'sheet_id'] = 'DIFFERENT'
        _write(path, {'P1': frame})

    with TestClient(published_files.app()) as http:
        with pytest.raises(ApiCallError) as error:
            fetch_anomalies('http://api.test', FIXTURE_KEY, {'product': 'P1', 'sub_domain': 'SPC'},
                            page_size=1, opener=AsgiOpener(http, publish_different_identity))
    assert error.value.status == 410
