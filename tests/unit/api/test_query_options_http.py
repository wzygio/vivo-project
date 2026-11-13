"""Optional, cross-source queries through the approved HTTP boundary."""
from datetime import datetime
from hashlib import sha256
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from api.main import create_app
from api.schemas import AnomalyQuery
from api.settings import ApiClient, ApiSettings

KEY = 'synthetic-query-options-not-a-real-key'
HEADERS = {'X-API-Key': KEY}
URL = '/api/v1/sheet-anomalies'
NOW = datetime(2026, 11, 2, 10, tzinfo=ZoneInfo('Asia/Shanghai'))
QUERY_FIELDS = tuple(AnomalyQuery.model_fields)
BLANK_VALUES = ('', '   ', '\t\r\n', '\u3000\u00a0')


def record(query, identifier='S1'):
    row = dict(分类=query.sub_domain, 产品型号=query.product, 部门='ARRAY', 发生站点='00100',
                层别='PI2', 监控参数='THK', 异常事件=query.event_type, 对象类型='sheet',
                对象编号=identifier, 发生时间='2026-10-28T09:30:00+08:00')
    if query.event_type in ('CPK', 'CPM'):
        row.update(对象类型='capability', 对象编号=[identifier], 监控参数=f'THK_{identifier}',
                   发生时间='2026-11-01', 指标值=1.2, 达标下限=1.33,
                   统计周期类型='week', 统计周期='2026-W44',
                   统计开始日期='2026-10-26', 统计截止日期='2026-11-01')
    return row


@pytest.fixture
def environment():
    state = SimpleNamespace(enabled=['P1', 'P2', 'P3'], calls=[], failures=set(), empty_sources=set(),
                            malformed_sources=set(), count=1)
    principal = ApiClient('query-options', sha256(KEY.encode()).hexdigest(),
                          ('P1', 'P2', 'DISABLED'), ('ARRAY',), ('SPC', 'CTQ'))

    class Source:
        def read(self, query, factories, reference_date):
            state.calls.append((query, factories, reference_date))
            if (query.product, query.sub_domain) in state.failures:
                raise ValueError('private-path/internal decision')
            if (query.product, query.sub_domain) in state.empty_sources:
                return []
            rows = [record(query, f'S{index:04d}') for index in range(state.count)]
            if (query.product, query.sub_domain) in state.malformed_sources:
                rows.append({'private_source_column': 'must not be returned'})
            return rows

    app = create_app(service=Source(), clients_provider=lambda: (principal,),
                     enabled_products=lambda: state.enabled, clock=lambda: NOW)
    with TestClient(app) as client:
        yield client, state


def test_no_parameters_queries_authorized_enabled_scopes_without_row_truncation(environment):
    client, state = environment
    state.count = 30
    response = client.get(URL, headers=HEADERS)
    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body['data']) == 360
    assert {(q.product, q.sub_domain) for q, _, _ in state.calls} == {
        ('P1', 'SPC'), ('P1', 'CTQ'), ('P2', 'SPC'), ('P2', 'CTQ'),
    }
    assert {(q.sub_domain, q.event_type) for q, _, _ in state.calls} == {
        ('SPC', 'OOS'), ('SPC', 'OOC'), ('SPC', 'CPK'), ('SPC', 'CPM'), ('CTQ', 'OOS'), ('CTQ', 'OOC')}
    assert all(factories == ('ARRAY',) for _, factories, _ in state.calls)
    assert body['meta']['has_more'] is False and body['meta']['next_cursor'] is None
    assert body['meta']['period_start'] == '2026-10-26T00:00:00+08:00'
    assert all('起始站点' not in row for row in body['data'])


def test_partial_sources_are_explicit_and_never_leak_failure_details(environment):
    client, state = environment
    state.failures = {('P1', 'CTQ')}
    response = client.get(URL, headers=HEADERS, params={'event_type': 'OOS'})
    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body['data']) == 3
    assert body['meta']['complete'] is False
    assert body['meta']['failed_sources'] == [dict(product='P1', sub_domain='CTQ',
                                                  event_type='OOS', code='result_unavailable')]
    assert 'private-path' not in response.text and 'decision' not in response.text
    state.count = 0
    empty = client.get(URL, headers=HEADERS, params={'event_type': 'OOS'}).json()
    assert empty['data'] == [] and empty['meta']['complete'] is False
    state.failures = {(p, d) for p in ('P1', 'P2') for d in ('SPC', 'CTQ')}
    unavailable = client.get(URL, headers=HEADERS, params={'event_type': 'OOS'})
    assert unavailable.status_code == 503
    assert unavailable.json()['error']['code'] == 'result_unavailable'
    assert len(unavailable.json()['error']['failed_sources']) == 4


def test_invalid_source_discards_its_entire_prefix(environment):
    client, state = environment
    state.malformed_sources = {('P1', 'CTQ')}
    response = client.get(URL, headers=HEADERS, params={'event_type': 'OOS'})
    assert response.status_code == 200
    body = response.json()
    assert len(body['data']) == 3 and body['meta']['complete'] is False
    assert not any(row['产品型号'] == 'P1' and row['分类'] == 'CTQ' for row in body['data'])
    assert 'private_source_column' not in response.text


def test_global_cursor_visits_each_source_once_and_binds_effective_scope(environment):
    client, state = environment
    seen, cursor = [], None
    while True:
        params = {'limit': 1, **({'cursor': cursor} if cursor else {})}
        response = client.get(URL, params={'event_type': 'OOS', **params}, headers=HEADERS)
        assert response.status_code == 200, response.text
        body = response.json()
        seen.extend((row['产品型号'], row['分类']) for row in body['data'])
        cursor = body['meta']['next_cursor']
        if not cursor:
            break
    assert seen == [('P2', 'SPC'), ('P2', 'CTQ'), ('P1', 'SPC'), ('P1', 'CTQ')]
    state.empty_sources = {('P2', 'SPC'), ('P2', 'CTQ')}
    first = client.get(URL, params={'event_type': 'OOS', 'limit': 1}, headers=HEADERS).json()
    state.enabled = ['P1']  # Public rows unchanged; effective authorized scope changed.
    changed = client.get(URL, params={'event_type': 'OOS', 'limit': 1, 'cursor': first['meta']['next_cursor']}, headers=HEADERS)
    assert changed.status_code == 410


def test_cursor_changes_when_an_empty_available_source_fails(environment):
    client, state = environment
    state.empty_sources = {('P1', 'CTQ')}
    first = client.get(URL, params={'event_type': 'OOS', 'limit': 1}, headers=HEADERS).json()
    assert first['meta']['complete'] is True
    state.failures = {('P1', 'CTQ')}
    response = client.get(URL, params={'event_type': 'OOS', 'limit': 1, 'cursor': first['meta']['next_cursor']}, headers=HEADERS)
    assert response.status_code == 410


@pytest.mark.parametrize('name', QUERY_FIELDS)
@pytest.mark.parametrize('blank', BLANK_VALUES)
def test_each_blank_query_parameter_is_equivalent_to_omission(environment, name, blank):
    client, state = environment
    expected = client.get(URL, headers=HEADERS)
    expected_calls = list(state.calls)
    state.calls.clear()
    response = client.get(URL, params={name: blank}, headers=HEADERS)
    assert response.status_code == 200, response.text
    assert response.json() == expected.json()
    assert state.calls == expected_calls


@pytest.mark.parametrize('blank', BLANK_VALUES)
def test_all_blank_query_parameters_use_the_complete_default_scope(environment, blank):
    client, state = environment
    expected = client.get(URL, headers=HEADERS)
    expected_calls = list(state.calls)
    state.calls.clear()
    response = client.get(URL, params=dict.fromkeys(QUERY_FIELDS, blank), headers=HEADERS)
    assert response.status_code == 200, response.text
    assert response.json() == expected.json()
    assert state.calls == expected_calls


def test_blank_filters_do_not_change_cursor_binding(environment):
    client, _ = environment
    params = {'event_type': 'OOS', 'limit': 1}
    first = client.get(URL, params={**params, 'cursor': ' '}, headers=HEADERS)
    assert first.status_code == 200, first.text
    first = first.json()
    blank_filters = dict.fromkeys(QUERY_FIELDS, ' ')
    second = client.get(URL, params={**blank_filters, **params, 'cursor': first['meta']['next_cursor']},
                        headers=HEADERS)
    assert second.status_code == 200, second.text
    assert second.json()['data'] != first['data']


def test_blank_limit_does_not_allow_a_cursor_without_pagination(environment):
    client, state = environment
    response = client.get(URL, params={'limit': ' ', 'cursor': 'nonempty-cursor'}, headers=HEADERS)
    assert response.status_code == 422
    assert state.calls == []


def test_blank_unknown_parameters_are_still_rejected(environment):
    client, state = environment
    response = client.get(URL, params={'param_name': ''}, headers=HEADERS)
    assert response.status_code == 422
    assert state.calls == []


def test_nonblank_text_is_not_trimmed(environment):
    client, state = environment
    response = client.get(URL, params={'parameter_name': ' THK '}, headers=HEADERS)
    assert response.status_code == 200
    assert all(query.parameter_name == ' THK ' for query, _, _ in state.calls)


@pytest.mark.parametrize('params', [
    {'start_date': '2026-02-30'}, {'end_date': '2026-11-01T12:00:00'},
    {'start_date': '1793000000'}, {'start_date': '2026-11-02'},
    {'start_date': '2026-11-01', 'end_date': '2026-10-31'},
    {'cursor': 'synthetic-token-without-limit'}, {'end_date': '9999-12-31'},
    {'step_id': 'S' * 129}, {'layer': 'L' * 257}, {'parameter_name': 'P' * 513},
])
def test_invalid_dates_and_filters_fail_before_any_read(environment, params):
    client, state = environment
    response = client.get(URL, params={'event_type': 'OOS', **params}, headers=HEADERS)
    assert response.status_code == 422
    assert state.calls == []


@pytest.mark.parametrize('params,start,end', [
    ({'start_date': '2026-10-25'}, '2026-10-25', '2026-11-01'),
    ({'end_date': '2026-10-28'}, '2026-10-26', '2026-10-28'),
    ({'start_date': '2026-10-28', 'end_date': '2026-10-28'}, '2026-10-28', '2026-10-28'),
])
def test_date_defaults_resolve_independently_and_reach_each_source(environment, params, start, end):
    client, state = environment
    response = client.get(URL, params={'event_type': 'OOS', **params}, headers=HEADERS)
    assert response.status_code == 200
    assert all(str(q.start_date) == start and str(q.end_date) == end for q, _, _ in state.calls)
    assert response.json()['meta']['period_start'] == start + 'T00:00:00+08:00'


def test_openapi_has_no_required_query_parameters_or_duplicate_station(environment):
    client, _ = environment
    document = client.get('/openapi.json', headers=HEADERS).json()
    parameters = document['paths'][URL]['get']['parameters']
    assert all(not p.get('required', False) for p in parameters if p['in'] == 'query')
    assert '起始站点' not in document['components']['schemas']['AnomalyRecord']['properties']


@pytest.mark.parametrize('params', [{}, {'limit': 1}])
def test_public_byte_budget_remains_enforced_with_and_without_pagination(params):
    principal = ApiClient('bounded', sha256(KEY.encode()).hexdigest(), ('P1',), ('ARRAY',), ('SPC',))

    class Source:
        def read(self, query, *args):
            return [record(query, identifier) for identifier in ('S1', 'S2', 'S3')]

    app = create_app(service=Source(), clients_provider=lambda: (principal,), enabled_products=lambda: ['P1'],
                     clock=lambda: NOW, settings=ApiSettings(max_snapshot_bytes=500))
    with TestClient(app) as client:
        response = client.get(URL, headers=HEADERS, params=params)
    assert response.status_code == 503
    assert response.json()['error']['code'] == 'result_unavailable'
