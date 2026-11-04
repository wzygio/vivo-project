"""Omitted event selection expands only to supported, authorized events."""
from datetime import date
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from api.query import resolve_query
from api.schemas import AnomalyQuery
from api.settings import ApiClient
from tests.integration.test_anomaly_api_capability import capability_files
from tests.integration.test_anomaly_api_files import HEADERS, published_files
from tests.e2e.test_anomaly_api_sheet_membership_e2e import _live_server
from tools.fetch_sheet_anomalies import fetch_anomalies


EVENTS = {
    'SPC': ('OOS', 'OOC', 'CPK', 'CPM'),
    'CTQ': ('OOS', 'OOC'), 'AOI_TT': ('OOS', 'OOC'), 'AOI_RS': ('OOS', 'OOC'),
    'Yield': ('OOS',), 'Q-Time': ('OOS',),
}


@pytest.mark.parametrize('domains', [tuple(EVENTS), ('CTQ',), ('Yield', 'Q-Time')])
def test_default_plan_expands_supported_events_without_requiring_spc_permission(domains):
    principal = ApiClient('default-events', sha256(b'synthetic').hexdigest(), ('P1',), ('ARRAY',), domains)
    plan = resolve_query(AnomalyQuery(), principal, ['P1'], date(2026, 11, 4))
    assert [(q.sub_domain, q.event_type) for q in plan.queries] == [
        (domain, event) for domain in domains for event in EVENTS[domain]]
    assert all(q.product == 'P1' for q in plan.queries)


def test_spc_omitted_event_returns_all_four_real_publications(capability_files):
    with TestClient(capability_files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                              params={'product': 'P1', 'sub_domain': 'SPC'})
        explicit = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                              params={'product': 'P1', 'sub_domain': 'SPC', 'event_type': 'OOS'})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body['meta']['complete'] is True
    assert {row['异常事件'] for row in body['data']} == set(EVENTS['SPC'])
    assert len(body['data']) == 4
    assert explicit.status_code == 200
    assert [row['异常事件'] for row in explicit.json()['data']] == ['OOS']


@pytest.mark.parametrize('domain', ['CTQ', 'Yield', 'Q-Time'])
def test_omitted_event_is_valid_for_other_domains(capability_files, domain):
    with TestClient(capability_files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                              params={'sub_domain': domain})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body['meta']['complete'] is True
    assert {row['异常事件'] for row in body['data']} <= set(EVENTS[domain])


def test_partial_failure_names_specific_event_and_preserves_other_events(capability_files):
    files = capability_files
    files.paths[('inline_domain', 'spc_sheet_ooc_decoration')].unlink()
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'sub_domain': 'SPC'})
    assert response.status_code == 200, response.text
    body = response.json()
    assert {row['异常事件'] for row in body['data']} == {'OOS', 'CPK', 'CPM'}
    assert body['meta']['failed_sources'] == [dict(product='P1', sub_domain='SPC', event_type='OOC', code='result_unavailable')]
    assert body['meta']['complete'] is False


def test_openapi_documents_default_all_and_keeps_authentication_required(capability_files):
    with TestClient(capability_files.app()) as client:
        assert client.get('/api/v1/sheet-anomalies').status_code == 401
        assert client.get('/api/v1/sheet-anomalies', headers={'X-API-Key': 'wrong-fixture-key'}).status_code == 401
        schema = client.get('/openapi.json', headers=HEADERS).json()
    assert schema['info']['version'] == '1.4.0'
    parameter = next(item for item in schema['paths']['/api/v1/sheet-anomalies']['get']['parameters']
                     if item['name'] == 'event_type')
    assert parameter['required'] is False and parameter['schema'].get('default') is None
    assert '全部' in parameter['description']


def test_real_network_default_events_pages_all_four_kinds(capability_files):
    from tests.integration.test_anomaly_api_files import FIXTURE_KEY
    record = Path(__file__).resolve().parents[2] / 'output/test-results/api-default-events' / f'listener-{uuid4().hex}.json'
    with _live_server(capability_files.app(), record) as base:
        all_rows = fetch_anomalies(base, FIXTURE_KEY, params={'sub_domain': 'SPC'})
        paged = fetch_anomalies(base, FIXTURE_KEY, params={'sub_domain': 'SPC'}, page_size=1)
    assert all_rows['data'] == paged['data']
    assert {row['异常事件'] for row in paged['data']} == set(EVENTS['SPC'])
    assert paged['client']['pages_fetched'] == 4 and paged['meta']['complete'] is True
