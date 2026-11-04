"""Capability membership is complete, scoped and part of the public snapshot."""
from datetime import date, datetime
from hashlib import sha256
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pandas as pd
import pytest

from api.main import create_app
from api.schemas import AnomalyQuery
from api.service import PublishedAnomalyService, ResultUnavailable
from api.settings import ApiClient, ApiSettings


DAY = date(2026, 11, 4)
NOW = datetime(2026, 11, 4, 10, tzinfo=ZoneInfo('Asia/Shanghai'))
KEY = 'capability-members-fixture-not-a-production-key'
HEADERS = {'X-API-Key': KEY}
URL = '/api/v1/sheet-anomalies'


def capability_row(**changes):
    return dict(factory='ARRAY', step_id='0100', param_name='THK', flag=False,
                period_type='week', period_label='2026-W44',
                calendar_start='2026-10-26', calendar_end='2026-11-01',
                period_start='2026-10-27 10:00:00', period_end='2026-10-29 10:00:00',
                cpk=1.2, cpm=1.1) | changes


def service(rows, membership, *, hidden=False):
    return PublishedAnomalyService(
        source_reader=lambda *_: {'records': pd.DataFrame(rows)},
        hidden=lambda *_: False, capability_hidden=lambda *_: hidden,
        metadata=SimpleNamespace(enrich=lambda domain, product, identity, parameter, factory, step:
            dict(factory=factory, step_id=step, layer='PI2')),
        sheet_membership=membership,
    )


def app(source, **settings):
    principal = ApiClient('fixture-members', sha256(KEY.encode()).hexdigest(),
                          ('P1',), ('ARRAY',), ('SPC',))
    return create_app(service=source, clients_provider=lambda: (principal,),
                      enabled_products=lambda: ['P1'], clock=lambda: NOW,
                      settings=ApiSettings(**settings))


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
def test_member_filter_returns_complete_sorted_project_and_preserves_leading_zeroes(event):
    query = AnomalyQuery(product='P1', sub_domain='SPC', event_type=event, entity_id='00002')
    calls = []

    def members(actual_query, row):
        calls.append((actual_query, row))
        return ['00002', '00001', '00002']

    rows = service([capability_row()], members).read(query, ('ARRAY',), DAY)
    assert len(rows) == 1 and rows[0]['对象类型'] == 'capability'
    assert rows[0]['对象编号'] == ['00001', '00002']
    assert rows[0]['命中边界'] == 'spec'
    assert calls[0][0] == query and calls[0][1]['period_label'] == '2026-W44'


def test_unknown_member_returns_no_project_instead_of_changing_the_membership():
    rows = service([capability_row()], lambda *_: ['S1', 'S2']).read(
        AnomalyQuery(product='P1', sub_domain='SPC', event_type='CPK', entity_id='S3'),
        ('ARRAY',), DAY)
    assert rows == []


@pytest.mark.parametrize('query_changes', [
    {'entity_type': 'sheet'}, {'step_id': 'OTHER'}, {'parameter_name': 'OTHER'}, {'layer': 'OTHER'},
])
def test_unrequested_project_does_not_read_members(query_changes):
    def forbidden(*_):
        pytest.fail('Unrequested membership was accessed')

    rows = service([capability_row()], forbidden).read(
        AnomalyQuery(product='P1', sub_domain='SPC', event_type='CPK', **query_changes),
        ('ARRAY',), DAY)
    assert rows == []


def test_hidden_or_unauthorized_project_never_accesses_members():
    def forbidden(*_):
        pytest.fail('Private membership was accessed')

    query = AnomalyQuery(product='P1', sub_domain='SPC', event_type='CPK')
    assert service([capability_row()], forbidden, hidden=True).read(query, ('ARRAY',), DAY) == []
    assert service([capability_row(factory='OLED')], forbidden).read(query, ('ARRAY',), DAY) == []


@pytest.mark.parametrize('members', [None, [], [''], [' '], [1], [None], 'S1'])
def test_missing_or_invalid_members_are_unavailable(members):
    with pytest.raises(ValueError):
        service([capability_row()], lambda *_: members).read(
            AnomalyQuery(product='P1', sub_domain='SPC', event_type='CPK'), ('ARRAY',), DAY)


def test_no_membership_dependency_cannot_return_a_synthetic_project_number():
    with pytest.raises(ResultUnavailable, match='membership'):
        service([capability_row()], None).read(
            AnomalyQuery(product='P1', sub_domain='SPC', event_type='CPK'), ('ARRAY',), DAY)


def test_membership_failure_discards_the_entire_source_and_hides_details():
    def members(query, row):
        if row['param_name'] == 'BROKEN':
            raise ValueError('private-membership-path')
        return ['S1']

    with TestClient(app(service([capability_row(), capability_row(param_name='BROKEN')], members))) as client:
        response = client.get(URL, headers=HEADERS, params={'event_type': 'CPK'})
    assert response.status_code == 503
    assert 'S1' not in response.text and 'private-membership-path' not in response.text


def test_pagination_counts_projects_and_member_change_invalidates_cursor():
    membership = {'THK': ['S1', 'S2'], 'SECOND': ['00001', '00002', '00003']}
    source = service([capability_row(), capability_row(param_name='SECOND')],
                     lambda query, row: membership[row['param_name']])
    with TestClient(app(source)) as client:
        params = {'event_type': 'CPK', 'limit': 1}
        first = client.get(URL, headers=HEADERS, params=params).json()
        assert first['meta']['returned_count'] == 1 and first['meta']['has_more'] is True
        assert first['data'][0]['对象编号'] == ['S1', 'S2']
        next_params = {**params, 'cursor': first['meta']['next_cursor']}
        second = client.get(URL, headers=HEADERS, params=next_params)
        assert second.status_code == 200
        assert second.json()['data'][0]['对象编号'] == ['00001', '00002', '00003']
        membership['SECOND'].append('00004')
        changed = client.get(URL, headers=HEADERS, params=next_params)
        assert changed.status_code == 410


def test_member_source_order_does_not_change_a_public_snapshot():
    members = ['00002', '00001']
    source = service([capability_row(), capability_row(param_name='SECOND')], lambda *_: members)
    with TestClient(app(source)) as client:
        params = {'event_type': 'CPM', 'limit': 1}
        first = client.get(URL, headers=HEADERS, params=params).json()
        members.reverse()
        members.append('00001')
        second = client.get(URL, headers=HEADERS,
                            params={**params, 'cursor': first['meta']['next_cursor']})
        assert second.status_code == 200
        assert second.json()['data'][0]['对象编号'] == ['00001', '00002']


@pytest.mark.parametrize('limit', [None, 1])
def test_member_lists_are_never_truncated_to_fit_the_byte_budget(limit):
    source = service([capability_row()], lambda *_: [f'S{n:06d}' for n in range(100)])
    with TestClient(app(source, max_snapshot_bytes=600)) as client:
        response = client.get(URL, headers=HEADERS,
                              params={'event_type': 'CPK', **({'limit': limit} if limit else {})})
    assert response.status_code == 503
    assert response.json()['error']['code'] == 'result_unavailable'


def test_openapi_distinguishes_capability_member_lists_and_default_spec_boundary():
    with TestClient(app(service([capability_row()], lambda *_: ['00001']))) as client:
        document = client.get('/openapi.json', headers=HEADERS).json()
    schemas = document['components']['schemas']
    capability = schemas['CapabilityRecord']['properties']
    anomaly = schemas['AnomalyRecord']['properties']
    assert document['info']['version'] == '1.4.0'
    assert capability['对象编号']['type'] == 'array'
    assert capability['对象编号']['items']['type'] == 'string'
    assert anomaly['对象编号']['type'] == 'string'
    assert capability['命中边界']['type'] == 'string' and capability['命中边界']['default'] == 'spec'
