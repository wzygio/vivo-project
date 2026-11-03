"""Retry one whole source only for an explicitly changed workbook version."""
from collections import Counter
from datetime import datetime
from hashlib import sha256
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from api.main import create_app
from api.service import PublishedAnomalyService, ResultUnavailable
from api.settings import ApiClient
from src.shared_kernel.infrastructure.api_workbook_cache import (
    WorkbookLimitError, WorkbookReadError, WorkbookVersionError,
)


KEY = 'workbook-retry-fixture-not-production'
HEADERS = {'X-API-Key': KEY}
URL = '/api/v1/sheet-anomalies'
NOW = datetime(2026, 11, 3, 10, tzinfo=ZoneInfo('Asia/Shanghai'))


def _record(query):
    row = dict(分类='SPC', 产品型号=query.product, 部门='ARRAY', 发生站点='00100', 层别='PI2',
               监控参数='THK', 异常事件=query.event_type, 对象类型='sheet', 对象编号='FRESH',
               发生时间='2026-10-28T09:30:00+08:00', 命中边界='USL')
    if query.event_type == 'CPK':
        row.update(对象类型='capability', 对象编号='capability:' + 'a' * 64,
                   发生时间='2026-11-01', 命中边界='LOWER', 指标值=1.2, 达标下限=1.33,
                   统计周期类型='week', 统计周期='2026-W44',
                   统计开始日期='2026-10-26', 统计截止日期='2026-11-01')
    return row


def _app(service, products=('P1',)):
    principal = ApiClient('fixture-retry', sha256(KEY.encode()).hexdigest(), products, ('ARRAY',), ('SPC',))
    return create_app(service=service, clients_provider=lambda: (principal,),
                      enabled_products=lambda: list(products), clock=lambda: NOW)


@pytest.mark.parametrize('event', ['OOS', 'CPK'])
def test_one_version_change_retries_entire_source_and_returns_only_fresh_rows(event):
    calls = []

    def read(query, factories, reference):
        calls.append((query, factories, reference))
        if len(calls) == 1:
            raise WorkbookVersionError('private workbook changed')
        return [_record(query)]

    with TestClient(_app(SimpleNamespace(read=read))) as client:
        response = client.get(URL, headers=HEADERS, params={'event_type': event})
    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body['data']) == 1 and body['meta']['complete'] is True
    assert body['meta']['failed_sources'] == []
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[0][0].start_date.isoformat() == '2026-10-26'
    assert 'private workbook' not in response.text


def test_retry_budget_is_one_per_source_instead_of_one_per_request():
    calls = Counter()

    def read(query, *args):
        calls[query.product] += 1
        if calls[query.product] == 1:
            raise WorkbookVersionError('changed')
        return [_record(query)]

    with TestClient(_app(SimpleNamespace(read=read), ('P1', 'P2'))) as client:
        response = client.get(URL, headers=HEADERS, params={'event_type': 'CPK'})
    assert response.status_code == 200 and response.json()['meta']['complete'] is True
    assert {row['产品型号'] for row in response.json()['data']} == {'P1', 'P2'}
    assert calls == {'P1': 2, 'P2': 2}


@pytest.mark.parametrize('products,status', [(('P1', 'P2'), 200), (('P1',), 503)])
def test_two_consecutive_version_changes_remain_explicitly_unavailable(products, status):
    calls = Counter()

    def read(query, *args):
        calls[query.product] += 1
        if query.product == 'P1':
            raise WorkbookVersionError('private changed version')
        return [_record(query)]

    with TestClient(_app(SimpleNamespace(read=read), products)) as client:
        response = client.get(URL, headers=HEADERS, params={'event_type': 'CPK'})
    assert response.status_code == status
    body = response.json()
    failures = body['meta']['failed_sources'] if status == 200 else body['error']['failed_sources']
    assert failures == [dict(product='P1', sub_domain='SPC', event_type='CPK', code='result_unavailable')]
    assert calls['P1'] == 2
    if status == 200:
        assert body['meta']['complete'] is False
        assert [row['产品型号'] for row in body['data']] == ['P2'] and calls['P2'] == 1
    assert 'private changed version' not in response.text


@pytest.mark.parametrize('error', [
    ResultUnavailable, FileNotFoundError, OSError, ValueError, KeyError, TypeError,
    WorkbookReadError, WorkbookLimitError,
])
def test_ordinary_missing_or_invalid_publications_are_not_retried(error):
    calls = []

    def read(*args):
        calls.append(args)
        raise error('private unavailable source')

    with TestClient(_app(SimpleNamespace(read=read))) as client:
        response = client.get(URL, headers=HEADERS, params={'event_type': 'CPK'})
    assert response.status_code == 503 and len(calls) == 1
    assert 'private unavailable source' not in response.text


def test_retry_rechecks_public_policy_before_accessing_a_new_source_version():
    calls = {'policy': 0, 'source': 0}
    hidden = False

    def policy(*args):
        calls['policy'] += 1
        return hidden

    def source(*args):
        nonlocal hidden
        calls['source'] += 1
        hidden = True  # Operator suppresses the source while the old version changes.
        raise WorkbookVersionError('private changed workbook')

    def metadata(*args):
        pytest.fail('A newly hidden source must not access station metadata')

    service = PublishedAnomalyService(source_reader=source, hidden=policy,
        capability_hidden=policy, metadata=SimpleNamespace(enrich=metadata))
    with TestClient(_app(service)) as client:
        response = client.get(URL, headers=HEADERS, params={'event_type': 'CPK'})
    assert response.status_code == 200, response.text
    assert response.json()['data'] == [] and response.json()['meta']['complete'] is True
    assert response.json()['meta']['failed_sources'] == []
    assert calls == {'policy': 2, 'source': 1}
    assert 'private changed workbook' not in response.text
