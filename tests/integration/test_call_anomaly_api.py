"""Query-vs-header behavior through the real API adapter, without a listener."""
from datetime import datetime
from hashlib import sha256
from io import BytesIO
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest

from api.main import create_app
from api.settings import ApiClient
from tools import call_anomaly_api


KEY = 'synthetic-domain-query-check'
URL = '/api/v1/sheet-anomalies'


@pytest.fixture
def api_client():
    calls = []
    principal = ApiClient('domain-query-check', sha256(KEY.encode()).hexdigest(),
                          ('M626',), ('ARRAY',), ('SPC', 'AOI_TT'))

    class PublishedResults:
        def read(self, query, factories, reference_date):
            calls.append(query)
            if query.event_type in ('CPK', 'CPM'):
                return []
            return [{
                '分类': query.sub_domain, '发生站点': '00100', '部门': 'ARRAY',
                '层别': 'PI2', '监控参数': 'test-parameter', '异常事件': query.event_type,
                '对象类型': 'sheet', '对象编号': '00001', '产品型号': query.product,
                '发生时间': '2026-10-28T09:30:00+08:00', '命中边界': 'USL',
            }]

    app = create_app(service=PublishedResults(), clients_provider=lambda: (principal,),
                     enabled_products=lambda: ['M626'], clock=lambda: datetime(
                         2026, 11, 2, 10, tzinfo=ZoneInfo('Asia/Shanghai')))
    with TestClient(app) as http:
        yield http, calls


def test_header_only_selection_reproduces_screenshot_and_does_not_filter(api_client):
    http, calls = api_client
    response = http.get(URL, headers={'X-API-Key': KEY, 'sub_domain': 'AOI_TT', 'event_type': ''})
    assert response.status_code == 200
    assert {row['分类'] for row in response.json()['data']} == {'SPC', 'AOI_TT'}
    assert {query.sub_domain for query in calls} == {'SPC', 'AOI_TT'}


@pytest.mark.parametrize('headers', [
    {'X-API-Key': KEY}, {'X-API-Key': KEY, 'sub_domain': 'SPC'},
])
def test_query_selection_only_returns_aoi_tt_even_with_conflicting_header(api_client, headers):
    http, calls = api_client
    response = http.get(URL, headers=headers, params={'sub_domain': 'AOI_TT'})
    assert response.status_code == 200
    assert {row['分类'] for row in response.json()['data']} == {'AOI_TT'}
    assert {query.sub_domain for query in calls} == {'AOI_TT'}
    assert {query.event_type for query in calls} == {'OOS', 'OOC'}


def test_direct_python_caller_only_returns_selected_domain(api_client, monkeypatch):
    http, calls = api_client

    def open_request(request, timeout):
        response = http.get(request.full_url, headers=dict(request.header_items()))
        assert response.status_code == 200
        return BytesIO(response.content)

    monkeypatch.setattr(call_anomaly_api, 'urlopen', open_request)
    result = call_anomaly_api.main(api_key=KEY, params={'sub_domain': 'AOI_TT'})
    assert result['meta']['complete'] is True
    assert {row['分类'] for row in result['data']} == {'AOI_TT'}
    assert {query.sub_domain for query in calls} == {'AOI_TT'}
