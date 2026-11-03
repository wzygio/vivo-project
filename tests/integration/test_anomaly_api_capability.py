"""Capability workbook → authenticated HTTP without COM, database or listeners."""
import hashlib

from fastapi.testclient import TestClient
import pandas as pd
import pytest

from api.main import create_app
from api.settings import ApiClient
from src.shared_kernel.config import ConfigLoader
from tests.integration.test_anomaly_api_files import (
    FIXTURE_KEY, HEADERS, NOW, _write, published_files,
)


CAPABILITY_NAMES = {'CPK': 'SPC 趋势波动（CPK）', 'CPM': 'SPC 趋势波动（CPM）'}
BASE_FIELDS = {'分类', '发生站点', '部门', '层别', '监控参数', '异常事件',
               '对象类型', '对象编号', '产品型号', '发生时间', '命中边界'}
CAPABILITY_FIELDS = {'指标值', '达标下限', '统计周期类型', '统计周期', '统计开始日期', '统计截止日期'}


def _capability_rows(metric, *, parameter='PPA_X'):
    base = dict(prod_code='P1', factory='ARRAY', step_id='0100', param_name=parameter,
                period_type='week', period_label='2026-W44', period_sort=201,
                period_start='2026-10-27 10:00:00', period_end='2026-10-29 10:00:00',
                flag=False, **{f'{metric}_corrected': 1.2, f'{metric}_replacement': 99.0})
    return pd.DataFrame([
        base,
        {**base, 'param_name': 'DENIED', 'flag': True, f'{metric}_corrected': .8},
        {**base, 'param_name': 'AT_THRESHOLD', f'{metric}_corrected': 1.33},
        {**base, 'param_name': 'OLD_WEEK', 'period_label': '2026-W43',
         'period_start': '2026-10-20 10:00:00', 'period_end': '2026-10-22 10:00:00'},
    ])


@pytest.fixture
def capability_files(published_files, monkeypatch):
    files = published_files
    path = files.metadata_path.parent / 'capability.xlsx'
    files.paths[('inline_domain', 'spc_cpk_cpm_decoration')] = path
    _write(path, {'P1': _capability_rows('cpk'), 'P1_cpm': _capability_rows('cpm')})
    files.policy = pd.concat([files.policy, pd.DataFrame({'监控参数': list(CAPABILITY_NAMES.values()), 'P1': False})], ignore_index=True)
    _write(files.policy_path, {'全指标预警看板': files.policy})
    monkeypatch.setattr(ConfigLoader, 'get_spc_capability_alert_min_span_hours', lambda: 0)
    return files


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
def test_capability_workbook_reaches_http_with_full_public_identity(capability_files, event):
    files = capability_files
    before = {path: path.read_bytes() for path in (*files.paths.values(), files.policy_path) if path.exists()}
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': event})
    assert response.status_code == 200, response.text
    row, = response.json()['data']
    assert set(row) == BASE_FIELDS | CAPABILITY_FIELDS
    assert {key: row[key] for key in ('分类', '发生站点', '部门', '层别', '监控参数', '异常事件')} == {
        '分类': 'SPC', '发生站点': '0100', '部门': 'ARRAY', '层别': 'PI2', '监控参数': 'PPA_X', '异常事件': event}
    assert row['对象类型'] == 'capability' and row['对象编号'].startswith('capability:')
    assert row['指标值'] == 1.2 and row['达标下限'] == 1.33
    assert row['统计周期类型'] == 'week' and row['统计周期'] == '2026-W44'
    assert row['统计开始日期'] == '2026-10-26' and row['统计截止日期'] == '2026-11-01'
    assert row['发生时间'] == '2026-11-01' and row['命中边界'] == 'LOWER'
    assert response.json()['meta']['complete'] is True
    assert all(path.read_bytes() == data for path, data in before.items())


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
@pytest.mark.parametrize('domain', ['CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time'])
def test_capability_event_rejects_non_spc_sources(capability_files, event, domain):
    with TestClient(capability_files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                              params={'sub_domain': domain, 'event_type': event})
    assert response.status_code == 422


def test_capability_event_without_spc_authorization_is_forbidden(capability_files):
    principal = ApiClient('fixture', hashlib.sha256(FIXTURE_KEY.encode()).hexdigest(), ('P1',), ('ARRAY',), ('CTQ',))
    app = create_app(clients_provider=lambda: (principal,), clock=lambda: NOW)
    with TestClient(app) as client:
        for params in ({'event_type': 'CPK'}, {'event_type': 'CPM', 'sub_domain': 'SPC'}):
            response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
            assert response.status_code == 403


def test_capability_policy_is_independent_and_precedes_source_access(capability_files):
    files = capability_files
    policy = files.policy.copy()
    policy.loc[policy['监控参数'].eq('SPC 单片异常'), 'P1'] = True
    policy.loc[policy['监控参数'].eq(CAPABILITY_NAMES['CPK']), 'P1'] = True
    _write(files.policy_path, {'全指标预警看板': policy})
    with TestClient(files.app()) as client:
        cpm = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPM'})
        assert cpm.status_code == 200 and len(cpm.json()['data']) == 1
        files.paths[('inline_domain', 'spc_cpk_cpm_decoration')].unlink()
        files.metadata_path.unlink()
        cpk = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'})
        assert cpk.status_code == 200 and cpk.json()['data'] == []
        assert cpk.json()['meta']['complete'] is True


@pytest.mark.parametrize('policy_value', ['missing', 'invalid'])
def test_capability_policy_never_defaults_to_public(capability_files, policy_value):
    files = capability_files
    policy = files.policy.astype({'P1': object}).copy()
    if policy_value == 'missing':
        policy = policy.loc[policy['监控参数'].ne(CAPABILITY_NAMES['CPM'])]
    else:
        policy.loc[policy['监控参数'].eq(CAPABILITY_NAMES['CPM']), 'P1'] = 'invalid'
    _write(files.policy_path, {'全指标预警看板': policy})
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPM'})
    assert response.status_code == 503 and 'PPA_X' not in response.text
    assert 'capability.xlsx' not in response.text


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
def test_capability_dates_select_complete_iso_weeks_and_fields_filter(capability_files, event):
    with TestClient(capability_files.app()) as client:
        params = dict(event_type=event, start_date='2026-10-19', end_date='2026-10-25')
        old = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
        assert old.status_code == 200, old.text
        row, = old.json()['data']
        assert row['统计周期'] == '2026-W43' and row['监控参数'] == 'OLD_WEEK'
        partial = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
            params={**params, 'start_date': '2026-10-20'})
        assert partial.status_code == 200 and partial.json()['data'] == []
        for extra in ({'step_id': '100'}, {'layer': 'OTHER'}, {'parameter_name': 'unmatched'}, {'entity_type': 'sheet'}):
            omitted = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                params={'event_type': event, **extra})
            assert omitted.status_code == 200 and omitted.json()['data'] == []
        by_id = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
            params={**params, 'entity_type': 'capability', 'entity_id': row['对象编号']})
        assert by_id.status_code == 200 and by_id.json()['data'] == [row]


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
def test_capability_missing_station_dictionary_is_unavailable(capability_files, event):
    files = capability_files
    _write(files.metadata_path, {'站点字典': pd.DataFrame([dict(step_id='UNMATCHED', description='PI2_OTHER')])})
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': event})
    assert response.status_code == 503
    assert response.json()['error']['failed_sources'] == [dict(product='P1', sub_domain='SPC', event_type=event, code='result_unavailable')]
    assert 'PPA_X' not in response.text


def test_missing_capability_product_retains_successful_product_rows(capability_files, monkeypatch):
    files = capability_files
    policy = files.policy.assign(P2=False)
    _write(files.policy_path, {'全指标预警看板': policy})
    monkeypatch.setattr(ConfigLoader, 'get_enabled_products', lambda **_: ['P1', 'P2'])
    principal = ApiClient('fixture', hashlib.sha256(FIXTURE_KEY.encode()).hexdigest(), ('P1', 'P2'), ('ARRAY',))
    app = create_app(clients_provider=lambda: (principal,), clock=lambda: NOW)
    with TestClient(app) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPM'})
    assert response.status_code == 200, response.text
    assert len(response.json()['data']) == 1 and response.json()['meta']['complete'] is False
    assert response.json()['meta']['failed_sources'] == [dict(product='P2', sub_domain='SPC', event_type='CPM', code='result_unavailable')]


def test_capability_identity_stable_but_changed_value_invalidates_page(capability_files):
    files = capability_files
    path = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    rows = _capability_rows('cpk')
    rows = pd.concat([rows, rows.iloc[[0]].assign(param_name='SECOND')], ignore_index=True)
    _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
    with TestClient(files.app()) as client:
        params = dict(event_type='CPK', limit=1)
        first = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
        assert first.status_code == 200 and first.json()['meta']['has_more'] is True
        full_before = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'}).json()['data']
        cursor = first.json()['meta']['next_cursor']
        rows.loc[rows['param_name'].eq('SECOND'), 'cpk_corrected'] = .9
        _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
        next_page = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={**params, 'cursor': cursor})
        assert next_page.status_code == 410
        full_after = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'}).json()['data']
        assert {r['对象编号'] for r in full_before} == {r['对象编号'] for r in full_after}
        assert any(row['指标值'] == .9 for row in full_after)


def test_capability_policy_change_invalidates_next_page(capability_files):
    files = capability_files
    path = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    rows = _capability_rows('cpk')
    rows = pd.concat([rows, rows.iloc[[0]].assign(param_name='SECOND')], ignore_index=True)
    _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
    with TestClient(files.app()) as client:
        params = dict(event_type='CPK', limit=1)
        first = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
        assert first.status_code == 200 and first.json()['meta']['has_more'] is True
        policy = files.policy.copy()
        policy.loc[policy['监控参数'].eq(CAPABILITY_NAMES['CPK']), 'P1'] = True
        _write(files.policy_path, {'全指标预警看板': policy})
        next_page = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
            params={**params, 'cursor': first.json()['meta']['next_cursor']})
        assert next_page.status_code == 410


def test_original_sheet_json_shape_and_default_query_remain_unchanged(capability_files):
    with TestClient(capability_files.app()) as client:
        result = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'sub_domain': 'SPC'})
    assert result.status_code == 200
    row, = result.json()['data']
    assert set(row) == BASE_FIELDS and row['异常事件'] == 'OOS' and row['对象类型'] == 'sheet'
