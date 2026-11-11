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
    from src.inline_domain import composition
    from src.inline_domain.application.spc.sheet_membership import (
        SpcSheetMembershipQuery, publish_spc_sheet_memberships,
    )
    from src.inline_domain.infrastructure.spc.sheet_membership_store import SpcSheetMembershipJsonStore

    files.membership_path = files.metadata_path.parent / 'members'
    files.membership_store = SpcSheetMembershipJsonStore(files.membership_path)

    def publish_membership(points, *, product='P1', start_date='2026-10-19', end_date='2026-11-01',
                           capability_records=None):
        frame = points if isinstance(points, pd.DataFrame) else pd.DataFrame(points)
        if capability_records is None:
            keys = ['prod_code', 'factory', 'step_id', 'param_name', 'period_type',
                    'period_label', 'period_start', 'period_end']
            blocks = []
            for block in pd.read_excel(path, sheet_name=None, dtype=object).values():
                metrics = [name for name in ('cpk', 'cpk_corrected', 'cpm', 'cpm_corrected') if name in block]
                blocks.append(block[keys + metrics])
            capability_records = pd.concat(blocks, ignore_index=True).groupby(
                keys, sort=False, dropna=False).first().reset_index()
        return publish_spc_sheet_memberships(frame, store=files.membership_store, product=product,
            start_date=start_date, end_date=end_date, reference_date=NOW.date(),
            policy_signature='fixture-policy', capability_records=capability_records)

    files.publish_membership = publish_membership
    points = []
    for parameter, ids, start, end in (
        ('PPA_X', ('00001', '00002'), '2026-10-27 10:00:00', '2026-10-29 10:00:00'),
        ('SECOND', ('00101', '00102'), '2026-10-27 10:00:00', '2026-10-29 10:00:00'),
        ('OLD_WEEK', ('00003', '00004'), '2026-10-20 10:00:00', '2026-10-22 10:00:00'),
    ):
        points.extend(dict(prod_code='P1', factory='ARRAY', step_id='0100', param_name=parameter,
                           sheet_id=identifier, sheet_start_time=stamp, param_value=1.)
                      for identifier, stamp in zip(ids, (start, end)))
    files.member_points = pd.DataFrame(points)
    publish_membership(files.member_points)
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_query',
        lambda: SpcSheetMembershipQuery(files.membership_store, policy_signature='fixture-policy'))
    return files


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
def test_capability_workbook_reaches_http_with_full_public_identity(capability_files, event):
    files = capability_files
    before = {path: path.read_bytes() for path in (*files.paths.values(), files.policy_path) if path.exists()}
    member_before = {path: path.read_bytes() for path in files.membership_path.rglob('*.json')}
    assert member_before
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': event})
    assert response.status_code == 200, response.text
    row, = response.json()['data']
    assert set(row) == BASE_FIELDS | CAPABILITY_FIELDS
    assert {key: row[key] for key in ('分类', '发生站点', '部门', '层别', '监控参数', '异常事件')} == {
        '分类': 'SPC', '发生站点': '0100', '部门': 'ARRAY', '层别': 'PI2', '监控参数': 'PPA_X', '异常事件': event}
    assert row['对象类型'] == 'capability' and row['对象编号'] == ['00001', '00002']
    assert row['指标值'] == 1.2 and row['达标下限'] == 1.33
    assert row['统计周期类型'] == 'week' and row['统计周期'] == '2026-W44'
    assert row['统计开始日期'] == '2026-10-26' and row['统计截止日期'] == '2026-11-01'
    assert row['发生时间'] == '2026-11-01' and row['命中边界'] == 'spec'
    assert response.json()['meta']['complete'] is True
    assert all(path.read_bytes() == data for path, data in before.items())
    assert all(path.read_bytes() == data for path, data in member_before.items())


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
            params={**params, 'entity_type': 'capability', 'entity_id': row['对象编号'][0]})
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
    assert len(response.json()['data']) == 1 and response.json()['meta']['complete'] is True
    assert response.json()['meta']['failed_sources'] == []


def test_capability_identity_stable_but_changed_value_invalidates_page(capability_files):
    files = capability_files
    path = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    rows = _capability_rows('cpk')
    rows = pd.concat([rows, rows.iloc[[0]].assign(param_name='SECOND')], ignore_index=True)
    _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
    files.publish_membership(files.member_points)
    with TestClient(files.app()) as client:
        params = dict(event_type='CPK', limit=1)
        first = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
        assert first.status_code == 200 and first.json()['meta']['has_more'] is True
        full_before = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'}).json()['data']
        cursor = first.json()['meta']['next_cursor']
        rows.loc[rows['param_name'].eq('SECOND'), 'cpk_corrected'] = .9
        _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
        files.publish_membership(files.member_points)
        next_page = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={**params, 'cursor': cursor})
        assert next_page.status_code == 410
        full_after = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'}).json()['data']
        assert {tuple(r['对象编号']) for r in full_before} == {tuple(r['对象编号']) for r in full_after}
        assert any(row['指标值'] == .9 for row in full_after)


def test_capability_policy_change_invalidates_next_page(capability_files):
    files = capability_files
    path = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    rows = _capability_rows('cpk')
    rows = pd.concat([rows, rows.iloc[[0]].assign(param_name='SECOND')], ignore_index=True)
    _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
    files.publish_membership(files.member_points)
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


def test_explicit_oos_retains_original_sheet_json_shape(capability_files):
    with TestClient(capability_files.app()) as client:
        result = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'sub_domain': 'SPC', 'event_type': 'OOS'})
    assert result.status_code == 200
    row, = result.json()['data']
    assert set(row) == BASE_FIELDS and row['异常事件'] == 'OOS' and row['对象类型'] == 'sheet'


@pytest.mark.parametrize('event', ['CPK', 'CPM'])
def test_capability_missing_member_snapshot_is_unavailable_without_fabricating_ids(capability_files, event):
    files = capability_files
    for path in files.membership_path.rglob('*.json'):
        path.unlink()
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': event})
    assert response.status_code == 503
    assert response.json()['error']['failed_sources'] == [
        dict(product='P1', sub_domain='SPC', event_type=event, code='result_unavailable')]
    assert 'capability:' not in response.text and 'PPA_X' not in response.text


def test_capability_member_snapshot_cannot_mix_with_another_statistical_span(capability_files):
    files = capability_files
    points = files.member_points.copy()
    selected = points['param_name'].eq('PPA_X')
    points.loc[selected, 'sheet_start_time'] = (pd.to_datetime(
        points.loc[selected, 'sheet_start_time']) + pd.Timedelta(days=1)).dt.strftime('%Y-%m-%d %H:%M:%S')
    files.publish_membership(points)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'})
    assert response.status_code == 503


def test_new_capability_value_cannot_use_members_from_an_older_publication(capability_files):
    files = capability_files
    path = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    rows = _capability_rows('cpk')
    rows.loc[rows['param_name'].eq('PPA_X'), 'cpk_corrected'] = 1.1
    _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
    with TestClient(files.app()) as client:
        mismatch = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'})
        assert mismatch.status_code == 503
        files.publish_membership(files.member_points)
        refreshed = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'CPK'})
    assert refreshed.status_code == 200
    row, = refreshed.json()['data']
    assert row['指标值'] == 1.1 and row['对象编号'] == ['00001', '00002']


def test_capability_complete_membership_matches_week_and_changes_invalidate_cursor(capability_files):
    files = capability_files
    path = files.paths[('inline_domain', 'spc_cpk_cpm_decoration')]
    rows = _capability_rows('cpk')
    rows = pd.concat([rows, rows.iloc[[0]].assign(param_name='SECOND')], ignore_index=True)
    _write(path, {'P1': rows, 'P1_cpm': _capability_rows('cpm')})
    files.publish_membership(files.member_points)
    with TestClient(files.app()) as client:
        params = dict(event_type='CPK', limit=1)
        first = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params).json()
        assert first['meta']['returned_count'] == 1 and first['meta']['has_more'] is True
        assert first['data'][0]['监控参数'] == 'SECOND'
        assert first['data'][0]['对象编号'] == ['00101', '00102']
        points = pd.concat([files.member_points, pd.DataFrame([dict(
            prod_code='P1', factory='ARRAY', step_id='0100', param_name='SECOND',
            sheet_id='00103', sheet_start_time='2026-10-28 10:00:00', param_value=1.)])], ignore_index=True)
        files.publish_membership(points)
        changed = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
            params={**params, 'cursor': first['meta']['next_cursor']})
        assert changed.status_code == 410
        full = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
            params={'event_type': 'CPK', 'entity_id': '00103'}).json()
        assert full['meta']['complete'] is True
        assert full['data'][0]['对象编号'] == ['00101', '00102', '00103']
