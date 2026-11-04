"""Real published workbooks through the default HTTP composition, without a DB."""
from datetime import date, datetime
import hashlib
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pandas as pd
import pytest

from api.composition import DISPLAY_NAMES
from api.main import create_app
from api.settings import ApiClient
from src.indicator_domain.application.qtime.alert_results import QTimeAlertResults
from src.indicator_domain.infrastructure.qtime.alert_workbook import QTimeAlertWorkbook
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.shared_kernel.utils import excel_tools
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

DAY = date(2026, 11, 2)
NOW = datetime(2026, 11, 2, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
FIXTURE_KEY = "integration-fixture-not-a-production-key"
HEADERS = {"X-API-Key": FIXTURE_KEY}


def _write(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)


def _inline_rows(scope: str) -> pd.DataFrame:
    base = dict(factory="ARRAY", prod_code="P1", step_id="0100", flag=False,
        sheet_id="00000000001", sheet_start_time="2026-10-28 10:00:00", param_name="THK",
        sheet_mean=8., sheet_max=11., sheet_min=2., usl=10., lsl=3.,
        oos_type="USL", ooc_type="UCL", ucl=9., lcl=4.)
    if scope == "aoi_tt":
        base.update(tt_name="TDSUM", start_time=base["sheet_start_time"], tt_qty=12.)
    if scope == "aoi_rs":
        base.update(chart_kind="sheet", point_id=base["sheet_id"], rs_code="01234", value=12., spec=10.)
    rows = [base, {**base, "flag": True, "sheet_id": "DENIED", "point_id": "DENIED"},
        {**base, "sheet_start_time": "2026-11-02 00:00:00", "start_time": "2026-11-02 00:00:00",
         "sheet_id": "CURRENT", "point_id": "CURRENT"}]
    if scope == "aoi_rs":
        rows.append({**base, "chart_kind": "lot", "point_id": "000000001"})
    return pd.DataFrame(rows)


@pytest.fixture
def published_files(tmp_path, monkeypatch):
    paths = {}
    for scope in ("spc", "ctq", "aoi_tt", "aoi_rs"):
        for event in ("oos", "ooc"):
            key = f"{scope}_sheet_{event}_decoration"
            paths[("inline_domain", key)] = tmp_path / f"{key}.xlsx"
            _write(paths[("inline_domain", key)], {"P1": _inline_rows(scope)})
    paths[("indicator_domain", "qtime_alerts")] = tmp_path / "qtime.xlsx"
    paths[("yield_domain", "lot_oos_results")] = tmp_path / "yield-lot.xlsx"
    paths[("yield_domain", "trend_alert_results")] = tmp_path / "yield-trend.xlsx"
    metadata_path = tmp_path / "metadata.xlsx"
    paths[("shared_kernel", "api_alert_metadata")] = metadata_path
    _write(metadata_path, {
        "站点字典": pd.DataFrame([{"step_id": "0100", "description": "PI2_CURE_THK"},
                                      {"step_id": "0200", "description": "OTHER_PROCESS"}]),
    })
    policy_path = tmp_path / "policy.xlsx"
    policy = pd.DataFrame({"监控参数": list(DISPLAY_NAMES.values()), "P1": False})
    _write(policy_path, {"全指标预警看板": policy})

    def qtime_service(*, workbook_reader=None):
        return QTimeAlertResults(QTimeAlertWorkbook(paths[("indicator_domain", "qtime_alerts")],
            workbook_reader=workbook_reader), ttl_seconds=86400,
            policy_signature=lambda: "fixture-policy", clock=lambda: NOW)

    def yield_service(*, workbook_reader=None):
        return YieldAlertResults(YieldAlertWorkbookStore({
            "lot": paths[("yield_domain", "lot_oos_results")],
            "trend": paths[("yield_domain", "trend_alert_results")],
        }, workbook_reader=workbook_reader), ttl_seconds=86400,
            policy_signature=lambda _: "fixture-policy", clock=lambda: NOW)

    qtime_rows = pd.DataFrame([dict(prodcode="P1", f_step="0100", t_step="0200",
        step_desc="SOURCE_DESTINATION", lot_id="LOT001", q_spec=1., wait_time=2.,
        over_hours=1., timekey="20261028100000", flag=False)])
    qtime_service().publish("ARRAY", qtime_rows, qtime_rows,
        {"status": "fresh", "refreshed_at": NOW.isoformat()}, DAY)
    from src.yield_domain.core.sheet_lot.lot_alerts import compute_lot_oos_records
    computed, _ = compute_lot_oos_records({'code_level_details': {'Array_Line': pd.DataFrame([{
        'lot_id': 'LOT001', 'defect_desc': 'D1', 'defect_group': 'Array_Line',
        'warehousing_time': '20261028', 'array_input_time': '20261001',
        'defect_rate': .02, 'defect_panel_count': 2,
    }])}}, {'D1': {'upper': .01}}, factory_mapping={'Array_Line': 'ARRAY'})
    lot, = computed
    yield_service().publish("P1", "lot", [lot], reference_date=DAY,
        coverage_start="2026-10-01", coverage_end=str(DAY), has_data=True,
        health={"status": "fresh", "refreshed_at": NOW.isoformat()}, calculation_signature="fixture")

    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda domain, key, *_, **__: paths[(domain, key)])
    monkeypatch.setattr(ConfigLoader, "get_compliance_config_path", lambda: policy_path)
    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda **_: ["P1"])
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_indicator_data_exclusion", lambda: None)
    monkeypatch.setattr(ConfigLoader, "get_spc_sheet_oos_alert_param_exclusions", lambda: ("PPA",))
    from src.indicator_domain import composition as indicator_composition
    from src.yield_domain import composition as yield_composition
    monkeypatch.setattr(indicator_composition, "build_qtime_alert_results", qtime_service)
    monkeypatch.setattr(yield_composition, "build_yield_alert_results", yield_service)

    def forbidden(*_, **__):
        pytest.fail("HTTP attempted database access or protected-file fallback")

    monkeypatch.setattr(DatabaseManager, "__init__", forbidden)
    monkeypatch.setattr(pd, "read_sql", forbidden)
    monkeypatch.setattr(excel_tools, "read_workbook_sheets", forbidden)
    monkeypatch.setattr(excel_tools, "_read_all_sheets_via_com", forbidden)
    principal = ApiClient("fixture-client", hashlib.sha256(FIXTURE_KEY.encode()).hexdigest(),
        ("P1",), ("ARRAY", "CELL"))

    def app():
        return create_app(clients_provider=lambda: (principal,), clock=lambda: NOW)

    return SimpleNamespace(paths=paths, metadata_path=metadata_path,
        policy_path=policy_path, policy=policy, app=app,
        qtime_service=qtime_service, yield_service=yield_service)


def _bytes(files) -> dict[Path, bytes]:
    return {path: path.read_bytes() for path in {*files.paths.values(), files.policy_path} if path.exists()}


def _assert_unchanged(snapshot: dict[Path, bytes]) -> None:
    assert all(path.read_bytes() == before for path, before in snapshot.items())


def _release_yield(files) -> None:
    path = files.paths[("yield_domain", "lot_oos_results")]
    decisions = pd.read_excel(path, sheet_name="P1__flags", dtype=object).assign(flag=False)
    assert excel_tools.replace_workbook_sheets(path, {"P1__flags": decisions}).written


@pytest.mark.parametrize('headers', ['blank', 'missing_flag', 'missing_identity', 'valid'])
@pytest.mark.parametrize('event', ['OOS', 'OOC'])
def test_empty_publication_requires_complete_headers(published_files, headers, event):
    files = published_files
    frame = _inline_rows('spc').iloc[:0]
    if headers == 'blank':
        frame = pd.DataFrame()
    elif headers == 'missing_flag':
        frame = frame.drop(columns='flag')
    elif headers == 'missing_identity':
        frame = frame.drop(columns='sheet_id')
    path = files.paths[('inline_domain', f'spc_sheet_{event.lower()}_decoration')]
    _write(path, {'P1': frame})
    before = path.read_bytes()
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                              params={'product': 'P1', 'sub_domain': 'SPC', 'event_type': event})
    assert response.status_code == (200 if headers == 'valid' else 503)
    if headers == 'valid':
        assert response.json()['data'] == [] and response.json()['meta']['status'] == 'ok'
    assert path.read_bytes() == before


@pytest.mark.parametrize("domain", ["SPC", "CTQ", "AOI_TT", "AOI_RS"])
@pytest.mark.parametrize("event", ["OOS", "OOC"])
def test_real_inline_publications_reach_default_http_with_exact_identity(published_files, domain, event):
    files = published_files
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={"product": "P1", "sub_domain": domain, "event_type": event})
    assert response.status_code == 200, response.text
    rows = response.json()["data"]
    assert {row["对象类型"] for row in rows} == ({"sheet", "lot"} if domain == "AOI_RS" else {"sheet"})
    assert {row["对象编号"] for row in rows} == ({"00000000001", "000000001"} if domain == "AOI_RS" else {"00000000001"})
    assert all(row["发生站点"] == "0100" and row["层别"] == "PI2" and row["部门"] == "ARRAY" for row in rows)
    assert all(row["异常事件"] == event and row["分类"] == domain for row in rows)
    assert all("flag" not in row for row in rows)
    assert response.json()["meta"]["period_start"] == "2026-10-26T00:00:00+08:00"
    assert response.json()["meta"]["period_end_exclusive"] == "2026-11-02T00:00:00+08:00"
    _assert_unchanged(before)


def test_real_qtime_uses_from_station_and_its_dictionary_layer(published_files):
    files = published_files
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "Q-Time"})
    assert response.status_code == 200, response.text
    row, = response.json()["data"]
    assert row["发生站点"] == "0100" and "起始站点" not in row
    assert row["层别"] == "PI2" and row["层别"] != "OTHER"
    assert row["部门"] == "ARRAY" and row["对象类型"] == "lot"
    _assert_unchanged(before)


def test_authorized_factory_outside_qtime_scope_is_valid_empty(published_files, monkeypatch):
    from src.indicator_domain import composition
    files = published_files
    monkeypatch.setattr(composition, 'build_qtime_alert_results',
                        lambda **kwargs: pytest.fail('No Q-Time shop was requested'))
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                              params={'event_type': 'OOS', 'product': 'P1', 'sub_domain': 'Q-Time', 'factory': 'CELL'})
    assert response.status_code == 200
    assert response.json()['data'] == [] and response.json()['meta']['status'] == 'ok'
    _assert_unchanged(before)


def test_real_yield_publishes_automatic_factory_and_null_location_without_metadata(published_files):
    files = published_files
    with TestClient(files.app()) as client:
        before = _bytes(files)
        denied = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "Yield"})
        assert denied.status_code == 200 and denied.json()["data"] == []
        _assert_unchanged(before)
        _release_yield(files)
        published = pd.read_excel(files.paths[('yield_domain', 'lot_oos_results')], sheet_name='P1')
        assert published['defect_group'].tolist() == ['Array_Line']
        assert published['factory'].tolist() == ['ARRAY']
        files.metadata_path.unlink()
        before = _bytes(files)
        released = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "Yield"})
        assert released.status_code == 200, released.text
        row, = released.json()["data"]
        assert row["部门"] == "ARRAY" and row["发生站点"] is None and row["层别"] is None
        assert row["发生时间"] == "2026-10-28"
        _assert_unchanged(before)
        path = files.paths[('yield_domain', 'lot_oos_results')]
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        sheets['P1'] = sheets['P1'].drop(columns=['defect_group', 'factory'])
        _write(path, sheets)  # A legacy publication cannot infer attribution on GET.
        before = _bytes(files)
        incomplete = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "Yield"})
        assert incomplete.status_code == 503
        assert "LOT001" not in incomplete.text
        _assert_unchanged(before)


def test_yield_factory_filter_uses_published_factory_before_metadata_access(published_files):
    files = published_files
    _release_yield(files)
    files.metadata_path.unlink()
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
            params={'event_type': 'OOS', 'product': 'P1', 'sub_domain': 'Yield', 'factory': 'CELL'})
    assert response.status_code == 200 and response.json()['data'] == []
    _assert_unchanged(before)


@pytest.mark.parametrize('domain', ['SPC', 'CTQ', 'AOI_TT', 'AOI_RS'])
def test_custom_dates_and_field_filters_reach_inline_source_selection(published_files, domain):
    files = published_files
    before = _bytes(files)
    parameter = {'SPC': 'THK', 'CTQ': 'THK', 'AOI_TT': 'TDSUM', 'AOI_RS': '01234'}[domain]
    params = dict(event_type='OOS', sub_domain=domain, start_date='2026-11-02', end_date='2026-11-02',
                  step_id='0100', layer='PI2', parameter_name=parameter)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
        assert response.status_code == 200, response.text
        assert [row['对象编号'] for row in response.json()['data']] == ['CURRENT']
        assert response.json()['meta']['period_end_exclusive'] == '2026-11-03T00:00:00+08:00'
        for change in ({'parameter_name': 'unmatched'}, {'step_id': '100'}, {'layer': 'OTHER'}):
            excluded = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={**params, **change})
            assert excluded.status_code == 200 and excluded.json()['data'] == []
    _assert_unchanged(before)


def test_no_parameters_and_partial_failure_across_real_published_sources(published_files):
    files = published_files
    _release_yield(files)
    with TestClient(files.app()) as client:
        before = _bytes(files)
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS)
        assert response.status_code == 200, response.text
        assert len(response.json()['data']) == 12
        assert response.json()['meta']['complete'] is False
        capability_failures = [dict(product='P1', sub_domain='SPC', event_type=event,
                                    code='result_unavailable') for event in ('CPK', 'CPM')]
        assert response.json()['meta']['failed_sources'] == capability_failures
        _assert_unchanged(before)
        files.paths[('inline_domain', 'ctq_sheet_oos_decoration')].unlink()
        before = _bytes(files)
        partial = client.get('/api/v1/sheet-anomalies', headers=HEADERS)
        assert partial.status_code == 200, partial.text
        assert len(partial.json()['data']) == 11
        assert partial.json()['meta']['complete'] is False
        assert partial.json()['meta']['failed_sources'] == capability_failures + [dict(
            product='P1', sub_domain='CTQ', event_type='OOS', code='result_unavailable')]
        assert str(files.paths[('inline_domain', 'ctq_sheet_oos_decoration')]) not in partial.text
        _assert_unchanged(before)


def test_omitted_category_for_ooc_selects_only_supported_authorized_sources(published_files):
    files = published_files
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'OOC'})
    assert response.status_code == 200, response.text
    assert {row['分类'] for row in response.json()['data']} == {'SPC', 'CTQ', 'AOI_TT', 'AOI_RS'}
    assert response.json()['meta']['complete'] is True
    _assert_unchanged(before)


def test_inline_date_boundaries_use_shanghai_for_timezone_aware_published_times(published_files):
    files = published_files
    frame = _inline_rows('spc').iloc[[0, 0]].copy()
    frame['sheet_id'] = ['INCLUDED', 'NEXT_DAY']
    frame['sheet_start_time'] = ['2026-11-01T16:00:00+00:00', '2026-11-02T16:00:00+00:00']
    _write(files.paths[('inline_domain', 'spc_sheet_oos_decoration')], {'P1': frame})
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=dict(event_type='OOS',
            sub_domain='SPC', start_date='2026-11-02', end_date='2026-11-02'))
    assert response.status_code == 200, response.text
    row, = response.json()['data']
    assert row['对象编号'] == 'INCLUDED' and row['发生时间'] == '2026-11-02T00:00:00+08:00'
    _assert_unchanged(before)


@pytest.mark.parametrize('scope', ['spc', 'ctq', 'aoi_tt', 'aoi_rs'])
@pytest.mark.parametrize('event', ['OOS', 'OOC'])
def test_timezone_normalization_precedes_public_date_exclusion(published_files, monkeypatch, scope, event):
    files = published_files
    monkeypatch.setattr(ConfigLoader, 'get_inline_data_exclusion',
                        lambda: (('ARRAY',), '2026-11-02', '2026-11-02'))
    frame = _inline_rows(scope).iloc[[0]].copy()
    frame['sheet_start_time'] = frame['start_time'] = '2026-11-01T16:00:00+00:00'
    _write(files.paths[('inline_domain', f'{scope}_sheet_{event.lower()}_decoration')], {'P1': frame})
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=dict(
            sub_domain={'aoi_tt': 'AOI_TT', 'aoi_rs': 'AOI_RS'}.get(scope, scope.upper()),
            event_type=event, start_date='2026-11-02', end_date='2026-11-02'))
    assert response.status_code == 200, response.text
    assert response.json()['data'] == [] and response.json()['meta']['complete'] is True
    _assert_unchanged(before)


@pytest.mark.parametrize('failure', ['com', 'xml'])
def test_expected_protected_workbook_errors_preserve_other_sources(published_files, monkeypatch, failure):
    from pywintypes import com_error
    from xml.etree.ElementTree import ParseError
    files = published_files
    failed_path = files.paths[('inline_domain', 'ctq_sheet_oos_decoration')]
    failed_path.write_bytes(b'not-a-valid-xlsx')

    def fallback(path):
        assert Path(path) == failed_path
        raise (com_error(-2147024891, 'private-path', None, None) if failure == 'com'
               else ParseError('private-xml'))

    monkeypatch.setattr(excel_tools, 'read_workbook_sheets', fallback)
    before = _bytes(files)
    with TestClient(files.app(), raise_server_exceptions=False) as client:
        response = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params={'event_type': 'OOS'})
    assert response.status_code == 200, response.text
    assert response.json()['meta']['complete'] is False
    assert response.json()['meta']['failed_sources'] == [dict(product='P1', sub_domain='CTQ',
                                                             event_type='OOS', code='result_unavailable')]
    assert len(response.json()['data']) == 5
    assert 'private-' not in response.text
    _assert_unchanged(before)


@pytest.mark.parametrize('domain', ['Q-Time', 'Yield'])
def test_lot_sources_honor_dates_outside_default_week_and_retain_coverage_checks(published_files, domain):
    files = published_files
    if domain == 'Q-Time':
        rows = pd.DataFrame([dict(prodcode='P1', f_step='0100', t_step='0200',
            step_desc='SOURCE_DESTINATION', lot_id='HISTORICAL', q_spec=1., wait_time=2.,
            over_hours=1., timekey='20261025000000', flag=False)])
        files.qtime_service().publish('ARRAY', rows, rows,
            {'status': 'fresh', 'refreshed_at': NOW.isoformat()}, DAY)
        filters = dict(step_id='0100', layer='PI2', parameter_name='SOURCE_DESTINATION')
    else:
        lot = pd.read_excel(files.paths[('yield_domain', 'lot_oos_results')], sheet_name='P1', dtype=object)
        lot.loc[:, '入库时间'] = '2026/10/25'
        lot.loc[:, '超规 Lot ID'] = 'HISTORICAL'
        files.yield_service().publish('P1', 'lot', lot.to_dict('records'), reference_date=DAY,
            coverage_start='2026-10-01', coverage_end=str(DAY), has_data=True,
            health={'status': 'fresh', 'refreshed_at': NOW.isoformat()}, calculation_signature='fixture')
        _release_yield(files)
        filters = dict(parameter_name='D1')
    before = _bytes(files)
    params = dict(event_type='OOS', sub_domain=domain, start_date='2026-10-25', end_date='2026-10-25', **filters)
    with TestClient(files.app()) as client:
        result = client.get('/api/v1/sheet-anomalies', headers=HEADERS, params=params)
        assert result.status_code == 200, result.text
        assert [row['对象编号'] for row in result.json()['data']] == ['HISTORICAL']
        empty = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                           params={**params, 'start_date': '2026-10-26', 'end_date': '2026-10-26'})
        assert empty.status_code == 200 and empty.json()['data'] == []
        uncovered = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                               params={**params, 'start_date': '2026-09-01'})
        assert uncovered.status_code == 503
        if domain == 'Yield':
            station = client.get('/api/v1/sheet-anomalies', headers=HEADERS,
                                 params={**params, 'step_id': '0100'})
            assert station.status_code == 200 and station.json()['data'] == []
    _assert_unchanged(before)


@pytest.mark.parametrize("domain", list(DISPLAY_NAMES))
def test_hidden_public_policy_does_not_read_missing_sources_or_disclose_them(published_files, domain):
    files = published_files
    policy = files.policy.copy()
    policy.loc[policy["监控参数"].eq(DISPLAY_NAMES[domain]), "P1"] = True
    _write(files.policy_path, {"全指标预警看板": policy})
    for (owner, key), path in files.paths.items():
        if owner != "shared_kernel" and path.exists():
            path.unlink()
    files.metadata_path.unlink()
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": domain})
    assert response.status_code == 200 and response.json()["data"] == []
    assert response.json()["meta"]["status"] == "ok"
    _assert_unchanged(before)


def test_invalid_policy_fails_closed_without_source_details(published_files):
    files = published_files
    _write(files.policy_path, {"全指标预警看板": files.policy.assign(P1="invalid")})
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "SPC"})
    assert response.status_code == 503
    assert str(files.policy_path) not in response.text
    _assert_unchanged(before)


def test_policy_change_is_observed_before_a_cached_source_is_read_again(published_files):
    files = published_files
    with TestClient(files.app()) as client:
        initial = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "SPC"})
        assert initial.status_code == 200 and len(initial.json()["data"]) == 1
        policy = files.policy.copy()
        policy.loc[policy["监控参数"].eq(DISPLAY_NAMES["SPC"]), "P1"] = True
        _write(files.policy_path, {"全指标预警看板": policy})
        files.paths[("inline_domain", "spc_sheet_oos_decoration")].unlink()
        before = _bytes(files)
        hidden = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "SPC"})
        assert hidden.status_code == 200 and hidden.json()["data"] == []
        assert hidden.json()["meta"]["status"] == "ok"
        assert "SHEET" not in hidden.text and "flag" not in hidden.text
        _assert_unchanged(before)


def test_missing_required_layer_is_unavailable_instead_of_omitting_records(published_files):
    files = published_files
    sheets = pd.read_excel(files.metadata_path, sheet_name=None, dtype=object)
    sheets["站点字典"] = sheets["站点字典"].loc[sheets["站点字典"]["step_id"].ne("0100")]
    _write(files.metadata_path, sheets)
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": "SPC"})
    assert response.status_code == 503
    assert "PI2" not in response.text and "00000000001" not in response.text
    _assert_unchanged(before)


@pytest.mark.parametrize("domain", list(DISPLAY_NAMES))
def test_missing_release_evidence_never_becomes_a_successful_empty_result(published_files, domain):
    files = published_files
    if domain in ("SPC", "CTQ", "AOI_TT", "AOI_RS"):
        path = files.paths[("inline_domain", f"{domain.lower()}_sheet_oos_decoration")]
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        sheets["P1"] = sheets["P1"].drop(columns="flag")
    elif domain == "Q-Time":
        path = files.paths[("indicator_domain", "qtime_alerts")]
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        sheets["alerts_ARRAY"] = sheets["alerts_ARRAY"].drop(columns="flag")
    else:
        path = files.paths[("yield_domain", "lot_oos_results")]
        sheets = pd.read_excel(path, sheet_name=None, dtype=object)
        sheets["P1"] = sheets["P1"].drop(columns="flag")
        sheets["P1__meta"] = sheets["P1__meta"].drop(columns="release_schema_version")
        del sheets["P1__flags"]
    _write(path, sheets)
    before = _bytes(files)
    with TestClient(files.app()) as client:
        response = client.get("/api/v1/sheet-anomalies", headers=HEADERS,
            params={'event_type': 'OOS', "product": "P1", "sub_domain": domain})
    assert response.status_code == 503, response.text
    _assert_unchanged(before)
