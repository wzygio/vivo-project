"""Headless Inline adapters never use maintained business files in these tests."""
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from api.inspection.producers_inline import (
    InlineProductionSession, _configure_read_only_connection, _retryable_workbook_read,
    _safe_error_chain, produce_inline,
)


DAY = date(2026, 11, 12)


def test_dbapi_read_only_precedes_session_setting_transaction_and_source_queries():
    calls = []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): calls.append('cursor_closed')
        def execute(self, query): calls.append(query)
    class Connection:
        def set_session(self, **kwargs): calls.append(('set_session', kwargs))
        def cursor(self): return Cursor()
        def commit(self): calls.append('commit_settings')
    _configure_read_only_connection(Connection())
    assert calls == [('set_session', {'readonly': True}), 'SET statement_timeout = 120000',
                     'cursor_closed', 'commit_settings']


def source_bindings(monkeypatch, *, fresh=True):
    from src.inline_domain import composition

    calls = []
    class Shared:
        def refresh_measurements(self, product, end):
            calls.append(('shared', product, end))
            return SimpleNamespace(refreshed_from_db=fresh)
    class Rs:
        def refresh(self, query):
            calls.append(('rs', query))
            return True
    monkeypatch.setattr(composition, 'build_raw_measurement_repository', lambda *args: Shared())
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_coverage', lambda *args: lambda: {
        'start_date': '2026-10-01', 'end_date': '2026-11-12'})
    monkeypatch.setattr(composition, 'build_aoi_rs_repository', lambda *args, **kwargs: Rs())
    return calls


def test_one_shared_refresh_serves_three_modules_and_rs_stays_independent(monkeypatch):
    calls = source_bindings(monkeypatch)
    produced = []
    monkeypatch.setattr(InlineProductionSession, 'produce', lambda self, scope:
                        produced.append(scope) or {'verified_events': ['OOS', 'OOC']})
    @contextmanager
    def runtime(product, reference):
        yield InlineProductionSession(object(), product, reference)
    outcomes = produce_inline('DEMO', DAY, _session_factory=runtime)
    assert [row['status'] for row in outcomes] == ['success'] * 4
    assert produced == ['spc', 'ctq', 'aoi_tt', 'aoi_rs']
    assert [call[0] for call in calls] == ['shared', 'rs']
    rs_query = calls[1][1]
    assert rs_query.factory is None and rs_query.step_id is None and rs_query.rs_code is None


def test_database_fallback_fails_shared_scopes_without_publishing_but_rs_continues(monkeypatch):
    calls = source_bindings(monkeypatch, fresh=False)
    produced = []
    monkeypatch.setattr(InlineProductionSession, 'produce', lambda self, scope:
                        produced.append(scope) or {})
    @contextmanager
    def runtime(product, reference):
        yield InlineProductionSession(object(), product, reference)
    rows = produce_inline('DEMO', DAY, _session_factory=runtime)
    assert [row['status'] for row in rows] == ['failed', 'failed', 'failed', 'success']
    assert produced == ['aoi_rs'] and [call[0] for call in calls] == ['shared', 'rs']


def test_runtime_failure_is_safe_and_covers_every_requested_module():
    @contextmanager
    def unavailable(*args):
        raise OSError('private credentials that must not escape')
        yield
    outcomes = produce_inline('DEMO', DAY, _session_factory=unavailable)
    assert len(outcomes) == 4 and all(row['status'] == 'failed' for row in outcomes)
    assert 'credentials' not in str(outcomes)


@pytest.mark.parametrize('scopes', [(), ('other',), ('spc', 'spc')])
def test_invalid_scope_rejected_before_any_runtime(scopes):
    with pytest.raises(ValueError):
        produce_inline('DEMO', DAY, scopes=scopes)


def test_report_query_is_full_product_and_empty_generation_is_published(monkeypatch):
    from src.inline_domain import composition
    from src.inline_domain.application.ctq.ctq_service import CtqReportService

    captured, writes = [], []
    port = SimpleNamespace(get_spc_spec_limits=lambda product: pd.DataFrame({'usl': [1]}))
    monkeypatch.setattr(composition, 'build_ctq_repository', lambda *args: port)
    def service(**kwargs):
        captured.append(kwargs)
        return SimpleNamespace(raw_measurements_df=pd.DataFrame())
    monkeypatch.setattr(CtqReportService, 'get_ctq_report_data', service)
    monkeypatch.setattr(InlineProductionSession, '_publish_empty_oos', lambda self, *args: writes.append(args))
    monkeypatch.setattr(InlineProductionSession, '_verify_ledgers', lambda *args: {'oos_count': 0, 'ooc_count': 0})
    session = InlineProductionSession(object(), 'DEMO', DAY)
    outcome = session.produce('ctq')
    from src.inline_domain.application.spc.dtos import SpcQueryConfig
    query = SpcQueryConfig.model_validate_json(captured[0]['query_config_json'])
    assert query.factory is None and query.step_id is None and query.param_name is None
    assert query.prod_code == 'DEMO' and query.start_date == '2026-10-01'
    assert captured[0]['product_revision'] == session.revision
    assert writes and outcome['verified_events'] == ['OOS', 'OOC']


def test_missing_specs_cannot_be_converted_to_successful_empty(monkeypatch):
    from src.inline_domain import composition
    from src.inline_domain.application.ctq.ctq_service import CtqReportService

    port = SimpleNamespace(get_spc_spec_limits=lambda product: pd.DataFrame())
    monkeypatch.setattr(composition, 'build_ctq_repository', lambda *args: port)
    monkeypatch.setattr(CtqReportService, 'get_ctq_report_data', lambda **kwargs: pytest.fail('must not run'))
    with pytest.raises(ValueError, match='specifications'):
        InlineProductionSession(object(), 'DEMO', DAY).produce('ctq')


@pytest.mark.parametrize('scope', ['spc', 'ctq', 'aoi_tt', 'aoi_rs'])
def test_empty_oos_publication_preserves_existing_flags_and_other_sheets(tmp_path, monkeypatch, scope):
    from src.inline_domain.infrastructure.shared import resource_paths

    path = tmp_path / f'{scope}-oos.xlsx'
    pd.DataFrame({'operator_note': ['retained']}).to_excel(path, sheet_name='notes', index=False)
    with pd.ExcelWriter(path, mode='a', engine='openpyxl') as writer:
        pd.DataFrame({'manual_note': ['user-owned'], 'flag': [False]}).to_excel(
            writer, sheet_name='DEMO__flags', index=False)
    monkeypatch.setattr(resource_paths, 'scope_decoration_path', lambda *args: path)
    # Existing decision rows need normal business keys to retain their meaning.
    keys = ['prod_code', 'step_id', 'param_name', 'sheet_id']
    if scope == 'aoi_tt': keys = ['prod_code', 'step_id', 'tt_name', 'sheet_id']
    if scope == 'aoi_rs': keys = ['prod_code', 'step_id', 'rs_code', 'point_id']
    flags = pd.DataFrame([{**{key: 'DEMO' for key in keys}, 'flag': False, 'manual_note': 'user-owned'}])
    with pd.ExcelWriter(path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        flags.to_excel(writer, sheet_name='DEMO__flags', index=False)
    session = InlineProductionSession(object(), 'DEMO', DAY)
    session._publish_empty_oos(scope, pd.DataFrame())
    sheets = pd.read_excel(path, sheet_name=None)
    assert sheets['DEMO'].empty
    assert sheets['notes']['operator_note'].tolist() == ['retained']
    pd.testing.assert_frame_equal(sheets['DEMO__flags'], flags)
    assert sheets['__refresh_meta__']['product_revision'].tolist() == [session.revision]


def test_old_empty_ledger_is_not_production_proof(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.shared import resource_paths

    path = tmp_path / 'old.xlsx'
    pd.DataFrame([{'scope': 'ctq', 'prod_code': 'DEMO', 'last_generated_at': '2026-10-01',
                   'product_revision': 'old', 'detail_row_count': 0}]).to_excel(path, sheet_name='__refresh_meta__', index=False)
    monkeypatch.setattr(resource_paths, 'scope_decoration_path', lambda *args: path)
    with pytest.raises(ValueError, match='not from this production run'):
        InlineProductionSession(object(), 'DEMO', DAY)._verify_ledgers('ctq')


@pytest.mark.parametrize('scope', ['aoi_tt', 'aoi_rs'])
def test_aoi_report_evaluation_is_explicitly_published_then_read_back(monkeypatch, scope):
    from src.inline_domain import composition
    from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportService
    from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportService

    facts = pd.DataFrame({'sample': [1]})
    evaluation = pd.DataFrame({'prod_code': ['DEMO'], 'is_alert': [True]})
    report = SimpleNamespace(trend_evaluation_df=evaluation, tt_details_df=facts, rs_details_df=facts)
    port = SimpleNamespace(get_tt_spec_limits=lambda *args: pd.DataFrame({'usl': [1]}),
                           get_rs_spec_limits=lambda *args: pd.DataFrame({'spec': [1]}))
    monkeypatch.setattr(composition, f'build_{scope}_repository', lambda *args: port)
    cls = AoiTtReportService if scope == 'aoi_tt' else AoiRsReportService
    monkeypatch.setattr(cls, f'get_{scope}_report_data', lambda **kwargs: report)
    monkeypatch.setattr(InlineProductionSession, '_verify_ledgers', lambda *args: {})
    calls = []
    class Publisher:
        def publish(self, product, reference, data):
            calls.append(('publish', product, reference))
            pd.testing.assert_frame_equal(data, evaluation)
        def read(self, product, reference):
            calls.append(('read', product, reference))
            return {'state': 'alert'}
    monkeypatch.setattr(composition, 'build_trend_alert_results', lambda actual:
                        Publisher() if actual == scope else pytest.fail('wrong scope'))
    result = InlineProductionSession(object(), 'DEMO', DAY).produce(scope)
    assert calls == [('publish', 'DEMO', DAY), ('read', 'DEMO', DAY)]
    assert result['verified_events'] == ['OOS', 'OOC', 'trend']
    assert result['trend_state'] == 'alert'


def test_source_window_must_be_proved_before_any_shared_production(monkeypatch):
    from src.inline_domain import composition
    source_bindings(monkeypatch)
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_coverage', lambda *args: lambda: {
        'start_date': '2026-11-11', 'end_date': '2026-11-12'})
    with pytest.raises(ValueError, match='cover'):
        InlineProductionSession(object(), 'DEMO', DAY).refresh('spc')


def test_empty_capability_headers_are_added_without_replacing_maintained_data(tmp_path, monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    from src.inline_domain.core.spc.cpk_decoration import capability_decoration_columns, capability_detail_columns

    path = tmp_path / 'capabilities.xlsx'
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(columns=capability_decoration_columns('cpk')).to_excel(writer, sheet_name='DEMO', index=False)
        pd.DataFrame(columns=[*capability_detail_columns('cpm'), 'legacy_note']).to_excel(writer, sheet_name='DEMO_cpm', index=False)
        pd.DataFrame({'manual': ['retained']}).to_excel(writer, sheet_name='OTHER', index=False)
    monkeypatch.setattr(ConfigLoader, 'get_domain_resource_path', lambda *args: path)
    session = InlineProductionSession(object(), 'DEMO', DAY)
    session._spc_source_proof = {'start_date': '2026-10-01', 'end_date': '2026-11-12', 'source_generation': 'verified'}
    session._ensure_capability_headers()
    frames = pd.read_excel(path, sheet_name=None)
    assert set(capability_decoration_columns('cpm')) <= set(frames['DEMO_cpm'])
    assert 'legacy_note' in frames['DEMO_cpm'] and frames['DEMO_cpm'].empty
    assert frames['OTHER']['manual'].tolist() == ['retained']


def test_logical_policy_change_does_not_relabel_members(monkeypatch):
    session = InlineProductionSession(object(), 'DEMO', DAY)
    session._spc_logical_policy = 'original-rules'
    monkeypatch.setattr(session, '_logical_membership_policy', lambda: 'changed-rules')
    with pytest.raises(ValueError, match='logical preparation policy changed'):
        session._finalize_membership_policy(SimpleNamespace())


def test_only_physical_rule_view_change_republishes_same_prepared_generation(monkeypatch):
    from src.inline_domain import composition
    from src.inline_domain.application.spc import sheet_membership

    session = InlineProductionSession(object(), 'DEMO', DAY)
    session._spc_logical_policy, session._spc_policy = 'same-rules', 'protected-view-hash'
    proof = {'start_date': '2026-10-01', 'end_date': '2026-11-12', 'source_generation': 'generation-1'}
    session._spc_source_proof = proof
    monkeypatch.setattr(session, '_logical_membership_policy', lambda: 'same-rules')
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_policy_signature', lambda: 'plain-view-hash')
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_coverage', lambda product: lambda: proof)
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_store', lambda: object())
    points, capabilities = pd.DataFrame({'point': [1]}), pd.DataFrame({'metric': [1]})
    report = SimpleNamespace(raw_measurements_df=points, period_capability_df=capabilities)
    calls = []
    def publish(data, **kwargs):
        pd.testing.assert_frame_equal(data, points)
        calls.append(kwargs)
    monkeypatch.setattr(sheet_membership, 'publish_spc_sheet_memberships', publish)
    session._finalize_membership_policy(report)
    assert len(calls) == 1 and calls[0]['policy_signature'] == 'plain-view-hash'
    assert calls[0]['source_generation'] == 'generation-1'
    assert session._spc_policy == 'plain-view-hash'


def test_physical_view_change_still_rejects_a_different_raw_generation(monkeypatch):
    from src.inline_domain import composition
    from src.inline_domain.application.spc import sheet_membership
    from src.inline_domain.application.spc.spc_service import SpcReportBuildError

    session = InlineProductionSession(object(), 'DEMO', DAY)
    session._spc_logical_policy, session._spc_policy = 'same-rules', 'protected-view-hash'
    proof = {'start_date': '2026-10-01', 'end_date': '2026-11-12', 'source_generation': 'generation-1'}
    session._spc_source_proof = proof
    monkeypatch.setattr(session, '_logical_membership_policy', lambda: 'same-rules')
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_policy_signature', lambda: 'plain-view-hash')
    monkeypatch.setattr(composition, 'build_spc_sheet_membership_coverage', lambda product: lambda: {**proof, 'source_generation': 'generation-2'})
    monkeypatch.setattr(sheet_membership, 'publish_spc_sheet_memberships', lambda *args, **kwargs: pytest.fail('must not republish'))
    with pytest.raises(SpcReportBuildError):
        session._finalize_membership_policy(SimpleNamespace())


def _protected_read_failure():
    from src.inline_domain.application.shared.decoration_ports import CapabilityDecorationReadError
    try:
        raise PermissionError('private server/credential detail')
    except PermissionError as cause:
        try:
            raise CapabilityDecorationReadError('private workbook detail') from cause
        except CapabilityDecorationReadError as wrapped:
            return wrapped


def test_transient_protected_read_retries_once_without_reacquiring_source():
    calls = []
    class Session:
        phase = 'REPORT'
        def refresh(self, scope): calls.append('refresh')
        def retry(self, scope): calls.append('retry')
        def produce(self, scope):
            calls.append('produce')
            if calls.count('produce') == 1:
                raise _protected_read_failure()
            return {'verified_events': ['OOS', 'OOC', 'CPK', 'CPM']}
    @contextmanager
    def runtime(*args): yield Session()
    row, = produce_inline('DEMO', DAY, scopes=('spc',), _session_factory=runtime)
    assert calls == ['refresh', 'produce', 'retry', 'produce']
    assert row['status'] == 'success' and row['retry_count'] == 1


def test_failed_retry_is_bounded_and_keeps_only_safe_locations():
    calls = []
    class Session:
        phase = 'REPORT'
        def refresh(self, scope): calls.append('refresh')
        def retry(self, scope): calls.append('retry')
        def produce(self, scope):
            calls.append('produce')
            raise _protected_read_failure()
    @contextmanager
    def runtime(*args): yield Session()
    row, = produce_inline('DEMO', DAY, scopes=('spc',), _session_factory=runtime)
    assert calls.count('produce') == 2 and calls.count('refresh') == 1 and calls.count('retry') == 1
    assert row['status'] == 'failed' and row['retry_count'] == 1
    assert [entry['type'] for entry in row['error_chain'][:2]] == ['CapabilityDecorationReadError', 'PermissionError']
    assert len(row['error_chain']) <= 6
    assert 'private' not in str(row) and 'credential' not in str(row)
    assert all(set(entry) == {'type', 'file', 'line'} and '/' not in entry['file'] and '\\' not in entry['file']
               for entry in row['error_chain'])


@pytest.mark.parametrize('error', [ValueError('schema'), PermissionError('source read'),
                                 OSError('database failure')])
def test_non_workbook_transport_failures_do_not_retry(error):
    assert not _retryable_workbook_read(error)


def test_schema_failure_beneath_workbook_reader_does_not_retry():
    from src.inline_domain.core.monitor.excel_contract import ExcelAlarmReadError
    try:
        raise ValueError('invalid workbook schema')
    except ValueError as cause:
        try:
            raise ExcelAlarmReadError('bad column') from cause
        except ExcelAlarmReadError as error:
            assert not _retryable_workbook_read(error)


def test_retry_resets_result_cache_and_revision_but_preserves_source_proof_status():
    session = InlineProductionSession(object(), 'DEMO', DAY)
    session._refreshes['shared'] = None
    old_revision = session.revision
    session.retry('spc')
    assert session.revision != old_revision and session._refreshes == {'shared': None}
    assert session._spc_source_proof is None and session._spc_logical_policy is None
