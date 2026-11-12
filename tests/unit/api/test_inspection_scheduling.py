"""Scheduling gates are verified with synthetic receipts, without sending mail."""
from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
import os
import subprocess

import pytest

from api.inspection.application import GLOBAL_MODULES, MODULES, PRODUCT_MODULES
from api.inspection.scheduling import GateError, build_task_specs, wait_for_inspection


DAY = date(2026, 11, 12)
NOW = datetime.fromisoformat('2026-11-12T07:20:00+08:00')
PRODUCTS = ('M626', 'M678')


def receipt(status='success'):
    outcomes = [{'module': module, 'product': product, 'status': 'success'}
                for module in PRODUCT_MODULES for product in PRODUCTS]
    outcomes += [{'module': module, 'status': 'success'} for module in GLOBAL_MODULES]
    return {'schema_version': 1, 'run_id': 'fixture-run', 'reference_date': DAY.isoformat(),
            'products': list(PRODUCTS), 'modules': list(MODULES), 'status': status,
            'runner_pid': 1234, 'runner_created': 42.0,
            'started_at': '2026-11-12T07:15:00+08:00',
            'finished_at': '2026-11-12T07:19:00+08:00',
            'coverage_start': '2026-11-02', 'coverage_end': '2026-11-12',
            'outcomes': outcomes if status != 'running' else outcomes[:2]}


def wait(path, **kwargs):
    return wait_for_inspection(path, PRODUCTS, DAY, now=lambda: NOW, **kwargs)


def test_complete_receipt_accepts_only_full_current_scope(tmp_path):
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps(receipt()), encoding='utf-8')
    assert wait(path)['run_id'] == 'fixture-run'


def test_tuesday_eleven_accepts_fresh_daily_seven_fifteen_run(tmp_path):
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps(receipt()), encoding='utf-8')
    assert wait_for_inspection(path, PRODUCTS, DAY,
                               now=lambda: datetime.fromisoformat('2026-11-12T11:00:00+08:00'))['status'] == 'success'


@pytest.mark.parametrize('change', ['old_day', 'missing_module', 'duplicate', 'different_products',
                                   'failed_outcome', 'partial', 'incomplete_week', 'expired'])
def test_false_success_cannot_authorize_mail(tmp_path, change):
    value = receipt()
    if change == 'old_day': value['reference_date'] = '2026-11-11'
    if change == 'missing_module': value['outcomes'].pop()
    if change == 'duplicate': value['outcomes'].append(value['outcomes'][0])
    if change == 'different_products': value['products'] = ['M626']
    if change == 'failed_outcome': value['outcomes'][0]['status'] = 'failed'
    if change == 'partial': value['status'] = 'partial'
    if change == 'incomplete_week': value['coverage_start'] = '2026-11-04'
    if change == 'expired': value['finished_at'] = '2026-11-12T01:00:00+08:00'
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps(value), encoding='utf-8')
    with pytest.raises(GateError):
        wait(path, sleep=lambda _: pytest.fail('terminal receipt must not wait'))


def test_running_receipt_waits_only_for_owned_live_process(tmp_path):
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps(receipt('running')), encoding='utf-8')
    checked = []
    def alive(pid, created):
        checked.append((pid, created))
        return True
    def finish(_):
        path.write_text(json.dumps(receipt()), encoding='utf-8')
    assert wait(path, process_alive=alive, sleep=finish)['status'] == 'success'
    assert checked == [(1234, 42.0)]


def test_dead_or_pid_reused_runner_does_not_wait(tmp_path):
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps(receipt('running')), encoding='utf-8')
    with pytest.raises(GateError, match='INSPECTION_RUNNER_NOT_LIVE'):
        wait(path, process_alive=lambda *_: False,
             sleep=lambda _: pytest.fail('dead process must not wait'))


def test_running_gate_has_finite_timeout(tmp_path):
    path = tmp_path / 'latest.json'
    path.write_text(json.dumps(receipt('running')), encoding='utf-8')
    ticks = iter([0.0, 2.0])
    with pytest.raises(GateError, match='INSPECTION_WAIT_TIMEOUT'):
        wait(path, process_alive=lambda *_: True, monotonic=lambda: next(ticks),
             wait_seconds=1, sleep=lambda _: None)


def test_plan_replaces_repetition_and_logon_with_requested_times(tmp_path):
    specs = build_task_specs(tmp_path, env_file=tmp_path / '.env', mail_config=tmp_path / 'private.yaml')
    inspection, mail = specs
    assert inspection['name'] == 'vivo-project Yield IJP Alert Publication'
    assert inspection['module'] == 'api.inspection'
    assert inspection['triggers'] == [{'kind': 'daily', 'time': '07:15'}]
    assert mail['module'] == 'api.notification.scheduled'
    assert mail['triggers'] == [
        {'kind': 'weekly', 'weekday': 'Monday', 'time': '07:20'},
        {'kind': 'weekly', 'weekday': 'Tuesday', 'time': '11:00'},
    ]
    assert '--test-mode' not in mail['arguments']
    assert specs[0]['working_directory'] == str(tmp_path.resolve())


def test_scheduled_mail_never_calls_sender_without_verified_receipt(tmp_path, monkeypatch):
    from api.notification import scheduled
    called = []
    monkeypatch.setattr(scheduled, 'enabled_products', lambda: PRODUCTS)
    code = scheduled.main(['--receipt', str(tmp_path / 'absent.json'), '--log-dir', str(tmp_path / 'logs')],
                          sender=lambda _: called.append(True))
    assert code == 2 and not called
    logs = list((tmp_path / 'logs').glob('*.json'))
    assert len(logs) == 1
    report = json.loads(logs[0].read_text())
    assert report['status'] == 'failed' and report['code'] == 'INSPECTION_RECEIPT_UNAVAILABLE'


def test_scheduled_mail_calls_existing_entry_once_with_real_routes(tmp_path, monkeypatch):
    from api.notification import scheduled
    value = receipt()
    monkeypatch.setattr(scheduled, 'enabled_products', lambda: PRODUCTS)
    monkeypatch.setattr(scheduled, 'wait_for_inspection', lambda *a, **kw: value)
    calls = []
    def sender(argv):
        calls.append(argv)
        print(json.dumps({'status': 'complete', 'record_count': 5, 'notification_count': 2,
                          'sent_count': 2, 'complete': True, 'failed_source_count': 0, 'dry_run': False}))
        return 0
    code = scheduled.main(['--log-dir', str(tmp_path / 'logs'), '--mail-config', str(tmp_path / 'private.yaml')], sender=sender)
    assert code == 0 and len(calls) == 1
    assert '--send' in calls[0] and '--test-mode' not in calls[0]
    assert calls[0][calls[0].index('--mail-config') + 1] == str(tmp_path / 'private.yaml')
    report = json.loads(next((tmp_path / 'logs').glob('*.json')).read_text())
    assert report['inspection_run_id'] == 'fixture-run' and report['sent_count'] == 2


def test_scheduled_sender_unexpected_exception_is_secret_safe(tmp_path, monkeypatch):
    from api.notification import scheduled
    monkeypatch.setattr(scheduled, 'enabled_products', lambda: PRODUCTS)
    monkeypatch.setattr(scheduled, 'wait_for_inspection', lambda *a, **kw: receipt())
    def sender(_): raise RuntimeError('fixture-password-must-not-leak')
    assert scheduled.main(['--log-dir', str(tmp_path / 'logs')], sender=sender) == 2
    text = next((tmp_path / 'logs').glob('*.json')).read_text()
    assert 'fixture-password' not in text and 'NOTIFICATION_RUN_FAILED' in text


def test_manual_scheduled_e2e_can_isolate_recipients(tmp_path, monkeypatch):
    from api.notification import scheduled
    monkeypatch.setattr(scheduled, 'enabled_products', lambda: PRODUCTS)
    monkeypatch.setattr(scheduled, 'wait_for_inspection', lambda *a, **kw: receipt())
    seen = []
    def sender(argv):
        seen.append(argv)
        print(json.dumps({'status': 'complete', 'sent_count': 1}))
        return 0
    assert scheduled.main(['--test-mode', '--log-dir', str(tmp_path / 'logs')], sender=sender) == 0
    assert '--test-mode' in seen[0]


@pytest.mark.skipif(os.name != 'nt', reason='Windows registration plan')
def test_registration_plan_uses_future_boundaries_without_changing_tasks(tmp_path):
    root = Path(__file__).resolve().parents[3]
    environment = tmp_path / 'fixture.env'
    credentials = tmp_path / 'fixture.yaml'
    environment.write_text('API_KEY=fixture-only\n', encoding='utf-8')
    credentials.write_text('MAIL_USER: fixture-only\n', encoding='utf-8')
    completed = subprocess.run(['powershell.exe', '-NoProfile', '-File',
                                str(root / 'api/inspection/register_tasks.ps1'),
                                '-EnvFile', str(environment), '-MailConfigPath', str(credentials)],
                               capture_output=True, text=True, timeout=20)
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert report['Status'] == 'plan'
    inspection, mail = report['Tasks']
    assert inspection['Module'] == 'api.inspection'
    assert inspection['Triggers'][0]['Kind'] == 'daily'
    assert inspection['Triggers'][0]['Time'] == '07:15'
    assert datetime.fromisoformat(inspection['Triggers'][0]['StartBoundary']) > datetime.now()
    assert [trigger['Weekday'] for trigger in mail['Triggers']] == ['Monday', 'Tuesday']
    assert [trigger['Time'] for trigger in mail['Triggers']] == ['07:20', '11:00']
    assert '--test-mode' not in mail['Arguments']
    assert 'fixture-only' not in completed.stdout + completed.stderr


def test_scheduled_check_never_reads_receipt_credentials_or_writes_log(tmp_path, monkeypatch):
    from api.notification import scheduled
    monkeypatch.setattr(scheduled, 'enabled_products', lambda: PRODUCTS)
    monkeypatch.setattr(scheduled, 'wait_for_inspection', lambda *a, **kw: pytest.fail('check read receipt'))
    assert scheduled.main(['--check', '--receipt', str(tmp_path / 'absent'),
                           '--log-dir', str(tmp_path / 'logs'), '--mail-config', str(tmp_path / 'secret.yaml')]) == 0
    assert not (tmp_path / 'logs').exists()


@pytest.mark.skipif(os.name != 'nt', reason='Windows compatibility registration plans')
def test_legacy_registrars_cannot_restore_thirty_minute_schedule():
    root = Path(__file__).resolve().parents[3]
    retired = subprocess.run(['powershell.exe', '-NoProfile', '-File',
                              str(root / 'tools/register_alert_result_publication_task.ps1'), '-Force'],
                             capture_output=True, text=True, timeout=20)
    assert retired.returncode != 0 and 'registrar is retired' in retired.stderr
    qtime = subprocess.run(['powershell.exe', '-NoProfile', '-File',
                            str(root / 'tools/register_qtime_snapshot_task.ps1'), '-Plan'],
                           capture_output=True, text=True, timeout=20)
    assert qtime.returncode == 0, qtime.stderr
    report = json.loads(qtime.stdout)
    assert report['Status'] == 'plan'
    assert report['Arguments'] == '-m api.inspection.qtime_publication'
    assert report['WorkingDirectory'] == str(root)
    assert 'tools' not in report['Arguments']


@pytest.mark.skipif(os.name != 'nt', reason='Windows registration plan')
def test_inspection_only_plan_merges_warmup_without_touching_mail(tmp_path):
    root = Path(__file__).resolve().parents[3]
    environment = tmp_path/'fixture.env'
    environment.write_text('API_KEY=fixture-only\n', encoding='utf-8')
    result = subprocess.run(['powershell.exe', '-NoProfile', '-File',
                             str(root/'api/inspection/register_tasks.ps1'),
                             '-InspectionOnly', '-EnvFile', str(environment)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report['Status'] == 'plan' and len(report['Tasks']) == 1
    assert report['Tasks'][0]['Arguments'] == '-m api.inspection'
    assert report['RetireTasks'] == ['vivo-project Alert Matrix Warmup']
    retired = subprocess.run(['powershell.exe', '-NoProfile', '-File',
                              str(root/'tools/register_alert_matrix_task.ps1'), '-Force'],
                             capture_output=True, text=True, timeout=20)
    assert retired.returncode != 0 and 'registrar is retired' in retired.stderr
