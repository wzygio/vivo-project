"""SMTP contract failures and explicit configuration are tested without real mail."""
from dataclasses import replace
import json
from pathlib import Path
import smtplib

import pytest
import yaml

from api.notification.__main__ import main
from api.notification.application import DeliveryFailure, run_notification
from api.notification.core import NotificationError, plan_notifications
from api.notification.infrastructure import HttpAlertSource, SmtpDelivery
from api.notification.settings import MailCredentials, load_environment, load_mail_credentials, load_settings
from tests.unit.api.test_notification import ROOT, TEST_INBOX, result, row


@pytest.fixture
def settings():
    return load_settings(ROOT / 'config/domain/api_domain.yaml')


class FakeSmtp:
    def __init__(self, *, auth_fail=False, refused=False, send_error=False):
        self.auth_fail, self.refused, self.send_error = auth_fail, refused, send_error
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.calls.append('closed')

    def ehlo(self):
        self.calls.append('ehlo')

    def starttls(self, **kwargs):
        self.calls.append('tls')
        assert kwargs['context'].check_hostname

    def login(self, user, password):
        self.calls.append('login')
        if self.auth_fail:
            raise smtplib.SMTPAuthenticationError(535, b'private-server-detail')

    def send_message(self, message, *, from_addr, to_addrs):
        self.calls.append('send')
        assert to_addrs == [TEST_INBOX] and message['To'] == TEST_INBOX
        assert message['Cc'] is None and message['Bcc'] is None
        if self.send_error:
            raise smtplib.SMTPServerDisconnected('private-server-detail')
        return {TEST_INBOX: (550, b'refused')} if self.refused else {}


def deliver(settings, servers):
    factories = iter(servers)
    delivery = SmtpDelivery(settings, MailCredentials('user', 'sender@example.com', ('bad', 'good')),
                            smtp_factory=lambda *args, **kwargs: next(factories))
    plan, = plan_notifications(result([row()]), settings, test_mode=True)
    delivery.send(plan)


def test_authentication_only_retry_and_tls(settings):
    refused, success = FakeSmtp(auth_fail=True), FakeSmtp()
    deliver(settings, [refused, success])
    assert refused.calls == ['ehlo', 'tls', 'ehlo', 'login', 'closed']
    assert success.calls == ['ehlo', 'tls', 'ehlo', 'login', 'send', 'closed']


@pytest.mark.parametrize('failure', ['auth', 'refused', 'send'])
def test_delivery_failure_never_exposes_server_details_or_retries_ambiguous_send(settings, failure):
    first = FakeSmtp(auth_fail=failure == 'auth', refused=failure == 'refused', send_error=failure == 'send')
    second = FakeSmtp(auth_fail=True)
    with pytest.raises(NotificationError) as exc:
        deliver(settings, [first, second])
    assert 'private-server-detail' not in str(exc.value)
    assert bool(second.calls) == (failure == 'auth')


def test_ssl_uses_verified_context(settings):
    seen = []
    server = FakeSmtp()
    def factory(host, port, **kwargs):
        seen.append(kwargs)
        return server
    plan, = plan_notifications(result([row()]), settings, test_mode=True)
    SmtpDelivery(replace(settings, smtp_security='ssl'),
                 MailCredentials('user', 'sender@example.com', ('password',)), smtp_factory=factory).send(plan)
    assert seen[0]['context'].check_hostname and 'tls' not in server.calls


def test_http_adapter_no_query_parameters(settings):
    class Response:
        status, headers = 200, {}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self, limit): return json.dumps(result([row()])).encode()
    class Opener:
        def open(self, request, timeout):
            assert request.full_url == settings.api_base_url + '/api/v1/sheet-anomalies'
            assert request.get_header('X-api-key') == 'fixture-only'
            return Response()
    assert len(HttpAlertSource(settings, 'fixture-only', opener=Opener()).fetch()['data']) == 1


def test_abort_and_bad_route_before_any_delivery(settings):
    sent = []
    class Delivery:
        def send(self, plan): sent.append(plan)
    class Source:
        def fetch(self): return result([row(), row(factory='UNKNOWN')])
    with pytest.raises(NotificationError):
        run_notification(Source(), Delivery(), settings)
    assert not sent
    class Partial:
        def fetch(self):
            return result([row()], [{'product': 'M626', 'sub_domain': 'CTQ', 'event_type': 'OOS'}])
    with pytest.raises(NotificationError):
        run_notification(Partial(), Delivery(), replace(settings, incomplete_policy='abort'))
    assert not sent


@pytest.mark.parametrize('change', ['duplicate', 'missing', 'unknown', 'invalid_email', 'timeout', 'security'])
def test_invalid_configuration_fails_closed(tmp_path, change):
    config = yaml.safe_load((ROOT / 'config/domain/api_domain.yaml').read_text())
    conf = config['notification']
    if change == 'duplicate': conf['routes'].append(conf['routes'][0])
    if change == 'missing': conf['routes'].pop()
    if change == 'unknown': conf['routes'][0]['sub_domains'] = ['OTHER']
    if change == 'invalid_email': conf['routes'][0]['recipients'] = ['bad\r\nBcc: other@example.com']
    if change == 'timeout': conf['api_timeout_seconds'] = float('nan')
    if change == 'security': conf['smtp']['security'] = 'auto'
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    with pytest.raises(NotificationError): load_settings(path)


def test_credentials_have_no_repr_and_reference_recipients_ignored(tmp_path):
    path = tmp_path / 'private.yaml'
    path.write_text(yaml.safe_dump({'MAIL_USER': 'fixture-user', 'MAIL_SENDER': 'sender@example.com',
                                  'MAIL_PASSWORDS': ['fixture-password'],
                                  'MAIL_RECEIVERS': ['unwanted@example.com'], 'MAIL_CC': ['unwanted@example.com']}))
    credentials = load_mail_credentials(path)
    assert credentials.passwords == ('fixture-password',)
    assert 'fixture-password' not in repr(credentials) and 'fixture-user' not in repr(credentials)
    assert not hasattr(credentials, 'receivers')


def test_cli_defaults_preview_and_partial_exit(monkeypatch, capsys):
    from api.notification import __main__ as cli
    class Source:
        def __init__(self, *args): pass
        def fetch(self): return result([row()])
    monkeypatch.setattr(cli, 'HttpAlertSource', Source)
    monkeypatch.setattr(cli, 'load_mail_credentials', lambda *args: pytest.fail('preview read mail credentials'))
    assert main(['--test-mode']) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['dry_run'] and report['sent_count'] == 0 and report['notification_count'] == 1
    Source.fetch = lambda self: result(failures=[{'product': 'M626', 'sub_domain': 'SPC', 'event_type': 'CPK'}])
    assert main(['--test-mode']) == 3
    assert json.loads(capsys.readouterr().out)['status'] == 'partial'


def test_cli_missing_credentials_safe_failure(monkeypatch, capsys):
    for key in ('MAIL_USER', 'MAIL_SENDER', 'MAIL_PASSWORD', 'MAIL_PASSWORDS'):
        monkeypatch.delenv(key, raising=False)
    assert main(['--send']) == 2
    assert json.loads(capsys.readouterr().out)['status'] == 'failed'


def test_partial_delivery_failure_preserves_confirmed_counts(settings, monkeypatch, capsys):
    from api.notification import __main__ as cli
    class Source:
        def __init__(self, *args): pass
        def fetch(self): return result([row(), row('AOI_TT')])
    class Delivery:
        def __init__(self, *args): self.sent = 0
        def send(self, plan):
            if self.sent:
                raise NotificationError('SMTP 投递未确认成功。')
            self.sent += 1
    monkeypatch.setattr(cli, 'HttpAlertSource', Source)
    monkeypatch.setattr(cli, 'SmtpDelivery', Delivery)
    monkeypatch.setattr(cli, 'load_mail_credentials', lambda *args, **kwargs: object())
    assert main(['--send']) == 2
    failure = json.loads(capsys.readouterr().out)
    assert failure['status'] == 'failed' and failure['sent_count'] == 1
    assert failure['notification_count'] == 2 and failure['record_count'] == 2


def test_explicit_env_file_only_reads_notification_keys_and_does_not_change_process(tmp_path, monkeypatch):
    monkeypatch.delenv('MAIL_PASSWORD', raising=False)
    monkeypatch.setenv('API_KEY', 'process-fixture')
    path = tmp_path / 'fixture.env'
    path.write_text('API_KEY=file-fixture\nMAIL_PASSWORD=fixture-only\nUNRELATED=ignored\n', encoding='utf-8')
    env = load_environment(path)
    assert env['API_KEY'] == 'process-fixture' and env['MAIL_PASSWORD'] == 'fixture-only'
    assert 'UNRELATED' not in env
    import os
    assert 'MAIL_PASSWORD' not in os.environ


def test_malformed_env_does_not_log_source_lines(tmp_path, capsys, caplog):
    path = tmp_path / 'invalid.env'
    path.write_text('API_KEY="fixture-secret" trailing junk\n', encoding='utf-8')
    with pytest.raises(NotificationError):
        load_environment(path, environ={})
    assert 'fixture-secret' not in capsys.readouterr().err + caplog.text


def test_process_aliases_override_file_aliases(tmp_path):
    path = tmp_path / 'aliases.env'
    path.write_text('API_KEY=file-fixture\nMAIL_PASSWORDS=file-fixture\n', encoding='utf-8')
    env = load_environment(path, environ={'VIVO_API_KEY': 'process-fixture', 'MAIL_PASSWORD': 'process-password'})
    assert env['API_KEY'] == 'process-fixture'
    assert env['MAIL_PASSWORD'] == 'process-password' and not env['MAIL_PASSWORDS']
