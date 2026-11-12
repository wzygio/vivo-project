"""Real HTTP → configured routing → real SMTP socket, with owned-service cleanup."""
from contextlib import contextmanager
from dataclasses import replace
from email import policy
from email.parser import BytesParser
import json
import os
from pathlib import Path
import socket
import socketserver
import threading

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import psutil
import pytest

from api.notification.application import run_notification
from api.notification.core import NotificationError
from api.notification.infrastructure import HttpAlertSource, SmtpDelivery
from api.notification.settings import MailCredentials, load_settings
from tests.e2e.test_anomaly_api_sheet_membership_e2e import _live_server
from tests.unit.api.test_notification import ROOT, TEST_INBOX, result, row


EVIDENCE = ROOT / 'output/test-results/alert-notification'


@contextmanager
def smtp_server(evidence: Path):
    messages = []
    class Handler(socketserver.StreamRequestHandler):
        timeout = 5
        def reply(self, line):
            self.wfile.write(line + b'\r\n')
            self.wfile.flush()
        def handle(self):
            self.reply(b'220 fixture SMTP')
            recipients = []
            while line := self.rfile.readline():
                command = line.decode('ascii').strip()
                verb = command.split(' ', 1)[0].upper()
                if verb in ('EHLO', 'HELO'):
                    self.reply(b'250-localhost')
                    self.reply(b'250 AUTH PLAIN')
                elif verb == 'AUTH': self.reply(b'235 authenticated')
                elif verb == 'MAIL': self.reply(b'250 sender accepted')
                elif verb == 'RCPT':
                    recipients.append(command.split(':', 1)[1].strip('<> '))
                    self.reply(b'250 recipient accepted')
                elif verb == 'DATA':
                    self.reply(b'354 send data')
                    body = []
                    while (line := self.rfile.readline()) != b'.\r\n':
                        if not line: return
                        body.append(line[1:] if line.startswith(b'..') else line)
                    messages.append((list(recipients), BytesParser(policy=policy.default).parsebytes(b''.join(body))))
                    recipients.clear()
                    self.reply(b'250 accepted')
                elif verb == 'QUIT':
                    self.reply(b'221 goodbye')
                    return
                elif verb == 'RSET': self.reply(b'250 reset')
                else: self.reply(b'500 unsupported')
    process = psutil.Process(os.getpid())
    before = {(p.pid, p.create_time()) for p in process.children(recursive=True)}
    server = socketserver.TCPServer(('127.0.0.1', 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02},
                              name=f'notification-smtp-{port}', daemon=True)
    record = {'runner_pid': process.pid, 'runner_created': process.create_time(),
              'owned_processes': [], 'owned_thread': thread.name, 'port': port, 'cleaned': False}
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(json.dumps(record), encoding='utf-8')
    started = False
    try:
        thread.start()
        started = True
        yield port, messages
    finally:
        if started:
            server.shutdown()
            thread.join(timeout=10)
        server.server_close()
        with socket.socket() as probe:
            probe.settimeout(.2)
            listening = probe.connect_ex(('127.0.0.1', port)) == 0
        after = {(p.pid, p.create_time()) for p in process.children(recursive=True)}
        record.update(thread_alive=thread.is_alive(), listening=listening, unexpected_children=sorted(after - before))
        record['cleaned'] = not thread.is_alive() and not listening and after == before
        evidence.write_text(json.dumps(record), encoding='utf-8')
        assert record['cleaned']


@pytest.mark.parametrize('outcome', ['normal', 'failure', 'interrupt'])
def test_both_listener_cleanup_on_all_exit_paths(outcome):
    app = FastAPI()
    @app.get('/health/live')
    def health(): return {'status': 'ok'}
    try:
        with smtp_server(EVIDENCE / f'lifecycle-{outcome}-smtp.json'):
            with _live_server(app, EVIDENCE / f'lifecycle-{outcome}-http.json'):
                if outcome == 'failure': raise RuntimeError('expected fixture failure')
                if outcome == 'interrupt': raise KeyboardInterrupt()
    except (RuntimeError, KeyboardInterrupt):
        assert outcome != 'normal'


@pytest.mark.parametrize('partial', [False, True])
def test_real_http_routing_smtp_attachment(partial):
    rows = [row(d, f) for d in ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time')
            for f in ('ARRAY', 'OLED', 'TP')]
    failures = [{'product': 'M626', 'sub_domain': 'SPC', 'event_type': 'CPM'}] if partial else []
    payload, requests = result(rows, failures), []
    app = FastAPI()
    @app.get('/health/live')
    def health(): return {'status': 'ok'}
    @app.get('/api/v1/sheet-anomalies')
    def alerts(request: Request):
        requests.append(request.url.query)
        assert request.headers.get('X-API-Key') == 'fixture-key'
        return JSONResponse(payload)
    label = 'partial' if partial else 'complete'
    with smtp_server(EVIDENCE / f'{label}-smtp.json') as (port, messages):
        with _live_server(app, EVIDENCE / f'{label}-http.json') as base:
            settings = replace(load_settings(ROOT / 'config/domain/api_domain.yaml'),
                               api_base_url=base, smtp_host='127.0.0.1', smtp_port=port, smtp_security='plain')
            report = run_notification(HttpAlertSource(settings, 'fixture-key'),
                                      SmtpDelivery(settings, MailCredentials('fixture', 'sender@example.com', ('fixture',))),
                                      settings, test_mode=True)
            assert report.sent_count == 1 and report.record_count == 18 and report.complete is not partial
    assert requests == ['']
    (recipients, message), = messages
    assert recipients == [TEST_INBOX] and message['To'] == TEST_INBOX
    assert message['Cc'] is None and message['Bcc'] is None
    attachment, = message.iter_attachments()
    import csv
    from io import StringIO
    records = list(csv.DictReader(StringIO(attachment.get_payload(decode=True).decode('utf-8-sig'))))
    assert len(records) == 18
    assert all(json.loads(r['对象编号']) == ['00001', '00002'] for r in records)
    assert ('部分来源不可用' in str(message['Subject'])) is partial
