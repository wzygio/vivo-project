"""Minimal real listeners: host ownership, crash restart, every-exit cleanup."""
from hashlib import sha256
import json
import os
import secrets
import socket
import time
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

import pytest

from tools import anomaly_api_service as host

pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows user-session host')


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    runtime = host.ROOT / 'output/test-results/api-host' / uuid4().hex
    monkeypatch.setattr(host, 'LOGS', runtime)
    monkeypatch.setattr(host, 'STATE', runtime / 'service.json')
    with socket.socket() as reserve:
        reserve.bind(('127.0.0.1', 0))
        port = reserve.getsockname()[1]
    key = secrets.token_urlsafe(32)
    profile = tmp_path / 'deployment.json'
    profile.write_text(json.dumps({
        'version': 1, 'project_root': str(host.ROOT), 'host': '127.0.0.1', 'port': port,
        'clients': [{'client_id': 'lifecycle-fixture', 'key_sha256': sha256(key.encode()).hexdigest(),
                     'products': ['FIXTURE'], 'factories': ['ARRAY'], 'sub_domains': ['SPC']}],
    }), encoding='utf-8')
    yield profile, port, key
    if host.STATE.exists():
        state = json.loads(host.STATE.read_text(encoding='utf-8'))
        if host.same_process(state['supervisor']):
            host.stop()
        assert not host.same_process(state['supervisor'])
        assert not any(host.same_process(record) for record in state['owned'])
    with socket.socket() as check:
        check.settimeout(.1)
        assert check.connect_ex(('127.0.0.1', port)) != 0


@pytest.mark.parametrize('mode', ['normal', 'failure', 'interrupt'])
def test_host_owned_listener_and_cleanup_on_every_exit(deployment, monkeypatch, mode):
    profile, port, key = deployment
    if mode != 'normal':
        def interrupted(*args, **kwargs):
            if mode == 'failure':
                raise RuntimeError('simulated failure')
            raise KeyboardInterrupt('simulated interrupt')
        monkeypatch.setattr(host, 'health', interrupted)
        expected = RuntimeError if mode == 'failure' else KeyboardInterrupt
        with pytest.raises(expected, match='simulated'):
            host.start(profile, False)
        intents = list(host.LOGS.glob('launch-*.json'))
        assert intents and all(json.loads(p.read_text())['cleanup_complete'] for p in intents)
    else:
        started = host.start(profile, False)
        assert started['status'] == 'ready'
        assert host.start(profile, False)['status'] == 'already_running'
        request = Request(f'http://127.0.0.1:{port}/openapi.json', headers={'X-API-Key': key})
        with build_opener(ProxyHandler({})).open(request, timeout=3) as response:
            assert response.status == 200
        assert host.stop()['owned_listener_released']


def test_host_restarts_only_its_crashed_worker(deployment):
    profile, port, _ = deployment
    host.start(profile, False)
    state = json.loads(host.STATE.read_text())
    for record in reversed(state['owned']):
        process = host.same_process(record)
        if process:
            process.kill()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        state = json.loads(host.STATE.read_text())
        if state['restarts'] and host.listener_owned(state['owned'], port) and host.health(port):
            break
        time.sleep(.2)
    else:
        pytest.fail('Supervisor did not recover the crashed worker')
    assert host.same_process(state['supervisor'])


def test_occupied_port_is_preserved_without_launch(deployment):
    profile, port, _ = deployment
    with socket.socket() as existing:
        existing.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        existing.bind(('127.0.0.1', port))
        existing.listen()
        with pytest.raises(OSError):
            host.start(profile, False)
        assert not host.STATE.exists()
        assert existing.getsockname()[1] == port


def test_profile_rejects_raw_key_fields(deployment):
    profile, _, _ = deployment
    config = json.loads(profile.read_text())
    config['clients'][0]['key'] = 'invalid-fixture-field'
    profile.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        host.load_profile(profile)
