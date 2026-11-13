"""Restart preflight and ordering; no subprocesses or network listeners."""
from contextlib import nullcontext
import json
import os

import pytest

from tools import anomaly_api_service as host


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    state = tmp_path / 'service.json'
    monkeypatch.setattr(host, 'STATE', state)
    monkeypatch.setattr(host, 'host_lock', lambda *args: nullcontext())
    config = {'project_root': str(host.ROOT), 'host': '127.0.0.1', 'port': 18090}
    monkeypatch.setattr(host, 'load_profile', lambda _: config)
    calls = []
    monkeypatch.setattr(host, 'stop', lambda: calls.append('stop') or {'owned_listener_released': True})
    monkeypatch.setattr(host, 'start', lambda profile, startup: calls.append('start') or {'status': 'ready'})
    return state, config, calls, tmp_path / 'profile.json'


def test_restart_stops_existing_instance_before_starting(deployment):
    state, config, calls, profile = deployment
    state.write_text(json.dumps(config), encoding='utf-8')
    assert host.restart(profile)['status'] == 'ready'
    assert calls == ['stop', 'start']


def test_first_launch_does_not_require_existing_state(deployment):
    _, _, calls, profile = deployment
    assert host.restart(profile)['status'] == 'ready'
    assert calls == ['start']


def test_equivalent_profile_path_does_not_block_restart(deployment):
    state, config, calls, profile = deployment
    state.write_text(json.dumps(config), encoding='utf-8')
    config['project_root'] = str(host.ROOT / 'tools' / '..')
    assert host.restart(profile)['status'] == 'ready'
    assert calls == ['stop', 'start']


def test_invalid_profile_leaves_existing_instance_untouched(deployment, monkeypatch):
    state, config, calls, profile = deployment
    state.write_text(json.dumps(config), encoding='utf-8')

    def reject(_):
        raise ValueError('invalid profile')

    monkeypatch.setattr(host, 'load_profile', reject)
    with pytest.raises(ValueError):
        host.restart(profile)
    assert calls == []


@pytest.mark.parametrize('change', [
    {'project_root': 'another-project'}, {'host': '0.0.0.0'}, {'port': 18091},
])
def test_different_deployment_identity_is_not_stopped(deployment, change):
    state, config, calls, profile = deployment
    state.write_text(json.dumps(config | change), encoding='utf-8')
    with pytest.raises(RuntimeError):
        host.restart(profile)
    assert calls == []


def test_cleanup_failure_does_not_start_another_instance(deployment, monkeypatch):
    state, config, calls, profile = deployment
    state.write_text(json.dumps(config), encoding='utf-8')

    def failed_stop():
        calls.append('stop')
        raise RuntimeError('listener remains active')

    monkeypatch.setattr(host, 'stop', failed_stop)
    with pytest.raises(RuntimeError):
        host.restart(profile)
    assert calls == ['stop']


@pytest.mark.skipif(os.name != 'nt', reason='Windows process lock')
def test_busy_restart_lock_does_not_start_or_stop_processes(tmp_path, monkeypatch):
    monkeypatch.setattr(host, 'LOGS', tmp_path)
    monkeypatch.setattr(host, 'load_profile', lambda _: {})
    monkeypatch.setattr(host, 'start', lambda *args: pytest.fail('Unexpected start'))
    monkeypatch.setattr(host, 'stop', lambda: pytest.fail('Unexpected stop'))
    with host.host_lock('restart.lock'):
        with pytest.raises(OSError):
            host.restart(tmp_path / 'profile.json')
