"""Real Windows restart lifecycle against an isolated, DB-free health app."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

import psutil
import pytest


ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows API launcher')


@pytest.fixture
def isolated_host(monkeypatch):
    directory = ROOT / 'output/test-results/api-one-click-restart' / uuid4().hex
    project = directory / 'isolated project'
    scripts, tools, api = (project / name for name in ('.venv/Scripts', 'tools', 'api'))
    for path in (scripts, tools, api):
        path.mkdir(parents=True)
    shutil.copy2(sys.executable, scripts / 'python.exe')
    shutil.copy2(Path(sys.executable).parents[1] / 'pyvenv.cfg', project / '.venv/pyvenv.cfg')
    for name in ('anomaly_api_service.py', 'start_anomaly_api.ps1'):
        shutil.copy2(ROOT / 'tools' / name, tools / name)
    shutil.copy2(ROOT / 'start_anomaly_api.cmd', project / 'start_anomaly_api.cmd')
    shutil.copy2(ROOT / 'api/settings.py', api / 'settings.py')
    (api / '__init__.py').write_text('', encoding='utf-8')
    (api / 'main.py').write_text('''from fastapi import FastAPI
def create_app():
    app = FastAPI()
    @app.get('/health/live')
    @app.get('/health/ready')
    def health():
        return {'status': 'ok'}
    return app
''', encoding='utf-8')
    monkeypatch.setenv('PYTHONPATH', str(ROOT / '.venv/Lib/site-packages'))
    local = directory / 'local'
    monkeypatch.setenv('LOCALAPPDATA', str(local))
    with socket.socket() as reserve:
        reserve.bind(('127.0.0.1', 0))
        port = reserve.getsockname()[1]
    profile = local / 'Tianzhu/AnomalyApi/deployment.json'
    profile.parent.mkdir(parents=True)
    profile.write_text(json.dumps({
        'version': 1, 'project_root': str(project), 'host': '127.0.0.1', 'port': port,
        'clients': [{'client_id': 'isolated-lifecycle', 'key_sha256': '1' * 64,
                     'products': ['FIXTURE'], 'factories': ['ARRAY'], 'sub_domains': ['SPC']}],
    }), encoding='utf-8')
    original_path = list(sys.path)
    spec = importlib.util.spec_from_file_location('isolated_host_' + uuid4().hex,
                                                 tools / 'anomaly_api_service.py')
    host = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(host)
    records = {}
    evidence = directory / 'lifecycle.json'

    def record(items):
        for item in items:
            records[item['pid'], item['create_time']] = item

    def remember():
        paths = list(host.LOGS.glob('launch-*.json'))
        if host.STATE.exists():
            paths.append(host.STATE)
        for path in paths:
            state = json.loads(path.read_text(encoding='utf-8'))
            if state.get('supervisor'):
                record([state['supervisor']])
                process = host.same_process(state['supervisor'])
                if process:
                    record(host.family(process))
            record(state.get('owned', []))
        evidence.write_text(json.dumps({'port': port, 'project': str(project),
                                       'processes': list(records.values()), 'cleanup_complete': False}, indent=2),
                            encoding='utf-8')

    def run_launcher():
        # Record the command process immediately, before waiting for its children.
        process = subprocess.Popen(['powershell.exe', '-NoProfile', '-File', str(tools / 'start_anomaly_api.ps1')],
                                   cwd=project, env=os.environ.copy(), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        record([host.identity(psutil.Process(process.pid))])
        remember()
        try:
            stdout, stderr = process.communicate(timeout=45)
            assert process.returncode == 0, (stdout, stderr)
        finally:
            if process.poll() is None:
                record(host.family(psutil.Process(process.pid)))
                host.stop_owned(list(records.values()))
                process.wait(timeout=5)
            remember()

    remember()  # Establish port/process ownership evidence before any service launch.
    try:
        yield SimpleNamespace(host=host, profile=profile, port=port, project=project,
                              remember=remember, run_launcher=run_launcher)
    finally:
        try:
            remember()
            if host.STATE.exists():
                host.stop()
        finally:
            try:
                remember()
                host.stop_owned(list(records.values()))
                remaining = [item for item in records.values() if host.same_process(item)]
                with socket.socket() as check:
                    check.settimeout(.2)
                    released = check.connect_ex(('127.0.0.1', port)) != 0
                evidence.write_text(json.dumps({'port': port, 'project': str(project),
                    'processes': list(records.values()), 'remaining': remaining,
                    'listener_released': released, 'cleanup_complete': not remaining and released}, indent=2),
                    encoding='utf-8')
                assert not remaining and released
            finally:
                sys.path[:] = original_path


def test_one_click_replaces_old_process_family_and_keeps_profile(isolated_host):
    fixture = isolated_host
    before = fixture.profile.read_bytes()
    fixture.run_launcher()
    previous = json.loads(fixture.host.STATE.read_text(encoding='utf-8'))
    fixture.remember()
    fixture.run_launcher()
    current = json.loads(fixture.host.STATE.read_text(encoding='utf-8'))
    assert current['supervisor'] != previous['supervisor']
    assert not any(fixture.host.same_process(item) for item in [previous['supervisor'], *previous['owned']])
    assert fixture.host.listener_owned(current['owned'], fixture.port)
    assert fixture.host.health(fixture.port)
    assert fixture.profile.read_bytes() == before


def test_foreign_listener_is_preserved_without_starting_service(isolated_host):
    fixture = isolated_host
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        listener.bind(('127.0.0.1', fixture.port))
        listener.listen()
        with pytest.raises(OSError):
            fixture.host.restart(fixture.profile)
        assert not fixture.host.STATE.exists()
        with socket.socket() as check:
            check.settimeout(.2)
            assert check.connect_ex(('127.0.0.1', fixture.port)) == 0


@pytest.mark.parametrize('mode', ['failure', 'interrupt'])
def test_restart_cleans_replacement_after_failure_or_interruption(isolated_host, monkeypatch, mode):
    fixture = isolated_host
    fixture.host.restart(fixture.profile)
    previous = json.loads(fixture.host.STATE.read_text(encoding='utf-8'))
    fixture.remember()

    def interrupted(*args, **kwargs):
        if mode == 'failure':
            raise RuntimeError('simulated readiness failure')
        raise KeyboardInterrupt('simulated interruption')

    monkeypatch.setattr(fixture.host, 'health', interrupted)
    expected = RuntimeError if mode == 'failure' else KeyboardInterrupt
    with pytest.raises(expected, match='simulated'):
        fixture.host.restart(fixture.profile)
    fixture.remember()
    current = json.loads(fixture.host.STATE.read_text(encoding='utf-8'))
    assert current['cleanup_complete'] is True
    assert not any(fixture.host.same_process(item) for item in [previous['supervisor'], *previous['owned'],
                                                                              current['supervisor'], *current['owned']])
