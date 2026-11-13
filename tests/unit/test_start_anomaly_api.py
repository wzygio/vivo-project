"""Exercise the Windows launcher against a disposable CLI, without listeners."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / 'tools/start_anomaly_api.ps1'
pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows PowerShell launcher')


@pytest.fixture
def deployment(tmp_path):
    project = tmp_path / 'project with spaces'
    scripts = project / '.venv/Scripts'
    scripts.mkdir(parents=True)
    shutil.copy2(sys.executable, scripts / 'python.exe')
    shutil.copy2(Path(sys.executable).parents[1] / 'pyvenv.cfg', project / '.venv/pyvenv.cfg')
    tools = project / 'tools'
    tools.mkdir()
    (tools / 'anomaly_api_service.py').write_text('''import json, os, sys
from pathlib import Path
action = sys.argv[1]
with Path(os.environ['HOST_TEST_CALLS']).open('a') as stream:
    stream.write(action + '\\n')
mode = os.environ['HOST_TEST_MODE']
if mode == 'restart_error' and action == 'restart':
    print(json.dumps({'status': 'error', 'cause': 'OSError'}), file=sys.stderr)
    sys.exit(2)
if action == 'restart':
    print(json.dumps({'status': 'already_running' if mode == 'already' else 'ready'}))
elif action == 'status':
    ready = mode != 'unready'
    print(json.dumps({'ready': ready, 'supervisor_alive': ready, 'listener_owned': ready, 'port': 8890}))
else:
    raise AssertionError('Launcher must only call restart and status')
''', encoding='utf-8')
    local = tmp_path / 'local'
    profile = local / 'Tianzhu/AnomalyApi/deployment.json'
    profile.parent.mkdir(parents=True)
    profile.write_text('{}', encoding='utf-8')
    calls = tmp_path / 'calls.txt'
    return project, profile, calls, local


@pytest.mark.parametrize('mode,expected', [('ready', 0), ('already', 0), ('unready', 2), ('restart_error', 2)])
def test_launcher_checks_owned_readiness_and_preserves_cli_failure(deployment, mode, expected):
    project, profile, calls, local = deployment
    original = profile.read_bytes()
    result = subprocess.run(['powershell.exe', '-NoProfile', '-File', str(LAUNCHER),
                             '-ProjectRoot', str(project)], env=dict(os.environ, LOCALAPPDATA=str(local),
                             HOST_TEST_CALLS=str(calls), HOST_TEST_MODE=mode),
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == expected, (result.stdout, result.stderr)
    assert calls.read_text().splitlines() == (['restart'] if mode == 'restart_error' else ['restart', 'status'])
    assert profile.read_bytes() == original
    assert not (project / 'output').exists()


@pytest.mark.parametrize('missing', ['profile', 'python', 'host'])
def test_launcher_missing_prerequisite_does_not_start_or_configure(deployment, missing):
    project, profile, calls, local = deployment
    target = {'profile': profile, 'python': project / '.venv/Scripts/python.exe',
              'host': project / 'tools/anomaly_api_service.py'}[missing]
    target.unlink()
    result = subprocess.run(['powershell.exe', '-NoProfile', '-File', str(LAUNCHER),
                             '-ProjectRoot', str(project)], env=dict(os.environ, LOCALAPPDATA=str(local),
                             HOST_TEST_CALLS=str(calls), HOST_TEST_MODE='ready'),
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert not calls.exists()
    assert not target.exists()


@pytest.mark.parametrize('mode,expected', [('ready', 0), ('unready', 2)])
def test_double_click_entry_finds_its_project_and_preserves_exit_code(deployment, mode, expected):
    project, profile, calls, local = deployment
    shutil.copy2(LAUNCHER, project / 'tools/start_anomaly_api.ps1')
    entry = project / 'start_anomaly_api.cmd'
    shutil.copy2(ROOT / 'start_anomaly_api.cmd', entry)
    result = subprocess.run(['cmd.exe', '/d', '/c', str(entry)], cwd=local,
                            env=dict(os.environ, LOCALAPPDATA=str(local),
                            HOST_TEST_CALLS=str(calls), HOST_TEST_MODE=mode),
                            input='\n', capture_output=True, text=True, timeout=20)
    assert result.returncode == expected, (result.stdout, result.stderr)
    assert calls.read_text().splitlines() == ['restart', 'status']
    assert profile.read_text() == '{}'
