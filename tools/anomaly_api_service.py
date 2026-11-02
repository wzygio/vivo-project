"""Manage the per-user Windows API host without touching the Streamlit service.

The durable user profile contains authorization digests only. Workers receive
VIVO_API_CLIENTS through their environment; raw API keys are never stored here.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging.config
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
LOGS = ROOT / 'output/logs/anomaly-api'
STATE = LOGS / 'service.json'
PROFILE = Path(os.environ.get('LOCALAPPDATA', str(Path.home()))) / 'Tianzhu/AnomalyApi/deployment.json'


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{uuid4().hex}.tmp')
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=True, indent=2), encoding='utf-8')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_profile(path: Path) -> dict:
    from api.settings import ApiClient
    config = json.loads(path.read_text(encoding='utf-8-sig'))
    if (set(config) != {'version', 'project_root', 'host', 'port', 'clients'}
            or config['version'] != 1 or Path(config['project_root']).resolve() != ROOT.resolve()
            or config['host'] not in ('127.0.0.1', '0.0.0.0')
            or type(config['port']) is not int or not 1024 <= config['port'] <= 65535
            or not isinstance(config['clients'], list) or not 1 <= len(config['clients']) <= 256):
        raise ValueError('Invalid deployment profile')
    identities, digests = set(), set()
    for item in config['clients']:
        if set(item) != {'client_id', 'key_sha256', 'products', 'factories', 'sub_domains'}:
            raise ValueError('Invalid client profile')
        if any(not isinstance(item[name], list) for name in ('products', 'factories', 'sub_domains')):
            raise ValueError('Invalid client scope')
        client = ApiClient(item['client_id'], item['key_sha256'], tuple(item['products']),
                           tuple(item['factories']), tuple(item['sub_domains']))
        if client.key_hash == '0' * 64 or client.client_id in identities or client.key_hash in digests:
            raise ValueError('Invalid or duplicate client identity')
        identities.add(client.client_id)
        digests.add(client.key_hash)
    return config


def identity(process: psutil.Process) -> dict:
    return {'pid': process.pid, 'create_time': process.create_time()}


def same_process(record: dict) -> psutil.Process | None:
    try:
        process = psutil.Process(record['pid'])
        return process if process.create_time() == record['create_time'] else None
    except psutil.NoSuchProcess:
        return None


def family(process: psutil.Process) -> list[dict]:
    records = []
    try:
        candidates = [process, *process.children(recursive=True)]
    except psutil.NoSuchProcess:
        candidates = []
    for member in candidates:
        try:
            records.append(identity(member))
        except psutil.NoSuchProcess:
            pass
    return records


def stop_owned(records: list[dict]) -> None:
    processes = [process for record in reversed(records) if (process := same_process(record))]
    for process in processes:
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(processes, timeout=5)
    for process in alive:
        record = next(item for item in records if item['pid'] == process.pid)
        candidate = same_process(record)
        if candidate:
            try:
                candidate.kill()
            except psutil.NoSuchProcess:
                pass
    psutil.wait_procs(alive, timeout=3)
    if any(same_process(record) for record in records):
        raise RuntimeError('Owned process cleanup incomplete')


def hidden_flags() -> int:
    return subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0


def health(port: int, route: str = 'ready') -> bool:
    try:
        with build_opener(ProxyHandler({})).open(f'http://127.0.0.1:{port}/health/{route}', timeout=1) as response:
            return response.status == 200 and json.load(response) == {'status': 'ok'}
    except OSError:
        return False


def listener_owned(records: list[dict], port: int) -> bool:
    for record in records:
        process = same_process(record)
        if process:
            try:
                if any(c.status == psutil.CONN_LISTEN and c.laddr.port == port
                       for c in process.net_connections(kind='tcp')):
                    return True
            except psutil.NoSuchProcess:
                pass
    return False


@contextmanager
def host_lock():
    import msvcrt
    LOGS.mkdir(parents=True, exist_ok=True)
    with (LOGS / 'host.lock').open('a+b') as handle:
        if handle.tell() == 0:
            handle.write(b'0')
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def worker(config: dict) -> None:
    import uvicorn
    os.environ['VIVO_API_CLIENTS'] = json.dumps(config['clients'])
    LOGS.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig({
        'version': 1, 'disable_existing_loggers': False,
        'formatters': {'safe': {'format': '%(asctime)s %(levelname)s %(name)s %(message)s'}},
        'handlers': {'file': {'class': 'logging.handlers.TimedRotatingFileHandler',
                             'filename': str(LOGS / 'server.log'), 'when': 'midnight',
                             'backupCount': 14, 'encoding': 'utf-8', 'formatter': 'safe'}},
        'root': {'handlers': ['file'], 'level': 'INFO'},
        'loggers': {'uvicorn': {'handlers': [], 'propagate': True, 'level': 'INFO'},
                    'uvicorn.error': {'handlers': [], 'propagate': True, 'level': 'INFO'},
                    'uvicorn.access': {'handlers': [], 'propagate': False}},
    })
    uvicorn.run('api.main:create_app', factory=True, host=config['host'], port=config['port'],
                workers=1, access_log=False, proxy_headers=False, server_header=False,
                timeout_graceful_shutdown=20, log_config=None)


def supervise(profile: Path, launch_id: str) -> None:
    owned = {}
    launched = None
    stop_file = LOGS / f'stop-{launch_id}'
    with host_lock():
        config = load_profile(profile)
        state = {'launch_id': launch_id, 'supervisor': identity(psutil.Process()),
                 'host': config['host'], 'port': config['port'], 'project_root': str(ROOT),
                 'started_at': datetime.now(timezone.utc).isoformat(), 'restarts': 0,
                 'status': 'starting', 'owned': []}
        save(STATE, state)
        try:
            while not stop_file.exists():
                if launched is None or launched.poll() is not None:
                    # Kill any verified orphan before restarting its launcher.
                    stop_owned(list(owned.values()))
                    owned.clear()
                    if launched is not None:
                        state['restarts'] += 1
                        delay_end = time.monotonic() + min(30, state['restarts'] * 2)
                        while time.monotonic() < delay_end and not stop_file.exists():
                            time.sleep(.2)
                        if stop_file.exists():
                            break
                    load_profile(profile)  # Reject invalid replacements before another launch.
                    launched = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'worker',
                                                 '--profile', str(profile), '--runtime-dir', str(LOGS)], cwd=ROOT,
                                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                                stderr=subprocess.DEVNULL, creationflags=hidden_flags())
                    state['status'] = 'running'
                try:
                    for member in family(psutil.Process(launched.pid)):
                        owned[(member['pid'], member['create_time'])] = member
                except psutil.NoSuchProcess:
                    continue
                state['owned'] = list(owned.values())
                save(STATE, state)
                time.sleep(.5)
        finally:
            if launched is not None:
                try:
                    for member in family(psutil.Process(launched.pid)):
                        owned[(member['pid'], member['create_time'])] = member
                except psutil.NoSuchProcess:
                    pass
            stop_owned(list(owned.values()))
            state.update(status='stopped', owned=list(owned.values()), cleanup_complete=True)
            save(STATE, state)
            stop_file.unlink(missing_ok=True)


def install_startup(profile: Path) -> Path:
    import win32com.client
    startup = Path(os.environ['APPDATA']) / 'Microsoft/Windows/Start Menu/Programs/Startup'
    link = startup / 'Tianzhu Anomaly API.lnk'
    if link.exists():
        raise FileExistsError('Startup entry already exists; verify it before replacing it')
    shortcut = win32com.client.Dispatch('WScript.Shell').CreateShortcut(str(link))
    shortcut.TargetPath = str(ROOT / '.venv/Scripts/pythonw.exe')
    shortcut.Arguments = subprocess.list2cmdline([str(Path(__file__).resolve()), 'start', '--profile', str(profile)])
    shortcut.WorkingDirectory = str(ROOT)
    shortcut.WindowStyle = 7
    shortcut.Description = 'Tianzhu published anomaly API; current-user logon startup'
    shortcut.Save()
    return link


def start(profile: Path, startup: bool) -> dict:
    config = load_profile(profile)
    if STATE.exists():
        previous = json.loads(STATE.read_text(encoding='utf-8'))
        if same_process(previous['supervisor']):
            if (previous['project_root'] != str(ROOT) or previous['port'] != config['port']
                    or previous['host'] != config['host'] or previous['status'] == 'stopped'):
                raise RuntimeError('A different host identity is already active')
            return {'status': 'already_running', 'port': config['port'], 'ready': health(config['port'])}
    # Refuse to touch a port owned by another application.
    with socket.socket() as reservation:
        if os.name == 'nt':
            reservation.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        reservation.bind((config['host'], config['port']))
    launch_id = uuid4().hex
    LOGS.mkdir(parents=True, exist_ok=True)
    intent_path = LOGS / f'launch-{launch_id}.json'
    intent = {'launch_id': launch_id, 'host': config['host'], 'port': config['port'],
              'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'starting'}
    save(intent_path, intent)
    launched = None
    successful = False
    try:
        with (LOGS / f'host-{launch_id}.log').open('ab') as log:
            launched = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'run',
                                         '--profile', str(profile), '--launch-id', launch_id,
                                         '--runtime-dir', str(LOGS)], cwd=ROOT,
                                        stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                        creationflags=hidden_flags())
        intent['supervisor'] = identity(psutil.Process(launched.pid))
        save(intent_path, intent)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if launched.poll() is not None:
                raise RuntimeError('Host exited before readiness')
            state = json.loads(STATE.read_text(encoding='utf-8')) if STATE.exists() else {}
            if state.get('launch_id') == launch_id and listener_owned(state['owned'], config['port']) and health(config['port']):
                link = install_startup(profile) if startup else None
                intent.update(status='ready', startup_entry=str(link) if link else None)
                save(intent_path, intent)
                successful = True
                return {'status': 'ready', 'port': config['port'], 'host': config['host'],
                        'supervisor': intent['supervisor'], 'startup_entry': intent['startup_entry']}
            time.sleep(.25)
        raise TimeoutError('Host readiness deadline exceeded')
    finally:
        if not successful and launched is not None:
            (LOGS / f'stop-{launch_id}').touch()
            records = family(psutil.Process(launched.pid)) if launched.poll() is None else []
            try:
                launched.wait(timeout=10)
            except subprocess.TimeoutExpired:
                stop_owned(records)
                launched.wait(timeout=3)
            intent.update(status='failed', cleanup_complete=not any(same_process(item) for item in records))
            save(intent_path, intent)


def stop() -> dict:
    state = json.loads(STATE.read_text(encoding='utf-8'))
    process = same_process(state['supervisor'])
    if process:
        records = family(process)
        (LOGS / f"stop-{state['launch_id']}").touch()
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and same_process(state['supervisor']):
            time.sleep(.25)
        stop_owned(records)
    else:
        stop_owned(state['owned'])
    released = not listener_owned(state['owned'], state['port'])
    if not released:
        raise RuntimeError('API listener remains active')
    return {'status': 'stopped', 'owned_listener_released': released}


def main() -> int:
    global LOGS, STATE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'stop', 'status', 'run', 'worker'))
    parser.add_argument('--profile', type=Path, default=PROFILE)
    parser.add_argument('--launch-id')
    parser.add_argument('--runtime-dir', type=Path, default=LOGS)
    parser.add_argument('--install-startup', action='store_true')
    args = parser.parse_args()
    try:
        runtime = args.runtime_dir.resolve()
        if not runtime.is_relative_to((ROOT / 'output').resolve()):
            raise ValueError('Runtime artifacts must stay under output')
        LOGS, STATE = runtime, runtime / 'service.json'
        if args.action == 'worker':
            worker(load_profile(args.profile))
        elif args.action == 'run':
            if not args.launch_id:
                raise ValueError('Missing launch identity')
            supervise(args.profile, args.launch_id)
        elif args.action == 'start':
            print(json.dumps(start(args.profile, args.install_startup)))
        elif args.action == 'stop':
            print(json.dumps(stop()))
        else:
            state = json.loads(STATE.read_text(encoding='utf-8'))
            print(json.dumps({'status': state['status'], 'port': state['port'],
                              'supervisor_alive': same_process(state['supervisor']) is not None,
                              'listener_owned': listener_owned(state['owned'], state['port']),
                              'ready': health(state['port']), 'restarts': state['restarts']}))
        return 0
    except Exception as exc:
        print(json.dumps({'status': 'error', 'cause': type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
