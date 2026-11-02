"""Actual minimal listeners are always cleaned by verified process identity."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

import psutil
import pytest

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).with_name("fixtures") / "api_lifecycle_app.py"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    assert port != 8890  # Never use the formal/report-server port.
    return port


def _listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(.1)
        return connection.connect_ex(("127.0.0.1", port)) == 0


def _same_process(identity: dict) -> psutil.Process | None:
    try:
        process = psutil.Process(identity["pid"])
        return process if process.create_time() == identity["create_time"] else None
    except psutil.NoSuchProcess:
        return None


def _capture_owned(process: subprocess.Popen, owned: dict[int, dict]) -> None:
    try:
        root = psutil.Process(process.pid)
        previous = owned.get(root.pid)
        if previous and previous["create_time"] != root.create_time():
            return
        candidates = [root, *root.children(recursive=True)]
        for candidate in candidates:
            try:
                owned[candidate.pid] = {"pid": candidate.pid, "create_time": candidate.create_time(),
                                        "parent_pid": candidate.ppid()}
            except psutil.NoSuchProcess:
                continue
    except psutil.NoSuchProcess:
        return


def _owns_listener(owned: dict[int, dict], port: int) -> bool:
    for identity in owned.values():
        process = _same_process(identity)
        if process is None:
            continue
        try:
            if any(connection.status == psutil.CONN_LISTEN and connection.laddr.port == port
                   for connection in process.net_connections(kind="tcp")):
                return True
        except psutil.NoSuchProcess:
            continue
    return False


def _stop_owned(process: subprocess.Popen, owned: dict[int, dict]) -> None:
    if process.poll() is None:
        _capture_owned(process, owned)
    # Descendants may include the actual interpreter below the Windows venv
    # launcher. Stop them before the launcher; never kill by name or port.
    identities = [identity for pid, identity in owned.items() if pid != process.pid]
    identities += [identity for pid, identity in owned.items() if pid == process.pid]
    for identity in identities:
        candidate = _same_process(identity)
        if candidate is not None:
            try:
                candidate.terminate()
            except psutil.NoSuchProcess:
                pass
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and any(_same_process(identity) for identity in identities):
        time.sleep(.05)
    for identity in identities:
        candidate = _same_process(identity)
        if candidate is not None:
            try:
                candidate.kill()
            except psutil.NoSuchProcess:
                pass
    # A Popen handle proves launch ownership even if a failed startup exited
    # before psutil could capture its creation timestamp.
    if process.poll() is None:
        process.kill()
    process.wait(timeout=3)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and any(_same_process(identity) for identity in identities):
        time.sleep(.05)


@contextmanager
def _running_minimal_service(mode: str):
    artifact = ROOT / "output" / "test-results" / "api-lifecycle" / f"{mode}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    port = _free_port()
    record = {"mode": mode, "port": port, "before_creation": datetime.now(timezone.utc).isoformat(),
              "controller_pid": os.getpid(), "port_free_before_creation": not _listening(port),
              "ready_seen": False, "owned": [], "cleanup_complete": False}
    evidence = artifact / "lifecycle.json"

    def save() -> None:
        evidence.write_text(json.dumps(record, indent=2), encoding="utf-8")

    save()  # Launch intent and port are durable before creating a process.
    owned: dict[int, dict] = {}
    process = None
    opener = build_opener(ProxyHandler({}))
    with (artifact / "stdout.log").open("wb") as stdout, (artifact / "stderr.log").open("wb") as stderr:
        try:
            environment = {key: value for key, value in os.environ.items() if not key.startswith("DB_")}
            environment["PYTHONIOENCODING"] = "utf-8"
            process = subprocess.Popen(
                [sys.executable, str(FIXTURE), "--port", str(port)], cwd=ROOT, env=environment,
                stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            record["popen_pid"] = process.pid
            _capture_owned(process, owned)
            record["owned"] = list(owned.values())
            save()
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                _capture_owned(process, owned)
                if process.poll() is not None:
                    raise RuntimeError("API fixture exited before readiness")
                try:
                    with opener.open(f"http://127.0.0.1:{port}/health/live", timeout=.25) as response:
                        ready = response.status == 200 and json.loads(response.read()) == {"status": "ok"}
                    if ready:
                        assert _owns_listener(owned, port), "Listener must belong to the created process tree"
                        record["ready_seen"] = True
                        record["owned"] = list(owned.values())
                        save()
                        break
                except (URLError, TimeoutError, ConnectionError, OSError):
                    pass
                time.sleep(.05)
            else:
                raise RuntimeError("API fixture startup exceeded eight-second deadline")
            yield SimpleService(port, opener, record)
        except BaseException as exc:
            record["exit_reason"] = type(exc).__name__
            raise
        finally:
            if process is not None:
                try:
                    _stop_owned(process, owned)
                finally:
                    record["owned"] = list(owned.values())
                    record["popen_exited"] = process.poll() is not None
                    record["all_owned_exited"] = all(_same_process(identity) is None for identity in owned.values())
                    record["port_released"] = not _listening(port)
                    record["cleanup_complete"] = (
                        record["popen_exited"] and record["all_owned_exited"] and record["port_released"]
                    )
                    save()
                    assert record["cleanup_complete"], f"Owned API service cleanup incomplete: {evidence}"


class SimpleService:
    def __init__(self, port: int, opener, record: dict) -> None:
        self.port, self.opener, self.record = port, opener, record

    def verify_health(self) -> None:
        with self.opener.open(f"http://127.0.0.1:{self.port}/health/live", timeout=.25) as response:
            assert json.loads(response.read()) == {"status": "ok"}
        # No configured clients/products means not ready, rather than reaching
        # into a data source or creating a database connection to fill them.
        with pytest.raises(HTTPError) as unavailable:
            self.opener.open(f"http://127.0.0.1:{self.port}/health/ready", timeout=.25)
        try:
            assert unavailable.value.code == 503
        finally:
            unavailable.value.close()


@pytest.mark.parametrize("mode", ["normal", "failure", "interrupt"])
def test_minimal_api_process_tree_and_listener_are_cleaned_on_every_exit(mode: str) -> None:
    record = None
    if mode == "normal":
        with _running_minimal_service(mode) as service:
            service.verify_health()
            record = service.record
    else:
        expected = RuntimeError if mode == "failure" else KeyboardInterrupt
        with pytest.raises(expected, match=f"simulated {mode}"):
            with _running_minimal_service(mode) as service:
                service.verify_health()
                record = service.record
                raise expected(f"simulated {mode}")
    assert record is not None and record["ready_seen"] and record["cleanup_complete"]
    assert record["port_free_before_creation"] and record["all_owned_exited"] and record["port_released"]
