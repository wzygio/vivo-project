"""Owned temporary Streamlit/browser process trees; no production database/service."""

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.request import urlopen

import psutil

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    output = ROOT / "output/test-results/aoi-rs-monitor"
    output.mkdir(parents=True, exist_ok=True)
    evidence_path = output / "processes.json"
    owned: dict[int, float] = {}
    # Recover only positively identified descendants recorded by a prior interrupted run.
    if evidence_path.exists():
        previous = json.loads(evidence_path.read_text(encoding="utf-8"))
        for identity in previous.get("owned", []):
            try:
                process = psutil.Process(identity["pid"])
                if process.create_time() == identity["created"]:
                    if identity.get("executable") != process.exe():
                        raise RuntimeError("Cannot verify ownership of previous test process")
                    process.kill()
                    process.wait(timeout=10)
            except psutil.NoSuchProcess:
                pass
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    record = {"port": port, "runner_pid": os.getpid(), "owned": [], "cleaned": False}

    executables: dict[int, str] = {}

    def capture(process: subprocess.Popen) -> None:
        try:
            root = psutil.Process(process.pid)
            for item in [root, *root.children(recursive=True)]:
                owned[item.pid] = item.create_time()
                executables[item.pid] = item.exe()
        except psutil.NoSuchProcess:
            pass
        record["owned"] = [{"pid": pid, "created": created, "executable": executables[pid]} for pid, created in owned.items()]
        evidence_path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    environment = os.environ.copy()
    environment["VIVO_RS_MONITOR_FIXTURE_ROOT"] = str(output / f"facts-{time.time_ns()}")
    environment["VIVO_RS_BROWSER_OUTPUT"] = str(output)
    environment["VIVO_RS_BROWSER_URL"] = f"http://127.0.0.1:{port}"
    environment["PLAYWRIGHT_MODULE_PATH"] = str(
        Path(os.environ["USERPROFILE"]) / ".npm-global/node_modules/@playwright/cli/node_modules/playwright")
    server = browser = None
    with (output / "server.log").open("w", encoding="utf-8") as log, (output / "browser.log").open("w", encoding="utf-8") as browser_log:
        try:
            server = subprocess.Popen([
                sys.executable, "-m", "streamlit", "run", "tests/e2e/fixtures/aoi_rs_monitor_app.py",
                "--server.address", "127.0.0.1", "--server.port", str(port),
                "--server.headless", "true", "--browser.gatherUsageStats", "false",
                "--server.fileWatcherType", "none",
            ], cwd=ROOT, env=environment, stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW)
            capture(server)
            for _ in range(200):
                capture(server)
                if server.poll() is not None:
                    raise RuntimeError("Fixture server exited; inspect server.log")
                try:
                    with urlopen(f"http://127.0.0.1:{port}/_stcore/health", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise TimeoutError("Fixture server startup timed out")
            browser = subprocess.Popen(["node", "tests/e2e/aoi_rs_monitor_browser.js"],
                                       cwd=ROOT, env=environment, stdout=browser_log, stderr=browser_log,
                                       creationflags=subprocess.CREATE_NO_WINDOW)
            deadline = time.monotonic() + 180
            while browser.poll() is None:
                if time.monotonic() > deadline:
                    raise TimeoutError("AOI_RS browser test timed out")
                capture(browser)
                capture(server)
                time.sleep(0.2)
            if browser.returncode:
                raise RuntimeError(f"Browser test failed with code {browser.returncode}")
        finally:
            for process in (browser, server):
                if process is not None:
                    capture(process)
            for pid, created in reversed(list(owned.items())):
                try:
                    process = psutil.Process(pid)
                    if process.create_time() == created:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                except psutil.NoSuchProcess:
                    pass
            alive = []
            for pid, created in owned.items():
                try:
                    if psutil.Process(pid).create_time() == created:
                        alive.append(pid)
                except psutil.NoSuchProcess:
                    pass
            with socket.socket() as probe:
                listening = probe.connect_ex(("127.0.0.1", port)) == 0
            record.update(cleaned=not alive and not listening, alive=alive, listening=listening)
            evidence_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
            if alive or listening:
                raise RuntimeError("Temporary process/listener cleanup could not be verified")


if __name__ == "__main__":
    main()
