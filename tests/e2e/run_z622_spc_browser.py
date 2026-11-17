"""Own and clean temporary Z622 browser/services; never restart production.

--lifecycle-check exercises normal completion, failure and interruption using a
minimal page with no database or business inputs. The normal run uses only the
bounded PDS capture already saved by the task's read-only import.
"""
from __future__ import annotations

import argparse
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
OUTPUT = ROOT / "output/test-results/z622-spc"


def available_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


def reachable(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def matching_process(identity: dict):
    try:
        process = psutil.Process(identity["pid"])
        if process.create_time() != identity["created"]:
            return None
        if process.exe() != identity["executable"]:
            raise RuntimeError("Cannot confirm temporary process identity")
        return process
    except psutil.NoSuchProcess:
        return None


def stop_recorded(identities: list[dict]) -> None:
    # Capture descendants before terminating their parents; a forced interruption
    # may have happened between the last polling capture and a child launch.
    family = {identity["pid"]: identity for identity in identities}
    for identity in list(family.values()):
        process = matching_process(identity)
        if process is None:
            continue
        try:
            for child in process.children(recursive=True):
                family[child.pid] = {"pid": child.pid, "created": child.create_time(), "executable": child.exe()}
        except psutil.NoSuchProcess:
            continue
    for identity in reversed(list(family.values())):
        process = matching_process(identity)
        if process is None:
            continue
        process.terminate()
        try:
            process.wait(timeout=5)
        except psutil.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def owned_run(app_path: Path, output: Path, *, scenario="normal", browser=True) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    record_path = output / "processes.json"
    # Exact identities permit recovery following a forcibly interrupted runner.
    if record_path.exists():
        prior = json.loads(record_path.read_text(encoding="utf-8"))
        if not prior.get("cleaned"):
            stop_recorded(prior.get("owned", []))
            if any(matching_process(item) is not None for item in prior.get("owned", [])):
                raise RuntimeError("Previous temporary services did not terminate")
    port = available_port()
    record = {"scenario": scenario, "runner_pid": os.getpid(), "port": port,
              "owned": [], "listeners": [], "cleaned": False}
    identities: dict[int, dict] = {}
    listeners: set[tuple[int, int]] = set()
    handles: list[subprocess.Popen] = []

    def capture() -> None:
        for handle in handles:
            try:
                if handle.pid in identities and matching_process(identities[handle.pid]) is None:
                    continue
                if handle.poll() is not None:
                    continue
                parent = psutil.Process(handle.pid)
                for process in [parent, *parent.children(recursive=True)]:
                    identities[process.pid] = {"pid": process.pid, "created": process.create_time(),
                                               "executable": process.exe()}
                    for connection in process.net_connections(kind="tcp"):
                        if connection.status == psutil.CONN_LISTEN:
                            listeners.add((process.pid, connection.laddr.port))
            except psutil.NoSuchProcess:
                continue
        record["owned"] = list(identities.values())
        record["listeners"] = [{"pid": pid, "port": listen_port} for pid, listen_port in sorted(listeners)]
        record_path.write_text(json.dumps(record, indent=2), encoding="utf-8")

    environment = os.environ.copy()
    environment["VIVO_Z622_CAPTURE"] = str(ROOT / "output/tmp/z622/pds-import.parquet")
    environment["VIVO_Z622_FIXTURE_ROOT"] = str(output / f"isolated-{time.time_ns()}")
    environment["VIVO_Z622_BROWSER_OUTPUT"] = str(output)
    environment["VIVO_Z622_BROWSER_URL"] = f"http://127.0.0.1:{port}"
    environment["PLAYWRIGHT_MODULE_PATH"] = str(
        Path(os.environ["USERPROFILE"]) / ".npm-global/node_modules/@playwright/cli/node_modules/playwright")
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    outcome = "normal"
    with (output / "server.log").open("w", encoding="utf-8") as server_log, (output / "browser.log").open("w", encoding="utf-8") as browser_log:
        try:
            handles.append(subprocess.Popen([
                sys.executable, "-m", "streamlit", "run", str(app_path),
                "--server.address", "127.0.0.1", "--server.port", str(port),
                "--server.headless", "true", "--browser.gatherUsageStats", "false",
                "--server.fileWatcherType", "none",
            ], cwd=ROOT, env=environment, stdout=server_log, stderr=server_log, creationflags=flags))
            capture()
            deadline = time.monotonic() + 45
            while True:
                capture()
                if handles[0].poll() is not None:
                    raise RuntimeError("Temporary Streamlit process exited")
                try:
                    with urlopen(f"http://127.0.0.1:{port}/_stcore/health", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError("Temporary Streamlit startup timed out")
                time.sleep(0.1)
            capture()
            if scenario == "failure":
                raise RuntimeError("EXPECTED_LIFECYCLE_FAILURE")
            if scenario == "interruption":
                raise KeyboardInterrupt("EXPECTED_LIFECYCLE_INTERRUPTION")
            if browser:
                handles.append(subprocess.Popen(["node", "tests/e2e/z622_spc.js"], cwd=ROOT,
                    env=environment, stdout=browser_log, stderr=browser_log, creationflags=flags))
                deadline = time.monotonic() + 420
                while handles[-1].poll() is None:
                    capture()
                    if time.monotonic() > deadline:
                        raise TimeoutError("Z622 browser verification timed out")
                    time.sleep(0.15)
                if handles[-1].returncode:
                    raise RuntimeError("Z622 browser verification failed; inspect browser.log")
        except RuntimeError as exc:
            if scenario != "failure" or str(exc) != "EXPECTED_LIFECYCLE_FAILURE":
                raise
            outcome = "expected_failure"
        except KeyboardInterrupt as exc:
            if scenario != "interruption" or str(exc) != "EXPECTED_LIFECYCLE_INTERRUPTION":
                raise
            outcome = "expected_interruption"
        finally:
            # Ownership was recorded immediately after launch, before readiness.
            try:
                capture()
            finally:
                stop_recorded(list(identities.values()))
                for handle in handles:
                    try:
                        handle.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                alive = [item["pid"] for item in identities.values() if matching_process(item) is not None]
                listening_ports = [p for p in sorted({port, *(p for _, p in listeners)}) if reachable(p)]
                record.update(cleaned=not alive and not listening_ports, alive=alive,
                              listening_ports=listening_ports, outcome=outcome)
                record_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
                if alive or listening_ports:
                    raise RuntimeError("Temporary process/listener cleanup could not be confirmed")
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lifecycle-check", action="store_true")
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    if args.lifecycle_check:
        minimal = OUTPUT / "minimal-lifecycle.py"
        minimal.write_text("import streamlit as st\nst.write('Lifecycle test: no database')\n", encoding="utf-8")
        cases = [owned_run(minimal, OUTPUT / f"lifecycle-{mode}", scenario=mode, browser=False)
                 for mode in ("normal", "failure", "interruption")]
        assert all(case["cleaned"] for case in cases)
        (OUTPUT / "lifecycle-result.json").write_text(json.dumps(cases, indent=2), encoding="utf-8")
        print("Three no-database lifecycle scenarios passed; all processes and listeners cleaned.")
    else:
        if not (ROOT / "output/tmp/z622/pds-import.parquet").exists():
            raise FileNotFoundError("Run the bounded read-only PDS capture before the browser test")
        owned_run(ROOT / "tests/e2e/fixtures/z622_spc_app.py", OUTPUT)
        print("Z622 browser verification passed; all temporary processes and listeners cleaned.")


if __name__ == "__main__":
    main()
