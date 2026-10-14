"""Real Windows process checks use minimal pages and never query a database."""

import json
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[3]
POWERSHELL = (
    Path(os.environ.get("SystemRoot", "C:/Windows"))
    / "System32/WindowsPowerShell/v1.0/powershell.exe"
)
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows startup scripts")


@pytest.mark.parametrize("interrupt_runner", [False, True])
def test_restart_real_process_lifecycle_without_database(interrupt_runner: bool) -> None:
    evidence = ROOT / "output/test-results" / f"streamlit-lifecycle-{uuid4().hex}.json"
    script = Path(__file__).with_name("streamlit_restart_lifecycle.ps1")
    command = [
        str(POWERSHELL), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(script), "-EvidencePath", str(evidence),
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        if interrupt_runner:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    record = json.loads(evidence.read_text(encoding="utf-8-sig"))
                except (FileNotFoundError, json.JSONDecodeError):
                    record = {}
                if record.get("controller"):
                    break
                assert process.poll() is None, "Lifecycle runner exited before interruption checkpoint"
                time.sleep(0.1)
            else:
                pytest.fail("Lifecycle runner did not reach the interruption checkpoint")
            # Kill the test runner itself: its finally block cannot clean children.
            process.kill()
            process.wait(timeout=10)
        else:
            stdout, stderr = process.communicate(timeout=180)
            assert process.returncode == 0, stdout + stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        # Recover owned descendants even when the test controller was killed.
        cleanup = subprocess.run(command + ["-CleanupOnly"], capture_output=True, text=True, timeout=90)
        assert cleanup.returncode == 0, cleanup.stdout + cleanup.stderr
        process.stdout.close()
        process.stderr.close()
