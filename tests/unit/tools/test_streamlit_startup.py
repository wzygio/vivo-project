"""Windows startup checks; fixtures never target the production server port."""

import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[3]
POWERSHELL = Path(os.environ.get("SystemRoot", "C:/Windows")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows startup scripts")


def test_restart_process_ownership_and_health_contract():
    script = Path(__file__).with_name("restart_streamlit.Tests.ps1")
    command = (
        "$result = Invoke-Pester -Script '" + str(script).replace("'", "''")
        + "' -PassThru; if ($result.FailedCount -gt 0) { exit 1 }"
    )
    result = subprocess.run(
        [str(POWERSHELL), "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True, text=True, timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("exit_code", [0, 23])
def test_hidden_launcher_waits_and_returns_batch_exit_code(tmp_path, exit_code):
    project = tmp_path / "project with spaces"
    project.mkdir()
    shutil.copyfile(ROOT / "run_hidden.vbs", project / "run_hidden.vbs")
    (project / "start_streamlit.bat").write_text(
        f"@echo off\r\nexit /b {exit_code}\r\n", encoding="ascii",
    )
    result = subprocess.run(
        ["cscript.exe", "//nologo", str(project / "run_hidden.vbs")],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == exit_code, result.stdout + result.stderr


@pytest.mark.parametrize("exit_code", [0, 23])
def test_batch_launcher_returns_powershell_exit_code(tmp_path, exit_code):
    project = tmp_path / "project with spaces"
    (project / "tools").mkdir(parents=True)
    shutil.copyfile(ROOT / "start_streamlit.bat", project / "start_streamlit.bat")
    (project / "tools/restart_streamlit.ps1").write_text(f"exit {exit_code}\n", encoding="ascii")
    result = subprocess.run(
        [os.environ["ComSpec"], "/d", "/c", str(project / "start_streamlit.bat")],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == exit_code, result.stdout + result.stderr
