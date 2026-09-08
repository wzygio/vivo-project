"""Prove the lock serializes separate Python processes, not only threads."""

from pathlib import Path
import subprocess
import sys

from src.inline_domain.infrastructure.shared.rolling_snapshot import snapshot_process_lock


def _probe(identity: Path) -> subprocess.CompletedProcess:
    script = (
        "from pathlib import Path\n"
        "from src.inline_domain.infrastructure.shared.rolling_snapshot import snapshot_process_lock\n"
        "try:\n"
        f"    with snapshot_process_lock(Path({str(identity)!r}), timeout_seconds=0.1):\n"
        "        print('acquired')\n"
        "except TimeoutError:\n"
        "    print('blocked')\n"
    )
    return subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=15,
        check=True,
    )


def test_other_process_waits_and_lock_releases_after_exception(tmp_path):
    identity = tmp_path / "aoi_rs_M626"
    try:
        with snapshot_process_lock(identity):
            assert _probe(identity).stdout.strip() == "blocked"
            # Different products must remain independently refreshable.
            assert _probe(tmp_path / "aoi_rs_M678").stdout.strip() == "acquired"
            raise RuntimeError("simulated publication failure")
    except RuntimeError:
        pass
    assert _probe(identity).stdout.strip() == "acquired"
    assert not identity.exists()
