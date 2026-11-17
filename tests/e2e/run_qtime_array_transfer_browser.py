"""Use the verified process-family runner for captured and fixture Q-Time E2E."""

from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_z622_spc_browser import owned_run


if __name__ == "__main__":
    capture = ROOT / "output/test-results/qtime-route"
    if not (capture / "bounded-events.parquet").exists():
        raise FileNotFoundError("A bounded read-only source capture is required")
    output = ROOT / "output/test-results/qtime-route/browser"
    result = owned_run(
        ROOT / "tests/e2e/fixtures/qtime_array_transfer_app.py", output,
        browser_script="tests/e2e/qtime_array_transfer_browser.js",
        environment_overrides={
            "VIVO_QTIME_CAPTURE_ROOT": str(capture),
            "VIVO_QTIME_FIXTURE_ROOT": str(output / f"isolated-{time.time_ns()}"),
            "VIVO_QTIME_BROWSER_OUTPUT": str(output),
        },
    )
    assert result["cleaned"]
    print("Q-Time fixed-route browser E2E passed; all temporary processes and listeners cleaned.")
