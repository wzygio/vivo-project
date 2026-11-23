"""Run the lifetime specs browser scenario with an isolated Streamlit fixture."""

from pathlib import Path

from run_z622_spc_browser import owned_run


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    result = owned_run(
        root / "tests/e2e/fixtures/iqc_lifetime_app.py",
        root / "output/test-results/iqc-lifetime-specs/browser",
        browser_script="tests/e2e/iqc_lifetime_specs_browser.js",
    )
    print(f"Lifetime browser E2E passed. Listener {result['port']} released.")
