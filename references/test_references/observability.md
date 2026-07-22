# Observability

This is the Harness entrypoint for traces, logs, smoke checks, and diagnostics.

## Runtime Logs

- `output/logs/app.log_error.log`: application errors and boundary failures.
- `output/logs/app.log_info.log`: general application info.
- `output/logs/app.log_shared.log`: shared-kernel logs.
- `output/logs/app.log_spc.log`: SPC-domain logs.
- `output/logs/app.log_yield.log`: Yield-domain logs.
- `output/logs/app.log_trace.log`: detailed trace/probe output.

Daily rotated logs may appear as `output/logs/*.YYYY-MM-DD`. Treat logs as runtime artifacts unless a task explicitly asks to preserve an excerpt in `docs/generated/`.

## Generated Outputs

- `output/`: the canonical generated-artifact root; see `output/README.md` for category ownership.
- `output/reports/`: generated business reports, analysis tables, and charts.
- `output/screenshots/`: browser screenshots and document-preview images.
- `output/test-results/`: test reports, traces, videos, and coverage artifacts.
- `output/decrypted_files/`: temporary decrypted or normalized working copies.
- `output/logs/`: runtime logs and probe exports.
- `docs/generated/`: rebuildable Harness audits and repository facts only.

## Smoke And Test Commands

```powershell
# Fast domain smoke (explicit opt-in; does not replace complete regression)
uv run --no-sync python tools/smoke.py spc
uv run --no-sync python tools/smoke.py yield
uv run --no-sync python tools/smoke.py equipment

# Conservative default: complete unit suite
uv run --no-sync python tools/smoke.py

# Unit tests
$env:PYTHONUTF8='1'; $env:PYTHONPATH='D:\wzy\Python\vivo-project\src;D:\wzy\Python\vivo-project'; uv run pytest tests/unit/ -v --tb=short

# All tests
$env:PYTHONUTF8='1'; $env:PYTHONPATH='D:\wzy\Python\vivo-project\src;D:\wzy\Python\vivo-project'; uv run pytest tests/ -v --tb=short

# Start Streamlit
uv run streamlit run app/Home.py --server.headless true --server.port 8503
```

The smoke runner prints every pytest target before execution. Fast scopes are
explicit because they can miss cross-domain regressions. Use `all` (the default)
for shared-kernel or uncertain changes, and run the complete suite before
release. Pytest's non-zero result, including zero collection, is preserved.

## Known Collection Risks

- Some legacy tests import modules as `yield_domain.*`, so set `PYTHONPATH` to include both repo root and `src`.
- Streamlit component import checks can fail during pytest collection when `streamlit-echarts` component metadata is incomplete in the local environment.
- The current unit baseline also contains existing failures and a stale
  `test_override_logic.py` import. Domain smoke must surface relevant existing
  failures; do not change business behavior merely to make the smoke green.
- Treat these as environment/collection blockers unless the current task edits the affected tests or component configuration.

## UI Verification

For visible Streamlit changes, backend tests are not enough. Start the app and run a small browser smoke that verifies page load, navigation surface, and the changed workflow.

## Diagnostics

- Environment probe: `tests/diagnose_streamlit_env.py`
- Unit-level diagnostics: `tests/unit/diagnose_streamlit_env.py`
- Current Harness audit: `docs/generated/harness-audit.md`
- Harness cleanup loop: `docs/generated/harness-garbage-collection.md`
