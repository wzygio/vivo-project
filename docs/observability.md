# Observability

This is the Harness entrypoint for traces, logs, smoke checks, and diagnostics.

## Runtime Logs

- `logs/app.log_error.log`: application errors and boundary failures.
- `logs/app.log_info.log`: general application info.
- `logs/app.log_shared.log`: shared-kernel logs.
- `logs/app.log_spc.log`: SPC-domain logs.
- `logs/app.log_yield.log`: Yield-domain logs.
- `logs/app.log_trace.log`: detailed trace/probe output.

Daily rotated logs may appear as `logs/*.YYYY-MM-DD`. Treat logs as runtime artifacts unless a task explicitly asks to preserve an excerpt in `docs/generated/`.

## Generated Outputs

- `output/`: task outputs, smoke artifacts, generated reports, and analysis products.
- `docs/generated/`: rebuildable Harness audits and repository facts only.

## Smoke And Test Commands

```powershell
# Unit tests
$env:PYTHONUTF8='1'; $env:PYTHONPATH='D:\wzy\Python\vivo-project\src;D:\wzy\Python\vivo-project'; uv run pytest tests/unit/ -v --tb=short

# All tests
$env:PYTHONUTF8='1'; $env:PYTHONPATH='D:\wzy\Python\vivo-project\src;D:\wzy\Python\vivo-project'; uv run pytest tests/ -v --tb=short

# Start Streamlit
uv run streamlit run app/Home.py --server.headless true --server.port 8503
```

## Known Collection Risks

- Some legacy tests import modules as `yield_domain.*`, so set `PYTHONPATH` to include both repo root and `src`.
- Streamlit component import checks can fail during pytest collection when `streamlit-echarts` component metadata is incomplete in the local environment.
- Treat these as environment/collection blockers unless the current task edits the affected tests or component configuration.

## UI Verification

For visible Streamlit changes, backend tests are not enough. Start the app and run a small browser smoke that verifies page load, navigation surface, and the changed workflow.

## Diagnostics

- Environment probe: `tests/diagnose_streamlit_env.py`
- Unit-level diagnostics: `tests/unit/diagnose_streamlit_env.py`
- Current Harness audit: `docs/generated/harness-audit.md`
- Harness cleanup loop: `docs/generated/harness-garbage-collection.md`
