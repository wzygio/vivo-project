# Findings

## Worktree state at start

- User has already deleted `app/pages/CPM监控报表.py`, `app/sections/spc_dashboard.py`, and `src/spc_domain/application/cpm_service.py`.
- User has added `app/pages/SPC监控报表.py`, `app/sections/monitor_dashboard.py`, and `src/spc_domain/application/monitor_service.py`.
- The added files still contain several legacy imports and symbols, so the migration is incomplete.
- `app/sections/spc_cpm_dashboard.py` remains and is the legacy CPM presentation module that must become the SPC presentation module.
- Existing dirty changes include prior CPM display-rule work and multiple unrelated resource/config edits. They must be preserved.

## Initial target mapping

| Legacy responsibility | Target responsibility |
|---|---|
| `spc_dashboard` / alert aggregation | `monitor_dashboard` / monitor aggregation |
| `spc_service` / alert aggregation | `monitor_service` / monitor aggregation |
| `spc_cpm_dashboard` / CPM capability UI | `spc_dashboard` / SPC capability UI |
| `cpm_service` / CPM capability service | `spc_service` / SPC capability service |
| `spc_domain` package | `inline_domain` package |
| physical SPC repository | `spc_repository.py` (name retained) |

## Import and symbol inventory

- Current `src/spc_domain/application/spc_service.py` contains the legacy CPM capability implementation, but still exports `CpmReportService` and `CpmReportViewModel`.
- Current `src/spc_domain/application/monitor_service.py` contains the legacy alert-monitoring implementation, but still exports `SpcAnalysisService` and `SpcDashboardViewModel`.
- Current `app/sections/monitor_dashboard.py` still imports/exports `Spc*` names.
- Current `app/sections/spc_cpm_dashboard.py` remains the capability UI and imports legacy `cpm_*` core modules.
- `SPC监控报表.py` is already the replacement capability page but imports the old section name and legacy class names.
- The automatic-monitor page still imports the former `spc_service` symbols.
- Tests currently retain legacy names and paths; they need to be moved/updated together with the public contracts.

## Errors observed

- The interrupted superseded UNI point-line task left partial dirty changes in current files. The refactor must carry only the behavior-compatible portions forward; it must not resurrect deleted legacy files.
- A directory-level move of `src/spc_domain` was rejected by Windows, probably because generated `__pycache__` files are in use. The section-file moves completed before that error. Continue with individual tracked-source moves and leave generated caches untouched.

## Open design decisions

- Apply `monitor`/`spc` subpackages at the UI section and application layers. At the core layer, move alert rules into `monitor` and capability rules into `spc`. At infrastructure, keep physical SPC source adapters in the `spc` subpackage, including the unchanged `spc_repository.py`.
- Keep query/repository names that identify the physical SPC database source where required by the user's repository exception.

## Baseline verification

- Current focused test collection is already broken because user migration deleted `cpm_service.py` while legacy tests still import it. `test_spc_cpm_service.py` and `test_cpm_page_alerts.py` fail collection for that reason.

## Verification

- The renamed inline smoke suite passes: 77 tests. The smoke-runner contract suite passes: 8 tests.
- Running `pytest tests/unit` directly lacks the repository's `src` import path for Yield tests. Running the project's `tools/smoke.py all` adds that path, then reaches an unrelated existing Yield-test collection error: `create_mwd_trend_data` is not exported by `yield_domain.core.mwd_trend.mwd_trend_processor`.
- The legacy `src/spc_domain` directory now contains only ignored `.pyc` caches. Its deletion was blocked by the environment's destructive-command policy; no executable source remains there.
