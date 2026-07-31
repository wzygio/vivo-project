# Progress Log

## Session: 2026-07-14

### Project performance optimization

- **Status:** completed
- Started the user-mandated `to-prd → to-issues → planning-with-files → tdd → adr → risk-directed tdd` workflow.
- Read all named Skill contracts and TDD guidance; created a dedicated active plan before repository exploration.
- Confirmed no CodeGraph index and recorded unrelated dirty-worktree boundaries.
- Read architecture, domain vocabulary, adr-0001, Local Markdown tracker/triage rules, validation/observability guidance, the persisted user prompt, and pytest configuration.
- Completed initial static hotspot inventory. Identified a likely low-risk architecture slice (fast smoke tier/impact routing) plus candidate numerical kernels; no optimization has been approved or implemented yet.
- Timed unit collection and the runnable unit baseline. Confirmed that dependency import/collection is the dominant smoke cost and recorded all pre-existing failures without modifying them.
- Inspected CPM and critical-parts kernels. CPM aggregation remains a candidate; stateful critical-parts decoration is excluded from the initial optimization scope.
- Benchmarked CPM against the real M626 snapshot shape and confirmed a 6.10 s Point Value hotspot.
- Published `.scratch/project-performance-optimization/PRD.md` and two vertical Issues. Both contain risk tables, Agent Briefs, rollback boundaries, and `ready-for-agent` status.
- Replaced the provisional plan with Issue-linked RED→GREEN, benchmark, and rollback gates. Architecture Issue 01 is now active.
- Issue 01 RED confirmed the smoke module was absent; GREEN added explicit domain routing and six passing router tests.
- Refactored the runner to execute pytest in its fresh command process instead of spawning another Python process. SPC smoke resolves 13 existing test files and preserves the relevant pre-existing failure.
- `uv run --no-sync python tools/smoke.py spc` initially completed in 4.49 s; after adding all risk tests and moving to the versioned path, the final 68-test scope completed in 4.65 s, about 60.0% faster than the 11.64 s runnable-unit baseline.
- Documented the explicit fast scopes, conservative default, zero-collection behavior, and full-regression boundary. Phase 6 / Issue 02 is now active.
- Added CPM characterization tests for valid-row filtering, first-value selection, NaN grouping, full-key Point sigma isolation, fallback, periods, counts, and dtypes.
- Replaced Python record-building loops with batch aggregation. A native groupby reducer initially changed two near-constant CPK values to `inf`; TDD risk audit caught it and that reducer was withdrawn.
- Retained legacy `Series.mean/std` floating-point behavior inside the batch plan. Final M626 Sheet Mean and Point Value outputs are bitwise identical to HEAD.
- Final benchmark: Sheet Mean 2.06→0.39 s (81.2% faster); Point Value 6.08→3.29 s (46.0% faster).
- Added adr-0002 and a consolidated risk checklist. Every listed risk has test, benchmark, diff, or command evidence; both Issues remain and moved to `ready-for-human`.
- Added internal import-path setup for legacy Yield tests. Moved the smoke tool from ignored `scripts/` to versioned `tools/` after delivery inspection found the ignore rule.
- Final broad regression: 141 passed and the same 7 pre-existing failures; Python compile and `git diff --check` passed.

## Test Results

| Test | Input | Expected | Actual | Status |
|---|---|---|---|---|
| Unit collection | `uv run pytest tests/unit/ --collect-only -q` | Collect current suite | 135 collected, 1 pre-existing import error; 12.34 s pytest / 22.04 s wall | baseline captured |
| Runnable unit baseline | `uv run pytest tests/unit/ --ignore=tests/unit/test_override_logic.py -q --durations=20` | Capture current behavior | 128 passed, 7 pre-existing failed; 9.04 s pytest / 11.64 s wall | baseline captured |
| CPM calculator target | `uv run pytest tests/unit/test_spc_cpm_calculator.py -q` | Existing CPM contract passes | 13 passed; 1.73 s wall | passed |
| CPM Sheet Mean baseline | M626-derived 92,849 Sheet features | Capture current runtime/output checksum | 1,106 rows; 2.01 s | baseline captured |
| CPM Point Value baseline | M626 1,041,518 points + Sheet features | Capture current runtime/output checksum | 1,106 rows; 6.10 s | baseline captured |
| Smoke router RED | `uv run pytest tests/unit/test_smoke_runner.py -q` before implementation | Missing smoke module | Import/collection error | expected RED |
| Smoke router GREEN | Same focused test after implementation/refactor | Router contracts pass | 8 passed in 0.08 s | passed |
| SPC domain smoke | `uv run --no-sync python tools/smoke.py spc` | ≥60% faster; surface real failures | 4.65 s; 67 passed, 1 pre-existing failed | performance gate passed |
| CPM risk contracts | Calculator characterization suite | Preserve formulas, periods, keys, nulls, counts, dtypes | 18 passed | passed |
| Exact real-data equivalence | HEAD vs worktree, M626, both sigma sources | Bitwise-identical DataFrames | `check_exact=True` passed for both | passed |
| Final CPM benchmark | M626 real shape | Point Value ≥30% faster | Sheet Mean 81.2%; Point Value 46.0% faster | passed |
| Equipment smoke | `uv run --no-sync python tools/smoke.py equipment` | Existing equipment tests pass | 13 passed | passed |
| Final broad unit | Unit suite excluding stale import module | No new failures vs baseline | 141 passed, same 7 failed | baseline-equivalent |
| Static sanity | compileall + `git diff --check` | Pass | Both passed | passed |

## Error Log

| Timestamp | Error | Attempt | Resolution |
|---|---|---:|---|
| — | None | 1 | — |
| 2026-07-14 | `rg: scripts: 系统找不到指定的文件` | 1 | Repository has no current `scripts/`; later searches will target existing roots only. |
| 2026-07-14 | `UnicodeDecodeError` loading `resources/critical_parts_baseline.csv` as UTF-8 | 1 | Recorded as an unrelated existing defect; excluded critical-parts optimization. |
| 2026-07-14 | Missing obsolete observability subfolder path | 1 | Located `references/test_references/observability.md`. |
| 2026-07-14 | Broad pytest without Harness `PYTHONPATH` produced 6 legacy Yield collection errors | 1 | Re-ran with documented Harness paths; smoke tool now configures them internally. |
| 2026-07-14 | Native groupby reducer changed near-constant finite CPK to `inf` | 1 | Withdrew native mean/std reducer; exact `Series` behavior retained and reverified. |
| 2026-07-14 | `ruff` executable unavailable | 1 | Recorded non-gating tool absence; compileall, focused tests, broad tests, and diff check used. |
| 2026-07-14 | `scripts/` ignored by repository rule | 1 | Migrated smoke tool and all routes to versioned `tools/`. |

## 5-Question Reboot Check

| Question | Answer |
|---|---|
| Where am I? | Complete |
| Where am I going? | User review of two `ready-for-human` Issues |
| What's the goal? | Faster smoke/runtime paths with unchanged business and numerical behavior |
| What have I learned? | See `findings.md` |
| What have I done? | Published PRD/Issues, retained two measured optimizations, completed adr and risk-directed audit |
