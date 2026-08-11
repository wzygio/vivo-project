# Progress — SPC 主制程设备/腔室追溯

## 2026-08-10 Session 1

- development-flow requirements module complete: created and triaged `.scratch/spc-main-process-chamber/issues/01-trace-main-process-equipment-chamber.md` to `ready-for-agent`.
- Read Task2, glossary, SPC page/service/repository/dashboard,北极星 SQL analysis and source SQL.
- Completed read-only database discovery for spec uniqueness, route distribution, history schemas/indexes/time keys, and M626 route coverage.
- Created isolated planning files and recorded the user's full-delivery instruction as advance approval of the plan gate.
- No production code changed during requirements/planning modules.
- Phase 0 regression baseline: `.venv/Scripts/python.exe tools/smoke.py spc` → 138 passed, 3 pre-existing AOI_RS pandas FutureWarnings.
- Phase 1 slice 1 RED: new spec-loader behavior failed because SQL omitted `main_step_id/main_eqp_type`.
- Phase 1 slice 1 GREEN: parameterized spec query now returns and normalizes the route fields; `test_spc_data_loader.py` → 2 passed.
- Phase 1/2 trace module delivered through sequential RED→GREEN slices: nearest-prior selection, three-key route attach, ARRAY EQP, ARRAY CHAMBER priority, OLED/TP EQP, TP CHAMBER, OLED CVD-normalized CHAMBER, and orchestration. Evidence: `test_main_process_trace.py` 8 passed.
- Repository refresh integration RED→GREEN: refreshed raw measurements are enriched before snapshot persistence; final policy upgraded to `spc-main-process-trace-v2`; repository tests 4 passed.
- Dashboard RED→GREEN: second chart now consumes only full `main_process_unit_id` labels and is titled `By主站点设备/腔室`; missing trace is `UNKNOWN`, never measurement `unit_id/site_name`. Dashboard tests 34 passed.
- Service payload contract retains trace columns through decoration/cache boundaries; service + dashboard combined 40 passed.
- Wrote `references/domain/spc/spec-data_source.md` with all field sources, route tables, selection/fallback rules, snapshot contract and probe evidence.
- Real DB sample integration: 72 rows across all 6 factory/routes remained 72 rows; all CHAMBER routes matched formal histories, TP EQP exercised both formal history and measurement fallback.
- Review hardening RED→GREEN: shared Glass EQP history now explicitly filters `factory LIKE 'OLED%'` or `factory='TP'`; trace tests increased to 9 passed.
- M626 runtime snapshot refreshed to final policy v2 after Glass factory isolation: 743503 rows, all five trace columns present; sources cover all six formal routes plus explicit fallbacks, with 9 `unmatched_chamber` rows remaining diagnosable.
- Final targeted regression: 55 passed. SPC smoke: 150 passed, with only 3 unrelated AOI_RS pandas FutureWarnings.
- Browser E2E passed on `/SPC监控报表`: final v2 run rendered 3 visible main-process charts and 3 time charts for OLED/21200; main-process labels were real equipment/chamber IDs, 1440px and 900px had no horizontal overflow, and no unexpected console errors occurred. Five precisely allowlisted Streamlit health/metrics network errors were recorded.
- Full unit suite is not globally green for unrelated pre-existing areas: `test_shadow_ema.py` fails collection; excluding it yields 331 passed / 5 failures in yield/config tests. No failing test touches SPC Task2 files.
- Static checks: compileall and task-scoped whitespace/diff checks passed. Ruff, Black and pytest-cov are not installed in the project environment, so no dependency was installed implicitly.
- Updated `ARCHITECTURE.md`; accepted `docs/ADR/0009-spc-main-process-equipment-chamber-trace.md`; all issue acceptance criteria checked.

## Phase status

| Phase | Status | Evidence |
|---|---|---|
| Requirements | complete | Issue ready-for-agent with Agent Brief |
| Planning | complete | task_plan/findings/progress created; approval recorded |
| Development & testing | complete | 55 targeted + 150 SPC smoke passed; E2E passed |
| Project record | complete | ADR-0009 accepted and architecture/spec updated |

## Test results

| Test | Expected | Actual | Status |
|---|---|---|---|
| Read-only DB schema probe | All required sources found | 7/7 sources found in mdw | passed |
| Spec key uniqueness | No three-key duplicates | 1352 rows = 1352 keys | passed |
| M626 chamber route probe | Main-step histories available | ARRAY/OLED/TP CHAMBER 100% sample coverage | passed |
| SPC baseline smoke | Existing SPC suite green | 138 passed, 3 unrelated warnings | passed |
| Spec route RED→GREEN | Main route fields + defaults | Initial fail, then 2 passed | passed |
| Main-process trace slices | Six routes + nearest OUT + no expansion + Glass factory isolation | 9 passed | passed |
| Repository trace integration | Enrich refresh and persist policy v2 | 4 passed | passed |
| Dashboard/service integration | Main-process-only labels and native payload | 40 passed | passed |
| Real DB trace sample | Six routes, no row expansion | 72→72 rows, 6 routes | passed |
| M626 runtime snapshot | Trace contract persisted | 743503 rows; 5/5 trace columns | passed |
| Final targeted SPC tests | Changed contracts remain green | 55 passed | passed |
| Final SPC smoke | Project SPC regression remains green | 150 passed, 3 unrelated warnings | passed |
| Browser E2E | Functional/visual/viewport/exploratory | passed; 0 unexpected console errors | passed |
| Full unit collection | Repository-wide collection | unrelated `test_shadow_ema.py` import error | pre-existing blocker |
| Full unit excluding collection blocker | Detect cross-domain regressions | 331 passed, 5 unrelated existing failures | task unaffected |

## Error log

| Error | Attempt | Resolution |
|---|---:|---|
| Incorrect repository file path in a parallel read | 1 | Located actual file with `rg --files`. |
| Invalid combined rg quoting | 1 | Re-ran with a simpler single-quoted expression. |
| Plotly element screenshot timed out on a hidden lazy-rendered node | 1 | Retained the independently captured chart screenshot; stable E2E asserts DOM chart contracts and captures the full page. |
| Ruff/Black/pytest-cov unavailable | 1 | Logged missing tools; did not mutate dependencies; used compileall/tests/smoke/E2E/scoped diff check. |

## Reboot check

| Question | Answer |
|---|---|
| Where am I? | Completed implementation, verification, and ADR closeout |
| Where am I going? | User handoff; no Task2 work remains |
| What's the goal? | Second SPC chart uses main-process equipment/chamber without row amplification |
| What have I learned? | See findings.md |
| What have I done? | Requirements, DB discovery, TDD implementation, regression, E2E, spec/architecture/ADR complete |
