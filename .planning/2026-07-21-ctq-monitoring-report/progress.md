# Progress Log: CTQ Monitoring Report

## 2026-07-21 — Requirements and planning

- **Status:** complete
- Read the user-selected `development-flow` skill and its requirements/planning references.
- Read and applied the required local-issue, triage, and planning-with-files dependency skills.
- Verified project context, domain glossary, issue policy, triage policy, relevant cache adr, and the absence of CodeGraph.
- Created and triaged `.scratch/ctq-monitoring-report/issues/01-create-ctq-monitoring-report.md` to `ready-for-agent`.
- Captured the user's explicit default-recommendation/execute-through authorization as plan approval.
- Created this isolated plan and switched `.planning/.active_plan` to it.
- No production code changed during requirements or planning.

## 2026-07-21 — TDD tracer bullet

- **Status:** in_progress
- Next action: read the development-testing workflow reference and required TDD/Streamlit skills, then write the first failing CTQ service contract test.
- Streamlit skill discovery failed because the installed project version is older than 1.57. Per the skill, no dependency upgrade will be made; official docs and existing compatible repository APIs are the fallback.
- Official Streamlit references verified the cache clear/copy behavior and direct-`pages/` discovery contract needed by the implementation.
- Added the first tracer test for the public CTQ service behavior: forced CTQ query, capability-free ViewModel, and backend chart-type metadata.
- RED confirmed: focused pytest failed at collection with `ModuleNotFoundError: src.inline_domain.application.ctq` (1 error).
- Implemented the minimal CTQ application service, CTQ OOS adapter/resource boundary, and backend chart-type rule required by the tracer.
- GREEN confirmed: the focused CTQ service tracer passes (`1 passed` in 0.77s).
- OOS isolation assertion passes (`1 passed` in 0.79s): CTQ files live under `resources/<product>/ctq` and no workbook is written into the SPC product root.
- Added the next single-behavior test: a backend-marked CTQ `UNI` indicator must render month/week/day Scatter traces even though CTQ has no capability frame.
- RED confirmed: the CTQ dashboard test failed at collection because `app.sections.ctq` did not exist (1 error).
- Added the CTQ-owned period-chart facade and taught the shared chart primitive to derive line periods from measurement points when no capability frame exists.
- GREEN confirmed: capability-free CTQ `UNI` period chart renders month/week/day Scatter traces (`1 passed` in 0.63s).
- Added the next behavior test: each CTQ indicator renders exactly three distribution figures using backend chart metadata and never invokes capability metrics/tables.
- RED confirmed: collection failed because `render_ctq_indicator_sections` was absent (1 error).
- Implemented the capability-free CTQ indicator renderer using the shared distribution primitives and CTQ-owned public facade.
- GREEN confirmed: CTQ indicator rendering produces exactly three distribution figures and no capability widgets (`2 passed` in 0.62s).
- Added the page vertical-slice test: one forced-CTQ load followed only by filters and distribution charts, using a ViewModel with no capability field.
- RED confirmed: the page test failed because the CTQ filter interface was absent (`1 failed`).
- Added CTQ-specific cascade filters/session keys, report filtering, OOS-only admin panel, and the direct `app/pages/CTQ监控报表.py` entrypoint.
- Extended the shared OOS renderer with backward-compatible report name/key-prefix options so CTQ labels and widget state remain isolated.
- GREEN confirmed: the page loads one CTQ ViewModel and renders only filters/charts (`1 passed` in 0.69s).
- Added a portal-navigation behavior test requiring both CTQ sidebar and skill-tree entries to target the new Streamlit page while preserving the legacy URL constant.
- RED confirmed: the portal had no `CTQ_REPORT` URL and both visible entries still used the legacy FineReport link (`1 failed`).
- Added the new Streamlit CTQ URL and routed the portal sidebar/skill-tree CTQ entries to it; the legacy `LINKS.CTQ` constant remains intact.
- Portal navigation GREEN confirmed together with the page slice (`2 passed` in 0.71s).
- Added a CTQ public-chart boundary test for the established `lsl=0` rule: only USL/UCL lines may be drawn.
- Zero-LSL chart boundary passes (`1 passed` in 0.60s); added the separate empty-filter-result behavior check.
- Empty-result behavior passes (`1 passed` in 0.51s); added the admin contract check requiring a CTQ-keyed OOS modifier and forbidding CPK tabs.
- CTQ-only admin contract passes (`1 passed` in 0.51s); added the adr-0001 cache-fill/module-reload regression for the CTQ public service.
- Cache-fill/module-reload regression passes (`1 passed` in 0.78s); added CTQ to the Header cache-discovery contract while excluding its ViewModel facade.
- Header cache discovery passes (`1 passed` in 0.72s). Phase 3 is complete; moving to focused/regression/runtime verification.
- Focused CTQ plus affected SPC regression passed: `43 passed` in 2.82s. This command includes the cache-fill/module-reload test and shared chart/OOS regressions.
- Read the repository validation/observability references and confirmed the required fast `spc`, complete unit, complete tests, static, and browser checks.
- Existing `spc` smoke passed (`82 passed` in 4.09s) but its target list omitted the new CTQ page/dashboard tests. Added a smoke-router contract requiring both.
- RED confirmed: the smoke-router contract failed because `test_ctq_page.py` was absent from resolved targets (`1 failed`). Added CTQ page/section patterns to the Inline `spc` smoke scope.
- Smoke routing GREEN confirmed (`1 passed`), then the expanded Inline/SPC smoke passed with CTQ included: `89 passed` in 4.06s.
- Complete unit command failed during collection on the documented pre-existing Yield import mismatch in `tests/unit/test_override_logic.py` (1 error). No CTQ test executed in this attempt; the unrelated test will be explicitly excluded for the remaining-suite signal.
- Remaining unit suite (excluding only the stale collection blocker) completed with `179 passed, 6 failed`. Failures are confined to existing Yield batch-code selector, Shadow EMA, and global data policy tests; no CTQ/Inline failures occurred.
- Targeted `compileall` and repository-wide `git diff --check` passed. Git emitted only existing LF→CRLF conversion warnings, with no whitespace error.
- Browser prerequisites are available (`playwright-cli`, Streamlit 1.55). A new hidden launch exited because port 8503 is already served by this repo's `app/Home.py` process (PID 25016, started 09:00); preserved and reused that existing process.
- Playwright functional QA passed on the live CTQ page: loaded M626, selected ARRAY/15260, queried auto-selected `4PP_Rs` + `4PP_UNI`, and observed 6 Plotly charts with correct box/line behavior and no visible CPM/CPK.
- Console inspection found only environment-level direct-route health/host-config 404s and blocked Streamlit telemetry; no Python/app rendering exception.
- Separate visual/viewport QA passed at 1440×900 and 768×900. Screenshots are stored in the plan directory; both sizes have zero horizontal body overflow and preserve six plots.
- Admin browser QA passed: `?admin=true` reveals only the CTQ-keyed OOS modifier; regex check found no CPM/CPK in the expanded page. Normal URL previously showed no admin expander.
- Exploratory product-switch QA passed for M626 → M678; Header and CTQ filter state rerendered without stale-product or empty/error output.
- The exact business combination M678 / ARRAY / 12140 / SE_L1T_UNI passed live QA: target section present, three Plotly Scatter figures, and zero CPM/CPK text; a sibling non-UNI parameter remained Box-based.
- Live Header cache refresh passed with M678/filter UI restored and no Python error. Product switches to Z517 and M673 also loaded CTQ filters normally.
- Browser empty-product fixture unavailable: every tested physical product has CTQ data. Automated empty-result contracts remain the verification source for that edge case.
- Post-browser cleanup kept the focused CTQ/shared-SPC suite green (`36 passed`); added an explicit service-level empty-physical-data contract to complement the unavailable live fixture.
- Empty-physical-data service contract passes (`1 passed` in 0.68s).
- Complete `tests/` collection was attempted and stopped with two pre-existing errors: `streamlit-echarts` component metadata in `test_top10_station.py` and the stale Yield import in `test_override_logic.py`.
- Remaining full-suite attempt (known failing files excluded) exceeded 120 seconds with no final pytest result. Per the error protocol, the next attempt will inspect/segment non-unit tests rather than repeat the same opaque command.
- Attempted cleanup of known generated CTQ workbooks/browser snapshots was blocked because the command used recursive removal over a verified list. Switching to explicit non-recursive file removal for CTQ workbooks; no user file was removed by the rejected attempt.
- Exact non-recursive removal was also policy-blocked. Cleanup attempts stopped; generated CTQ OOS workbooks and `.playwright-cli` evidence remain untracked and will be disclosed.
- Updated `ARCHITECTURE.md` and the Inline domain design reference with CTQ ownership, forced data-type, capability exclusion, native cache, and OOS isolation boundaries.
- Final expanded Inline/SPC smoke passed: `90 passed` in 4.55s. Targeted compile and `git diff --check` passed again.
- Development/testing remains `in_progress` only because the development-flow complete-regression gate cannot be met by the repository's pre-existing cross-domain failures/timeouts. CTQ implementation and all CTQ/SPC/browser evidence are complete; adr creation is intentionally not entered.
- Marked all CTQ issue acceptance criteria fulfilled and appended delivery evidence. Reviewed the dirty worktree without resetting prior monitor/SPC rename or unrelated user changes.
- Generated browser artifacts remain untracked at `.playwright-cli/` and `resources/{M626,M673,M678,Z517}/ctq/` because both cleanup attempts were policy-blocked; the CTQ workbooks are reproducible OOS audit outputs.

## Test results

| Test | Expected | Actual | Status |
|---|---|---|---|
| Requirements gate | One category, ready-for-agent, complete Agent Brief | `enhancement`, `ready-for-agent`, brief present | PASS |
| Planning gate | Three files, full acceptance mapping, approval recorded | Files and mapping created; user authorization recorded | PASS |

## Error log

| Timestamp | Error | Attempt | Resolution |
|---|---|---|---|
| 2026-07-21 | Streamlit skill discovery: installed version < 1.57 | 1 | Preserve project dependency; use official docs fallback and existing patterns |
| 2026-07-21 | Direct official `llms-full.txt` open rejected as unsafe | 1 | Switch to official-domain searches for narrowly relevant references |
| 2026-07-21 | Planning findings patch used a context line from the wrong file | 1 | Locate exact lines with `rg`; apply separate file-local contexts |
| 2026-07-21 | Optional requirements-file `rg` glob made an otherwise useful inspection command exit 1 | 1 | Use explicit existing paths for subsequent searches |
| 2026-07-21 | Optional `tests/unit/tools` search path made smoke inventory exit 1 | 1 | Read the discovered root-level smoke runner test directly |
| 2026-07-21 | Complete unit collection: stale Yield import `create_mwd_trend_data` | 1 | Preserve unrelated scope; run remaining suite with this known file excluded and report it |
| 2026-07-21 | Remaining unit suite has six unrelated Yield assertion/API failures | 1 | Record exact baseline; do not modify unrelated Yield behavior; continue CTQ verification |
| 2026-07-21 | Attempted Streamlit launch found port 8503 already occupied by the same repo app | 1 | Reuse verified existing process; do not terminate user state |
| 2026-07-21 | Cleanup `apply_patch` contained an invalid hunk separator | 1 | Reissued a file-local patch with valid hunks |
| 2026-07-21 | Complete test collection: `streamlit-echarts` component metadata plus stale Yield import | 1 | Keep dependencies/scope unchanged; run remaining suite with known files excluded |
| 2026-07-21 | Remaining full-suite command timed out at 120 seconds | 1 | Inventory and segment non-unit/manual tests; retain passing complete-unit signal |
| 2026-07-21 | Safety policy rejected recursive cleanup over verified generated-artifact paths | 1 | Use exact non-recursive workbook targets; do not retry the recursive command |
| 2026-07-21 | Safety policy also rejected exact non-recursive generated-workbook removal | 2 | Stop deletion attempts and disclose retained untracked outputs |

## 5-question reboot check

| Question | Answer |
|---|---|
| Where am I? | Phase 2, TDD tracer bullet |
| Where am I going? | Service → dashboard/page → verification → records/delivery |
| What's the goal? | Independent CTQ report with SPC-equivalent non-capability UX and forced CTQ data |
| What have I learned? | See `findings.md` |
| What have I done? | Requirements and plan gates completed; no production edits yet |
