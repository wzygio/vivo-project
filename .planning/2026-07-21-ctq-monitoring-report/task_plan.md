# Task Plan: CTQ Monitoring Report

## Goal

Deliver an independently discoverable CTQ Streamlit report that reuses the Inline physical SPC repository with a forced `CTQ` filter, matches SPC's non-CPM/CPK experience, and passes focused, regression, cache-reload, and browser smoke verification.

## Source and approval

- Ready issue: `.scratch/ctq-monitoring-report/issues/01-create-ctq-monitoring-report.md`
- Source prompt: `docs/dev_docs/dev_prompt/opt-SPC.md`, Task3
- Approval: On 2026-07-21 the user explicitly selected the default recommended design and authorized uninterrupted execution when no information is unavailable. This approves the plan, the interfaces below, and the test-first priority required by the TDD gate.

## Current Phase

Phase 2 — TDD tracer bullet.

## Phases

### Phase 1: Requirements, discovery, and plan

- [x] Convert Task3 into one `enhancement` issue with `ready-for-agent` status, complete acceptance criteria, Agent Brief, and no unresolved question.
- [x] Verify the shared physical repository supports a `CTQ` whitelist and record adr-0001's native-cache-payload boundary.
- [x] Approve the default architecture and validation priority from the user's explicit authorization.
- **Status:** complete

### Phase 2: TDD tracer bullet — CTQ service boundary

- [x] RED: prove a CTQ service forces `data_type = "CTQ"`, exposes only raw/Sheet/indicator/OOS data, and assigns `line` to names containing `UNI`; verify with focused unit tests.
- [x] GREEN: implement the smallest independent CTQ application/core boundary and native cached payload needed to satisfy the service contract without invoking capability calculation.
- [x] Verify CTQ OOS persistence resolves to a CTQ-specific resource boundary and cannot overwrite SPC resources.
- **Status:** complete

### Phase 3: Vertical slice — CTQ dashboard and page

- [x] RED: prove the CTQ dashboard consumes backend `chart_type`, renders line and box distributions, handles empty/one-sided specs, and exposes no CPM/CPK controls; verify with dashboard unit tests.
- [x] GREEN: create the independent CTQ section and Streamlit page with SPC-equivalent Header, filters, query gate, layout, statuses, and admin-only OOS modifier.
- [x] Verify the page constructs a forced-CTQ query, refresh discovers the CTQ cache, and its custom ViewModel is constructed outside `st.cache_data`.
- **Status:** complete

### Phase 4: Regression and runtime verification

- [x] Run CTQ service/core/dashboard/page unit tests and record exact counts.
- [x] Run the Inline/SPC domain smoke suite to prove existing SPC/monitor behavior remains compatible.
- [x] Run the cache-fill → module-reload regression and prove cached payloads remain native and reusable.
- [x] Run project-required syntax/static checks (`compileall` or targeted `py_compile`, plus `git diff --check`).
- [x] Launch the CTQ Streamlit page and perform browser functional smoke: page opens, Header/filter/query flow works, no visible CPM/CPK text, and empty-data handling is stable.
- [x] Perform browser visual/viewport smoke at desktop and a narrow viewport: no clipped controls, overlapping charts, horizontal page overflow, or unreadable labels.
- [x] Perform exploratory browser smoke for product switch/refresh, admin/non-admin OOS visibility, zero results, and at least one box/UNI-line indicator when fixture data permits; explicitly record unavailable-data limitations.
- [ ] Complete repository-wide unit/all-test regression with no failures. Blocked by the documented stale Yield import, six existing Yield failures, the `streamlit-echarts` collection issue, and non-unit DB/manual tests that exceeded the bounded run.
- **Status:** in_progress

### Phase 5: Records and delivery

- [x] Update architecture/design records for the separate CTQ application/UI ownership and shared physical repository boundary.
- [ ] Create an adr only after automated and browser checks pass, recording CTQ isolation, repository reuse, capability exclusion, cache contract, and rollback boundary.
- [x] Mark every fulfilled issue acceptance criterion and add delivery evidence without changing the approved scope.
- [x] Review the dirty worktree, preserve unrelated user changes, and report changed files, tests, known exclusions, and any pre-existing failure.
- **Status:** pending

## Acceptance-criteria mapping

| Issue criterion | Plan evidence |
|---|---|
| Independent page and SPC-equivalent non-capability UX | Phase 3 page/dashboard tests; Phase 4 browser functional and visual smoke |
| Forced CTQ repository query and unchanged SPC behavior | Phase 2 service test; Phase 4 Inline/SPC regression |
| Filters, M/W/D charts, Sheet points, backend UNI rule | Phase 2 metadata test; Phase 3 dashboard tests; Phase 4 browser smoke |
| Admin OOS modifier with isolated resources | Phase 2 persistence test; Phase 3 page test; Phase 4 admin/non-admin smoke |
| No CPM/CPK calculation or presentation | Phase 2 negative service test; Phase 3 negative UI test; Phase 4 browser text check |
| Safe empty/missing/one-sided-spec states | Phase 3 unit tests; Phase 4 exploratory smoke |
| Native cache, refresh, and hot reload | Phase 3 cache discovery test; Phase 4 reload regression |
| TDD, regression, static, and browser evidence | Phases 2–4 commands and progress log |

## Approved design decisions

| Decision | Rationale |
|---|---|
| Use independent `ctq` application/core/section packages and a standalone CTQ page | Satisfies ownership separation while keeping future Inline modules extensible |
| Reuse the existing physical SPC repository but override caller query type to `CTQ` in the CTQ service | Prevents UI business logic and guards against accidental cross-type reads |
| Reuse generic visualization/OOS primitives through CTQ-owned adapters, with CTQ-specific persisted resources | Keeps behavior aligned without coupling CTQ resources to SPC state |
| Omit capability payload fields entirely instead of returning empty CPM/CPK frames | Makes the exclusion enforceable at the type/API boundary |
| Keep the page cache native and build CTQ ViewModel outside cache | Required by adr-0001 and Streamlit module reload safety |
| Preserve the backend-owned `UNI` chart-type decision | UI remains free of parameter-name business rules |

## Out-of-scope guardrails

- No CPM/CPK formula, window, alert, or decorator changes.
- No new physical repository or database protocol.
- No AOI/AOI_RS report or visual redesign.
- No cleanup/reset of unrelated dirty worktree changes.

## Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| Streamlit skill discovery reported the installed project version is older than 1.57 | 1 | Do not upgrade dependencies; use the skill-prescribed official documentation fallback and existing repository APIs |
| Direct opening of Streamlit `llms-full.txt` was rejected by the web safety layer | 1 | Use official-domain search for the specific caching and multipage references instead of retrying the same URL |
| Planning findings patch used a context line belonging to another planning file | 1 | Re-read exact contexts and patch each planning file independently |
| Combined source/test inspection command exited 1 because an optional `rg` glob matched no `requirements*.txt` path | 1 | Treat captured source output as valid; use explicit existing paths for later searches |
| Smoke inventory command exited 1 because an optional `tests/unit/tools` path does not exist | 1 | Use the discovered `tests/unit/test_smoke_runner.py` directly and avoid optional directory arguments |
| Complete unit collection fails in the documented stale Yield test `test_override_logic.py` because `create_mwd_trend_data` is not exported | 1 | Preserve scope; verify the failure is unrelated/unchanged, run the remaining unit suite with this one known test explicitly excluded, and report it |
| Remaining complete unit suite exposes six unrelated existing Yield failures | 1 | Keep CTQ scope; retain the passing 179-test signal plus exact six failures, and continue CTQ static/browser verification |
| New Streamlit launch exited because port 8503 is already served by this repository | 1 | Do not terminate the user's existing process; verify its command line points to this repo and reuse it for Playwright QA |
| Cleanup patch had an invalid empty hunk separator | 1 | Reissue a narrower valid patch and record the failed attempt |
| Complete `tests/` collection also fails on the documented `streamlit-echarts` component metadata issue | 1 | Preserve dependencies; exclude the known component diagnostic and unrelated Yield blockers to run the remaining full suite |
| Remaining `tests/` run exceeded the 120-second command budget without producing a result | 1 | Inspect non-unit test inventory and use verbose bounded runs to identify manual/hanging scripts instead of repeating the same opaque command |
| Recursive cleanup of generated browser artifacts was blocked by the command safety policy | 1 | Remove only the eight explicitly named CTQ workbooks and then their empty directories without recursion; leave browser session artifacts if policy still disallows cleanup |
| Exact non-recursive removal of the generated CTQ workbooks was also blocked | 2 | Stop cleanup attempts; retain the auditable generated OOS workbooks and report their paths rather than bypass safety policy |
