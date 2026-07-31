# Task Plan: Equipment Real Snapshot with Fabricated Fallback

## Goal

Keep database and fabricated critical-parts snapshots independent, generate and update fabricated records through separate deterministic workflows, and build the report with real measurements first and fabricated values only for unmatched specifications.

## Source and approval

- Ready issue: `.scratch/equipment-snapshot-fallback/issues/01-use-real-snapshot-with-fabricated-fallback.md`
- Approval: On 2026-07-21 the user explicitly requested implementation, dataset update, continuous optimization, and completion through browser smoke. This approves the plan, public CLI split, independent snapshot naming, and test-first priority.

## Current Phase

Complete — implementation, dataset audit, automated regression, browser smoke, and project records are finished.

## Phases

### Phase 1: Requirements and planning

- [x] Create one enhancement issue, triage it to `ready-for-agent`, and record complete acceptance criteria and Agent Brief.
- [x] Verify current collision: database and fabricated data share one production-signature path.
- [x] Record adr-0001 cache constraints and explicit user execution approval.
- **Status:** complete

### Phase 2: TDD tracer bullet — independent fabrication lifecycle

- [x] RED/GREEN: generated values are within 0–100% specification and timestamps independently fall in the preceding two days; fixed seed/time is reproducible. Verify with focused unit tests.
- [x] RED/GREEN: fabricated snapshot uses an independent signature filename and cannot overwrite a real snapshot. Verify with temp-path storage tests.
- [x] RED/GREEN: update preserves keys/count, advances every timestamp one day, adds 30% specification, and resets crossed values within 0–30%. Verify exact boundary tests.
- [x] RED/GREEN: update fails on missing/invalid/unmappable input and respects 24-hour skip/force behavior. Verify CLI/domain tests.
- [x] Provide separate generation and update CLIs; neither silently performs the other's job. Verify command-level tests/help.
- **Status:** complete

### Phase 3: TDD vertical slice — real-first report fallback

- [x] RED/GREEN: when both sources match, report uses the real record even when fabricated timestamp is newer.
- [x] RED/GREEN: when real data is missing, report uses fabricated data; when both are missing, measurement remains empty.
- [x] Preserve non-empty LIKE matching and blank-parameter synthetic exact matching. Verify existing and new matcher tests.
- [x] Load the independent fabricated snapshot without changing database query/8-hour snapshot behavior.
- [x] Keep `st.cache_data` payload native and page-visible parameter columns hidden. Verify cache/page tests.
- **Status:** complete

### Phase 4: Dataset and verification

- [x] Generate a new full fabricated snapshot from the real 1,781-row specification baseline; record row count, value/time bounds, and output path.
- [x] Exercise the update program against a controlled expired copy, then update the production fabricated dataset only under the approved TTL/force contract.
- [x] Verify a mixed report against the current real snapshot: real matches retain exact real values/times; unmatched rows receive fabricated values.
- [x] Run focused generation/update/matcher/service tests and full `python tools/smoke.py equipment`.
- [x] Run syntax/static checks and `git diff --check` for touched files.
- [x] Launch/reuse Streamlit and run browser functional smoke: page loads, metrics/table render, no visible error, parameter columns remain hidden.
- [x] Run desktop and narrow viewport visual smoke; inspect clipping, overflow, readability, post-filter state, and one exploratory filter/reset path.
- **Status:** complete

### Phase 5: Records and delivery

- [x] Update `ARCHITECTURE.md` for independent snapshot ownership and real-first fallback.
- [x] Write adr after scoped automated and browser smoke pass, including lifecycle, precedence, failure behavior, and rollback.
- [x] Mark issue acceptance criteria with evidence and finalize checklist.
- [x] Review dirty worktree; preserve unrelated CTQ/SPC/user changes.
- **Status:** complete

## Acceptance-criteria mapping

| Issue behavior | Plan evidence |
| --- | --- |
| Independent snapshot files | Phase 2 storage test; Phase 4 filesystem inspection |
| Separate generation/update programs | Phase 2 CLI and failure-boundary tests |
| 0–100% values and near-two-day times | Phase 2 deterministic generation tests |
| +1 day, +30%, reset ≤30% | Phase 2 update boundary tests |
| 24-hour TTL, skip and force | Phase 2 repository/CLI tests |
| Real-first, fabricated fallback | Phase 3 matcher/service tests; Phase 4 mixed-data audit |
| Compatibility and cache/UI boundaries | Phase 3 regression; Phase 4 smoke/browser QA |
| Updated usable dataset | Phase 4 generated artifact and report audit |

## Approved design decisions

| Decision | Rationale |
| --- | --- |
| Name fabricated snapshots `part_life_fabricated_<signature>.parquet` | Prevents any DB refresh from overwriting fabrication output |
| Keep generation and update as separate pure functions plus separate CLIs | Enforces distinct preconditions and makes each lifecycle operation auditable |
| Match each specification against real data first, then fabricated data | Guarantees fabricated timestamps never override an available real measurement |
| Use file age for 24-hour update eligibility; `--force` is explicit | Aligns with existing snapshot TTL pattern while keeping controlled tests/operations possible |
| Keep provenance internal, not a visible report column | User requested data behavior, not a UI/debug-field change |

## Out-of-scope guardrails

- No database SQL, real snapshot TTL, alert threshold, decoration, trend-generation, or unrelated CTQ/SPC changes.
- No deletion or replacement of existing real snapshot files.
- No UI redesign or exposure of parameter/internal source fields.

## Errors Encountered

| Error | Attempt | Resolution |
| --- | --- | --- |
| Streamlit skill discovery reports project Streamlit <1.57 | 1 | Preserve dependency; use established project APIs and existing smoke tooling |
| Focused regression found two obsolete tests referencing removed status-distribution fields | 1 | Updated tests to the approved generation policy; rerun passed 32/32 |
| First dataset audit used literal Chinese column names in a PowerShell here-string and produced `KeyError` after shell encoding | 1 | Re-ran the read-only audit with Unicode escapes; mixed-source assertions passed |
| Starting a second hidden Streamlit process was rejected by the environment policy | 1 | Confirmed the existing port 8503 process belongs to this repository and reused it without changing process state |
| Installed Playwright CLI has no `wait` command | 1 | Used the documented `run-code` wait strategy |
| First dropdown option click timed out after the popup closed between commands | 1 | Reopened the native combobox and used normal keyboard End/Enter interaction; TP filter and Array reset both passed |
| Combined PowerShell cleanup command was rejected by the environment policy | 1 | Removed only this run's five Playwright temporary files through targeted patches; older unrelated artifacts were preserved |
