# Progress: Equipment Real/Fabricated Snapshot Fallback

## 2026-07-21 — Requirements and planning

- **Status:** complete
- Applied `development-flow`, `create-local-markdown-issue`, `triage`, and `planning-with-files` in inline mode.
- Read project context, issue/triage contracts, glossary, architecture, and ADR-0001.
- Created and triaged the issue to `ready-for-agent`; no unresolved decisions.
- Created this isolated plan and recorded the user's direct execution-through-smoke instruction as approval.
- No production code changed during requirements/planning.

## 2026-07-21 — TDD generation/update

- **Status:** complete
- Read development/testing, TDD, Playwright CLI, and project coding instructions.
- RED: initial-generation test failed at import because `generate_fabricated_snapshot` did not exist.
- GREEN: added the explicit generation interface and new policy fields; deterministic 0–100% values and independent two-day timestamps pass (`1 passed`).
- RED/GREEN: independent filename test first collided with a real `part_life_snapshot_*` sentinel; changed fabricated output to `part_life_fabricated_<signature>.parquet`, preserving the real file (`1 passed`).
- RED/GREEN: new update module advances times one day, adds 30% specification, resets only values exceeding 100% into 0–30% (`1 passed`).
- RED/GREEN: file updater skips a 23-hour snapshot and force-updates it; update-only CLI fails on missing input without creating a dataset (`2 passed`).
- RED/GREEN: matcher initially rejected the fallback argument; added per-spec real-first fallback. Unit tracer passed.
- RED/GREEN: service integration initially measured only the single real row; added isolated fabricated loader and service fallback. Full 1,781-row report passed with the real sentinel retained.
- Focused generation/update/matcher/service regression: `32 passed in 2.58s`.
- Two obsolete tests still referenced the superseded status-distribution policy; updated them to the approved 0–100% policy and reran green.
- Generated the production fabricated snapshot with 1,685 rows and confirmed a fresh no-force update is skipped by the 24-hour TTL.

## 2026-07-21 — Dataset, regression, and browser smoke

- **Status:** complete
- Mixed-source audit: 1,781 report rows = 248 real matches + 1,533 fabricated fallback matches + 0 unmatched; exact real value/time precedence retained.
- Focused regression: `32 passed in 2.58s`; updater boundary suite: `3 passed`; integration: `1 passed in 2.36s`.
- Final Equipment smoke: `34 passed in 1.39s`; `compileall` and scoped `git diff --check` passed.
- Browser functional smoke passed at the existing repository Streamlit server on port 8503: default Array view, TP filter, Array reset, metrics, table, hidden parameter columns, and no visible execution errors.
- Visual QA passed for desktop and narrow layouts. The page has no horizontal overflow; the narrow data grid scrolls internally and exposes its rightmost columns.
- Saved desktop, table, narrow, and narrow-right-scroll screenshots in this plan directory.

## 2026-07-21 — Project record and handoff

- **Status:** complete
- Updated `ARCHITECTURE.md` with independent snapshot ownership, read-only report flow, and real-first fallback.
- Added ADR-0003 after automated and browser gates passed.
- Updated the local issue acceptance checklist and appended delivery evidence.
- Reviewed the dirty worktree and left unrelated CTQ/SPC, resources, prompts, and page changes untouched.

## Test results

| Check | Result |
| --- | --- |
| Requirements gate | PASS — one category, `ready-for-agent`, Agent Brief present |
| Planning gate | PASS — three planning files, checklist mapping, approval recorded |
| Fabrication lifecycle | PASS — independent files, separate CLIs, deterministic generation/update contracts |
| Mixed-source report | PASS — 248 real + 1,533 fabricated + 0 unmatched; real precedence retained |
| Equipment smoke | PASS — 34 tests |
| Browser smoke | PASS — load, metrics, table, filter/reset, hidden parameter columns, desktop/narrow |
| Project record | PASS — architecture updated and ADR-0003 accepted |

## Error log

| Error | Attempt | Resolution |
| --- | --- | --- |
| Streamlit bundled skill unavailable because installed version <1.57 | 1 | No dependency upgrade; use repository-compatible APIs and browser tooling |
| Focused regression: two obsolete fabrication tests failed after policy replacement | 1 | Updated assertions/calls to the approved generation contract; 32 focused tests passed |
| Audit here-string corrupted Chinese column literals | 1 | Used Unicode escapes for the read-only audit; all assertions passed |
| Separate Streamlit launch rejected by environment policy | 1 | Reused the verified existing repository process on port 8503 |
| `playwright-cli wait` is unsupported | 1 | Used `run-code` with Playwright's documented wait API |
| Dropdown option click timed out after popup state changed | 1 | Used the normal keyboard interaction path; filter and reset succeeded |
| Combined PowerShell cleanup was rejected by environment policy | 1 | Deleted only the five temporary files created by this run with targeted patches |
