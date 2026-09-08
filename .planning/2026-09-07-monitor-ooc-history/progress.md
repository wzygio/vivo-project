# Progress

## 2026-09-07

- Read Task1-1-1, repository architecture/domain docs, Streamlit performance/dashboard guidance and ECC rules.
- Created `feat/monitor-ooc-history` from current master while preserving dirty worktree changes.
- Published approved PRD and five local tracker tickets.
- Chose complex plan and recorded OOC/SOOS/resource-routing contracts.

## Errors

- Initial Streamlit reference lookup used the global skill root instead of the project-discovered package
  path; corrected to `.venv/Lib/site-packages/streamlit/.../references/`.

## Verification

- OOC core/history/monitor/resource-path tests: 13 passed.
- Producer and adjacent targeted suite: 126 passed (3 pandas warnings).
- Browser E2E: query gate verified; OOS=1, OOC=1, SOOS=0; admin status hidden before
  query and visible after query; 0 console errors; 768 px viewport has no horizontal overflow.
- Full suite: 999 passed, 29 failed. Five migration-path failures were corrected; remaining
  failures are pre-existing/current-worktree date, Yield, Equipment, encrypted-resource and
  portal/header expectations outside Task1-1-1.
- Review high finding fixed: explicit/temp resource directories now route OOC snapshots to an
  isolated `.ooc_history`; two test-created project snapshots were identified by metadata and removed.
- Review medium finding fixed: monitor uses generic upper/lower-limit labels instead of presenting
  OOC UCL/LCL as USL/LSL. Custom `resources.dir` continues to honor configured file paths.
- Follow-up summary-table requirement completed: producers now update isolated daily throughput
  history; monitor renders overlapping Y/Q/M/W totals for throughput, OOC, zero SOOS, OOS and Total.
- OOC decision management is available only in admin mode and supports scope-specific keys.
- Follow-up tests: 9 period/query tests + 68 producer/shared tests + 10 OOC admin/service tests passed;
  compileall passed. Browser E2E verified query-before/after, 128/1/0/1/2 summary values,
  admin download/upload controls, 0 console errors and no 768 px page overflow.
- Three confirmed `NO_CTQ_DATA` test artifacts were moved out of production history into
  `output/tmp/test-pollution-quarantine/`; the regression test no longer recreates them.
- Final focused suite: 242 passed, 4 warnings. Full repository suite: 1014 passed, 19 failed;
  all 19 are outside this change (existing Equipment time shift, encrypted-SPC diagnostic,
  page/header, Q-Time dates, snapshot dates and Yield expectations). A final 17-test seam
  rerun plus compileall passed after tightening repository-only persistence authorization.
- Physical-Sheet follow-up: throughput and alarm totals now de-duplicate by factory/product/
  Sheet across parameters and scopes; the throughput store serializes read-modify-write across
  processes. The focused seam passed 12 tests, and the wider Inline/dashboard suite passed
  405 tests with four unrelated existing date-shift failures.
- Final browser E2E at 768 px verified query-gated rendering, Y/Q/M/W columns,
  128/1/0/1/2 summary values, admin-only status/decision controls, zero exceptions,
  zero console errors, and no horizontal page overflow.
