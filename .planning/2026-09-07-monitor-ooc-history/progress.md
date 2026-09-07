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
