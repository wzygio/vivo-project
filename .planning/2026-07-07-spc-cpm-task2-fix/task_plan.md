# Task Plan: SPC CPM/CPK Task2 Fix

## Goal
Optimize the CPM/CPK report charts:
- Use `unit_id` to derive chamber labels for Sheet point boxplots.
- Select the last available 2 months, 3 weeks, and 7 days with data rather than continuous empty calendar slots.
- Add visible CPM/CPK numeric labels to period metric points.
- Render three charts per indicator row: period overview, Sheet point boxplot by chamber, and Sheet point boxplot by pass time.

## Scope
- `src/spc_domain/infrastructure/data_loader.py`
- `src/spc_domain/infrastructure/repositories/spc_repository.py`
- `src/spc_domain/core/cpm_calculator.py`
- `app/sections/spc_cpm_dashboard.py`
- Existing CPM tests under `tests/unit/`

## TDD Approach
Use vertical red-green-refactor slices:
1. Add failing behavior test for `unit_id` chamber derivation and DAO field preservation.
2. Add failing behavior test for non-contiguous available-period axis.
3. Add failing behavior test for CPM/CPK point labels.
4. Add failing behavior test for rendering both Sheet point chart modes without dropdown-dependent generation where feasible.

## Phases
1. [complete] Inspect current CPM/SPC flow and create tests for requested behavior.
2. [complete] Implement `unit_id` extraction/preservation and chamber label normalization.
3. [complete] Implement available-period axis selection for 2 months/3 weeks/7 days with data.
4. [complete] Add CPM/CPK point labels and three-chart layout.
5. [complete] Run focused tests, compile checks, update planning files, and restart Streamlit if code changes require it.
6. [complete] Align CPM/CPK line windows with the period boxplot axis by backtracking to recent valid capability periods.
7. [complete] Change By-chamber Sheet point chart from chamber-level aggregation to Sheet-level sorting/coloring.
8. [complete] Make chart y-axis ranges expand when actual boxplot values exceed USL/LSL.
9. [complete] Run focused regression tests and compile checks.
10. [complete] Design CPM Sheet out-of-spec decoration files, matching keys, and admin-only upload/download flow.
11. [complete] Implement out-of-spec detail detection and product-scoped file persistence.
12. [complete] Implement deterministic default point clipping with flag-based opt-out.
13. [complete] Add CPM page admin controls for downloading details and uploading decoration flags.
14. [complete] Add focused tests, compile checks, and page smoke verification.
15. [complete] Remove CPM/CPK metric selector and replace period metric lines with a compact CPM+CPK table above the charts.
16. [complete] Replace expander summary metrics with median/min CPK and median/min CPM.
17. [complete] Move Sheet OOS data decoration from the CPM page into a shared backend SPC data preparation flow.
18. [complete] Wire both CPM/CPK report and Auto Warning dashboard to consume the shared decorated SPC data, while keeping scrap data separate.
19. [complete] Add focused regression tests for backend decoration sharing and service behavior.
20. [complete] Run focused tests, compile checks, smoke the pages, and update planning notes.
21. [complete] Refactor period capability table to a 2-row CPM/CPK matrix over the backfilled 2M+3W+7D window.
22. [complete] Show concrete USL/LSL/UCL/LCL values in CPM chart specification line annotations.

## Errors Encountered
| Error | Attempt | Resolution |
|---|---|---|
| Metric label test expected unavailable 2026-05 point | 1 | Updated test to respect available-period axis: labels only render for periods backed by Sheet data. |
| CPM/CPK line could not backfill if backend had already dropped older periods | 1 | Added all-available period capability calculation within the active query window, then let the dashboard select the latest valid visible periods. |
| Pandas Excel boolean reload returned `numpy.bool_`, not Python `False` singleton | 1 | Changed the unit assertion to check boolean semantics instead of object identity. |
