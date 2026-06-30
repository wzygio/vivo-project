# Plan: SPC CPM Monitoring Report

## Objective
Build an SPC-only CPM monitoring report that calculates Lot-level CPM per monitoring indicator and provides a Streamlit page for filtering, trend review, and Lot-to-Sheet drilldown.
The page also supports CPK with the same filters, layout, and drilldown behavior.

## User Journeys
- As a quality engineer, I want to filter CPM by product, factory, parameter, and station, so that I can focus on the SPC indicators I own.
- As a quality engineer, I want each monitoring indicator in its own expander, so that I can scan Lot trends independently.
- As a quality engineer, I want to click a Lot and see all Sheet measurements under that Lot, so that I can diagnose whether the Lot CPM is driven by drift or dispersion.

## TDD Checklist
1. [x] Add core CPM formula and Lot aggregation tests.
2. [x] Add application service test proving only SPC data contributes to CPM.
3. [x] Implement `spc_domain.core.cpm_calculator`.
4. [x] Implement `spc_domain.application.cpm_service`.
5. [x] Add Streamlit CPM page and reusable section renderers.
6. [x] Run focused unit tests.
7. [x] Run compile checks for touched modules/pages.
8. [x] Run Streamlit smoke test for the new page.

## Task1-Fix Checklist
1. [x] Remove custom select-all/clear buttons and global expand/collapse buttons.
2. [x] Change Lot CPM trend from line/scatter to bar chart.
3. [x] Replace product filter with Header-controlled product only.
4. [x] Add factory -> station and station -> parameter filter cascade.
5. [x] Add query-triggered rendering after filter selection.
6. [x] Switch default data window to the first day of the three-month reporting window.
7. [x] Run focused unit tests, compile checks, and Streamlit smoke test.

## Task2 Checklist
1. [x] Remove the CPM=1.0 reference line from the Lot trend chart.
2. [x] Use one bar color for all Lot trend bars.
3. [x] Trim Sheet detail columns to lot_id, sheet_id, factory, station, parameter, and sheet_mean.
4. [x] Add CPK calculation to the Lot aggregation output.
5. [x] Add a page-level selector to switch between CPM and CPK.

## Touched Files
- `src/spc_domain/core/cpm_calculator.py`: CPM/CPK formulas, Lot ID derivation, Lot-level aggregation.
- `src/spc_domain/application/cpm_service.py`: data orchestration using existing SPC Repository and Sheet feature reducer.
- `app/sections/spc_cpm_dashboard.py`: CPM/CPK report UI helpers.
- `app/pages/CPM监控报表.py`: Streamlit page with CPM/CPK selector.
- `tests/unit/test_spc_cpm_calculator.py`: core unit tests.
- `tests/unit/test_spc_cpm_service.py`: application service unit test with fake repository.
- `tests/unit/test_spc_cpm_dashboard.py`: UI helper tests for filter cascade, date window, chart type, chart styling, and drilldown detail columns.

## Validation
- `uv run pytest tests/unit/test_spc_cpm_calculator.py tests/unit/test_spc_cpm_dashboard.py tests/unit/test_spc_cpm_service.py -q`
- `uv run python -m compileall src/spc_domain app/pages app/sections`
- Streamlit HTTP/browser smoke for `CPM监控报表`.

## Rollback
Remove the new CPM files and tests. No existing L1 Parquet TTL, DB singleton, or SPC alarm logic is modified by this plan.
