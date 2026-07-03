# Findings: Period-Scoped Code Baseline

## User Principle
- In the absence of user intervention, historical yield should not change.
- User clarified that multiplier/config changes are intentional intervention.
- The current unacceptable behavior is automatic whole-file `code_baseline` rebuild changing anchors for old months.

## Current Baseline Behavior
- `Z571_codebaseline.xlsx` currently stores one global `baseline_rate` per `defect_desc`.
- Z571 metadata observed:
  - `source_start`: `2026-06-23`
  - `source_end`: `2026-07-03`
  - `refresh_reason`: `missing_codes`
  - `code_count`: `12`
- `_build_code_baseline` computes `sum(defect_panel_count) / sum(total_panels)` over the current padded Code-level daily window.
- `_calc_code_ema_noise` uses the same `baseline_map.get(code, 0.0)` for every month of that Code.
- `_ensure_code_baseline_current` can rebuild the whole file for missing file, expired/legacy metadata, missing current Codes, or multiplier signature changes.

## Desired Behavior
- Store baselines by target month: each target month uses the previous month's mean.
- Example: May trend uses April Code mean; June uses May Code mean; July uses June Code mean.
- The first displayed month needs an earlier source month, so `get_time_window` should cover the first day three months before the end date through the end date.
- Existing `YieldAnalysisService.get_time_window` already does `end - relativedelta(months=3)` then `replace(day=1)`.

## Implemented Behavior
- `Sheet1` now uses columns: `baseline_month`, `source_month`, `defect_desc`, `baseline_rate`.
- A source month writes the next month's anchor:
  - `source_month=2026-04` -> `baseline_month=2026-05`
  - `source_month=2026-05` -> `baseline_month=2026-06`
- Existing period-scoped rows are preserved during automatic missing-row refresh.
- Missing period rows are appended with refresh reason `missing_period_rows`.
- Legacy two-column files are migrated to period-scoped rows.
- Multiplier signature changes still rebuild the current extracted window because that is a user intervention.

## Risk/Compatibility Notes
- Existing Excel files may be legacy two-column format.
- Tests currently exercise private helpers in `test_code_baseline_refresh.py`; for this narrow baseline behavior, continuing there is acceptable, but prefer behavior-oriented assertions.
- `defect_multipliers` are applied before Code raw daily aggregation, so period-scoped baseline generation from the processed window naturally reflects the current multiplier version.
