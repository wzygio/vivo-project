# Findings: SPC CPM/CPK Task2 Fix

## Initial Findings
- CPM page entry: `app/pages/CPM监控报表.py`.
- Service layer: `src/spc_domain/application/cpm_service.py`.
- Raw SPC measurements come from `SpcRepository.get_spc_measurements` and DAO `load_spc_measurements`.
- Current DAO selects `factory`, `prod_code`, `sheet_start_time`, `sheet_id`, `step_id`, `param_name`, `site_name`, `param_value`; it does not select `unit_id` yet.
- Current Sheet point chart chamber resolution prefers `chamber`, `chamber_id`, `sub_equip_id`, `eqp_id`, `main_eqp_type`, then `site_name`.
- Current period axis is fixed continuous 2 months / 3 weeks / 7 days relative to end date, which creates blank slots when no passing data exists in intermediate periods.
- Current period metric traces are split by month/week/day, but only markers/lines are shown; no text labels are visible.

## Decisions
- Treat `unit_id` as the canonical chamber source for CPM Sheet point charts.
- Derive chamber label as text before `-`; if no `-`, use first 6 characters.
- Keep fallback to previous chamber-like fields only when `unit_id` is absent.
- For the period overview chart, use valid CPM/CPK capability periods to drive the visible axis when they exist, so the metric lines and M/W/D boxplots share the same backfilled labels.
- For the By-chamber Sheet point chart, keep one box per Sheet. Chamber only controls sort order, legend group, and color.
- Keep USL/LSL as the default y-axis bounds only when all plotted values are inside the spec window; expand the range when actual values exceed the spec lines.
- Task2-feat data decoration will be scoped to the CPM/CPK Sheet point charts rather than changing period CPM/CPK calculations.
- Use product-scoped files under `resources/<prod_code>/`:
  - `spc_sheet_oos_detail.xlsx` for generated out-of-spec Sheet details.
  - `spc_sheet_oos_decoration.xlsx` for user-editable flags.
- Match user overrides by `prod_code`, `step_id`, `param_name`, and `sheet_id`; `factory` and timestamps remain informational.
- Default clipping should be deterministic per point/key, so the same refresh does not create visually moving random values.
- Admin controls should support the existing `?admin=true` convention and the user's `?admin-true` wording.

## Final Findings
- DAO `load_spc_measurements` now selects `unit_id`, so `raw_measurements_df` can drive chamber grouping.
- Repository snapshot freshness now treats old SPC snapshots without `unit_id` as stale, triggering a schema refresh while preserving old-cache fallback if DB refresh fails.
- The available-period axis chooses periods from actual `sheet_start_time` values:
  - latest 2 months with data,
  - latest 3 ISO weeks with data,
  - latest 7 calendar days with data.
- CPM/CPK metric traces now render visible text labels formatted to three decimals.
- The indicator expander now renders three charts in one row:
  - M/W/D Sheet Mean boxplot with CPM/CPK trend,
  - Sheet point boxplot By chamber,
  - Sheet point boxplot By pass time.
- The period capability dataframe now preserves all available M/W/D periods within the active query window, so the dashboard can backfill past recent single-sample periods and still display the latest valid 2 month / 3 week / 7 day CPM/CPK points.
- The visible period overview axis is selected per metric and period type. If CPM/CPK exists for a type, those valid periods drive both the line and box labels; if not, the chart falls back to recent Sheet-data periods for that type.
- The By-chamber chart no longer aggregates all points into one box per chamber. It sorts Sheet-level boxes by derived chamber and uses chamber only for color/legend grouping.
- Measurement charts expand their y-axis beyond USL/LSL when actual Sheet Mean or point values exceed the specification window.
- CPM Sheet OOS decoration now has a product-scoped file contract:
  - `resources/<prod_code>/spc_sheet_oos_detail.xlsx` is generated from original Sheet features and lists current sheets whose `sheet_max > usl` or `sheet_min < lsl`.
  - `resources/<prod_code>/spc_sheet_oos_decoration.xlsx` mirrors the detail table and adds `flag`; missing or truthy flags mean default clipping, while `False` keeps that Sheet's real point values.
- Default clipping affects the backend raw point values used by the CPM/CPK report and Auto Warning dashboard after snapshot load. It does not rewrite snapshots or raw parquet data.
- Clipping values are deterministic pseudo-random values just inside USL/LSL, keyed by Sheet/parameter/point context, preventing visual jitter across refreshes.
- Admin controls are available on CPM page when either `?admin=true` or `?admin-true` is present in the URL.
- The previous warning `当前监控指标可用于箱线图展示，但周期 CPM/CPK 样本不足。` meant Sheet-level data existed for boxplots, but period capability rows for that indicator were missing, usually because the period grouping did not have enough valid Sheet means/specs to calculate standard deviation-based CPM/CPK.
- As of the frontend cleanup, M/W/D period charts are pure Sheet Mean boxplots. CPM/CPK values are displayed in a table above the three charts instead of line overlays.
- CPM/CPK table display is now a 2-row matrix (`CPM`, `CPK`) over the same compact backfilled 2-month, 3-week, and 7-day period window.
- Current CPM/CPK calculation uses the sample standard deviation of Sheet Mean values (`std(ddof=1)`), so one period needs at least two valid Sheet Mean values with USL/LSL to compute a finite/non-missing CPM or CPK. A boxplot can still render with one Sheet Mean, but capability indices need a variance estimate.
- The Sheet OOS decoration layer now lives in `src/spc_domain/application/spc_data_decoration.py`.
- Shared backend decoration first computes original Sheet features, writes product-scoped OOS detail/flag files, clips flagged raw point values, and then recomputes Sheet features from the decorated raw data.
- `CpmReportService` and `SpcAnalysisService` now both consume the shared decorated SPC measurements/features. This makes CPM/CPK report data and Auto Warning dashboard data consistent for non-scrap SPC measurements.
- Scrap data still follows the existing `get_scrap_data` branch and is intentionally not passed through Sheet OOS decoration.
- Drill-down details use the same decorated Sheet features with `persist_files=False`, so they respect existing flags without rewriting files during a click-through query.
