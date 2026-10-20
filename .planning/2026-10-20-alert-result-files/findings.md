# Findings

- Matrix defaults currently call complete Yield services; four OOS rows use uncached per-sheet Excel/COM fallback unless `oos_product_loader` is injected.
- `ExcelAlarmStore` / `read_cached_alarm_workbook` already cache all workbook sheets by path, mtime and size; CPK/CPM reuse this.
- Yield report already computes `compute_lot_oos_records(time_period=30)` and structured `AlertService.get_dashboard_alert_records` (monthly + weekly, no benchmark text).
- Existing report loads Sheet/Mapping and stops on missing unrelated data before alerts; publication must precede those gates and publish Lot/trend independently.
- Global resource registry is mandatory; preserve current workbook bytes and unrelated changes. Read-only matrix state can be unavailable, without expensive fallback.
- User approved preserving source L1 and detail charts. Web memory is optional read acceleration, never required source of results.

## Implemented contract
- Configured keys: lot_oos_results and trend_alert_results, separate files under sheet_lot/mwd_trend. Product sheet + product__meta committed together.
- Metadata records schema/product/kind, reference date, coverage, row count/has_data, generated and underlying source refresh times, policy identity and calculation identity. TTL checks both timestamps.
- Failed status preserves previous successful rows; valid zero alerts, zero facts, missing, failed, stale and uncovered results are distinct.
- Default matrix uses saved results; unavailable publications render error rather than the existing UI's green no_data projection. Explicit legacy injected test contexts remain supported.
- Inline default context injects the existing ExcelAlarmReader/ExcelAlarmStore; all products share parsed workbook data.
- Moved the unchanged Lot alert selector into Yield core and re-export it from existing presentation component so producer and UI use one rule implementation.
