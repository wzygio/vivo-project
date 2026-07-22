# Progress: Defect Panel Count Alignment

## 2026-07-22

- Read Task2, the simplified plan decision, repository coding conventions, relevant ADR, and TDD instructions.
- Confirmed existing user edits in `mwd_trend_processor.py` only remove comments in the weekly rebuild block; those edits will be preserved.
- Confirmed UI smoke is not applicable: requested changes are Core/Application calculations with no presentation changes.
- Simplified `docs/dev_docs/generated/plan-defect_count_alignment.md` to remove alignment ratios and Mapping monthly dependencies.
- RED 1: missing `reconcile_code_daily_counts` raised `AttributeError`.
- GREEN 1: proportional monthly integer reconciliation passed.
- RED/GREEN 2: added zero-EMA fallback to daily input weights; implemented capacity-weighted fallback.
- RED/GREEN 3: added capacity overflow case; implemented capped redistribution.
- RED/GREEN 4: added post-calibration Monthly -> Weekly -> Daily precedence; implemented unified daily-table overrides.
- RED/GREEN 5: public Code pipeline initially reduced a single raw defect to zero after EMA rounding; wired reconciliation into the public entrypoint and reaggregated final week/month results from final daily data.
- Added explicit `T=0`, zero-input date, single-defect EMA-tail, and public daily-override reaggregation coverage.
- Restored the legacy module-level `create_mwd_trend_data` entrypoint as a Code-to-Group compatibility adapter.
- Made the old override integration test deterministic by injecting its override and stubbing baseline workbook I/O.
- Added Mapping cascade regression: with 20/100 raw bad Panels per batch, factors 0.5 then 0.95 retain `[10, 9, 9]` Panels.
- Focused Task2 regression: `35 passed`.
- Mapping regression: `13 passed` across matrix scripts, config parsing, and rate cascade.
- Full Yield smoke audit: `60 passed, 6 failed`; all six failures are outside Task2 call paths and reproduce existing selector-signature, Shadow EMA expectation, and global-policy fixture drift.
- Python compilation passed. Ruff and coverage are not installed in the project environment.
- Added function-level MWD and Mapping algorithm references and updated the shallow architecture runtime flow.
