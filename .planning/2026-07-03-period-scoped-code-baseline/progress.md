# Progress: Period-Scoped Code Baseline

## 2026-07-03
- Read requested `planning-with-files` and `tdd` skills.
- Restored existing planning files; previous plan was complete and unrelated to this baseline task.
- Session catchup script was unavailable at `~/.claude/skills/planning-with-files/scripts/session-catchup.py`.
- Replaced planning files with current task plan, findings, and progress.
- Confirmed with CodeGraph that current Code baseline is global per Code and can be automatically rebuilt whole-file.
- Confirmed `YieldAnalysisService.get_time_window` already starts at the first day three months before the end date.
- Added RED test for per-month previous-month Code EMA anchors.
- Default `python -m pytest` failed because the system Python has no `pytest`; switching to project `.venv`.
- RED confirmed with `.venv`: May first day returned 8 instead of 5, proving the current global baseline is reused across months.
- Implemented period-scoped baseline rows and month-specific EMA anchor lookup.
- The new per-month anchor test now passes.
- Focused baseline test file passed after making multiplier-change metadata take precedence over legacy-schema migration.
- Added regression coverage that existing scoped baseline rows are preserved when later month rows are appended.
- Added regression coverage that `YieldAnalysisService.get_time_window` starts on the first day three months before the analysis end date.
- Focused baseline tests passed: `8 passed`.
- Compile check passed for `mwd_trend_processor.py`, `yield_service.py`, and `test_code_baseline_refresh.py`.
- `test_shadow_ema.py` requires `PYTHONPATH=src`; with that set, it still has pre-existing failures in `_calculate_adaptive_shadow_ema` expectations unrelated to this change.
- Updated baseline comments/test names to reflect the new no-age-based-auto-rewrite rule.
- Final focused baseline tests passed: `8 passed`.
- Final compile check passed for touched yield modules and baseline tests.
