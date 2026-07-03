# Task Plan: Period-Scoped Code Baseline

## Goal
Make Code-level MWD trend baselines stable by month:
- Each analysis month uses the previous month's Code mean as its EMA anchor.
- Baseline rows are scoped by `product_code + defect_desc + baseline_month`.
- Closed months are not rewritten by automatic baseline refresh.
- `defect_multipliers` still affect the previous-month mean used for future months.
- Preserve the widened query window from the first day three months ago through today.

## Scope
- `src/yield_domain/core/mwd_trend_processor.py`
- `src/yield_domain/application/yield_service.py`
- `tests/unit/test_code_baseline_refresh.py`

## Assumptions
- The displayed monthly trend keeps the last three periods; the extra earliest month in the query window exists to seed the first displayed month.
- A month `M` should use baseline source month `M-1`.
- If a month has no previous-month baseline for a Code, fallback should be explicit and deterministic, not silently rebuild old months.
- Existing legacy baseline files may only have `defect_desc` and `baseline_rate`; code must tolerate them during transition.

## Checklist
1. [complete] Add focused TDD coverage for month-scoped baseline behavior.
2. [complete] Implement period-scoped baseline generation and loading.
3. [complete] Wire monthly baseline lookup into Code EMA.
4. [complete] Verify `get_time_window` behavior remains first day three months ago through end date.
5. [complete] Run focused tests and compile checks.

## Errors Encountered
| Error | Attempt | Resolution |
|---|---|---|
| Default `python -m pytest` had no `pytest` | 1 | Used project `.venv\\Scripts\\python.exe`. |
| `test_shadow_ema.py` import failed without `PYTHONPATH` | 1 | Re-ran with `PYTHONPATH=src`; remaining failures are unrelated existing Shadow EMA expectations. |
