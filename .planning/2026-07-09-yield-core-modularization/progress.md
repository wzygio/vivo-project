# Progress

## 2026-07-09

- Started modularization plan for yield core algorithms.
- Confirmed current target dirty files: `mwd_trend_processor.py`, `test_code_baseline_refresh.py`.
- Completed module inventory with CodeGraph and `rg`.
- Selected compatibility-preserving extraction seams for the three bulky modules.
- Error: attempted mechanical deletion in `mwd_trend_processor.py`; PowerShell write failed while file was locked and left the file empty. Recovery plan: restore from `HEAD`, then reapply the explicit baseline fallback and extraction changes.
- Recovered `mwd_trend_processor.py` from `HEAD` using byte-preserving `cmd` redirection after PowerShell restore corrupted some UTF-8 comment/newline boundaries.
- Extracted MWD code baseline helpers to `src/yield_domain/core/mwd_trend/code_baseline.py`.
- Verified MWD extraction with `py_compile` and `tests/unit/test_code_baseline_refresh.py` (`4 passed`).
- Extracted mapping panel-position helpers and hotspot modification logic to `src/yield_domain/core/mapping/`.
- Verified mapping extraction with `py_compile` and `tests/unit/test_mapping_random_modification.py` (`8 passed`).
- Extracted sheet/lot helper clusters to `src/yield_domain/core/sheet_lot/`: `aggregation.py`, `simulation.py`, `overrides.py`, and `capping.py`.
- Kept compatibility imports in the original processor modules so existing callers can continue importing from `mwd_trend_processor.py`, `mapping_processor.py`, and `sheet_lot_processor.py`.
- Added a rate-only compatibility path to `_apply_random_cap_and_floor`; internal count-based capping remains unchanged when panel/count arguments are provided.
- Verified all touched modules with `py_compile`.
- Ran focused regression tests: `tests/unit/test_code_baseline_refresh.py`, `tests/unit/test_mapping_random_modification.py`, `tests/unit/test_yield_task3.py`, and `tests/unit/test_capping_mechanism.py` (`17 passed`, one pandas `FutureWarning`).
