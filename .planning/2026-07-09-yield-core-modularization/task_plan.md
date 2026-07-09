# Yield Core Modularization

Goal: split bulky yield core algorithm modules into module-specific subpackages under `src/yield_domain/core/` while preserving current public imports and behavior.

## Phases

| Phase | Status | Notes |
|---|---|---|
| 1. Inventory current module responsibilities and import/call paths | complete | Extraction seams: MWD code baseline, mapping position/hotspot helpers, sheet/lot aggregation/simulation/override/capping helpers. |
| 2. Extract MWD trend baseline logic | complete | Moved code baseline helpers to `core/mwd_trend/code_baseline.py`; focused tests pass. |
| 3. Extract mapping processor helpers | complete | Moved panel position and hotspot modification helpers to `core/mapping/`; compatibility imports preserved. |
| 4. Extract sheet/lot processor helpers | complete | Moved aggregation, simulation, override, and capping helpers to `core/sheet_lot/`; public workflow module now remains import-compatible. |
| 5. Run focused regression tests and compile checks | complete | `py_compile` passed for touched modules; 17 focused tests passed. |

## Guardrails

- Preserve unrelated dirty files.
- Keep existing public modules import-compatible.
- Prefer extraction-only changes unless tests expose a behavior gap.
- Do not modify Excel/resource files as part of this refactor.
