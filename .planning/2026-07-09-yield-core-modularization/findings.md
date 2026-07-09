# Findings

## Initial State

- `.codegraph/` exists, so code lookup starts with CodeGraph.
- `src/yield_domain/core/mwd_trend_processor.py` and `tests/unit/test_code_baseline_refresh.py` already contain uncommitted baseline fallback changes from the prior task.
- Prior active plan points to `2026-07-07-spc-cpm-task2-fix`; this task uses a new plan directory.

## Module Inventory

- `mwd_trend_processor.py` is roughly 65 KB. It mixes entrypoint orchestration, baseline workbook persistence, EMA/noise, aggregation, formatting, and manual override logic.
- `mapping_processor.py` is roughly 23 KB. It mixes mapping data preparation with panel-id coordinate mutation and hotspot script matching/distribution.
- `sheet_lot_processor.py` is roughly 66 KB. It mixes sheet/lot orchestration, raw-rate calculation, lot-to-sheet distribution, Excel override loading, override application, group reaggregation, pass-rate filtering, and capping.
- External imports mostly target stable entry modules: `MWDTrendProcessor`, `prepare_mapping_data`, `apply_hotspot_modification_to_matrix`, and sheet/lot public calculation functions. Keeping these modules as compatibility facades reduces blast radius.

## Extraction Plan

- Create `core/mwd_trend/code_baseline.py` for baseline workbook and anchor resolution logic.
- Create `core/mapping/hotspot_modification.py` for mapping script matching and matrix/random distribution helpers.
- Create `core/sheet_lot/overrides.py` for override workbook loading, heuristic override derivation, and override application helpers.

## Final Extraction

- `mwd_trend_processor.py` now delegates code-baseline persistence and lookup to `core/mwd_trend/code_baseline.py`.
- `mapping_processor.py` now delegates panel-id coordinate mutation to `core/mapping/panel_position.py` and hotspot modification logic to `core/mapping/hotspot_modification.py`.
- `sheet_lot_processor.py` now acts as the workflow facade for sheet/lot calculations. Helper clusters moved to:
  - `core/sheet_lot/aggregation.py` for base info, raw rates, code details, desc/group mapping, and group reaggregation.
  - `core/sheet_lot/simulation.py` for lot-to-sheet distribution and EMA-driven simulation.
  - `core/sheet_lot/overrides.py` for Excel override loading and override application.
  - `core/sheet_lot/capping.py` for pass-rate filtering and spec capping.
- Compatibility imports remain in the original processor modules, so existing callers can still import the previous function names from the previous module paths.
