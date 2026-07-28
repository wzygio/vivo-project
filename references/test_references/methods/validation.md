# Validation Guidance

Run the smallest relevant verification first. Broaden checks when shared behavior, public contracts, or UI flows changed.

## Mapping mode boundary

When Mapping mode matching or coordinate handling changes, run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q `
  tests/unit/test_mapping_random_modification.py `
  tests/unit/test_mapping_original_pipeline.py `
  tests/unit/test_mapping_config_excel.py `
  tests/unit/test_yield_dashboard_plotly_keys.py
```

The `original` regression must prove both boundaries: matching Panel IDs bypass
`get_deterministically_modified_panel_id`, and `YieldAnalysisService` forwards
the injected Mapping scripts and product code into `prepare_mapping_data`.
The mode-parallelism regression must also prove that explicit `random`,
`additive`, and `multiplicative` plans preserve source coordinates, while an
unmatched context alone uses the default deterministic position modification.
Priority tests must prove that only the highest-specificity product/Code/batch
layer executes and lower-priority `ALL` rules do not supplement it.
