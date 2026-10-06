# Final verification

## Runtime command

Run from the repository root:

```powershell
.venv\Scripts\python.exe -m pytest -q tests/unit/inline_domain/infrastructure/shared/test_inline_configured_workbooks.py tests/unit/inline_domain/application/shared tests/unit/inline_domain/application/aoi_tt tests/unit/inline_domain/application/aoi_rs tests/unit/inline_domain/application/spc tests/unit/inline_domain/application/monitor tests/architecture tests/unit/app/sections/monitor tests/unit/app/sections/test_cpk_monitor_latest_style.py tests/unit/app/sections/aoi_tt/test_aoi_tt_sheet_oos_alerts.py tests/unit/app/sections/aoi_rs tests/unit/test_global_resource_registry.py tests/unit/test_yield_service_modifier_wiring.py tests/unit/app/components/test_file_uploader_product_sheets.py tests/unit/test_equipment_data_commands.py tests/unit/test_equipment_parts.py tests/integration/test_equipment_fake_dataset.py tests/integration/test_equipment_parts_db.py::TestSpecBaseline tests/integration/test_sheet_oos_refresh_integration.py tests/integration/test_alert_matrix_integration.py tests/unit/test_report_cutoff.py tests/unit/test_repository_report_cutoff.py
```

Final result: **470 passed**, 30 existing pandas FutureWarnings, 61.59 seconds. All identified failures resolved.

Earlier scoped evidence: 88 passed (architecture/resources/Yield/equipment/integration/cutoff); 43 passed (global registry/configured Inline paths/repository cutoff). The first final combined run had 40 missing temporary-global-config failures and 430 passes; six fixture owners now provide an actual temporary global resource registry. Their targeted suite passed 70 tests before the successful final run.

## Static and preservation commands

```powershell
.venv\Scripts\python.exe output/tmp/audit_harness.py assets
.venv\Scripts\python.exe output/tmp/audit_harness.py links
.venv\Scripts\python.exe output/tmp/final_harness_checks.py
git -c core.safecrlf=false diff --check
```

Temporary audit programs live in output/tmp; JSON evidence lives beside this file.
Resource/source hash audit: 116/116 and 99/99 match initial hashes. All 195 migration targets exist.
Live links/anchors: zero failures; five missing historical attachments outside the migration scope remain documented.
Root document ownership, triggers, filename grammar and directory spelling checked. All 57 changed Python files compile; git diff --check passes.

No production DB, Office conversion, snapshot refresh or browser interaction run.
