# Findings

Baseline observations below refer tobe925c9e; current implementation and progress are indexed by progress.md and the feature tickets. User has confirmed all pending questions.

- API request currently requires product and sub_domain, defaults limit100 and OOS, only factory/entity filters; response duplicates QTime origin as 起始站点.
- API main authenticates and authorizes before one injected service.read call, all reads finite via BoundedReads. Pagination binds full query, principal and reference week, recomputes public fingerprint to invalidate changed results. Byte cap16MiB already exists even for one-page results.
- SheetAlertQuery calls core.build_sheet_oos_alerts, which hardcodes prior ISO week. Need optional explicit dates at this existing selection seam, preserving no-arg UI behavior and release/parameter exclusions.
- QTimeAlertResults.read already accepts start_date/end_date; validates date coverage and current published metadata, plus freshness and explicit release evidence. YieldAlertResults.read has inclusive explicit dates and coverage checks too. API composition currently forwards neither.
- Root working tree has unrelated data_loader/tests/config/docs changes and protected workbooks. Isolated branch avoids exposing incomplete changes to live services. Do not touch original workbooks or those user edits.
- Prior workbook operation found API supervisor identity not alive while worker listener still ready. Do not treat a prior status as current evidence; verify ownership again if authorized existing service update is needed.
- Existing baseline24 test failures separately tracked; prior3 Timestamp/NaT type diagnostics reproduce in unchanged alert_results. Do not claim a full green result without actual new evidence.

- Review exposed aware timestamp normalization must precede existing Inline display-date exclusion; this is now at the established ExcelAlarmStore boundary, verified across4 sources*2 events with active exclusion. API workbook I/O now converts only known parser/COM/Windows failures to typed source unavailability, keeping programming errors distinct.
- Pre-review full regression2797 pass/24 fail, node set matches tracked24 exactly. A broader post-review targeted run355 pass/1 AppTest3-second timeout under concurrent full load; do not mark that timeout resolved until isolated verification. Final full rerun is on079b8822 production code.

- Closure: both temporary failures resolved via24 serial module tests; final full2808 pass/25 fail includes tracked24 plus that resolved temporary file-lock case. Postmerge158 API/file tests passed; original-root audit recorded1 existing Yield workbook byte change and wrapper3 despite0 write attempts, origin unknown. Preserve the current user workbook and report this limitation, never restore old bytes to manufacture a clean audit.
