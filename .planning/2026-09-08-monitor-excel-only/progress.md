# Progress

## 2026-09-08
- Read approved user clarification, skills, existing snapshot implementations.
- Created isolated complex-mode plan/spec and two snapshot tickets.
- No production snapshot mutation yet. Tests and rebuild results pending.

## Continuation completed code / pending maintenance
- Production composition now uses ExcelAlarmReader/Store; Inline initial/query paths do not construct DatabaseManager or live throughput/raw source.
- Weekly replacement and current parent delta persisted under workbook lock; missing sources/baselines preserved with visible messages. Added AOI_RS sheet-only count and inconsistent-contribution guard.
- CPK all periods read maintained summary; no raw calculations or CPK workbook writes.
- Query start includes crossyear week and current-date cache key avoids midnight staleness.
- Raw snapshot transaction uses per-product process+thread lock; AOI_RS no valid fallback errors explicitly.
- Migration supports both hashed and nested legacy throughput conflicts without overwrites. Latest dryrun75moves,60Inline,17preserved; no production moves/deletes executed.
- Safe refresh CLI defaults dryrun, deletion exact known raw families (including legacy hash names), sanitized DB initialization/runtime status.
- Related unit suite:455passed,15warnings,32.79s. CLI/scoped tests and compilation/diff-check pass.
- Browser E2E: query gating, idempotence, Flag refresh, historical777preserved, CPK8fromworkbook, admin timestamps; no console errors,768px no page overflow. Artifacts output/test-results/monitor-excel.
- Real production Excel read-only benchmark:56product/type/scope reads3.57s (28missing); summary190rows+CPK96rows1.57s. No production query/write executed by these checks.
- Standards/Spec findings addressed: crossyear cutoff, legacy polluted cleanup, DB init sanitization, raw process locking/fake-empty fallback, PRD source decision.
- Updated docs/dev_docs/generated/Inline_domain/warning-dashboard-generation.md and task-specific ARCHITECTURE paragraphs; preserved concurrent SPC/Yield user changes.
- Pending: user stops writers before real migration/delete/rebuild; full-repository failures outside task need separate triage; no commit/merge or final ADR claim.
- Final full-unit sweep:1137passed/23failed/74warnings (60.29s), XML output/test-results/monitor-excel/full-unit.xml. Failures in global header/navigation, QTime data-forward assumptions, concurrently edited SPC capability tests, Yield selection/data policy. None of these failing files were changed to fix unrelated behavior by this task. Prior full run overlapped raw test changes; latest raw/monitor targeted rerun is authoritative for task scope, not evidence of full repository green.
- Browser E2E rerun after review fixes again passed with0consoleerrors. No visual baseline exists, so screenshots are manual layout evidence rather than a historical visual-regression pass.
- Final frozen task-only selection:187passed in9.63s; output/test-results/monitor-excel/task-unit.xml. Closed only our monitor-excel browser session and8516testserver; user applications untouched.

## Production maintenance completed 2026-09-08
- User confirmed Inline writers stopped. Migrated75files, verified all75destination SHA256 match sources.
- Deleted58recognized Inline raw/metadata files without backup per authorization; rebuilt7products×2modules,14/14success.
- Verified21Parquet files,3,025,495rows, valid generation metadata and actual date range2026-06-01..2026-09-08.
- Fresh repository reads reused all21snapshots with database loaders set to fail; no database calls.
- No remaining migrations or legacy Inline raw families. Removed13known empty legacy directories, no recursive deletion.
- Other domain hashes unchanged throughout Inline rebuild. Maintenance tests101passed in4.64s.
- Generation document updated with executed production result; no Git merge/commit or claim of full-repository test green.
