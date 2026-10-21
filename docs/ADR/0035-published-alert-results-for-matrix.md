# ADR-0035: Publish report alert results for file-only matrix consumption

- Status: Accepted (implementation authorized by the user on 2026-10-20)
- Date: 2026-10-20
- Scope: Yield alert publication, matrix status inputs, Inline workbook parsing

## Context

The 2026-10-20 matrix warmup spent 83.54% of its recorded sequential interval in Yield Lot/trend paths. The matrix invoked full report services to obtain status cells. Inline OOS already had persisted result inputs, but the default matrix read each product sheet separately instead of using the existing workbook cache. Independent scheduled processes cannot share Streamlit process memory.

The user approved three P0 changes in the revised [performance report](../dev_docs/generated/inline_domain/monitor/alert-matrix-performance-analysis-2026-10-20.md): publish two Yield result files through the original report workflow, consume those files without analysis fallback, and reuse existing Inline workbook parsing. Source L1 updates and click-to-render details remain unchanged.

## Decision

1. Keep report calculations and existing business rules as the producer. Application orchestration publishes Lot/trend independently before Sheet/Mapping/chart gates. Move the pure Lot selector to Yield core while preserving its presentation import.
2. Configure separate Lot and trend XLSX paths in the global resource registry. Each product has a result sheet and metadata sheet, replaced atomically while retaining other products. OS publication locks protect cross-process writers.
3. Store coverage, reference date, source health and actual source refresh time, policy/calculation identities and record count. Empty successful results are explicit. Failed attempts retain successful records but mark their status unavailable; write failures remain visible.
4. Production matrix and warmup consume public application result reads. Missing, invalid, failed or stale files never initiate analysis or SQL. Memory caches only parsed workbook frames; file results are sufficient in a new process.
5. Default Inline OOS readers consume the existing `ExcelAlarmReader`/`ExcelAlarmStore`, sharing workbook parsing across products. Actual file changes invalidate parsing.
6. A CLI calls the same Yield producer for all/selected enabled products; it does not register a new scheduler or introduce a separate calculation engine.
7. On 2026-10-21 the user requested a backfill from August 1 through today and confirmed that all historical monthly/weekly alarms should be retained. The file stores full-window Lot alerts and trend history; generated current markers keep the matrix's existing latest-series semantics. CLI date arguments select the calculation window without changing global service state. Legacy naive source-health times mean Asia/Shanghai local time.

## Consequences

- First-time/expired products require report publication or the shared producer CLI. The matrix shows unavailable status until that happens.
- The original report's calculation cost remains; it is removed from matrix status construction rather than hidden or eliminated.
- Files preserve the report's actual prepared policy and manual resource-refresh semantics. No additional user decision flags or L1 redesign are introduced.
- Unchanged publication skips rewriting; publication and source TTL are both checked. The existing daily JSON snapshot remains a status summary.
- Existing detail charts can still trigger report data loads after a red cell is selected. They are outside this P0 status optimization.

## Verification

Focused tests cover report-page publication before Sheet failure, independent result failures, zero alerts, metadata/TTL validation, parse reuse, cold-process reads, concurrent publishers, and warmup output. No production database reads or maintained workbook writes are required for these tests. Operational rules and exact test routes belong to [Yield result rules](../../references/domain/yield_domain/shared/rules-yield-alert-results.md).
