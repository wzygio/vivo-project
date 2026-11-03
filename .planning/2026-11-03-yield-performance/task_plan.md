# Yield P0/P1 performance implementation

## Scope and authorization
User authorized all five P0/P1 items in the 2026-11-02 performance report, disabling Array-time queries/sorting while retaining code, updating owning Yield knowledge, and explaining P2-A/P2-B. Prior ISO-week alert changes remain in force. No production database queries, maintained workbook writes, SQL/index redesign, or random-allocation/Mapping-cascade rewrites are needed.

## Phases
1. [complete] Trace current service/cache/UI seams and tests; choose cache/version and lazy-detail contracts.
2. [complete] P0 shared input identity, single coverage read, month keys; disabled Array-time feature gate; behavior tests.
3. [complete] P1 small health payloads, shared chart projections, macro-first and explicitly requested detail UI; UI tests.
4. [complete] Offline frozen-input equivalence/performance evidence, scoped regression and review.
5. [complete] Update owning domain/architecture documents and performance report with implementation and plain-language P2 explanations; final checks.

## Constraints
- Preserve unrelated modified files, existing user assets, snapshots, manual controls, source TTL/fallback, full refresh, public display boundary, products and time policy isolation.
- Cache native data only; do not share mutable DataFrames through cache_resource. Explicit adapters remain isolated.
- Read/write modifier contexts must remain distinct. Indicator refresh must invalidate dependent shared calculations without invalidating other products.
- Whole Sheet/Mapping results stay numerically equivalent; defer loading without slicing before stochastic allocation or cascade.
- Tests use temporary resource paths and deny database access. No temporary service may survive verification.

## Errors / decisions
- Source worktree includes previous-turn alert changes and unrelated SPC workbook modifications; preserve them.
- Streamlit docs CLI initially failed with Windows GBK UnicodeEncodeError. Run `python -X utf8 -m streamlit docs ...`; confirmed public expander `on_change="rerun"` and `.open` (1.60).
- Implement explicit `yield_report_source` product revision, separate display revisions, request-local memoization around existing native caches, bounded small health/override caches. No mutable global frame cache. Header cache/data refresh advances source + report indicators only for that product; matrix-only indicator refresh does not fabricate a raw source change.
- Detail loader stays in presentation orchestration; full Sheet/Mapping computations remain unchanged and are requested only after user action. Native dynamic expander gates and fragments isolate UI interactions.
- Review found stale per-Code figure generation and protected workbook macro/formula risks. Fix with source-health generation token, raw-return validation, actual output digest, full-page rerun on captured-generation mismatch; hardened COM and formula-triggered fallback. All are covered by targeted new tests; continue UI verification.
- Final paired compute replay uses real 1,258,622-row Parquet plus fixed native maintenance inputs, not enterprise XLSX: old/new complete business outputs match cold and warm; cold total 62.23s -> 14.58s; report stage 37.32s -> 7.88s. Arrays 2 -> 0, source reads 3 -> 1, Code 4 -> 1, Lot 2 -> 1, override port 3 -> 1. Do not generalize these numbers to SQL/Office/browser latency.
- Initial whole-XLSX hash verification failed because samples were enterprise protected (inspect_file confirmed 00 header); discarded that comparison. Final replay explicitly denies SQL/Office and hashes only unchanged Parquet and native output values.
- Final scoped regression completed: 439 passed, exit 0 (verification.json), including integration and architecture. Both read-only reviewers report no remaining blockers. No test service was started.
- Owning documentation updated. Link validation found an existing stale glossary route in references/index.md; corrected it to the actual references/domain/GLOSSARY.md without moving files or changing instructions. Final link/diff checks recorded in progress.
