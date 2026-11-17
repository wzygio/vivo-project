# CPK/CPM summary Task0–Task3

Objective: complete [authorized specification](../../docs/dev_docs/dev_spec/inline_domain/monitor/task-cpk_cpm_summary.md), including actual maintained-file migration and bounded historical recalculation, frontend and E2E.

User authorization: execute through completion using default decisions; preserve read-only DB policy and unrelated work. Supplied Task0–3 serve as approved vertical work items. No commit/push requested.

## Requirements and evidence gates

1. Task0: backup original; split sheets by alarm/CPK/CPM preserving other contents, register independent paths, remove redundant durable display labels, fill missing numeric cells and regenerate quarter/year from months. Verify every source sheet accounted for, numerical reconciliation, snapshots/ledger unchanged.
2. Task1: recompute latest five complete ISO weeks using existing SPC denominator calculation and saved flag-false alerts; replace only target weeks and their month/quarter/year ancestors. Verify source coverage and untouched periods.
3. Task2: recompute complete weeks from previous-previous-month through today, including full cross-month week inputs; update only ancestors. Verify counts and persisted files.
4. Task3: independent query-gated fragments; rename CPK/CPM pass-rate trends; total bars by period colors, percentage line broken by period type; remove latest details. CPM remains admin-only; all expanders collapsed. Update owning rules and technical/config references. Browser E2E normal/empty/failure/admin/narrow-screen, independent query counters and process/listener cleanup.
5. Audit all requirements and record concrete delivered evidence before marking goal complete.

## Status

- User approval received: allow ONLY the anomalous historical weeks outside the original windows and their owning month/quarter/year updates. Previous authorization blocker is resolved; resume full Task0–Task3 delivery. Do not extend to unrelated historical weeks.
- Requirements, architecture and source inventory: complete; exact authorized extension implemented and independently audited.
- Pure normalization/rollup: implemented and verified, including closed-week aggregation/delta guards.
- Snapshot denominator/source coverage: implemented and verified; guarded read-only metadata capture completed.
- Frontend and E2E: implemented and verified.
- Historical weekly calculations: requested CPK40/CPM88 product-week rows plus authorized sparse CPK3/CPM20 rows complete. Required missing source prefix captured read-only into isolated historical inputs; existing raw snapshots and decision ledgers were not written by this task.
- Published evidence: output/test-results/capability-summary/delivery-final/. Prepared manifests bind the input versions used at publication; subsequent config registration changes source signatures, so old staged manifests are not eligible for blind reuse.
- Formal Task0 split/config registration and Task1/Task2 persisted ancestors: complete. Original and timestamped backup retained; separate alarm, CPK and CPM paths registered. Formal comparison verifies alarm logical values, 267 CPK and 177 CPM rows, eight metric sheets each and no durable display labels.
- Actual composition queries on copies of the formal files pass with database access prohibited; first and repeated queries both perform zero workbook writes. Final regression521, page/AppTest24 and architecture15 all pass (560 total); browser E2E passes with owned processes/listener cleaned. Formal alarm values/formulas/full cell styles/layout agree with original and backup; only first-tab selection changes after splitting. Owned Excel exit confirmed.
- Documentation: owning rules, consumers, final delivery record and ADR0046 synchronized with published evidence. Task0–Task3 complete; no required work remains.

## Decisions

- Missing warning formula in Task0 is self-referential; use total minus qualified, consistent with main requirements.
- ISO weeks are owned by Sunday calendar month/year.
- Display labels are presentation-only derived values, absent from maintained CPK/CPM files.
- Full-month rollups require explicit coverage; partial replacements use old/new week deltas to preserve other baseline contributions.
- Source facts and capability alert ledger are inputs; do not republish or decorate them as a side effect.
- Explicit reply permits anomaly-only out-of-window recalculation. If an authorized early week is outside snapshot coverage, use bounded read-only source capture into isolated evidence; preserve existing raw snapshots and decision ledgers.

## Errors

1. Late-process pd.read_excel cannot read enterprise-reencrypted staging XLSX; fixed native pickle/parquet staging.
2. Broad suite initially295 tests with21 failures:20 newresourcekeymissing during staging,1 obsolete CPM details assertion. Added explicit pre-migration composition compatibility only for unregistered keys; updated obsolete UI test. Re-run295passed.
3. Preview split initially rejected stable workbook because enterprise re-encryption changed cipher bytes. Replaced cipher identity check with fresh logical workbook comparison plus mtime concurrency guard. Actual protected-copy split passed; owned Excel exit verified.
4. New plan test first lacked rate in updates; eliminated redundant second application of already merged weeks. Both plan tests now pass.
5. Real plan correctly rejects Z576 CPK 2026-W39 whose warning count exceeds missing/zero denominator outside Task1 scope. Awaiting scope decision; no formal files overwritten.
6. Continuation2 review fixed five migration safety/scope defects and warm-cache metadata-file integrity gap;340tests and browserE2E passed. Fresh plan independently proves both CPK and CPM history blockers remain, with no publication.
7. Continuation3 read the unchanged specification and the formal maintained workbook again: Z576 CPK/W39 total0 warning8; CPM August M626/M678/Z517/Z571 totals0 with warnings58/42/75/13. No formal split targets or independent resource registration exist. This is a revalidated data-scope authorization impasse, not an unverified process wait.
