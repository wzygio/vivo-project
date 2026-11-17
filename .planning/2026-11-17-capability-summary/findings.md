# Findings

- Current page/query service uses shared combined workbook and updates saved latest-week warnings only; denominator comes from manually maintained baseline. This is insufficient for new required recomputation.
- Actual raw measurement snapshots live under data/inline_domain/shared, with Aug–Nov coverage (Z553 retains May). Named spc folder contains throughput/member artifacts. Source snapshots hold raw measurement fields, no specs.
- Core SPC calculator build_period_capability_report is reusable after shared final-point preparation. Do not invoke full SPC report producer for migration: it persists decisions and Sheet membership.
- Existing zero-total rate is 0 or unknown; new contract is 1 (100%).
- Previous unrelated modifications are committed in current worktree; at task start only an unrelated removed task-cpk_cpm_alert_center.md was tracked as deleted. Preserve it.
- Three independent workers have non-overlapping ownership; root owns store/application/config/migration.
- Actual metadata needed for denominators is not in raw snapshots. Captured catalog/spec dimensions in a guarded read-only transaction after schema and low-cost size inspection. Native atomic generation is under data/inline_domain/spc/capability_metadata; source adapters cannot access DB.
- Historical CPM August and some old CPK weekly records have warnings greater than missing/zero totals. Literal Task0 fill and strict count invariants conflict. Asked whether to repair only those additional historical weeks; waiting on scope authorization before altering them. Original data remains the input snapshot.
- Enterprise DLP re-encrypts XLSX outputs between processes; normalized values must be consumed in the same process or staged as trusted local pickle/parquet. The first later-process pd.read_excel failed; corrected staging to pickle native frames under output/test-results/capability-summary.
- CPK requested W42–W46 recalculated across enabled products. Z622 has zero capability totals by measurement-only source contract. CPM W36–W46 recomputation in progress.

## Final findings after authorized resume

- User explicitly permitted anomaly-only historical repair. Exact sparse coverage is Z576 CPK W36/W37/W39 and M626/M678/Z517/Z571 CPM W31–W35. Original requested windows plus these rows reconcile with ledger flags and affected ancestors; outside-scope weekly totals and warnings are preserved.
- Missing July prefix was captured under read-only schema/volume/time/product guards into isolated historical inputs. Existing raw snapshots and maintained alert ledgers were not written by this task.
- Formal three-file migration and global resource registration completed at 13:56:56. Published CPK has 267 rows, CPM has 177; each has eight product sheets. Alarm has seven original sheets. The original combined file and timestamped pre-migration backup remain available.
- Actual composition queries against formal copies need no database access and do not write unchanged histories on either first or repeated request. Shared normalization rederives rates from counts; persisted Excel numeric precision did not create spurious writes in this verification.
- A broader 521-test run exposed one obsolete factory-widget assertion after the required selector removal. It has been replaced with product-query invalidation and all-factory scope assertions, passed the five-test target file, and is included in the final combined rerun.
