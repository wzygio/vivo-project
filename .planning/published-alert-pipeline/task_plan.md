# Published alert pipeline

Authorization: user 2026-10-27 explicitly requested implementation across SPC, Inline, all 13 matrix rows, shared read/selection/memory, and design documentation. No merge/push or maintained-data mutation requested.
Mode: complex. Existing local user changes must be preserved.

## Scope and contract
Report producers publish existing result workbooks; displays consume saved results without raw-data fallback. Both reports and matrix call the same public consumer and process-local native-payload cache. Parameters, dates, policy and file versions identify cache entries. Missing/error are distinct from successful empty results. Detailed plots remain optional and use original report services.

## Steps
1. [complete] Verified each producer/consumer and recorded common native payload contract; existing publication formats preserved.
2. [complete] SPC/Inline common saved-result display and matrix integration; AOI trend evaluation publication.
3. [complete] Yield, Q-Time, IJP, parts, IQC consumers; Q-Time dedicated result publication. Explicit scope parameters preserve original report windows.
4. [complete] Main regression sets 228 + 156 passed; final targeted compatibility/UI/architecture set29 passed and policy/persistence set40 passed. Review followup found no unresolved important issues.
5. [complete] Design document, ADR-0036, matrix architecture, SPC flow, knowledge route, context and architecture updated. New/changed documentation links and59 Python syntax checks passed; git diff --check passed.

## Delivery boundary
Changes remain in the shared working tree; no commit, merge, deployment, live database query, scheduled producer run or maintained-result backfill was requested/performed. New Q-Time/AOI trend files are registered, not prepopulated. Normal producers publish them; consumers retain explicit unavailable/no_data behavior until then.

## Followup repair (user authorized 2026-10-27)
- [complete] Reproduced Yield Lot/trend stale despite existing current files; proved unrelated global.yaml resource registrations changed legacy mtime policy identity.
- [complete] Replaced policy identity with scoped semantic v2 content signature, retaining real-policy/date/TTL invalidation.
- [complete] Verified global changes are only the three unrelated registrations and product/domain configs predate publication. Backed up original files and migrated14 product metadata rows, preserving all result data and original source/publication timestamps.
- [complete] Created requested Q-Time header-only initialization template; no success/freshness fabricated. Nonempty/missing metadata still error; formal partial publication still requires requested shop coverage.
- [complete] Real local files: all21 report/matrix states agree with noerror (Lot4 records forZ576, trend1 forZ571, others appropriateok/no_data). Focused regression69 passed; added strict template-corruption case in final check.

## Verification
Meaningful unit/integration tests for PPA exclusion, boundary weeks, valid empty vs missing/error, file/config invalidation, cache reuse, report failures not hiding saved alerts, domain publication contracts, normal-mode safe columns. Existing uncommitted files are part of baseline, never reset them.

## Workflow adaptation
User has authorized the design and execution order in conversation. Do not add redundant approvals. Development-flow's unavailable Matt-specific workflow tools are substituted with repository-native spec/plan and focused pytest; do not infer permission to commit/merge.
