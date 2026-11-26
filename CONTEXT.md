# Project Context

## Purpose

Tianzhu provides manufacturing quality reports for OLED/Array display production: warehouse-entry defect rates (Yield), SPC/CTQ and AOI analysis, automatic warnings, Q-Time monitoring, IJP overflow monitoring, and critical-parts lifetime management. IQC reads incoming-material inspection characteristics, specifications and measurements from WMS. Evaporation IQC/COA decisions follow the supplied SQL CASE rules: any failing point makes the corresponding overall result NG; otherwise it is OK, including missing measurements, as explicitly requested on 2026-09-21. Missing measurement values themselves remain blank. IQC lifetime reporting reads M3 measurements and presents anonymous samples within each product, batch and test-screen group.

## Operating Model

- Streamlit pages present reports; domain application services coordinate business rules and data adapters. The code ownership map is in [ARCHITECTURE.md](ARCHITECTURE.md).
- Automatic alert displays consume published local results through one shared report/matrix reader and selection cache; original data processing and optional chart acquisition remain independent. New AOI trend evaluations and Q-Time alert projections are registered resource workbooks, separate from raw snapshots and manual decision ledgers. Missing or invalid published results never trigger raw-data analysis. See [display contract](references/design/inline_domain/shared/architecture-published-alert-display.md).
- Automatic alert displays treat absent result workbooks, business/product sheets and empty selections as passing (no alerts), as requested on 2026-11-17. Actual read/schema failures and invalid existing publications remain visibly unavailable. Display defaults do not prove successful publication or bypass inspection/API completeness checks; details belong to the shared display contract.
- The independent authenticated anomaly API consumes published local results with the same release and public-visibility rules. SPC CPK/CPM are explicit complete-week capability events with separate visibility policies; their object IDs are complete Sheet ID arrays from the corresponding SPC By Sheet point population, while Sheet/Lot records retain scalar IDs. Scheduled inspection and explicit maintenance publish weekly member snapshots and inspection-selected Inline alert rows. The ordinary SPC page consumes its report data. HTTP reads published results without revalidating calculation policy or publication age. HTTP requests never start producers, database fallbacks or maintenance writes. Its station dictionary is a maintained resource. Yield Lot factories are determined during publication from the configurable global Defect Group mapping; Yield station and layer are unknown and returned as null. API caches and pagination snapshots are process-local and rebuildable. Setup, field-completeness constraints and maintenance commands belong to the [API deployment guide](references/domain/api/interaction-sheet-anomaly-api-deployment.md).
- Alerts with independently maintained decisions keep the local, subdomain-owned tri-state ledger across inspection runs. Their report warning center provides administrator maintenance. See the [decision and publication contract](references/design/inline_domain/shared/architecture-data-decoration.md).
- PostgreSQL supplies manufacturing facts. Local Parquet snapshots support reuse and defined fallback behavior. Workbooks can contain user-maintained decisions and historical baselines, not merely disposable exports.
- Weekly outbound alert email is an independent `api.notification` job. It queries the published-anomaly HTTP API with no query parameters; `config/domain/api/notification.yaml` owns recipient routes, SMTP parameters, account and sender identity. Passwords belong to private runtime inputs. Its credential inputs, test-recipient override, completeness policies and scheduling boundary are owned by the [notification rules](references/domain/api/notification/rules-alert-notification.md).
- Independent dashboard alert production is owned by `api.inspection`, using existing domain application producers and atomic completion receipts. The same daily command then warms the display matrix; there is no separate matrix warmup schedule. Daily inspection and weekly scheduled email entry points and their runtime boundaries are documented in the [inspection guide](references/domain/api/architecture-automatic-inspection.md).
- Yield publishes generated Lot/trend alert workbooks through its existing report workflow; matrix status consumers read these files without analysis fallback. Their lifecycle and validity are owned by [Yield alert result rules](references/domain/yield_domain/shared/rules-yield-alert-results.md); source snapshots and user-maintained decisions keep their existing lifecycle.
- Global and domain/submodule configuration controls runtime behavior. Resolve configuration and resource locations through the existing loaders and composition roots; do not assume every resource lives under a product-named directory.

## Important Routes

Search by the **Domain Submodule Architecture** in [ARCHITECTURE.md](ARCHITECTURE.md#domain-submodule-architecture): select the domain, then the business submodule, and look in the corresponding scoped directories below. This one map routes code, development artifacts, knowledge and resources; each tree does not need a separate file catalog. Use `shared` for domain-wide material and `shared_kernel/shared` for project-wide mechanisms. Confirm actual paths before reading; source packages and existing data lifecycle groups retain their documented exceptions.

| Need | Location and lifecycle |
|---|---|
| DDD Python project-type instructions | [AGENTS.md](AGENTS.md); shared by NTFS hard links |
| Project-specific instructions and task routes | This CONTEXT, including Project Agent Rules |
| Domain/submodule ownership, dependencies and scoped lookup | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Business terminology, rules, algorithms and current designs | [references/index.md](references/index.md); `references/{domain,design}/<domain>/<submodule>/`; preserve project-owned knowledge |
| Streamlit UI interaction, loading/query boundaries, fragments, cache coordination and report scrolling | [UI interaction and performance standard](references/design/shared_kernel/shared/interaction-streamlit-ui-standard.md); canonical project UI requirements and verification routes |
| Development requirements and implementation specifications | `docs/dev_docs/dev_spec/<domain>/<submodule>/`; retain authorized task descriptions |
| Supplied development reference files, report templates and screenshots | `docs/project_files/<domain>/<submodule>/`; preserve original files and distinct versions; runtime resources remain under their configured resource locations |
| Generated assessments and delivery evidence | `docs/dev_docs/generated/<domain>/<submodule>/`; state scope/date and distinguish proposals from verified behavior |
| Architectural decisions and tradeoffs | `docs/ADR/`; durable accepted decisions and rationale |
| Project issue, triage and domain-documentation procedures | `docs/agents/` |
| Runtime configuration and resource locations | `config/global.yaml` owns resource paths; `config/domain/<domain>.yaml` lists its submodules; `config/domain/<domain>/<submodule>.yaml` owns business policy and product/sheet overrides. Inline monitor scrap rules are in the domain entry under `monitor.scrap_factory_mapping` |
| Specifications, manual decisions, baselines and reference inputs | `resources/<domain>/<submodule>/`; preserve maintained files, companions and historical extraction copies |
| Source snapshots and maintained fact/history groups | `data/<domain>/<lifecycle-group>/`; follow the owning lifecycle and preserve existing groups |
| Rebuildable runtime artifacts and temporary verification files | [output/README.md](output/README.md); screenshots/logs under `output/test-results/` or `output/tmp/` |
| Operational/analysis commands and tests | `tools/`, `tests/`; scope execution to the affected capability |
| Local PRDs, issues and task discussions | `.scratch/`; follow [issue-tracker conventions](docs/agents/issue-tracker.md), not an automatically disposable cache |
| Optional persistent execution plans, findings and progress | `.planning/<task>/`; use one task identifier across requirements, plan and evidence; small tasks do not require duplicate work directories |
| Independent delivery projects | `projects/`; inspect that project's instructions before working there |

Promote verified durable knowledge into its owning `references` document and link to it; task notes and generated recommendations do not silently become policy. Folder indexes stay folder-only; `references/index.md` is the exception for file-level knowledge routes and filename rules.

## Hard Boundaries

### Business Submodule Organization

- Organize distinct business capabilities within a domain as `src/<domain>/<layer>/<submodule>/`, keeping the same business submodule name across `application`, `core`, and `infrastructure` where those layers contain implementation.
- Put capability-specific presentation sections under `app/sections/<domain>/<submodule>/`. Domain composition roots assemble dependencies; genuinely shared components remain shared. Create only directories with actual responsibilities, and preserve existing example resources until their owning capability is developed.
- Keep concrete ownership and dependency details in [ARCHITECTURE.md](ARCHITECTURE.md).

### Resource Path Configuration

- Every maintained resource file and resource collection directory must be configurable under `config/global.yaml` -> `resources.<domain>.files` / `directories`. Full file paths are repository-relative or absolute; `.dir` identifies the domain root. Domain/submodule YAML must not introduce a second resource-location source of truth.
- Resolve locations through `ConfigLoader.get_domain_resource_path()` / `get_domain_resource_directory()` and composition roots. Preserve configurable filenames and independent locations in readers, writers, uploads/downloads and cache signatures; deriving a fixed filename from the configured parent is insufficient.
- `ConfigLoader.load_config()` hydrates Yield runtime file descriptors from the global registry while Yield submodule overrides retain product sheet selection. Directly injected descriptors, paths and ports remain caller-owned; tests must isolate them from maintained workbooks. An active global registry rejects missing or invalid entries instead of selecting a legacy default.
- Keep backups and signature sidecars with their owning workbook. Historical extraction copies under `resources/<domain>/<submodule>/decrypted_files/` remain local archives outside the runtime resource registry. New rebuildable decryption outputs belong under `output/`. Upload collections are configured directories; arbitrary uploaded filenames are resolved within those directories.
- Preserved historical report references under `resources/report` are a collection registered as `resources.shared_kernel.directories.archived_report_inputs`; members do not each require a separate file key. This registration preserves the files and does not add a new report entry point.
- Yield's annual-period L1 history is a durable resource workbook owned by `mwd_trend`, resolved through the global resource registry. Its Panel source snapshots retain their existing `data/` lifecycle. See [period history rules](references/domain/yield_domain/mwd_trend/rules-period-trend-history.md).
- Relocation must preserve file bytes and maintained state, update all active consumers and cache identities, and verify source-snapshot preservation. A missing optional input remains missing until its owning use case supplies it; directory cleanup is not permission to delete user data.

### Product Availability

- `config/global.yaml` -> `product_registry.enabled_products` is the single source of truth for report product selectors, including page-local selectors that replace the shared page-header selector. Read it through `ConfigLoader.get_enabled_products()` and preserve its configured order.
- Product selectors append optional annotations from `product_registry.product_annotations`, for example `M678（CPD2455）`. Missing or blank annotations leave the code unchanged. Apply annotations only to widget labels; selected values, queries, caches and report data retain the original product codes. Annotation entries do not enable products.
- An empty product selection means all enabled products. Restrict report data to the enabled scope before rendering charts, tables, options or exports; hiding disabled products only in selector options is insufficient. Clear obsolete selections when the available scope changes.

- IQC evaporation reports retain the V3 organic-material scope. As confirmed on 2026-10-13, product selection filters material membership through evaporation cycles and product-request master data, including unwarehoused work orders; it does not filter the WMS product attribute. The public product column uses the selected associated product codes; multiple values are aggregated per inspection fact. Evaporation cycles remain internal association data. The selector uses the strict enabled-products scope without a compatibility default. See [evaporation product-scope rules](references/domain/iqc_domain/eva_materials/rules-evaporation-product-scope.md); this supersedes the 2026-09-22 no-product-filter exception.

### Time and Data Semantics

- Date forwarding changes the report display time axis at the repository output boundary. Database facts and raw Parquet snapshots retain source time. Translate direct-query display windows back to source windows, and include the time policy in relevant cache signatures.
- Z553 shared Inline measurement snapshots retain source history from 2026-05-01, including incremental and full refreshes. The product anchor is configured in `config/domain/inline_domain/shared.yaml: measurement_snapshots.retain_from`; other products and AOI_RS retain their rolling policy. See [measurement snapshot retention](references/domain/inline_domain/shared/architecture-inline-infrastructure.md#21-共享测量快照保留起点).
- Z622 SPC reads ARRAY PDS Lots from `spc.z622_pds.lot_ids` in `config/domain/inline_domain/spc.yaml` and uses the configured monitoring-pair workbook. Since 2026-11-17 it consumes its own specifications from the standard database table and shares normal point decoration, capability analysis and CPK/CPM alerts; the original measurement-only restriction is superseded. Loading and forced refresh retain the same PDS adapter; see [SPC data flow](references/domain/inline_domain/spc/data-flow-spc.md#21-获取三厂测量数据与共享快照) and [ADR-0045](docs/ADR/0045-z622-spc-pds-source.md).
- Indicator reports share the inclusive display-date exclusion configured in `config/domain/indicator_domain/shared.yaml: data_exclusion`. Infrastructure applies it before decoration/statistics; preserve source snapshots, chamber workbooks and maintained decisions. Policy changes invalidate derived caches and submitted page results. See [Indicator date exclusion rules](references/domain/indicator_domain/shared/rules-indicator-date-exclusion.md).
- The latest report day is the server's current date. Apply the global `report_cutoff.latest_day_time` at the repository output boundary; the default cutoff is noon, inclusive. Historical selected dates are not reduced to a partial day, and raw snapshots retain their complete source windows.
- Inline reports and monitoring share the factory/date exclusion configured in `config/domain/inline_domain/shared.yaml: data_exclusion`. Infrastructure output adapters apply it to display event times before deduplication, feature aggregation, decoration and statistics, including alarm projections and denominators; preserve source snapshots and maintained decisions. The fact-level execution boundary is owned by [Inline date exclusion rules](references/domain/inline_domain/shared/rules-inline-date-exclusion.md). Maintained period aggregates that cannot separate excluded facts are unavailable in the current projection. Exception confirmed on 2026-10-09: when the current week intersects the exclusion, alarm-rate summaries persist zeros only for the selected current weekly row and its tracking counts. Existing year/quarter/month totals, rates and recorded week contributions remain unchanged in both the workbook and presentation, as do existing historical weeks. Missing calendar periods are completed with zeros. The detailed alarm-summary policy is owned by `references/domain/inline_domain/shared/overview-inline.md`, section 7.6. CPK/CPM trend summaries use the independent weekly-count and affected-ancestor rollup policy adopted on 2026-11-17, with zero-project rates of 100% and all-factory product queries. Their local-input, separate-resource and historical-repair boundaries are owned by [CPK/CPM summary workbook rules](references/domain/inline_domain/monitor/rules-cpk-summary-workbook.md).
- Do not casually change the `DatabaseManager` singleton or retry lifecycle. Do not simplify snapshot refresh or database-failure fallback without authorization covering that behavior change.
- Preserve the page data-cache boundary. Cache DataFrames, scalars, and native containers; construct project-defined ViewModels outside the cache. Detailed cache and health contracts are routed by `ARCHITECTURE.md`.
- SPC CPK/CPM reconciles the last complete ISO week on the server calendar (Monday through Sunday), independently of the report query end. Create eligible missing anomalies, refresh matched corrected values and sample bounds, and delete matched rows when the newly computed metric strictly exceeds 1.33. Preserve unmatched or incalculable records and operator decisions on retained rows. Administrator upload can still replace the selected sheet. See [capability ledger rules](references/domain/inline_domain/spc/rules-spc-cpk&cpm-decoration.md).
- SPC retains the latest measurement per Sheet/site within each ISO week, then builds capability Sheet means and sigma strictly inside the previous complete week. Current-week reinspection must not displace or mix into ended-week facts; CTQ/AOI retain their existing deduplication policies. Caches and saved membership policy identities include this preparation contract. Fixed past-week points and rules must yield unchanged CPK/CPM across report dates and refreshes.

### SPC Final Point Data Boundary

- Inline measurement infrastructure excludes the explicit invalid point value `-299.99` before value correction, latest-point selection, decoration and statistics. Snapshot reuse/fallback and offline capability inputs apply the same rule; other negative values remain valid unless another explicit rule excludes them. See [SPC data flow](references/domain/inline_domain/spc/data-flow-spc.md).

- All current and future SPC point-data decoration must complete in the shared backend point pipeline **before the final point-data output** (`raw_measurements_df`). This includes point-value corrections, clipping, central-band decoration, time shifts, point-attribute replacements and business-rule point exclusions. Reports, charts and point exports must consume this same final point contract and preparation context; they must not maintain independent decoration formulas.
- Treat the final point facts as immutable. Neither backend chart adapters nor frontend rendering may apply further point decoration, including modifying, replacing, scaling, clipping, synthesizing or perturbing point values or attributes for display. A new point-decoration requirement must be implemented upstream of the shared final point output; chart-specific exceptions are prohibited.
- Plotting may select records using explicit user dimensions/time windows, sort/group them, format labels and derive genuine statistics such as Sheet means or box-plot statistics from the final points. These operations must not rewrite the underlying point facts or substitute decorated values for them. Text formatting may round displayed labels while preserving the underlying numeric values. CPK/CPM metric-level rules remain separate and must never feed modified values back into the point population.
- Before changing any SPC backend, frontend or export path, verify that the last point-data decoration still precedes this shared output and that page and export consumers use it unchanged. The project-local [SPC point export skill](tools/spc_point_export/SKILL.md) consumes this boundary; its scope remains final point data rather than downstream metric or chart results.

### Non-administrator Presentation Boundary

The default presentation mode applies unless the URL sets `admin=true` or its supported alias `admin=ture`. This is the current display-mode convention; it does not by itself establish authenticated authorization.

- In non-administrator mode, no user-visible or retrievable content may reveal administrator privileges, entry points, or an alternative version of the data. Use clear business terms and the current report values.
- Apply this rule to every page and called component: navigation, buttons/help, messages, tables and column menus, legends, axes, hover text, downloads/exports, and empty, loading, failure, or degraded states.
- Apply the same boundary to HTTP API records, counts, errors, metadata and pagination. Authentication alone does not authorize another public data projection; API credentials and authorization scopes are supplied through the API process environment, not business YAML or generated reports.
- Do not expose administrator-mode prompts, original-versus-modified or actual-versus-displayed comparisons, `admin=true`, internal `flag` fields, modification rules, configuration paths, or debugging details.
- Remove unneeded internal columns and source values before passing data to frontend components or export generation. CSS, `column_order`, and hidden-column settings alone do not satisfy this boundary.
- Still communicate loading failures, unavailable data, and applicable data-health limitations in safe business language. Keep technical causes in server-side logs; do not forward raw exceptions to the ordinary UI.
- Before delivering presentation changes, check normal, empty, and failure branches in non-administrator mode, including column menus, hover content, and exports.

## Maintenance and Document Ownership

| Owner | Owns |
|---|---|
| `AGENTS.md` | DDD Python project-type workflow and common reading/update routes; shared by NTFS hard links |
| `CONTEXT.md` | Project-specific rules and task triggers; purpose, operating assumptions, project-wide constraints, important routes and artifact lifecycle |
| `ARCHITECTURE.md` | Canonical domain/submodule map, path grammar, ownership, dependency and verification routes |
| `references/index.md` | Task-to-knowledge routes and knowledge-file naming rules |
| The scoped `references` document | Feature behavior, domain rules, current design and implementation rationale |

Load documents through task triggers: read the relevant context/map first, then only matched source, contracts, knowledge and tests. A route states its trigger and destination. If a route is missing or stale, inspect scoped evidence and correct it; a missing document does not prove a missing capability. Keep one authoritative owner per fact and link to it rather than maintaining competing copies.

Root-document prose is English; preserve actual identifiers, filenames and Chinese business documents. Documentation changes need link/anchor, directory, trigger, ownership and conflicting-rule checks. Executable changes additionally need the affected runtime checks; `tools/smoke.py all` covers unit tests rather than all verification categories. Report commands, results and material omissions; a documentation check does not establish runtime correctness.

Preserve unrelated edits and business assets. Add hooks, nested instructions or orchestration only when the current task authorizes them and repeated failures or scale justify them. An explicit current request may authorize a behavior change; do not invent an additional approval step for already-authorized work.

## Project Agent Rules

This CONTEXT owns this project's specific instructions, constraints and task
triggers. The global AGENTS owns user-wide operating/safety rules; this project's
AGENTS owns the shared DDD Python workflow. Update the appropriate owner and any
affected routes together. This user-requested ownership model takes precedence
over earlier template descriptions of where project instructions belong.

- Before developing, changing, debugging or reviewing Streamlit UI, read and
  follow the [UI interaction and performance standard](references/design/shared_kernel/shared/interaction-streamlit-ui-standard.md).
  Operator messages, downloads and exports must also follow the Non-administrator
  Presentation Boundary in this context, including documented page exceptions.
- Manufacturing terminology uses
  `references/domain/shared_kernel/shared/GLOSSARY.md`. Consult relevant `docs/ADR/`
  decisions before changing established architecture boundaries.
- Browser snapshots, DOM dumps, console logs and screenshots belong only in
  `output/test-results/` or `output/tmp/`, following `output/README.md`.
- Follow the verified Domain Submodule Architecture in ARCHITECTURE when locating
  code and knowledge. Domain/business facts use the matching
  `references/{domain,design}/<domain>/<submodule>/` owner; existing source and data
  lifecycle exceptions remain in force.
- Folder indexes remain folder-only except the curated `references/index.md`.
  Update the knowledge router when its route or scope changes.

- For local Markdown issues and PRDs, follow `docs/agents/issue-tracker.md`.
- For issue state changes, follow `docs/agents/triage-labels.md`, including its
  configured canonical role/status vocabulary rather than inventing new states.
- Before domain behavior/documentation changes, follow `docs/agents/domain.md`.
