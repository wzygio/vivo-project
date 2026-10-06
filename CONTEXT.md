# Project Context

## Purpose

Tianzhu provides manufacturing quality reports for OLED/Array display production: warehouse-entry defect rates (Yield), SPC/CTQ and AOI analysis, automatic warnings, Q-Time monitoring, IJP overflow monitoring, and critical-parts lifetime management. IQC reads incoming-material inspection characteristics, specifications and measurements from WMS. Evaporation IQC/COA decisions follow the supplied SQL CASE rules: any failing point makes the corresponding overall result NG; otherwise it is OK, including missing measurements, as explicitly requested on 2026-09-21. Missing measurement values themselves remain blank. IQC lifetime reporting reads M3 measurements and presents anonymous samples within each product, batch and test-screen group.

## Operating Model

- Streamlit pages present reports; domain application services coordinate business rules and data adapters. The code ownership map is in [ARCHITECTURE.md](ARCHITECTURE.md).
- PostgreSQL supplies manufacturing facts. Local Parquet snapshots support reuse and defined fallback behavior. Workbooks can contain user-maintained decisions and historical baselines, not merely disposable exports.
- Global, domain, and product configuration controls runtime behavior. Resolve configuration and resource locations through the existing loaders and composition roots; do not assume every resource lives under a product-named directory.

## Important Routes

Search by the **Domain Submodule Architecture** in [ARCHITECTURE.md](ARCHITECTURE.md#domain-submodule-architecture): select the domain, then the business submodule, and look in the corresponding scoped directories below. This one map routes code, development artifacts, knowledge and resources; each tree does not need a separate file catalog. Use `shared` for domain-wide material and `shared_kernel/shared` for project-wide mechanisms. Confirm actual paths before reading; source packages and existing data lifecycle groups retain their documented exceptions.

| Need | Location and lifecycle |
|---|---|
| Agent entry, task-specific reading and iteration triggers | [AGENTS.md](AGENTS.md) |
| Domain/submodule ownership, dependencies and scoped lookup | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Business terminology, rules, algorithms and current designs | [references/index.md](references/index.md); `references/{domain,design}/<domain>/<submodule>/`; preserve project-owned knowledge |
| Development requirements and implementation specifications | `docs/dev_docs/dev_spec/<domain>/<submodule>/`; retain authorized task descriptions |
| Generated assessments and delivery evidence | `docs/dev_docs/generated/<domain>/<submodule>/`; state scope/date and distinguish proposals from verified behavior |
| Architectural decisions and tradeoffs | `docs/ADR/`; durable accepted decisions and rationale |
| Project issue, triage and domain-documentation procedures | `docs/agents/` |
| Runtime configuration and resource locations | `config/global.yaml` owns resource paths; `config/domain/` and `config/products/` own business policy and product/sheet differences |
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

- Every maintained resource file and resource collection directory must be configurable under `config/global.yaml` -> `resources.<domain>.files` / `directories`. Full file paths are repository-relative or absolute; `.dir` identifies the domain root. Domain/product YAML must not introduce a second resource-location source of truth.
- Resolve locations through `ConfigLoader.get_domain_resource_path()` / `get_domain_resource_directory()` and composition roots. Preserve configurable filenames and independent locations in readers, writers, uploads/downloads and cache signatures; deriving a fixed filename from the configured parent is insufficient.
- `ConfigLoader.load_config()` hydrates Yield runtime file descriptors from the global registry while product configuration retains sheet selection. Directly injected descriptors, paths and ports remain caller-owned; tests must isolate them from maintained workbooks. An active global registry rejects missing or invalid entries instead of selecting a legacy default.
- Keep backups and signature sidecars with their owning workbook; historical extraction copies remain archived within the owning module. New rebuildable decryption outputs belong under `output/`, not the maintained resource tree. Upload collections are configured directories; arbitrary uploaded filenames are resolved within those directories.
- Relocation must preserve file bytes and maintained state, update all active consumers and cache identities, and verify source-snapshot preservation. A missing optional input remains missing until its owning use case supplies it; directory cleanup is not permission to delete user data.

### Product Availability

- `config/global.yaml` -> `product_registry.enabled_products` is the single source of truth for report product selectors, including page-local selectors that replace the shared page-header selector. Read it through `ConfigLoader.get_enabled_products()` and preserve its configured order.
- Product selectors append optional annotations from `product_registry.product_annotations`, for example `M678（CPD2455）`. Missing or blank annotations leave the code unchanged. Apply annotations only to widget labels; selected values, queries, caches and report data retain the original product codes. Annotation entries do not enable products.
- An empty product selection means all enabled products. Restrict report data to the enabled scope before rendering charts, tables, options or exports; hiding disabled products only in selector options is insufficient. Clear obsolete selections when the available scope changes.

- Exception confirmed on 2026-09-22: IQC evaporation reports are scoped to V3 organic materials in infrastructure. Their product attribute is 通用; no product selector or enabled-products filter applies. Keep the source product value in the detail table.

### Time and Data Semantics

- Date forwarding changes the report display time axis at the repository output boundary. Database facts and raw Parquet snapshots retain source time. Translate direct-query display windows back to source windows, and include the time policy in relevant cache signatures.
- The latest report day is the server's current date. Apply the global `report_cutoff.latest_day_time` at the repository output boundary; the default cutoff is noon, inclusive. Historical selected dates are not reduced to a partial day, and raw snapshots retain their complete source windows.
- Inline reports and monitoring share the factory/date exclusion configured in `config/domain/inline_domain.yaml: data_exclusion`. Apply it to report event times before statistics, including alarm projections and denominators; preserve source snapshots and maintained decisions. Maintained period aggregates that cannot separate excluded facts are unavailable in the current projection.
- Do not casually change the `DatabaseManager` singleton or retry lifecycle. Do not simplify snapshot refresh or database-failure fallback without authorization covering that behavior change.
- Preserve the page data-cache boundary. Cache DataFrames, scalars, and native containers; construct project-defined ViewModels outside the cache. Detailed cache and health contracts are routed by `ARCHITECTURE.md`.

### Non-administrator Presentation Boundary

The default presentation mode applies when the URL does not set `admin=true`. This is the current display-mode convention; it does not by itself establish authenticated authorization.

- In non-administrator mode, no user-visible or retrievable content may reveal administrator privileges, entry points, or an alternative version of the data. Use clear business terms and the current report values.
- Apply this rule to every page and called component: navigation, buttons/help, messages, tables and column menus, legends, axes, hover text, downloads/exports, and empty, loading, failure, or degraded states.
- Do not expose administrator-mode prompts, original-versus-modified or actual-versus-displayed comparisons, `admin=true`, internal `flag` fields, modification rules, configuration paths, or debugging details.
- Remove unneeded internal columns and source values before passing data to frontend components or export generation. CSS, `column_order`, and hidden-column settings alone do not satisfy this boundary.
- Still communicate loading failures, unavailable data, and applicable data-health limitations in safe business language. Keep technical causes in server-side logs; do not forward raw exceptions to the ordinary UI.
- Before delivering presentation changes, check normal, empty, and failure branches in non-administrator mode, including column menus, hover content, and exports.

## Maintenance and Document Ownership

| Owner | Owns |
|---|---|
| `AGENTS.md` | Agent entry instructions, essential safety, task triggers and iteration routing |
| `CONTEXT.md` | Purpose, operating assumptions, project-wide constraints, important routes and artifact lifecycle |
| `ARCHITECTURE.md` | Canonical domain/submodule map, path grammar, ownership, dependency and verification routes |
| `references/index.md` | Task-to-knowledge routes and knowledge-file naming rules |
| The scoped `references` document | Feature behavior, domain rules, current design and implementation rationale |

Load documents through task triggers: read the relevant context/map first, then only matched source, contracts, knowledge and tests. A route states its trigger and destination. If a route is missing or stale, inspect scoped evidence and correct it; a missing document does not prove a missing capability. Keep one authoritative owner per fact and link to it rather than maintaining competing copies.

Root-document prose is English; preserve actual identifiers, filenames and Chinese business documents. Documentation changes need link/anchor, directory, trigger, ownership and conflicting-rule checks. Executable changes additionally need the affected runtime checks; `tools/smoke.py all` covers unit tests rather than all verification categories. Report commands, results and material omissions; a documentation check does not establish runtime correctness.

Preserve unrelated edits and business assets. Add hooks, nested instructions or orchestration only when the current task authorizes them and repeated failures or scale justify them. An explicit current request may authorize a behavior change; do not invent an additional approval step for already-authorized work.
