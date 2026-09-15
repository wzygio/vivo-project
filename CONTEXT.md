# Project Context

## Purpose

Tianzhu provides manufacturing quality reports for OLED/Array display production: warehouse-entry defect rates (Yield), SPC/CTQ and AOI analysis, automatic warnings, Q-Time monitoring, IJP overflow monitoring, and critical-parts lifetime management. IQC reads incoming-material inspection characteristics and stored IQC/COA results from WMS; missing decisions remain blank. IQC lifetime reporting reads M3 measurements and presents anonymous samples within each product, batch and test-screen group.

## Operating Model

- Streamlit pages present reports; domain application services coordinate business rules and data adapters. The code ownership map is in [ARCHITECTURE.md](ARCHITECTURE.md).
- PostgreSQL supplies manufacturing facts. Local Parquet snapshots support reuse and defined fallback behavior. Workbooks can contain user-maintained decisions and historical baselines, not merely disposable exports.
- Global, domain, and product configuration controls runtime behavior. Resolve configuration and resource locations through the existing loaders and composition roots; do not assume every resource lives under a product-named directory.

## Important Routes

| Need | Location |
|---|---|
| Agent instructions and task-specific reading triggers | [AGENTS.md](AGENTS.md) |
| Domains, DDD layers, submodules, and code lookup | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Knowledge ownership, Harness maintenance, and work artifacts | [HARNESS.md](HARNESS.md) |
| Business vocabulary and project-specific rules/designs | [references/index.md](references/index.md) |
| Architectural decisions and tradeoffs | [docs/ADR/](docs/ADR/) |
| Runtime configuration | `config/`; inspect the responsible loader before changing values |
| Specifications, manual decisions, baselines, and static inputs | `resources/`; preserve maintained state |
| Runtime snapshots | `data/`; follow the owning domain's lifecycle |
| Rebuildable runtime artifacts | [output/README.md](output/README.md) |
| Operational/analysis commands and tests | `tools/`, `tests/`; scope execution to the task |
| Independent delivery projects | `projects/`; inspect that project's instructions before working there |

## Hard Boundaries

### Business Submodule Organization

- Organize distinct business capabilities within a domain as `src/<domain>/<layer>/<submodule>/`, keeping the same business submodule name across `application`, `core`, and `infrastructure` where those layers contain implementation.
- Put capability-specific presentation sections under `app/sections/<domain>/<submodule>/`. Domain composition roots assemble dependencies; genuinely shared components remain shared. Create only directories with actual responsibilities, and preserve existing example resources until their owning capability is developed.
- Keep concrete ownership and dependency details in [ARCHITECTURE.md](ARCHITECTURE.md).

### Product Availability

- `config/global.yaml` -> `product_registry.enabled_products` is the single source of truth for report product selectors, including page-local selectors that replace the shared page-header selector. Read it through `ConfigLoader.get_enabled_products()` and preserve its configured order.
- An empty product selection means all enabled products. Restrict report data to the enabled scope before rendering charts, tables, options or exports; hiding disabled products only in selector options is insufficient. Clear obsolete selections when the available scope changes.

### Time and Data Semantics

- Date forwarding changes the report display time axis at the repository output boundary. Database facts and raw Parquet snapshots retain source time. Translate direct-query display windows back to source windows, and include the time policy in relevant cache signatures.
- The latest report day is the server's current date. Apply the global `report_cutoff.latest_day_time` at the repository output boundary; the default cutoff is noon, inclusive. Historical selected dates are not reduced to a partial day, and raw snapshots retain their complete source windows.
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

## Maintenance

This file owns stable project context and business constraints. Technical dependency rules belong in `ARCHITECTURE.md`; document lifecycle rules belong in `HARNESS.md`. Update the owning source and link to it instead of maintaining competing copies. An explicit current request may authorize a behavior change; do not invent an additional approval step for already-authorized work.
