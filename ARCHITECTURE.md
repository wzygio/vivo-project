# Architecture

## Purpose and Lookup Order

This document maps domains, their business submodules, dependencies, runtime entry points, and physical code locations. Project purpose and business constraints are owned by [CONTEXT.md](CONTEXT.md); document ownership and artifact lifecycle are owned by [CONTEXT.md](CONTEXT.md#maintenance-and-document-ownership).

Locate code in this order: **business question -> domain -> business submodule -> file/symbol -> consumers and tests**. Use [references/index.md](references/index.md) for algorithms, SQL contracts, product exceptions, and detailed designs. Do not maintain a program-by-program catalog here.

## Domain Submodule Architecture

This overview groups the current source by **domain -> business submodule**. Repeated occurrences of the same submodule are combined into one capability; technical directories such as `ports` and `repositories` are excluded from the business inventory. The tree is a logical ownership map, not a proposed filesystem move.

### Current Business Submodules

```text
src
|-- inline_domain
|   |-- aoi_rs
|   |-- aoi_tt
|   |-- ctq
|   |-- spc
|   |-- monitor
|   `-- shared
|-- indicator_domain
|   |-- qtime
|   |-- ijp
|   `-- ijp_hole
|-- iqc_domain
|   |-- eva_materials
|   `-- lifetime
|-- yield_domain
|   |-- mapping
|   |-- mwd_trend
|   `-- sheet_lot
|-- equipment_domain
|   `-- (no named business submodule packages)
`-- shared_kernel                  cross-domain support, not a business domain
```

### Submodule Responsibilities and Collaboration

| Domain | Submodule | Business responsibility and relationship |
|---|---|---|
| `inline_domain` | `aoi_rs` | AOI RS defect counts/density, Sheet/Lot and month/week/day reports, and RS-specific decoration; owns independent RS facts while reusing domain-shared throughput and decision capabilities |
| `inline_domain` | `aoi_tt` | AOI TT reports, Sheet/Lot and period statistics, TT decoration and Particle Size breakdown; reuses shared measurement and decoration capabilities |
| `inline_domain` | `ctq` | Critical-to-quality measurement reports, indicator selection and chart-type rules; reuses the shared measurement/feature/decoration pipeline |
| `inline_domain` | `spc` | Statistical process control, measurement features, CPK/CPM calculation and capability decoration; supplies SPC capability inputs used by monitoring |
| `inline_domain` | `monitor` | Inline warning analysis, OOS/OOC and CPK/CPM summaries, current/history inputs, period statistics and summary workbooks; aggregates results for the relevant indicator scopes |
| `inline_domain` | `shared` | Domain-local measurement preparation and snapshots, parameter/specification metadata, OOS/OOC decisions and decoration, throughput/history, process tracing and shared date filtering; supports multiple Inline report modules |
| `indicator_domain` | `qtime` | Q-Time monitoring with two internal use cases: station/Lot waiting time and evaporation-chamber residence time; each has its own source and calculation flow |
| `indicator_domain` | `ijp` | IJP border-overflow analysis, Glass ratios, printer summaries and period summaries |
| `indicator_domain` | `ijp_hole` | IJP large-hole analysis and Glass/total/day ratios; independent counting rules from `ijp`, while reusing its query/filter contracts |
| `iqc_domain` | `eva_materials` | Evaporation incoming-material measurements, specifications, per-point decisions and overall material results |
| `iqc_domain` | `lifetime` | Lifetime-test measurement validation, group-local anonymous sample numbering, product/batch filtering and report projection; uses an independent M3 data source |
| `yield_domain` | `mapping` | Panel coordinates, mapping layouts, defect distribution, hotspot modification and monthly scaling policies |
| `yield_domain` | `mwd_trend` | Month/week/day defect-rate trends, Code/Group aggregation, daily generation and maintained monthly target overrides |
| `yield_domain` | `sheet_lot` | Sheet/Lot defect rates, allocation, aggregation, capping, simulation and rate overrides |
| `equipment_domain` | No named package split | One integrated critical-parts reporting capability: part identity, baseline/source matching, usage/lifetime progress, warning/decoration and a dedicated CVD rule branch; these are file-level responsibilities, not existing `parts/` or `cvd/` submodule packages |

### Capabilities Outside Named Submodule Packages

Yield also keeps common Panel-data loading, defect processing, batch statistics, anomaly/alert handling, modifier persistence and exports outside its three named business packages. The three submodules use these common inputs and report coordination; `repositories` is a data-access grouping rather than another business capability.

IQC retains example-report loading/filtering outside `eva_materials` and `lifetime`. These examples are not a third production business submodule. Inline's `ports` directory groups shared measurement contracts and is not a seventh business submodule. Domain composition/configuration files support module assembly and are likewise excluded from the inventory.

`shared_kernel` is the separate cross-domain support package for configuration, source/display-time policies, cutoff handling, data health, cache/path/snapshot contracts, database connectivity and Excel/CSV utilities. Inline's `shared` belongs to Inline; it is not interchangeable with `shared_kernel`.

### Submodule Boundaries and Lookup

Select a domain and named business submodule from this overview first, then use the path grammar below to find its actual implementation. Keep the same stable capability name wherever its implementation is present; add a submodule for a distinct business responsibility, and keep domain-local reuse in that domain's `shared` area. The overview records existing packages and explicitly marks ungrouped capabilities; it does not require creating empty or hypothetical packages.

Within `qtime`, station monitoring uses [the documented decoration workflow](references/design/indicator_domain/qtime/algorithm-qtime-data-decoration.md); chamber monitoring uses independent residence rules. Sharing a submodule or page does not imply sharing an algorithm.

## Shared Path Grammar

Use the Domain Submodule Architecture as the sole maintained capability inventory across these trees:

```text
src/<domain>/<layer>/<submodule>/
app/{sections,charts}/<domain>/[<submodule>/]
docs/dev_docs/{dev_spec,generated}/<domain>/<submodule>/
references/{domain,design}/<domain>/<submodule>/
resources/<domain>/<submodule>/
data/<domain>/<lifecycle-group>/
```

### Three-Level Layout

Source code retains its implemented `application`, `core` and `infrastructure` ownership boundaries. Match a business submodule name across its implemented locations; keep existing ungrouped code and shared contracts where they are owned. Presentation and artifacts follow domain/submodule ownership without inserting source-code layer names. Create only directories that contain a real responsibility.

Shared/domain-wide artifacts use `<domain>/shared/`; project-wide material uses `shared_kernel/shared/`. Equipment artifacts use `equipment_domain/parts/` for its integrated parts capability even though the source remains ungrouped. IQC example inputs use `iqc_domain/shared/demo/`. Optional technical archive folders may follow the submodule, while filenames retain their existing naming contracts.

Data already uses lifecycle groups: Inline `aoi_rs`, `aoi_tt`, `ctq`, `spc`, `shared`; Indicator `qtime`; Equipment `parts`; Yield `yield` for common Panel source facts. These existing groups are retained, not relabeled to match every report submodule. Source snapshots and maintained history follow their owning lifecycle.

### Responsibility Map

The [Domain Submodule Architecture](#domain-submodule-architecture) owns the capability inventory and relationships. Select a capability there, then search its scoped paths above. `shared_kernel` supplies cross-domain configuration, time, health, cache/path/snapshot contracts, database connectivity and Excel/CSV helpers; domain-local shared work remains with its domain.

## Dependency Direction and Composition

The diagram shows code dependencies, not a sequential data pipeline. **Core does not depend on Infrastructure.**

```text
app/ -> application/ -> core/
             |
             +-> consumes application-owned outbound ports
                                  ^ implemented by
                            infrastructure/

domain composition -> assembles application services and concrete adapters
```

- Application services coordinate rules and outbound calls. Adapters manage SQL, workbooks, snapshots, and external resources; they may reuse pure Core rules, but Core must not import adapters.
- Reuse public same-layer shared APIs rather than another business module's private implementation. Cross-domain collaboration uses public application contracts rather than another domain's repository.
- Ports may live in a submodule's `ports.py`, a dedicated protocol file, a shared `ports/` package, or the layer root. Locate them by consumer scope rather than assuming a single naming pattern.
- Controlled default resolvers preserve existing static entry points during migration. Exact legacy import exceptions are recorded in the [dependency guard](tests/architecture/test_backend_dependencies.py); they are not precedents for new outward dependencies. The guard checks static imports, not all dynamic calls or implicit I/O.
- Configuration and database lifecycle belong to existing configuration entry points, composition roots, and shared infrastructure. Pages must not introduce direct SQL, workbook persistence, or independent database instances.

### Presentation and Operational Entry Points

| Path | Responsibility |
|---|---|
| `app/Home.py`, `app/pages/` | Portal initialization and thin page entry points; follow section calls before descending into a domain |
| `app/sections/<domain>/[<submodule>/]` | Query gates, session state, page sections, and presentation coordination; small domains may omit the submodule directory |
| `app/sections/iqc_domain/eva_materials/` | Evaporation material date query, native Streamlit dataframe and cached public payloads; the complete public result supports native sorting, search and CSV export |
| `app/sections/iqc_domain/lifetime/` | Enabled-product scope, linked product/status/batch selectors with query-applied filters, anonymous public cache and complete native dataframe details; page-header cache refresh invalidates the registered payload; `app/charts/iqc_domain/` builds efficiency- and luminance-decay curves in child expanders within each product/batch expander, with four test screens per row |
| `app/charts/<domain>/[<submodule>/]` | Chart adapters; Inline shared charts live in `app/charts/inline_domain/`, without a one-to-one directory for every backend submodule. AOI RS and SPC page rendering and PDF reports share UI-independent figure construction. Their presentation sections own administrator-only product/factory multiselects (all products and ARRAY by default), application data consumption, and separate generation/download actions; the generated PDF is retained in the session for explicit and repeat downloads. Their shared PDF adapter embeds three charts per indicator row and Chinese fonts, with temporary images cleaned on success and failure. Alert sections are excluded |
| `app/components/` | Cross-page components and filter/refresh coordination |
| `app/sections/inline_domain/monitor/` | Current cross-indicator matrix assembly; consumes results from their owning domains |
| `app/pages/超规预警看板.py` | Independent abnormal-sheet, CPK and CPM query sections; the two capability boards reuse metric-parameterized period rules and presentation, with separate product summary sheets and query state |
| `tools/` | Refresh, diagnostics, offline analysis, and scheduled entry points; independent offline tools need not become report-domain services |
| `tests/` | Unit, architecture, integration, and browser evidence; both mirrored directories and flat test names exist |

The scheduled matrix entry point in `tools/` reuses app-level cross-domain assembly. Its app-level snapshot adapter stores native JSON status under `output/cache/alert_matrix/`; consumers validate freshness and signatures and use the existing computation path for missing or invalid items. Locate this path through [the scheduled command](tools/warm_alert_matrix.py) and [the snapshot adapter](app/sections/inline_domain/monitor/alert_matrix_snapshot.py). This is an existing orchestration boundary, not a rule allowing business calculations in every page.

Configuration, maintained resources, runtime data, and output lifecycle are owned by `CONTEXT.md`. Resolve maintained resource locations from the global resource registry; product sheets do not imply product-named resource directories.

## Task-Directed Code Lookup

1. Select the domain and business submodule from the Domain Submodule Architecture. Start a cross-indicator task at the presentation aggregator, then trace each contributing domain.
2. Select the layer: calculations/invariants in Core; use-case steps, health propagation, and cache contracts in Application; SQL/files/source snapshots in Infrastructure; dependency choices in Composition; interaction/rendering in `app/`.
3. If `.codegraph/` exists, use CodeGraph for symbol/call-path discovery first, as required by `AGENTS.md`. Otherwise list the scoped directory with `rg --files`, then search definitions and references with `rg -n`. A filename is a clue, not proof of responsibility.
4. Trace one use case through its entry point, consumer port, composition binding, and adapter; enter Core when the rule is relevant. Stop unrelated scanning once inputs, outputs, ownership, consumers, and applicable tests are identified.
5. Consult relevant domain documents/ADRs when business semantics or established boundaries matter. Verify stale paths against actual code; no mirrored test directory does not mean no tests exist.

Examples from the repository root when CodeGraph is absent:

```powershell
# Q-Time query and health propagation
rg --files src/indicator_domain/application/qtime src/indicator_domain/infrastructure/qtime
rg -n 'data_health|QTimeDataPort' src/indicator_domain app/sections/indicator_domain/qtime

# Yield month/week/day algorithms
rg --files src/yield_domain/core/mwd_trend

# Shared Inline measurement and its composition binding
rg --files src/inline_domain/infrastructure/shared -g '*measurement*'
rg -n 'measurement|snapshot' src/inline_domain/composition.py

# Search both flat tests and nested test directories
rg --files tests -g '*qtime*' -g '*yield*' -g '*dependencies*'
rg -n 'QTimeReportService|data_health' tests/unit tests/integration tests/architecture
```


## Extension Rules

Add files to existing responsibility modules first. Create a submodule when a stable capability or reuse boundary warrants it; no empty symmetry or additional framework is required. Update this map when domains/submodules or ownership change. Keep per-program details and algorithms in code-adjacent or routed subject documents. Proposed structures must not be presented as existing implementation.
