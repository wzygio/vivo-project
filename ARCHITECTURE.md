# Architecture

## Purpose and Lookup Order

This document maps domain ownership, DDD layers, submodules, dependencies, and runtime entry points. Project purpose and business constraints are owned by [CONTEXT.md](CONTEXT.md); documentation lifecycle is owned by [HARNESS.md](HARNESS.md).

Locate code in this order: **business question -> domain -> DDD layer -> submodule -> file/symbol -> consumers and tests**. Use [references/index.md](references/index.md) for algorithms, SQL contracts, product exceptions, and detailed designs. Do not maintain a program-by-program catalog here.

## Three-Level Layout

```text
src/<domain>/<layer>/<submodule>/
     domain    DDD layer  submodule
```

- Domains follow business responsibilities, not page names or storage technologies.
- Layers are `application` (use cases), `core` (domain rules), and `infrastructure` (external adapters). Presentation lives in `app/`.
- Submodules represent stable business capabilities or shared responsibilities within a layer. Prefer the same business name across layers, such as `qtime`; technical directories may use role-based names.
- Small layers may keep files directly at the layer root. Do not create empty directories or move files solely to make every layer symmetrical.
- A domain-level `composition.py` assembles dependencies; it is not a fourth business layer. `shared_kernel` supplies cross-domain capabilities and does not require a full DDD layout.

### Current Directory Skeleton

Only domains, layers, and submodules are listed. Files, cache directories, and deeper implementation details are omitted. `(layer root)` indicates responsibilities also implemented directly in that layer.

```text
src/
├─ yield_domain/
│  ├─ application/       (layer root)
│  ├─ core/              (layer root)
│  │  ├─ mapping/
│  │  ├─ mwd_trend/
│  │  └─ sheet_lot/
│  └─ infrastructure/    (layer root)
│     └─ repositories/
├─ inline_domain/
│  ├─ application/
│  │  ├─ spc/
│  │  ├─ ctq/
│  │  ├─ aoi_tt/
│  │  ├─ aoi_rs/
│  │  ├─ monitor/
│  │  ├─ shared/
│  │  └─ ports/
│  ├─ core/
│  │  ├─ spc/
│  │  ├─ ctq/
│  │  ├─ aoi_tt/
│  │  ├─ aoi_rs/
│  │  ├─ monitor/
│  │  └─ shared/
│  └─ infrastructure/
│     ├─ spc/
│     ├─ ctq/
│     ├─ aoi_tt/
│     ├─ aoi_rs/
│     ├─ monitor/
│     └─ shared/
├─ indicator_domain/
│  ├─ application/
│  │  ├─ qtime/
│  │  └─ ijp/
│  ├─ core/
│  │  ├─ qtime/
│  │  └─ ijp/
│  └─ infrastructure/
│     ├─ qtime/
│     └─ ijp/
├─ equipment_domain/
│  ├─ application/       (layer root)
│  ├─ core/              (layer root)
│  └─ infrastructure/    (layer root)
├─ iqc_domain/
│  ├─ application/       (layer root)
│  └─ infrastructure/    (layer root; example data adapter)
└─ shared_kernel/        (shared contracts and configuration at root)
   ├─ infrastructure/
   └─ utils/
```

### Responsibility Map

Submodule paths are relative to `src/<domain>/<layer>/`. Read the target directory to find the actual implementations.

| Domain / business terms | Layer | Submodule -> responsibility |
|---|---|---|
| `yield_domain`: entry defect rates, Code/Group, Lot/Sheet, Mapping | `application` | Layer root -> report/alert use cases, data ports, modifier-table management, and Office export coordination |
| `yield_domain` | `core` | `mapping/` -> coordinates and defect distribution; `mwd_trend/` -> month/week/day aggregation and overrides; `sheet_lot/` -> allocation and capping; layer root -> common defect processing, batch statistics, and anomaly rules |
| `yield_domain` | `infrastructure` | `repositories/` -> Panel repository and snapshots; layer root -> source loading, modifier persistence, and application-port adapters |
| `inline_domain`: SPC, CTQ, AOI, Inline warnings | `application` | `spc/`, `ctq/`, `aoi_tt/`, `aoi_rs/` -> report use cases and ports; `monitor/` -> warnings/history/summaries; `shared/` -> decoration, features, decision signatures, and throughput coordination; `ports/` -> shared measurement snapshot contracts |
| `inline_domain` | `core` | `spc/` -> capability calculations/decoration; `ctq/` -> indicator chart-type rules; `aoi_tt/`, `aoi_rs/` -> statistics/decoration; `monitor/` -> period summaries/replacement; `shared/` -> OOS/OOC, measurement correction, and throughput facts |
| `inline_domain` | `infrastructure` | `spc/`, `ctq/`, `aoi_tt/` -> projections and specialized persistence/reads; `aoi_rs/` -> independent RS facts/snapshots; `monitor/` -> warning inputs and summary/history stores; `shared/` -> measurement loading/preparation/snapshots, process tracing, decoration, and resource paths |
| `indicator_domain`: Q-Time, IJP | `application` | `qtime/` -> monitoring, decoration, and cached use cases; `ijp/` -> overflow queries/filtering/report coordination; each owns its DTOs, ports, and errors |
| `indicator_domain` | `core` | `qtime/` -> shop, exceedance, and decoration rules; `ijp/` -> overflow, period, and printer aggregation |
| `indicator_domain` | `infrastructure` | `qtime/` -> queries, shop-level source snapshots, and decision workbooks; `ijp/` -> query adapters |
| `equipment_domain`: critical parts, lifetime, real/fabricated matching | `application` | Layer root -> reports, data ports, refresh, and caching |
| `equipment_domain` | `core` | Layer root -> identity, measurement matching, lifetime, and status calculations |
| `equipment_domain` | `infrastructure` | Layer root -> baselines, real/fabricated data, and snapshot maintenance |
| `iqc_domain`: incoming inspection and lifetime examples | `application` | Layer root -> example report reads and filtering |
| `iqc_domain` | `infrastructure` | Layer root -> example resources; no independent `core/` or production quality-decision implementation currently |

`shared_kernel` owns configuration, source/display time, data health, cache helpers, and path contracts at its root; `infrastructure/` owns shared database connectivity, and `utils/` owns Excel/CSV utilities. Logic reused only within one domain should remain in that domain's corresponding `shared/` layer.

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
| `app/charts/<domain>/[<submodule>/]` | Chart adapters; Inline shared charts live in `app/charts/inline_domain/`, without a one-to-one directory for every backend submodule |
| `app/components/` | Cross-page components and filter/refresh coordination |
| `app/sections/inline_domain/monitor/` | Current cross-indicator matrix assembly; consumes results from their owning domains |
| `tools/` | Refresh, diagnostics, offline analysis, and scheduled entry points; independent offline tools need not become report-domain services |
| `tests/` | Unit, architecture, integration, and browser evidence; both mirrored directories and flat test names exist |

The scheduled matrix entry point in `tools/` reuses app-level cross-domain assembly. Its app-level snapshot adapter stores native JSON status under `output/cache/alert_matrix/`; consumers validate freshness and signatures and use the existing computation path for missing or invalid items. Locate this path through [the scheduled command](tools/warm_alert_matrix.py) and [the snapshot adapter](app/sections/inline_domain/monitor/alert_matrix_snapshot.py). This is an existing orchestration boundary, not a rule allowing business calculations in every page.

Configuration, maintained resources, runtime data, and output ownership are summarized in `CONTEXT.md`; do not infer that every resource follows a uniform product-directory layout.

## Task-Directed Code Lookup

1. Select the domain and business submodule from the responsibility map. Start a cross-indicator task at the presentation aggregator, then trace each contributing domain.
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

## Technical Contracts and Verification Routes

| Change | Contract / evidence |
|---|---|
| Cache, hot reload, or injected ports | [Native payload boundary](docs/ADR/0001-streamlit-cache-native-payload-boundary.md) and [health/ports contract](references/design/feat_design/architecture-data-health-and-outbound-ports.md); construct project result types outside caches and isolate explicit ports from shared default cache entries |
| Failure, empty results, or fallback presentation | [Health/ports contract](references/design/feat_design/architecture-data-health-and-outbound-ports.md): Yield/Q-Time distinguish successful emptiness, unavailable, stale, and unknown; a failed Yield chunk must not publish a partial new window; stale data cannot support a current-normal conclusion. Do not assume every domain implements this contract |
| Time or source snapshots | [Source/display time ADR](docs/ADR/0022-source-and-display-time-boundary.md), [refresh design](references/design/system_design/data-flow-infrastructure-refresh.md), and the business time constraints in `CONTEXT.md`; verify domain-specific windows and fallback semantics |
| TTL or refresh | Use `config/global.yaml` -> `application.cache_ttl_hours` for project data caches and domain snapshots; preserve targeted product/indicator invalidation. See [cache semantics](docs/ADR/0006-rerun-slimming-cache-semantics.md) and [matrix cache](docs/ADR/0022-alert-matrix-board-and-qtime-cache.md) |
| Decoration, shared measurement, or maintained workbooks | [Decoration architecture](references/design/feat_design/architecture-data-decoration.md), then relevant [knowledge routes](references/index.md); distinguish source facts, decisions, projections, and maintained history |
| UI messages, tables, or exports | Read the [non-administrator presentation boundary](CONTEXT.md#non-administrator-presentation-boundary) before changing normal or exceptional paths |
| Choosing checks | [Architecture tests](tests/architecture/) enforce dependencies; unit/integration tests verify behavior; browser checks verify presentation. [Testing guidance](docs/dev_docs/generated/others/solo-developer-testing-and-release-explained.md) explains coverage gaps; `tools/smoke.py all` is not an all-category release check |

## Extension Rules

Add files to existing responsibility modules first. Create a submodule when a stable capability or reuse boundary warrants it; no empty symmetry or additional framework is required. Update this map when domains/submodules or ownership change. Keep per-program details and algorithms in code-adjacent or routed subject documents. Proposed structures must not be presented as existing implementation.
