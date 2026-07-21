# Architecture

## Project Purpose

`vivo-project`: 天柱专项报表系统 - 基于标准 src 布局

## Runtime Flow

- `ConfigLoader` deep-merges application defaults from `config/global.yaml` with
  the selected product configuration and validates one `AppConfig`.
- Yield application boundaries convert the validated config into an immutable
  `YieldDataPolicy` once per request. Dynamic product/date conditions remain in
  `YieldQueryConfig`.
- `PanelRepository` applies Work Order policy at the database boundary, stores
  raw Defect Group data in a policy-versioned Parquet snapshot, then applies the
  injected Defect Group policy once before returning data to upper layers.
- Yield services, alerts, and pages consume the resulting data without repeating
  the global Defect Group filter.
- Page-facing Streamlit data services cache only reload-stable native payloads
  and construct project ViewModels outside `st.cache_data`; see
  `docs/ADR/0001-streamlit-cache-native-payload-boundary.md`.
- The equipment report keeps database and fabricated current-value snapshots in
  independent signature-addressed files. Its application service matches each
  specification against the database snapshot first and consults the fabricated
  snapshot only when no real record matches. Fabricated generation and 24-hour
  update are explicit, separate command workflows; report reads never mutate
  either snapshot. See `docs/ADR/0003-equipment-real-first-fabricated-fallback.md`.
- Inline report services own their data-type boundary: the SPC report forces
  `SPC` and adds CPM/CPK capability output, while the CTQ report forces `CTQ`
  and exposes only Sheet/point distributions plus backend-selected chart type.
  Both reuse the physical `SpcRepository`; CTQ OOS resources are isolated under
  `resources/<product>/ctq/`.
- Plain Excel-to-CSV export used by the SPC probe workflow comes from the local
  `fr-common-utils[excel]` dependency through `fr_common_utils.excel`. The
  project-specific encrypted-workbook COM fallback, configuration models,
  database singleton/retry policy, application setup, and domain adapters stay
  locally owned.

## Project Map

| Path | Role |
|---|---|
| `app/` | Project area. |
| `app/__pycache__/` | Project subarea. |
| `app/charts/` | Project subarea. |
| `app/compliance/` | Project subarea. |
| `app/components/` | Project subarea. |
| `app/pages/` | Project subarea. |
| `app/sections/` | Project subarea. |
| `app/sections/ctq/` | Capability-free CTQ report presentation. |
| `app/utils/` | Project subarea. |
| `config/` | Project area. |
| `config/products/` | Project subarea. |
| `data/` | Project area. |
| `data/doc_cache/` | Project subarea. |
| `data/equipment/` | Project subarea. |
| `data/M626/` | Project subarea. |
| `data/M673/` | Project subarea. |
| `data/M678/` | Project subarea. |
| `data/processed/` | Project subarea. |
| `data/raw/` | Project subarea. |
| `data/Z571/` | Project subarea. |
| `docs/` | Project area. |
| `docs/design/` | Project subarea. |
| `docs/dev-docs/` | Project subarea. |
| `docs/exec-plans/` | Project subarea. |
| `docs/generated/` | Project subarea. |
| `docs/project_files/` | Project subarea. |
| `docs/prompt/` | Project subarea. |
| `docs/references/` | Project subarea. |
| `docs/spec/` | Project subarea. |
| `logs/` | Project area. |
| `OfflineRepo/` | Project area. |
| `OfflineRepo/wheels/` | Project subarea. |
| `output/` | Project area. |
| `output/task-Indicator_Improvement/` | Project subarea. |
| `references/` | Project area. |
| `references/design/` | Project subarea. |
| `references/dev_references/` | Project subarea. |
| `references/exec-plans/` | Project subarea. |
| `references/generated/` | Project subarea. |
| `references/plans/` | Project subarea. |
| `references/project-conf/` | Project subarea. |
| `references/project-info/` | Project subarea. |
| `references/test_references/` | Project subarea. |
| `resources/` | Project area. |
| `resources/decrypted_files/` | Project subarea. |
| `resources/M626/` | Project subarea. |
| `resources/M673/` | Project subarea. |
| `resources/M678/` | Project subarea. |
| `resources/project_files/` | Project subarea. |
| `resources/static/` | Project subarea. |
| `resources/xlsx_to_csv/` | Project subarea. |
| `resources/Z517/` | Project subarea. |
| `resources/Z571/` | Project subarea. |
| `scripts/` | Project area. |
| `scripts/__pycache__/` | Project subarea. |
| `skills/` | Project area. |
| `skills/templates/` | Project subarea. |
| `specs/` | Project area. |
| `src/` | Project area. |
| `src/equipment_domain/` | Project subarea. |
| `src/shared_kernel/` | Project subarea. |
| `src/inline_domain/` | Project subarea. |
| `src/inline_domain/application/ctq/` | CTQ query, cache payload, ViewModel, and OOS orchestration. |
| `src/inline_domain/core/ctq/` | CTQ-owned indicator presentation rules. |
| `src/yield_domain/` | Project subarea. |
| `tests/` | Project area. |
| `tests/__pycache__/` | Project subarea. |
| `tests/integration/` | Project subarea. |
| `tests/unit/` | Project subarea. |

## Boundaries

- Global config owns application-wide Yield data policy; product config owns
  product identity and product-specific paths/processing adjustments.
- Yield Application composes dynamic queries and static policy. Infrastructure
  owns database constraints, snapshots, incremental refresh, degradation, and
  the final data-policy application. Core and UI do not load configuration files.
- Inline UI selects products and report filters but does not choose business
  data types or infer UNI chart behavior. CTQ/SPC application and core services
  make those decisions before returning page data.

## Verification

- Config tests prove every enabled product inherits the single global Yield data
  policy and that global config changes trigger Session reload.
- Repository tests prove fresh and cached data receive the same policy while raw
  Defect Group values remain intact in snapshots.
- Service tests prove refresh injects validated config into a policy-versioned
  bottom data provider.
- CTQ tests prove forced `CTQ` repository queries, capability-free payloads,
  isolated OOS files, UNI Sheet point-line rendering from every raw point,
  month/week/day box distributions, native-cache hot reload, and independent
  Streamlit page behavior; the Inline `spc` smoke scope includes both CTQ and
  SPC report tests.
- The SPC page renders exact factory/station/parameter combinations from daily
  CPK alerts before the manual cascade filters, reusing the standard indicator
  chart renderer for both automatic and user-selected views.
