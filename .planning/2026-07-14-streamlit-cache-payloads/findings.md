# Findings

## Confirmed starting evidence

- `CpmReportService.get_cpm_report_data` is decorated with `st.cache_data` and returns project dataclass `CpmReportViewModel` containing another project dataclass, `SheetOosDecorationResult`.
- The page-header hard reset removes `src`, `app`, and domain modules from `sys.modules`, then reruns the page.
- Deterministic probe: a cold `CpmReportViewModel` pickles; after unloading and reimporting its module, pickling the old instance fails because it is not the same class object. An equivalent dict continues to pickle.
- Existing SPC snapshots for M626, M673, M678, Z517, and Z571 pickle successfully; sampled object columns contain ordinary strings.
- `SpcAnalysisService` already caches a native dict and constructs `SpcDashboardViewModel` outside the cache specifically to survive hot reload.

## External reference

- Streamlit documents that `st.cache_data` pickles return values and returns copies; `st.cache_resource` returns shared mutable resource instances and is not suitable for DataFrame report payloads.

## Audit notes

- AST inventory found 15 `st.cache_data` functions under `app/` and `src/`.
- Most return reload-stable values: DataFrames, dicts, tuples, scalars, or `None`.
- Two page-reachable cached functions directly return project-defined dataclasses and require remediation:
  - `CpmReportService.get_cpm_report_data -> CpmReportViewModel` for `CPM监控报表.py`.
  - `PartsReportService.get_report_data -> PartsReportViewModel` for `关键备件报表.py`.
- `SpcAnalysisService.get_spc_dashboard_data -> SpcDashboardViewModel` is already safe because only its inner dict-returning function is cached.
- Yield pages cache DataFrames or native dictionaries; their nested payload shapes still need a focused source review, but no project ViewModel crosses the cache boundary by annotation.

## CPM implementation

- Added `fetch_cpm_report_payload`, the only cached CPM function; it returns DataFrames, a native nested dict, and path strings.
- Kept `get_cpm_report_data` as the public ViewModel facade and reconstructed both `CpmReportViewModel` and `SheetOosDecorationResult` after the cached call.
- `extract_cached_funcs(CpmReportService)` can still discover the new public cached payload function, preserving page-header cache clearing.
- The exact reload-during-cache-fill regression test and the complete CPM service test module pass.

## Critical-parts implementation

- Added cached `fetch_report_payload`, returning one DataFrame and native scalar metrics.
- Kept `get_report_data` as the public facade constructing `PartsReportViewModel` outside the cache boundary.
- The exact reload-during-cache-fill regression and existing critical-parts unit tests pass (`13 passed` combined).

## Cache refresh contract

- `extract_cached_funcs` discovers both new public payload cache functions and excludes both uncached ViewModel facades.
- The page-header “刷新缓存” behavior therefore continues clearing the actual L2 caches before module reload.

## Verification

## SPC layout request

- `app/pages/CPM监控报表.py` only composes the dashboard; the affected Expander markup is owned by `app/sections/spc_cpm_dashboard.py`.
- `_create_period_capability_table` currently renders `CPM` and `CPK` as two rows with every month/week/day period as a separate column. The requested compact layout requires the inverse: one period per row and `CPM`/`CPK` as the two value columns.
- `render_cpm_indicator_sections` currently emits the capability table before one three-column chart row. The intended rendering sequence is a two-column first row (period chart + table), followed by the chamber distribution chart and time distribution chart as direct full-width rows.

## CPK alert request

- The source requirement is already traceable at `docs/dev_docs/dev_prompt/feat-CPM.md` under `Task2-fix：汇总图表优化`.
- The CPM page calls `CpmReportService.get_cpm_report_data()` immediately after the product-aware Header and before rendering the factory/step/parameter filters. The expensive raw-point loading, Sheet feature calculation, and M/W/D capability calculation therefore already occur on initial page entry or product change.
- `fetch_cpm_report_payload()` is already the single `st.cache_data` boundary for `raw_measurements_df`, `sheet_features_df`, `period_capability_df`, and indicator data. Reusing the resulting ViewModel for both the alert and post-query charts avoids duplicate computation and complies with ADR-0001; a second cache layer is not presently justified.
- The current query button gates only filtered chart rendering. A CPK alert Expander can be rendered from the unfiltered product-level `period_capability_df` before `render_cpm_filters()` without changing the existing query-gated chart behavior.
- The existing Yield `render_alert_center` demonstrates the desired presentation pattern, but its inputs and Lot/trend language are domain-specific. The CPM feature should reuse the Expander/status/detail-table interaction pattern through a dedicated SPC alert renderer rather than coupling CPM data to Yield alert contracts.
- The capability report contains month, week, and day rows. The requested detail includes an explicit over-limit date, so daily rows are the directly date-addressable records; this interpretation still needs confirmation from output schema and tests before the Issue is triaged ready.
- `period_capability_df` exposes `prod_code`, `factory`, `step_id`, `param_name`, period metadata, counts/statistics, `cpm`, and `cpk`. It does not expose a second parameter-code/parameter-description field.
- The SPC database boundary likewise treats `param_name` as the parameter identifier/name; no `param_code`, `param_desc`, or equivalent field is currently loaded. The requirement phrase “参数-参数名称” is therefore ambiguous and materially affects the alert-table contract.
- A daily-only alert interpretation is technically coherent: filter `period_type == "day"`, numeric finite `cpk < 1.33`, and expose `period_label` as the over-limit date. Including month/week rows would require a period-type/period-label contract rather than the requested single date field and could duplicate the same indicator across overlapping windows.
- The maintainer confirmed daily-only alerts, every below-threshold daily row, and the exact five-column display contract. The delivered builder performs a numeric CPK coercion, strict threshold comparison, and newest-date-first stable sort.
- The page-level execution contract proves `get_cpm_report_data()` is called once, then alerts render before filters and query-gated charts. No additional cache decorator or database call was introduced.

## PRD, Issue, and planning terminology request

- `to-prd` synthesizes the current conversation and codebase context into one product requirements document: problem, solution, user stories, implementation/testing decisions, out-of-scope boundaries, and notes. It is a requirements/specification artifact, not a task-sequencing log.
- `to-issues` consumes an existing plan, specification, or PRD and splits it into independently deliverable vertical-slice work items. It is not limited to debugging; feature, migration, refactor, and bug work can all be issues.
- `create-local-markdown-issue` is the repository-specific single-card intake path for a raw request. It establishes facts, scope, acceptance criteria, questions, and initial triage metadata in the Local Markdown tracker; it complements rather than duplicates `to-issues`.
- `grill-me` interrogates a plan or design to resolve decisions. `grill-with-docs` adds domain-document and terminology checks, with inline `CONTEXT.md` updates and optional ADR suggestions. Neither is primarily an execution-plan writer.
- `planning-with-files` is the complementary execution-coordination tool: `.planning/<plan-id>/task_plan.md` carries sequence/status, `findings.md` carries evidence, and `progress.md` carries the work log. It is separate from Matt's PRD/Issue lifecycle skills.

## Harness Skill architecture audit

- The current repository already expresses the user's workflow cleanly: design artifacts in root `.scratch/` plus `docs/PRD/`; execution state in `.planning/`; runtime code in `app/` and `src/`; durable decisions in `docs/ADR/`; supporting knowledge in `references/design_references/`, `references/dev_references/`, and `references/test_references/`.
- This topology is nonstandard only in naming. Its separation of deliverables from reusable guidance is coherent and easy to route. No severe project-side defect is evident yet.
- Both Harness Skills currently hard-code one older architecture in multiple layers: SKILL.md prose, separate JSON configs, Python constants/templates, reference architecture docs, and the generated checker. Creator and refactor duplicate several identical reference files.
- The old topology conflicts with this project: it expects `references/design/`, `references/project-info/`, `references/project-conf/`, `references/plans/`, and `references/exec-plans/`, while the desired workflow uses `references/design_references/`, root `.planning/`, root `.scratch/`, and `docs/PRD/`/`docs/ADR/`.
- A shared architecture profile is required. Creator/refactor-specific behavior should remain separate, but both must load the same profile so later path/workflow changes require editing one data file rather than scripts and docs.
- `skill-creator` confirms the intended separation: concise SKILL.md orchestration, detailed architecture in one-level `references/`, deterministic scripts, synchronized `agents/openai.yaml`, and executable validation. Existing Skill names remain valid, so initialization is correctly skipped and update/validation steps apply.
- The current repository implementation has stale generated routing from the old Harness: `references/index.md` still lists removed `references/design/`, `project-info/`, `project-conf/`, `plans/`, and `exec-plans/`; `ARCHITECTURE.md` lists the same old tree. This is a real routing defect, but conceptual workflow remains sound. It will be reported separately; current task does not require mutating project Harness files.
- The current repository has no `scripts/harness_check.py`, so its stale router/index state is not currently executable as a self-check. This is implementation drift rather than a flaw in the four-stage architecture.
- The isolated default-profile test generated 28 files and passed every generated check. Refactor dry-run detected legacy input without mutation; additive write preserved existing AGENTS and legacy source; two explicit overwrite runs created distinct archives and still passed the checker.

## Summary references micro-adjustment

- `references/summary_references/` exists with an empty `index.md`; it should become a reusable-summary knowledge route, not an additional artifact store.
- The user defines `references/retrospective.md` as the router into durable artifacts under `docs/`. Its existing content is obsolete Harness generation history and should be replaced by routes to the currently existing `docs/PRD/`, `docs/ADR/`, `docs/dev_docs/`, `docs/agents/`, and `docs/others/` folders.
- The shared profile currently gives the summary stage no references and omits `references/summary_references/` from root indexes and required paths. This is exactly a profile-only architecture change; Creator/Refactor code need not change.
- During verification, the former `roots.feedback` implementation exposed an additional coupling: Creator generated a retrospective log and Refactor appended run history to it. The profile now owns an `artifact_router` definition for `references/retrospective.md`; Refactor operational history moves to `references/generated/harness-refactor-log.md`. This repair is necessary for the original “architecture is independently modifiable” guarantee.

- Post-change AST audit reports no `st.cache_data` function annotated with a project-defined return class.
- Touched production and test modules compile successfully.
- Focused CPM, critical-parts, page-header, SPC decoration/dashboard/config, auto-warning, and calculator suite: `60 passed`.
- Broad unit suite excluding the known stale `test_override_logic.py` import: `124 passed, 2 failed`; both failures are the pre-existing Shadow EMA expectations previously observed before this task.
