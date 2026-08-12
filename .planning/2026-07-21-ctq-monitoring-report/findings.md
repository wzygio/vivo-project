# Findings & Decisions: CTQ Monitoring Report

## Requirements

- Model CTQ after the current SPC monitoring report.
- Exclude every CPM/CPK calculation, alert, decorator, metric, and table.
- Keep other behavior and styling aligned with SPC.
- Place CTQ backend code in its own `inline_domain` submodule and frontend code in an independent page/section.
- Reuse the physical SPC repository and filter `data_type = "CTQ"`.
- Use default recommended decisions and continue unless information is genuinely unavailable.

## Research findings

- `.codegraph/` is absent, so repository exploration uses `rg` and direct reads per project policy.
- The physical repository already applies `SpcQueryConfig.data_type_filter` through a whitelist; existing tests include both `SPC` and `CTQ` rows.
- The SPC report service forcibly replaces the query filter with `SPC`, then adds period capability, CPK alert, and CPK/OOS decoration responsibilities.
- The SPC page uses a native cached payload facade, Monitor service time/refresh helpers, product Header, admin decoration UI, filters, and chart sections.
- ADR-0001 requires `st.cache_data` functions to return only native payloads and requires cache-fill/module-reload regression tests for modified services.
- The existing dashboard already supports backend `chart_type` metadata and line-vs-box indicator rendering, but some helpers assume period capability data and require a capability-free CTQ path.
- No `.out-of-scope/` records conflict with the requested enhancement.
- The project Streamlit installation predates 1.57, so the version-matched bundled skill references are unavailable. Preserve the installed version and use official documentation plus established repository patterns.
- Streamlit's official `st.cache_data` reference confirms cached returns are pickled and copied per caller, and per-function caches can be cleared with `func.clear()`; this reinforces ADR-0001's native payload and refresh design.
- Streamlit's official multipage reference confirms that Python files placed directly in `pages/` are automatically discovered, while subdirectories are ignored; the CTQ entrypoint must therefore be a direct `app/pages/*.py` file.
- `SpcReportService.fetch_spc_report_payload()` combines repository loading, OOS decoration, capability calculation, CPK decoration, and native serialization. CTQ should copy the outer cache/facade pattern but stop after OOS-decorated Sheet/raw/indicator data.
- `prepare_decorated_spc_data()` already accepts an explicit product directory. A CTQ adapter can reuse this proven engine with `resources/<product>/ctq`, giving separate persistence without changing the SPC OOS core or filenames.
- The SPC dashboard's public filters and report filter are capability-neutral, while period overview and indicator-section renderers combine generic distribution charts with capability metrics. CTQ needs its own public renderer around the generic chart helpers.
- The generic period chart's line branch currently suppresses month/week/day traces unless matching capability rows exist. A capability-free CTQ wrapper therefore needs the helper to fall back to period types present in measurement points.
- The portal has a custom navigation source in `app/static/config.js`. Its current CTQ item still points to the legacy FineReport URL, so automatic Streamlit page discovery alone would not make the new report reachable from the portal.
- `extract_cached_funcs()` discovers any public callable with Streamlit's `.clear()` attribute; naming the native cache `fetch_ctq_report_payload` keeps Header refresh discoverable without exposing the ViewModel facade.
- Repository validation guidance requires `tools/smoke.py spc` for fast Inline feedback and both complete unit/all-test commands before release; browser smoke is mandatory for visible Streamlit changes.
- Complete unit collection still has the repository-documented pre-existing Yield blocker: `tests/unit/test_override_logic.py` imports `create_mwd_trend_data`, which the current Yield processor does not export. CTQ touched neither file.
- Excluding that stale file, the rest of the unit suite yields `179 passed, 6 failed`; all six failures are in existing Yield selector/EMA/global-policy tests and none import or exercise Inline/CTQ code.
- The complete `tests/` command also stops on the repository-documented `streamlit-echarts` v2 component metadata error in `tests/test_top10_station.py`; this is an environment/package collection issue outside CTQ.

## Technical decisions

| Decision | Rationale |
|---|---|
| CTQ owns service/ViewModel/page/section contracts | Keeps module boundaries explicit and prevents the page from importing capability-specific APIs |
| Shared physical repository, forced CTQ at service boundary | Meets reuse requirement and prevents caller/UI mistakes |
| CTQ-specific OOS resource location via a CTQ adapter | Retains SPC-like OOS behavior without cross-report file collisions |
| No period capability frame in CTQ ViewModel | Enforces the no-CPM/CPK requirement structurally |
| Backend emits `chart_type` | Preserves the established `UNI` business rule outside UI |

## Issues encountered

| Issue | Resolution |
|---|---|
| Existing worktree contains extensive prior user/task changes | Scope all edits to CTQ and strictly related shared helpers; inspect diffs instead of resetting |
| Streamlit bundled-doc discovery requires 1.57+ | Keep current dependency unchanged and follow the official-documentation fallback |
| First findings patch targeted an error-row context in the wrong planning file | Re-read exact locations with `rg` and applied a narrower patch |

## Resources

- `.scratch/ctq-monitoring-report/issues/01-create-ctq-monitoring-report.md`
- `docs/dev_docs/dev_prompt/opt-SPC.md`
- `docs/ADR/0001-streamlit-cache-native-payload-boundary.md`
- `ARCHITECTURE.md`, `CONTEXT.md`, `references/design_references/domain/GLOSSARY.md`

## Visual/browser findings

- Desktop functional QA at `http://localhost:8503/CTQ监控报表` loaded product M626 and exposed CTQ-specific factory/step/parameter filters.
- Selecting ARRAY / 15260 auto-selected `4PP_Rs` and `4PP_UNI`; clicking 查询 rendered six Plotly figures: three box distributions for Rs and three point-line distributions for UNI.
- The rendered CTQ body contains no `CPM` or `CPK`. The `4PP_UNI` figures show only USL/UCL because its LSL is zero, matching the one-sided rule.
- Browser console has no application exception. Five environmental entries are two direct-route `_stcore` 404s plus blocked Streamlit telemetry/metrics; the page websocket/data/render flow remains functional.
- Desktop visual QA at 1440×900 shows aligned Header controls, one-row cascade filters, full-width expanders, legible Plotly axes/spec labels, and no overlap; body width equals viewport width (1440), so there is no horizontal overflow.
- Narrow viewport QA at 768×900 retains all controls and six plots within the viewport (`scrollWidth == clientWidth == 768`). Factory text truncates inside its select as expected, while station/parameter chips and 查询 remain usable.
- Admin QA with `?admin=true` shows the CTQ data-modification expander. Expanding it exposes OOS detail download, decoration download/upload, and contains no CPM/CPK text or tabs; the same panel is absent from the normal URL.
- Product-switch exploration changed M626 → M678 through the normal Header combobox. The page reran, displayed M678, retained the CTQ filters, and did not show an empty/error state.
- On M678, ARRAY / 12140 exposes the requested `SE_L1T_UNI` parameter and enables 查询. Querying rendered the target section with three Scatter-based plots (period, chamber, time); the companion non-UNI parameter rendered Box traces. The full page still contains no CPM/CPK.
- Header cache refresh reran the M678 CTQ page with filters intact and no traceback/NameError, proving the CTQ cache is discoverable in the live app.
- Live products M626, M678, Z517, and M673 all have CTQ data, so a product-level empty-data page could not be produced from current physical data without fabricating state. Empty/filter-zero behavior remains covered by unit/page contracts; this is the only unavailable browser fixture.
- Live QA generated CTQ OOS detail/decoration workbooks under `resources/{M626,M673,M678,Z517}/ctq/`. Both recursive and exact-file cleanup were denied by the command safety policy, so the auditable outputs remain untracked rather than bypassing that guard.
