# Findings: Equipment Real/Fabricated Snapshot Fallback

## Established facts

- `.codegraph/` is absent; exploration uses `rg` and targeted reads.
- The report loads `resources/critical_parts_baseline.csv`, then `load_part_life_snapshot()` and `build_and_match_all()` through a native `st.cache_data` payload.
- Database snapshot path is `data/equipment/part_life_snapshot_<spec-hash>.parquet`, with an 8-hour TTL and fallback query to `eda.ARRAY_PDS_RESULT_T` for 90 days.
- Existing fabrication writer uses the same path. The formerly fabricated `e1f06d78da21` file is now a 2,501,155-row DB snapshot spanning 2026-04-22 through 2026-07-21, proving overwrite/collision.
- Existing blank-parameter matching uses deterministic `__FABRICATED_PART__...` identities; non-empty parameters use SQL LIKE semantics.
- Full real baseline contains 1,781 rows and produces 1,685 unique monitorable bottom keys.
- ADR-0001 requires cached page payloads to contain only DataFrames, native containers, and scalars; ViewModel construction stays outside `st.cache_data`.
- Worktree contains unrelated active CTQ/SPC changes. Only equipment files, new artifacts, architecture record, issue, plan, and ADR are in scope.

## Decisions

- Use independent fabricated snapshot filename `part_life_fabricated_<signature>.parquet`.
- Initial generation: one row per unique key, uniform value ratio in [0, 1], independent uniform time offset within the previous two days.
- Update: map each fabricated key to its specification, add one day to existing time, add 30% specification; if the result is greater than specification, sample replacement ratio in [0, 0.30].
- Update eligibility uses file mtime and a separate 24-hour config; explicit force bypasses only freshness, not structural validation.
- Report precedence is evaluated per specification, not by concatenating frames, so a newer fabricated timestamp cannot beat real data.

## Open questions

None blocking.

## Dataset and verification findings

- Generated `data/equipment/part_life_fabricated_e1f06d78da21.parquet` with 1,685 unique current-value rows from the 1,781-row baseline; 1,519 rows use blank-parameter synthetic identities.
- Generated value ratios range from 0.000331 to 0.999855. Generated times range from 2026-07-19 16:11:35 to 2026-07-21 16:09:56 for the fixed 2026-07-21 16:10 baseline.
- The current database snapshot contains 2,501,155 rows. Full report audit found 248 real matches, 1,533 fabricated fallback matches, and zero unmatched specifications; all real precedence comparisons retained the exact real value and timestamp.
- Running the production updater without `--force` against the newly generated file reported `snapshot-valid` at about 0.38 hours and did not mutate it, confirming the 24-hour guard.
- Browser smoke rendered Array totals 1,351/0/144/1,207 and TP totals 430/0/49/381. The latest visible timestamp was 2026-07-21 16:09:56, demonstrating that fabricated fallback reached the page.
- Desktop and narrow screenshots show the full report controls and metrics without page-level horizontal overflow. At narrow width the data grid owns horizontal scrolling; a right-scrolled capture confirms measurement, progress, warning, and measurement-time columns remain accessible.
- Page text contains neither `参数名称` nor `匹配参数名`; no Streamlit traceback or visible execution error appeared.
- Browser console errors are limited to direct multipage-route health/host-config 404s and blocked Streamlit metrics telemetry. They do not correspond to a Python application or report-render failure.
