# Findings & Decisions

## Requirements

- Analyze architecture opportunities first, then algorithm opportunities.
- Continue only when evidence supports worthwhile optimization.
- Publish a PRD, split vertical Issues, list per-Issue business/functional risks, and make Issues `ready-for-agent`.
- Use file-backed planning and TDD; architecture changes precede algorithm changes.
- Record successful decisions in ADR, consolidate risks, test every risk, and withdraw unsafe Issue slices.
- No business-domain or calculation-logic changes.

## Research Findings

- No `.codegraph/` index exists; use targeted `rg`, AST/source reads, tests, and timing tools.
- Worktree contains unrelated existing changes; preserve them.
- Performance claims require representative timing baselines and output-equivalence fixtures before implementation.
- Harness smoke guidance currently points at the entire `tests/unit/` or `tests/` suite; there is no documented fast changed-area smoke tier. This is an architecture/workflow opportunity independent of business logic.
- `ARCHITECTURE.md` and `CONTEXT.md` contain stale legacy routes, but runtime layering remains clear: Streamlit presentation → application services → core calculation → infrastructure repositories/snapshots.
- Existing session-scoped config fixtures already avoid repeated config loading inside pytest; collection/import and heavy module boundaries still need timing.
- Static hotspot scan found several candidate numerical kernels with Python row/group loops or `DataFrame.apply(axis=1)`: CPM period capability, critical-parts matching/calculation, SPC decoration, Yield capping/overrides/simulation, Mapping processing, and MWD trend generation.
- Yield concentration/Mapping, database singleton, page cache decorators, and snapshot refresh are explicit protected boundaries. Candidate work there requires exact regression proof and large measured benefit; otherwise exclude it.
- Infrastructure already uses Parquet snapshots and page services use `st.cache_data`; removing those caches is forbidden. Optimization should target duplicated orchestration, repeated derivations, data-shape reduction, vectorization, and test selection.
- Unit-test collection currently discovers 135 tests but fails on the stale `tests/unit/test_override_logic.py` import of `create_mwd_trend_data`; collection took 12.34 s inside pytest and 22.04 s wall-clock.
- Excluding that pre-existing broken module, the unit suite took 11.64 s wall-clock: 128 passed and 7 pre-existing assertions failed. Most individual tests are fast; collection/import dominates the feedback loop.
- The slowest individual unit test took 2.55 s. The next CPM tests were 0.72 s and 0.56 s, supporting a conservative fast-smoke tier rather than altering application behavior to solve test latency.
- `CpmCalculator` is an algorithm candidate because period rows are expanded threefold and then traversed again with Python `groupby` loops. Exact semantics for ordering, `std(ddof=1)`, missing limits, infinity, and first-value aggregation must be locked before vectorization.
- Critical-parts alert decoration is stateful and encodes business behavior. It is excluded from optimization unless a later benchmark proves exceptional benefit and exhaustive equivalence can be demonstrated.
- On the M626 snapshot shape (1,041,518 point rows, 92,849 Sheet-feature rows, 158 indicator groups), current CPM period capability takes 2.01 s with Sheet Mean sigma and 6.10 s with Point Value sigma.
- The existing CPM calculator test module runs in 1.73 s wall-clock versus 11.64 s for the runnable unit baseline, an observed 85% faster feedback path before adding a unified command.
- The current `resources/critical_parts_baseline.csv` cannot be read by the existing UTF-8-only loader (`UnicodeDecodeError`). This is a separate baseline defect and makes critical-parts performance work unsuitable for this task.
- Evidence supports exactly two independently reversible Issues: explicit domain-scoped smoke execution and CPM period aggregation vectorization.
- Final implementation keeps legacy `Series.mean/std` reducers inside a batch aggregation plan. This is necessary for bitwise equivalence on near-constant values while still removing Python record construction.
- Final real-shape result: Sheet Mean 2.06→0.39 s (81.2% faster), Point Value 6.08→3.29 s (46.0% faster), with exact DataFrame equality against HEAD.
- Final broad unit result is 141 passed with the same 7 pre-existing failures; no optimization regression was added.

## Technical Decisions

| Decision | Rationale |
|---|---|
| Use public service/page interfaces for correctness tests | Tests survive internal performance refactors and protect observable behavior. |
| Measure cold and warm paths separately | Cache and import costs can hide or exaggerate numerical improvements. |
| Assign one rollback boundary per Issue | Supports the user's final risk-directed withdrawal requirement. |
| Treat existing test failures as baseline defects, not optimization regressions | Prevents unrelated failures from being “fixed” by changing business behavior during performance work. |
| Prefer conservative test-impact routing with full-suite fallback | Reduces normal feedback time while preserving coverage for unknown/shared changes. |
| Keep exact Series floating-point reducers | Native groupby std changed a finite near-constant CPK to infinity; exact behavior takes priority over maximum speed. |
| Version the smoke entrypoint under `tools/` | The repository intentionally ignores `Scripts/`, so `tools/` is the deliverable path. |

## Issues Encountered

| Issue | Resolution |
|---|---|
| None | — |
| Broad `rg` included nonexistent `scripts/` and returned a benign path error after producing app/src/test results | Treat repository as having no current scripts directory; use only existing roots in later scans. |
| Critical-parts baseline raised `UnicodeDecodeError` under its production loader | Record as unrelated baseline defect; exclude critical-parts optimization and do not change file/loader semantics. |
| Attempted obsolete `references/test_references/observability/index.md` path | Located current flat route at `references/test_references/observability.md`; use it for smoke documentation. |

## Resources

- `ARCHITECTURE.md`
- `CONTEXT.md`
- `references/design_references/domain/GLOSSARY.md`
- `docs/ADR/`
- `references/test_references/`
- `docs/dev_docs/dev_prompt/refactor-speed_opt.md`
