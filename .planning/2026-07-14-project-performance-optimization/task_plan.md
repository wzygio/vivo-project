# Project Performance Optimization

## Goal

Reduce smoke-test and runtime latency through architecture and mathematical/software-engineering optimizations while preserving every existing business rule and numerical result contract.

## Current Phase

Complete

## Phases

### Phase 1: Baseline and bottleneck discovery

- [x] Read architecture, domain, ADR, validation, smoke, and command references.
- [x] Map expensive execution paths and distinguish architecture overhead from algorithmic cost.
- [x] Establish representative timing and output-equivalence baselines.
- **Status:** completed

### Phase 2: PRD publication

- [x] Define measurable user-facing performance outcomes and immutable behavior boundaries.
- [x] Publish the PRD under the Local Markdown tracker with `ready-for-agent` status.
- **Status:** completed

### Phase 3: Vertical Issue slices and readiness

- [x] Split approved scope into independently verifiable tracer-bullet Issues.
- [x] Record business/functional risks on every Issue.
- [x] Resolve discoverable questions and provide Agent Briefs until `ready-for-agent`.
- **Status:** completed

### Phase 4: Detailed execution plan

- [x] Replace provisional implementation phases with Issue-linked ordering and dependencies.
- [x] Define per-Issue RED→GREEN behavior, benchmark, rollback, and verification gates.
- **Status:** completed

### Phase 5: Architecture optimization via TDD

- [x] RED: prove the domain smoke router/entrypoint is absent.
- [x] GREEN: implement explicit `spc`/`yield`/`equipment`/`all` routing with conservative default and zero-collection failure.
- [x] REFACTOR: document fast-vs-full validation and measure SPC wall-clock improvement.
- **Status:** completed

### Phase 6: Algorithm optimization via TDD

- [x] Implement mathematical/software-engineering slices one behavior at a time.
- [x] Preserve boundary, null, ordering, dtype, and numerical-tolerance behavior.
- **Status:** completed

### Phase 7: Verification, ADR, and risk register

- [x] Run focused, smoke, and broad regressions plus before/after benchmarks.
- [x] Record successful design decisions under `docs/ADR/`.
- [x] Publish a consolidated post-optimization risk checklist.
- **Status:** completed

### Phase 8: Risk-directed TDD audit and rollback

- [x] Add or run one behavior check for each consolidated risk.
- [x] Revert only the corresponding Issue slice when risk behavior cannot be disproven or fixed safely.
- [x] Re-run complete verification after any rollback.
- **Status:** completed

### Phase 9: Delivery

- [x] Update Issue delivery records and plan evidence.
- [x] Report retained optimizations, rolled-back slices, timings, and residual risks.
- **Status:** completed

## Key Questions

1. Which smoke path is slow, and how much time belongs to collection/import, IO, repeated orchestration, and numerical kernels?
2. Which candidate changes can prove bitwise or explicitly tolerance-bounded equivalence through public interfaces?
3. What minimum speedup justifies each risk-bearing change?

## Decisions Made

| Decision | Rationale |
|---|---|
| Establish correctness and timing baselines before optimization | User permits risk only for large improvement; evidence is required to compare benefit and protect business behavior. |
| Optimize architecture before algorithms | User-specified order; removes duplicated work before changing numerical implementation. |
| Keep each optimization independently reversible | Final risk audit must be able to withdraw the corresponding Issue without discarding safe gains. |
| Implement only two Issues | Evidence supports a low-risk test-architecture slice and one CPM numerical kernel; other hotspots cross protected business boundaries or lack a clean baseline. |
| Require explicit smoke scope | Avoids unsafe inference from Git state while still removing unrelated test collection. |
| Require at least 30% CPM Point Value speedup | A smaller gain does not justify the vectorization/equivalence risk. |

## Issue-linked execution order

1. `01-domain-scoped-smoke-entrypoint`
   - Public behavior: explicit domain → printed pytest target set → native pytest exit code.
   - RED/GREEN tests: default `all`, domain target resolution, invalid domain, missing/empty targets.
   - Benchmark gate: SPC path at least 60% faster than the 11.64 s runnable-unit baseline.
   - Rollback: remove smoke runner, its tests, and command documentation only.
2. `02-vectorize-cpm-period-aggregation`
   - Public behavior: `build_period_capability_report` DataFrame contract.
   - RED/GREEN tests: multi-valued/NaN specs, NaN grouping keys, single sample, Point Value merge/fallback, cross-period ordering and numeric boundaries.
   - Benchmark gate: Point Value path at least 30% faster than 6.10 s on the captured M626 shape.
   - Rollback: restore only CPM aggregate internals and remove performance-specific tests.
3. Final risk audit
   - Convert both Issue risk tables into one checklist.
   - Associate every risk with an automated test or explicit full-suite command.
   - Withdraw only the Issue whose risk remains observable.

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| Native groupby std changed two near-constant CPK values from finite to `inf` | 1 | Withdrew that reducer and retained legacy `Series.std(ddof=1)` inside batch aggregation; exact comparison passed. |
| Repository ignores `Scripts/` / `scripts/` | 1 | Moved the versioned smoke entrypoint to unignored `tools/`. |

## Guardrails

- Do not alter business rules, formulas, thresholds, filtering semantics, ordering guarantees, null handling, or externally visible outputs.
- Limit changes to mathematical computation and software-engineering mechanics.
- Preserve unrelated dirty worktree changes.
- Require behavior-first RED→GREEN and a measured speedup for every retained slice.
- Prefer removal of duplicate work, cache-boundary correction, vectorization, batching, and data-shape reductions over domain redesign.
