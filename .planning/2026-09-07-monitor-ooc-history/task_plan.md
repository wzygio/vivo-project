# Monitor OOC history plan

## Goal

在不恢复旧全量 Monitor 链的前提下，让四个 Inline producer 通过共享 seam 维护真实 OOC
事实，自动预警展示 OOS/OOC 与恒零 SOOS，并完成配置驱动的资源分目录迁移。

## Source of truth

- PRD: `.scratch/monitor-ooc-history/PRD.md`
- Tickets: `.scratch/monitor-ooc-history/issues/01..05`
- Target branch: `feat/monitor-ooc-history`
- User approval: 2026-09-07 “除非有无法解决的业务问题，否则直接一步执行到底”

## Phases

1. [complete] Requirements — evidence review, OOC/SOOS contract, tickets.
2. [complete] Planning — implementation order, seams, test strategy, worktree protection.
3. [complete] Shared OOC facts/history — RED/GREEN unit tests.
4. [complete] Producer integration and filtered-query safety.
5. [complete] Resource layout migration and configured path routing.
6. [complete] Monitor read model/UI and admin freshness.
7. [complete] Related/full regression, dual-axis review, real-page E2E, commit.
8. [complete] ADR/domain/architecture record and merge handoff.

## Execution order

`01 shared facts → 02 producers → (03 UI || 04 resources) → 05 verification`.
All work stays on the feature branch; merge remains a separate user authorization point.

## Guardrails

- Preserve all pre-existing dirty files and their content, including current CPK/CPM changes.
- OOC requires a real control line; AOI_RS `spec` is not UCL.
- SOOS has no snapshot/workbook because its required value is always zero.
- Cache only native payloads and keep query-button gating.
- Binary moves preserve the current bytes; no duplicate legacy resource files remain.
