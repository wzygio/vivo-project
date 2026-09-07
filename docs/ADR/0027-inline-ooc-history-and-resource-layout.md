# ADR-0027: Inline OOC 历史与资源分模块布局

- Status: Accepted
- Date: 2026-09-07

## Context

自动预警看板需要恢复真实 OOC，并把 SOOS 固定为 0；同时 `resources/inline_domain`
需要按 SPC、CTQ、AOI_TT、AOI_RS、monitor 明确归属。现有 OOS 长期历史已经提供
覆盖窗口替换、Parquet 事实与 Excel 三态决策分离的可靠机制。

## Decision

- OOC 复用 OOS 的历史存储与决策账本机制，但使用独立工作簿和
  `data/inline_domain/ooc_history`，不把 `flag` 写入 Parquet。
- SPC/CTQ 以 Sheet Mean 越过真实 UCL/LCL 判定；AOI_TT 以 `tt_qty > ucl`
  判定；已越过规格限的行由 OOS 优先，不重复计为 OOC。
- OOC 的三态账本只控制是否进入自动预警，不修改四个业务报表的测量值或图点。
- AOI_RS 当前没有控制限数据源，只维护空但可刷新的 OOC 契约，不用 `spec` 伪造 UCL。
- SOOS 不建立事实或工作簿，在页面中始终显示 0。
- 自动预警继续由查询按钮门控；普通 URL 不显示更新时间，只有 `?admin=true` 显示
  OOS/OOC 的产品 × scope 状态。
- Inline 资源按 `spc/`、`ctq/`、`aoi_tt/`、`aoi_rs/`、`monitor/` 分目录，
  完整路径由 `config/domain/inline_domain.yaml` 注入组合根。

## Consequences

自动预警只读取已计算事实，不恢复旧 Monitor 全量分析。OOC 决策变化可通过工作簿
签名使页面缓存失效。AOI_RS 将在获得真实控制限后接入同一 builder，而无需改变页面契约。
