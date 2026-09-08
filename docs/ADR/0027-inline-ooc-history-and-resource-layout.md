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
- 各 producer 在原有访问计算时同步维护按天、厂别、物理 Sheet 去重的轻量过货历史；自动预警
  从该历史生成当年、截至当前季度、最近三个月和当前 ISO 周的重叠汇总，不用
  报警记录数伪装过货量，也不恢复旧 Monitor 全量链。
- 自动预警的 Y/Q/M/W 汇总采用
  `resources/inline_domain/monitor/北极星报警率与CPK汇总.xlsx` 作为持久化读模型：查询
  只 upsert 查询日所属的年、季、月、ISO 周，再从写入成功后的工作簿回读渲染。
- `报警率` sheet 的稳定键包含产品、监控类型、厂别、周期类型和完整时间标签；在原有
  过货量与三类报警率之外保存 OOC/SOOS/OOS/Total 精确片数及 Total 报警率。旧格式
  行按 `ALL` 口径迁移，闭合周期不重算；旧表无法反推的精确片数显示为 `—`，不得用
  报警率估算。`CPK` sheet 由原有流程所有，本链路不修改。
- 企业加密工作簿只能通过 Excel COM 修改同目录临时副本的 `报警率` 区域；确认加密保护
  和写回数据后再原子替换正式文件，否则中止查询，不允许把受保护工作簿降级为明文。
- 自动预警继续由查询按钮门控；普通 URL 不显示更新时间，只有 `?admin=true` 显示
  OOS/OOC 的产品 × scope 状态。
- OOC 决策下载/上传也只在 `?admin=true` 下提供；不同 scope 使用各自稳定业务键，
  `True/False/Delete` 分别表示抑制、释放、隐藏预警事实。
- Inline 资源按 `spc/`、`ctq/`、`aoi_tt/`、`aoi_rs/`、`monitor/` 分目录，
  完整路径由 `config/domain/inline_domain.yaml` 注入组合根。

## Consequences

自动预警只读取已计算事实，不恢复旧 Monitor 全量分析。OOC 决策变化可通过工作簿
签名使页面缓存失效。过货历史在各子模块下次成功访问后增量形成，因此尚未访问的
历史窗口显示 0，不触发补跑。汇总工作簿不具备站点和事件明细粒度，因此趋势、Top
站点和超规明细仍直接使用共享事实；部分筛选在功能上线前没有对应闭合周期时显示 `—`，
不会用当前事实回写历史。AOI_RS 将在获得真实控制限后接入同一 builder，而无需改变页面契约。
