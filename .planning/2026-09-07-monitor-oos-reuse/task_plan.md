# Monitor OOS reuse plan

## Goal

以四个 Inline 子模块已经计算的 OOS 明细构建长期增量历史，并让 monitor 与预警矩阵共享只读
结果；自动预警页面只在点击查询后渲染，刷新状态仅管理员可见。

## Phases

1. [complete] 实现并测试历史仓储、scope 契约、读模型与工作簿回退。
2. [complete] 接入 SPC、CTQ、AOI_TT、AOI_RS 生产者的覆盖窗口更新。
3. [complete] 替换 monitor 页面全量计算链并实现 admin-only 刷新状态。
4. [complete] 迁移预警矩阵消费者，执行回归、代码审查和 E2E。
5. [complete] 更新架构/domain 文档和交付记录。

## Guardrails

- 不触碰现有无关工作区改动。
- 不在 monitor 或矩阵读取路径查询全量量测/规格或重跑特征规则。
- Excel 仍是人工 flag 决策权威；Parquet 是长期事实权威。
- 不把 OOS 事实解释为告警率、OOC、SOOS 或报废。
- 不合并回 master，除非用户后续明确授权。
