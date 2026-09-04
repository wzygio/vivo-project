# Findings: AOI_TT Particle Size

## Requirements

- Total 继续由 SPC `param_value` 提供。
- O/L 由 `eda.ARRAY_DEFECT_T` 中 `item51='AOI'`、`item119 in ('O','L')` 的缺陷行计数。
- 页面增加 `Total/O/L` 多选，默认全选。
- 每个 Particle Size 均有月周天、By Lot、By Sheet，仍位于同一站点 + TT Expander。

## Repository Findings

- 当前 AOI_TT 端口只提供 TT 明细与规格；仓储由共享原始测量事实投影 Total。
- 当前 Total 明细在应用服务中先应用 Sheet OOS 三态修饰，再交给领域计算。
- 当前领域聚合键为厂别 + 站点 + TT；需要新增 Particle Size，避免 Total/O/L 相加。
- 当前页面先用共享级联筛选获得厂别、站点、TT，再一次性渲染每个指标的三张图。
- `ARRAY_DEFECT_T.glass_id` 对应 ARRAY SPC 的 `sheet_id`；缺陷源未提供 Lot，需由 Total Sheet 明细补充。
- 数据前推策略要求直接数据库查询将显示窗口反算为源窗口，返回后再映射为显示时间。

## Step1 Deduplication Analysis

- 用户给出的两个独立计数均为 218，说明目标 Sheet/站点下 SPC Total 与 defect 明细数量能够对齐。
- 参考关联子查询却返回不同计数，因为子查询对同一 `sheet_id` 返回多条 SPC 测量记录；每个 defect 行会复制为匹配 SPC 行数倍。
- 正确做法是 Sheet→产品映射先 `SELECT DISTINCT sheet_id, productcode`，再与 defect 明细连接；随后按产品、站点、Sheet、Particle Size 对 defect 行计数。
- 只在最终结果 `DISTINCT` 不可靠：不同真实 defect 行可能字段相同，事后去重会误删真实缺陷；去重应作用于映射关系，不作用于 defect 事实。

## Technical Decisions

| Decision | Rationale |
|---|---|
| Particle 查询作为 AOI_TT 基础设施专属适配器 | 数据源和规则仅服务 AOI_TT，不污染共享原始测量快照 |
| 返回按 Sheet/站点/等级聚合的计数 | 降低传输量，并把“一行缺陷算一个”的事实口径固定在数据边界 |
| Core 负责把计数左连接到 Total Sheet 集合并补零 | 补零和 TDSUM/ARRAY 范围属于可测试业务规则 |
| `particle_size` 使用长表值 Total/O/L | 现有三类聚合可按新增维度自然扩展，避免三套字段与算法 |
| 应用服务对 Particle 源异常单独降级 | 保留既有 Total 报表，不让可选增强成为单点故障 |

## Risks

- 缺陷表与 SPC 的时间可能有轻微差异；Particle 图采用 Total Sheet 的时间和 Lot 归属，保证三种等级周期一致。
- 同一 Sheet 若存在多个 Total 行，O/L 基准集合必须先按指标 + Sheet 归一，避免分项计数重复。
- 规格仅为 Total TT 定义。O/L 可展示相同参照线，但不使用该规格自动截断 O/L 真实计数。

## Resources

- `.scratch/aoi-tt-report/issues/02-distinguish-particle-size.md`
- `docs/dev_docs/dev_spec/Inline_domain/feat-AOI_TT.md`
- `docs/ADR/0008-aoi-tt-param-identification-and-denominator.md`
- `docs/ADR/0012-shared-inline-measurement-snapshot.md`
- `docs/ADR/0014-inline-decoration-unify-shared-single-source.md`
- `docs/ADR/0022-source-and-display-time-boundary.md`

