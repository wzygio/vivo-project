# Findings & Decisions: Q-Time 共享增量快照

## Requirements

- 权威范围：[PRD](../../.scratch/qtime-incremental-snapshot/PRD.md) 及 3 张 issues。
- 快照 24h 有效，每日 07:00 刷新，保留上月 1 日至今日并用 2 日重叠增量更新。

## Research Findings

- `.codegraph/` 不存在，本轮使用 `rg` 和定向文件读取。
- `Q_Time监控报表.py` 和自动预警矩阵都调用 `get_cached_shop_monitoring`，当前 L2 已共享。
- 当前 `QTimeSnapshotStore` 以查询窗口/站点/产品摘要生成 `qtime_details_<signature>.parquet`，选项另生成 `qtime_*_qtime-source-v1.parquet`，不符合单套统一 L1。
- 现有快照分支查询第三个先前自然月起的数据，且到期后全量重建；与新需求不符。
- Q-Time 明细包含 `step_desc/f_step/t_step`，可从统一事实快照派生站点选项。
- 项目已有 `data_forward` 显示时间策略；L1 保存源时间，查询和修剪窗口必须反向换算。
- 工作树含用户的无关 `config/global.yaml` 修改，必须保留。
- 统一快照的覆盖元数据必须显式记录 `source_start/source_end`；仅用 Parquet 最小/最大时间无法表达“查询成功但结果为空”的覆盖范围。
- 过期跨多日时，增量起点应为旧 `source_end - 2 days`，而不是新窗口末尾 2 日；这样可同时补齐中间漏跑日期。

## Technical Decisions

| Decision | Rationale |
|---|---|
| 原子 Parquet + JSON 覆盖元数据 | 新鲜度与空结果都需要可验证的时间窗口；JSON 是同一快照的元数据，不是第二套 L1 |
| 覆盖窗口整段替换 | 避免源表缺少稳定业务键时无法正确 upsert |
| 快照新鲜时 options 从快照派生 | 避免页面 rerun 访问 DB，同时取消 option L1 |

## Resources

- `.scratch/qtime-incremental-snapshot/PRD.md`
- `src/indicator_domain/application/qtime/cached_monitoring.py`
- `src/indicator_domain/infrastructure/qtime/repository.py`
- `src/indicator_domain/infrastructure/qtime/snapshot_store.py`
- `docs/ADR/0001-streamlit-cache-native-payload-boundary.md`
