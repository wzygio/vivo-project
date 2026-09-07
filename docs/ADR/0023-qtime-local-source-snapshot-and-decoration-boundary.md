# ADR-0023：Q-Time 本地源快照与修饰边界

- Status: Accepted
- Date: 2026-09-03
- Amended: 2026-09-04
- Scope: `src/indicator_domain/{application,core,infrastructure}/qtime/`、
  `src/indicator_domain/composition.py`
- Supersedes: ADR-0019 中“Q-Time 不提供本地快照降级”的运行约束；其权威数据库
  来源、参数化 SQL、安全错误和展示边界继续有效。

## Context

Q-Time 页面在获取厂别路径选项后查询 Lot 级明细。此前所有读取都直接依赖数据库，
数据库权限或连接临时失败时无法进入筛选，也不能复用最近一次成功结果。与此同时，
Q-Time 的人工修饰包含两类不同职责：Excel 决策台账属于外部持久化，而超规识别、
`True`/`False`/`Delete` 语义和确定性修饰属于领域规则。二者不能因“decoration”命名
相同而放入同一层。

## Decision

1. `QTimeRepository` 在生产组合根中使用 `data/indicator_domain/qtime/` 保存本地
   Parquet；TTL 复用全局 `application.cache_ttl_hours`。
2. 每个厂别仅保存一份不含产品、路径筛选的标准化源事实快照。覆盖窗口、刷新时间、
   策略版本和行数内嵌在同一个 Parquet 中并通过临时文件原子替换。产品和路径选项从
   统一事实派生；快照尚不存在时可执行轻量 DISTINCT 查询，但不生成第二套 L1。
3. 页面显示覆盖上月 1 日至当天结束；仓储先按 `data_forward` 反算源时间窗口。首次
   刷新读取整个窗口，后续从已有覆盖尾部前 2 日开始重新读取，先替换重叠段、再合并，
   最后按目标窗口裁剪。因此迟到/修正记录可更新，进入新月后上上月数据自动删除。
4. TTL 内且覆盖充分时直接读快照，不访问数据库；过期或覆盖不足时增量刷新。数据库
   失败且旧快照可读时允许显式告警后降级；不存在快照时继续抛出脱敏的
   `QTimeDataAccessError`，不得切换到业务字段不完整的视图。
5. `tools/refresh_qtime_snapshots.py` 经 application 端口依次刷新 ARRAY、OLED、TP；
   `tools/register_qtime_snapshot_task.ps1` 提供每日 07:00 的 Windows Task Scheduler
   注册适配器。Streamlit 进程不持有常驻调度线程。
6. 旧查询签名明细和选项 L1 的生成逻辑被移除；仅当三个厂别的新快照均已发布后，
   才清理旧 `qtime_details_*.parquet` 与 `qtime_*_qtime-source-v1.parquet` 文件。
7. `core/qtime/decoration.py` 继续拥有纯修饰规则；
   `infrastructure/qtime/decoration_repository.py` 仅作为 Excel 决策台账出站适配器。
   Excel sheet 名属于 application 上传/下载契约，不进入 core。

## Consequences

- Q-Time 报表与自动预警看板继续共享 `get_cached_shop_monitoring`；一次源事实计算可供
  两个页面及任意筛选组合复用。
- 数据库短时不可用时，旧快照仍可用，但调用方可能看到超过 24 小时的数据；日志和
  CLI 非零退出码必须暴露这一降级状态。
- 原始 Parquet 不受日期前推开关污染，可在不同显示策略间安全复用。
- 每个厂别只有一份 L1，文件数稳定；代价是首次刷新需要读取完整滚动窗口。
- 两日重叠不依赖业务唯一键，但更晚到达或修正的数据需要通过删除快照或显式补跑恢复。
- 首次运行且数据库不可用时仍无法构造数据；快照不是不等价数据源的替代品。

## Alternatives considered

- 保留按查询签名明细和独立 options 快照：会重复计算和存储，无法实现两页面统一复用，拒绝。
- 持久化修饰后告警结果：会把人工决策版本固化进 L1，并破坏 application 层复用边界，拒绝。
- 在 Streamlit 内启动定时线程：多进程部署时会重复执行且生命周期不可控，改用独立 CLI 与系统调度。
- 仅追加新数据：无法覆盖迟到和被修正的记录，改用两日重叠替换。

## Verification

- `.scratch/qtime-incremental-snapshot/PRD.md` 与 issues 01-03 记录需求、边界和验收标准。
- 单元测试覆盖单文件元数据、TTL 命中、两日重叠替换、月窗口裁剪、旧快照降级、
  三厂发布后旧 L1 清理、选项派生与显示窗口过滤。
- SQLite 集成测试继续覆盖绑定参数、厂别、路径、产品和半开时间窗口契约。
- 隔离 E2E 覆盖 CLI 生成三厂快照、跨 repository 实例读取以及两个页面共享 application
  缓存；浏览器 E2E 覆盖 Q-Time 查询门控和自动预警矩阵 Q-Time 明细。
- 最终测试结果和环境告警记录于
  `.planning/2026-09-04-qtime-incremental-snapshot/progress.md`。
