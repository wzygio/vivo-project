# ADR-0023：Q-Time 本地源快照与修饰边界

- Status: Accepted
- Date: 2026-09-03
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
   Parquet；TTL 复用全局 `data_snapshot.ttl_hours`。
2. 产品选项、分厂路径选项和明细分别保存。明细快照键包含源窗口、厂别、路径和
   产品筛选，禁止不同查询契约互相降级。
3. 明细全量加载起点为截止日第三个前序自然月的 1 日。数据库结果先规范化并以源
   `timekey` 原子落盘；读取完成后才映射为显示时间，并按调用方半开显示窗口过滤。
4. TTL 内优先读取快照；刷新数据库失败时只允许回退到同名旧快照。不存在可用
   快照时继续抛出脱敏的 `QTimeDataAccessError`，不得切换到业务字段不完整的视图。
5. `core/qtime/decoration.py` 继续拥有纯修饰规则；
   `infrastructure/qtime/decoration_repository.py` 仅作为 Excel 决策台账出站适配器。
   Excel sheet 名属于 application 上传/下载契约，不进入 core。

## Consequences

- 数据库短时不可用时，已有产品/路径选项和相同查询的明细仍可用。
- 原始 Parquet 不受日期前推开关污染，可在不同显示策略间安全复用。
- 查询组合会产生独立明细文件，需要依赖 `data/` 的运行时清理和容量监控。
- 首次运行且数据库不可用时仍无法构造数据；快照不是不等价数据源的替代品。

## Verification

- 单元测试覆盖源时间落盘、TTL 命中、旧快照降级、路径选项降级和显示窗口过滤。
- SQLite 集成测试继续覆盖绑定参数、厂别、路径、产品和半开时间窗口契约。
- Core 修饰、application 编排和 Excel 决策仓储分别保留独立测试。
- Q-Time 后端、页面 section/chart 与 SQL 集成聚焦回归：39 passed；Q-Time 三层
  模块覆盖率 91%。`compileall`、Ruff `E/F/I/UP` 与 `git diff --check` 通过。
