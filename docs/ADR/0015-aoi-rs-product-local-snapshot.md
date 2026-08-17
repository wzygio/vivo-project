# ADR-0015：AOI_RS 产品级专属本地快照

- Status: Accepted
- Date: 2026-08-17
- Scope: `src/inline_domain/{application,infrastructure}/aoi_rs/`、
  `src/inline_domain/composition.py`、`app/pages/AOI_RS监控报表.py`
- Trace: Issue `.scratch/aoi-rs-local-snapshot/issues/01-add-aoi-rs-local-snapshot.md`、
  Plan `.planning/2026-08-17-aoi-rs-local-snapshot/`

## Context

AOI_RS 原先只有 Streamlit payload 缓存；缓存 miss 时应用服务直接调用具体 DAO，
分别查询三厂 RS Code 明细、过货分母视图和规格表。它没有持久化数据快照、独立
「刷新数据」任务或数据库失败后的本地降级能力。ADR-0007 曾因月级数据量较小而
决定不引入 Parquet，但当前运维需求更重视重复查询削减、显式刷新和断库可用性。

AOI_RS 与 ADR-0012 的共享 measurement 都使用三厂表映射、产品字典和时间过滤，
但不是同一事实：measurement 是 `param_name/param_value` 连续量测；AOI_RS 是
`rs_code/code_qty` 缺陷计数，并依赖结构独立的过货分母。字段形态相似不足以扩展
共享 measurement 的稳定事实契约。

## Decision

1. AOI_RS 使用 `infrastructure/aoi_rs/` 内的专属产品级快照仓储，不把 RS Code
   或过货分母并入 `infrastructure/measurement/`。RS 明细与过货分母分别保存为
   `data/<prod>/aoi_rs_details_<prod>.parquet` 和
   `aoi_rs_pass_through_<prod>.parquet`；两者 schema 保持现有逻辑模型。
2. 两类快照采用三个月滚动数据库提取、8 小时 TTL、显式策略版本和
   `covered_through` sidecar。策略、TTL、覆盖日期或稳定字段不满足时刷新；返回页面
   前再按请求起止日期裁剪。
3. 同产品同数据集的冷启动用进程内锁合并；Parquet 与 sidecar 均先写同目录临时
   文件，再用 `os.replace` 替换，并清理临时文件。数据库异常、空结果、损坏或缺失
   快照均返回契约明确的结果；异常/空结果不覆盖最后一份有效快照。
4. 显式 `refresh()` 先加载明细和分母，任一异常或空结果即返回失败且不写入；仅当
   两类源数据均有效时更新两份快照。页面「刷新数据」调用该接口且不清除
   `st.cache_data`；「刷新缓存」继续只推进当前产品 revision 并重载代码/配置。
5. AOI_RS application 拥有 `AoiRsDataPort` 和查询 DTO。应用服务只通过端口获取
   明细、分母和规格，不导入 SQL/Parquet 实现；`composition.py` 在页面边界装配
   数据库和具体快照仓储。
6. RS 规格表保持独立的小型元数据查询，不进入本地快照。既有 `type_flag`、
   `code_desc`、月周天、By Lot、By Sheet 和三态修饰口径均不变。
7. DAO 的产品和时间值使用 SQLAlchemy 绑定参数；只有代码内固定的三厂表名、ID
   列和时间列参与 SQL 结构拼装。
8. 本 ADR 只取代 ADR-0007 Decision 8「不引入 parquet 快照」；ADR-0007 的数据源
   映射和计数口径、ADR-0012 的共享 measurement 契约继续有效。

## Alternatives considered

- **把 AOI_RS 并入共享 measurement 快照**：拒绝。两者物理表和事实语义不同，
  强行合并会把 `rs_code/code_qty` 与过货分母污染连续量测 schema，并扩大
  SPC/CTQ/AOI_TT 修改半径。
- **维持 ADR-0007 的仅 `st.cache_data` 方案**：拒绝。它只在进程内缓存最终 payload，
  无法提供持久化复用、底层显式刷新或断库降级。
- **同时快照 RS 规格**：拒绝。规格是小型可变元数据，和原始事实绑在同一 TTL 会
  延迟规格更新，也不是 AOI_TT/共享 measurement 的既有做法。
- **把异构明细与分母混入单一 Parquet**：拒绝。需要额外类型标记和大量空列，降低
  schema 可审计性；分文件更容易独立诊断和验证。

## Consequences

- 正面：同一产品和 TTL 内 AOI_RS 不重复查询两组大数据源；数据库短时不可用时可
  使用已有快照；管理员操作和 AOI_TT 一样区分底层数据刷新与页面结果缓存失效。
- 正面：application → infrastructure 反向依赖消除，service 可用 fake port 验证；
  SQL 值参数化消除原有产品字符串插值风险。
- 代价：每个产品新增两份 Parquet 和两份 sidecar；规格查询仍在页面缓存 miss 时访问
  数据库。进程内锁不能合并多个 Streamlit worker 的同时冷启动。
- 约束：两份快照各自原子替换，但不是跨文件事务；极端文件系统故障可能只替换其中
  一份，后续 freshness/schema 检查和显式刷新负责恢复。
- 运维：从本 ADR 前版本长驻热重载到新增组合根符号时，需在其他已加载页面点击
  「刷新缓存」或重启 Streamlit 一次；全新进程冷启动不受影响。

## Verification

- TDD 定向：AOI_RS snapshot/application/DAO/boundary/page Phase 3 汇总 34 passed；
  AOI_RS infrastructure/application/core/section/page 扩大回归 45 passed。
- Inline 回归：`tests/unit/inline_domain` 173 passed；SPC/CTQ/AOI_TT/AOI_RS 相邻页面
  8 passed；Python compileall exit 0，AST 边界 8 passed。
- 全量：451 passed / 8 个精确登记的跨域既有失败；deselect 该基线后
  451 passed、8 deselected、23 warnings、exit 0。
- Playwright（8504 冷启动）：M626/ARRAY/11629，4 个 Code 渲染 12 张图；底层刷新
  更新 Parquet mtime且图保持，缓存刷新更新产品 revision；768×900 无横向溢出或
  traceback。证据见 `output/tmp/aoi-rs-local-snapshot-qa/`。

