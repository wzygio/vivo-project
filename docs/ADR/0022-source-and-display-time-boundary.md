# ADR-0022：统一制造事实的源时间与显示时间边界

- Status: Accepted
- Date: 2026-09-03
- Scope: `src/shared_kernel/data_forward.py`、Yield、Inline、Indicator、Equipment
  仓储及其 Streamlit 派生缓存
- Recorded after implementation: 该决策于 2026-09-02 在 `feat/data-forward`
  worktree 完成实现和验证；合并至 `master` 仍需独立完成。
- Trace: PRD `docs/PRD/PRD-2026-09-02-报表数据日期前推.md`、Issue
  `.scratch/data-forward/issues/01-report-data-forward.md`、Plan
  `.planning/2026-09-02-data-forward/`

## Context

客户报表需要预留四天修饰时间，因此显示日期 `D` 应消费源系统日期 `D-4` 的
制造事实。Yield、Inline、Indicator 和 Equipment 使用不同数据库查询、Parquet
快照与时间字段；若各页面自行平移，容易造成查询窗口缺失、快照重复平移、设备
新鲜度误判及缓存跨模式复用。内部真实日期服务还需要复用同一代码并关闭前推，
不能复制一套业务实现。

## Decision

1. `shared_kernel` 提供不可变 `DataForwardPolicy`，全局配置统一定义
   `enabled` 和 `offset_days`。缺少配置时安全关闭；当前客户报表配置启用四天。
2. 数据库事实和原始 Parquet 永久使用源时间，不迁移、不重写已保存快照。仓储在
   返回应用服务前复制 DataFrame，并把约定的制造事实时间列映射为显示时间。
3. 直接数据库查询把调用方传入的显示窗口反向换算为源窗口后再查询，并将返回的
   时间字段映射回显示时间。启用本地源快照的仓储可加载覆盖显示窗口的更宽源窗口，
   但仍须在快照读取后映射并按原显示窗口过滤。现有区间开闭语义和 SQL 参数绑定
   保持不变。
4. 快照型仓储无论是否启用数据前推，均以截止日第三个前序自然月的月初作为加载
   起点。该源快照窗口独立于 `DataForwardPolicy`；原始数据落盘后，才在仓储读取
   输出边界复制、平移并按显示窗口过滤。
5. Equipment 的三天新鲜度和未来时间校验先在源时间上完成，再统一平移真实与
   仿造结果，避免四天偏移改变来源选择。
6. 承载派生报表数据的缓存签名包含策略启停状态和偏移天数。配置切换只使派生
   缓存失效，不修改底层原始快照。
7. 内部真实日期部署复用同一代码，将 `data_forward.enabled` 设为 `false`；部署
   端口和配置挂载由运行环境负责，不在领域代码中复制分支。

当前纳入的时间字段包括：Yield `warehousing_time`、`array_input_time`；Inline
共享测量/AOI_RS `start_time`、主制程履历事件时间和报废 `sheet_start_time`；
Q-Time `timekey`；IJP `print_time`、`day`；Equipment `glass_start_time`。

## Alternatives considered

### 在页面或图表层统一加四天

Rejected。页面只能改变已返回记录的标签，无法补回查询窗口头部的源数据，也会让
下载、应用服务与页面使用不同时间轴，并把相同规则复制到多个 Streamlit 页面。

### 在数据库 SQL 中直接为所有字段加四天

Rejected。各域数据库方言与时间字段格式不同，快照降级结果仍需单独处理；同时会
模糊调用方窗口究竟属于源时间还是显示时间，并扩大 SQL 修改与权限验证范围。

### 将平移后的时间写入 Parquet 或迁移历史快照

Rejected。配置切换或重复读取可能累加偏移，内部真实日期服务无法复用同一份
快照，历史数据还需要不可逆迁移。保存源时间可以让平移保持可逆和可审计。

### 复制一套关闭前推的内部服务源码

Rejected。复制会导致算法、修饰规则和缺陷修复长期漂移。统一策略加独立部署配置
已经能够隔离客户显示时间和内部真实时间。

## Consequences

### Positive

- 四个领域共享同一时间轴定义，页面、下载和应用服务看到一致的显示日期。
- 原始事实保持可审计，同一 Parquet 可以在开启和关闭模式间安全复用。
- 直接查询不会丢失显示窗口头部记录，快照结果不会泄漏显示截止日之后的数据。
- 设备真实优先及新鲜度语义不受显示偏移干扰。
- 策略签名阻止启停或修改天数后命中旧派生缓存。

### Negative

- 仓储维护者必须明确某个时间参数属于源时间还是显示时间；新增制造事实时间列时
  需要显式加入策略映射和测试。
- 客户看到的日期不再等于数据库日期，生产诊断必须同时记录或换算两条时间轴。
- 统一的自然月快照加载起点可能比旧的“同日减三个月”多读取月初若干天，增加少量
  数据库读取和本地存储成本；关闭数据前推时也保持这一窗口口径。

### Risks and constraints

- **重复平移**：只允许仓储输出边界调用平移；落盘对象必须是平移前的源事实，并
  通过重复读取及 Parquet 内容测试约束。
- **边界错位**：直接查询先反算窗口，快照结果在平移后过滤；固定时钟测试覆盖
  月初、跨月、空表和窗口边界。
- **缓存串模式**：新增派生缓存必须使用包含 `DataForwardPolicy.signature` 的签名；
  原始 Parquet freshness 不依赖显示时间缓存。
- **非制造时间误改**：日志、TTL、文件时间、人工修饰审计时间以及持续时长不参与
  平移。

## Verification

- 共享策略、配置、窗口换算、自然月起点、非原地平移及开关签名测试通过。
- Yield、Inline/AOI_RS、Q-Time/IJP、Equipment 聚焦跨域回归：354 passed。
- Domain smoke：SPC 334 passed；Equipment 43 passed；Yield 145 passed，3 个失败
  已在未改动基线复现。
- 全量 pytest：826 passed、9 failed、3 skipped；5 项在当前主工作区复现，其余
  4 项来自隔离基线与主工作区未提交 SPC/Excel 资源差异，无本决策新增失败。
- Playwright E2E 验证快照型 `2026-08-29 → 2026-09-02`、直接查询显示窗口
  `2026-09-02 →` 源窗口 `2026-08-29`，以及关闭模式恢复真实日期。证据位于
  `output/test-results/data-forward/`。
- `uv run python -m compileall -q src app tests` 与 `git diff --check` 通过。

## Follow-up

- 合并 `feat/data-forward` 后在 `master` 运行必要回归，并把合并提交和结果补充到
  `.planning/2026-09-02-data-forward/progress.md`。
- 若未来改变偏移方向、允许负数或引入分领域偏移，应新增或 supersede 本 ADR，
  不得静默改变 `data-forward-v1` 签名语义。
