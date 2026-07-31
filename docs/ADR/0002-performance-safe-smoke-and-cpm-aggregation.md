# adr-0002：以显式领域烟测和等价聚合缩短反馈周期

- Status: Accepted
- Date: 2026-07-14
- Scope: 测试反馈入口与 SPC CPM 月/周/日周期能力聚合

## Context

项目的单元测试入口会收集整个测试目录。在本次基线上，完整收集发现 135 项但被一个既有过期导入阻断；排除该模块后的可运行基线墙钟为 11.64 秒，并保留 7 个与本次任务无关的既有失败。多数单测本身很快，依赖导入与无关领域收集是更新后反馈慢的主要架构原因。

CPM 运行期另有独立的纯计算热点。仓库内 M626 快照包含 1,041,518 条点位，派生出 92,849 条 Sheet 特征和 158 个指标组。原实现为月、周、日生成周期数据后，再使用 Python 逐组循环计算能力统计：Sheet Mean 路径约 2.06 秒，Point Value 路径约 6.08 秒。

`CONTEXT.md` 明确保护 Yield concentration、Mapping、数据库生命周期、Streamlit 数据缓存和快照刷新语义。本次性能工作不能通过改变业务公式、周期口径、sigma 来源、fallback、排序或空值行为获得速度。

## Decision

### 1. 快速烟测采用显式领域范围

新增统一命令入口，支持 `spc`、`yield`、`equipment` 和 `all`：

1. 不传领域时默认 `all`，执行完整单元测试目录。
2. 快速领域必须显式选择，不从 Git diff 自动猜测。
3. 入口在执行前打印所有 pytest 目标。
4. 目标不存在、零收集或测试失败时，保留 pytest 的非零退出状态。
5. 快速烟测只优化开发反馈，不替代发布前完整回归。

### 2. CPM 聚合批量化，但保留旧版浮点归约

周期能力计算采用一次批量 `groupby/agg` 生成分组结果，替代 Python 中逐组构建 record：

1. 有效行仍在聚合前按 `sheet_mean`、`usl`、`lsl` 同时非空筛选。
2. 分组列、`dropna=False`、排序、唯一 Sheet 数、首个有效规格/控制限/target 和 Point-sigma 完整键保持不变。
3. Point Value 统计以批量聚合生成小型字典，再按原键查找；缺失时逐组回退 Sheet Mean。
4. 日期轴只对去重后的日期构造周、日集合，避免在百万行 Timestamp 上执行 Python 集合推导。
5. 均值和标准差继续调用旧实现使用的 `Series.mean()` 与 `Series.std(ddof=1)`，不使用 Pandas 原生 groupby 浮点归约。

第 5 点是强制等价边界。实验中，原生 groupby 标准差把两组近常数值归约为 `0.0`，使 CPK 从有限值约 `2.43e15` 变为 `inf`。该中间实现已被撤回，最终实现与优化前真实数据结果逐位相同。

## Consequences

### Positive

- 最终 SPC 领域烟测命令在当前环境约 4.65 秒，比 11.64 秒可运行单元基线快约 60.0%；该命令同时覆盖本次新增风险测试。
- 真实 M626 数据形状上，Sheet Mean 路径由 2.06 秒降至 0.39 秒，约快 81.2%。
- Point Value 路径由 6.08 秒降至 3.29 秒，约快 46.0%。
- 两条 CPM 路径的优化前后 DataFrame 通过 `check_exact=True` 逐位比较。
- 应用服务、Streamlit cache、ViewModel、数据库、快照和页面合同没有变化。
- 两个切片可分别回滚；测试工具变更不依赖 CPM 实现。

### Negative

- 快速领域烟测不能证明跨领域回归不存在，仍需完整验证。
- 测试文件新增或重命名时必须维护领域模式。
- 为保持逐位浮点行为，均值/标准差仍通过每组 `Series` reducer 执行；性能低于完全原生 groupby，但满足收益门槛且避免数值风险。
- 仓库既有失败与 Yield 过期导入会继续使相关完整/领域验证返回非零；本 adr 不通过改业务代码掩盖这些信号。

## Alternatives considered

### 根据 Git diff 自动选择测试

Rejected。当前项目存在共享配置、页面到服务的跨层依赖和本地未提交文件，自动推断容易漏测。显式领域选择更可理解，默认 `all` 更保守。

### 使用 pytest `-m smoke`

Rejected。若仍从整个单元目录收集，重依赖导入成本不会消失。按测试文件目标执行才能减少无关收集。

### 完全使用 Pandas 原生 groupby mean/std

Rejected。虽然更快，但改变近常数数据的标准差与 CPK `inf` 边界，不满足业务计算零变化约束。

### 优化 Yield、Mapping 或关键备件状态修饰

Rejected。它们位于明确保护或有状态业务边界；关键备件真实基线还存在独立编码读取故障。当前证据不足以承担风险。

## Verification

- 烟测路由合同：`8 passed`。
- CPM 计算合同与新增风险特征：`18 passed`。
- Focused 合并回归：全部通过。
- SPC 烟测：`67 passed, 1 failed`；失败为优化前已存在的页面 alerts 顺序断言。
- Equipment 烟测：`13 passed`。
- Yield 烟测：如实暴露优化前已存在的 `create_mwd_trend_data` 过期导入。
- 扩展 unit 回归：`141 passed, 7 failed`；7 个失败与优化前基线完全相同。
- 真实 M626 优化前后结果：Sheet Mean 与 Point Value 均逐位相同。
- Python 编译与 `git diff --check`：通过。
- Ruff：当前环境未安装，未执行；不作为仓库既有验证门禁。

## References

- `.scratch/project-performance-optimization/PRD.md`
- `.scratch/project-performance-optimization/issues/01-domain-scoped-smoke-entrypoint.md`
- `.scratch/project-performance-optimization/issues/02-vectorize-cpm-period-aggregation.md`
- `.scratch/project-performance-optimization/risk-checklist.md`
- `docs/adr/0001-streamlit-cache-native-payload-boundary.md`
