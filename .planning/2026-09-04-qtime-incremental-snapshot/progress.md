# Progress Log: Q-Time 共享增量快照

## Session: 2026-09-04

### Phase 1: Requirements design

- **Status:** complete
- 完成 tracker/领域/架构路由核验。
- 用户批准定时方式、原始事实快照、2 日重叠窗口与旧快照降级。
- 创建 `.scratch/qtime-incremental-snapshot/PRD.md` 和 issues 01-03。

### Phase 2: Planning

- **Status:** complete
- 选择 complex 模式，建立 ticket 拓扑、测试 seam 和 E2E 门。
- 记录用户对完成开发的批准。

### Phase 3: Development and testing

- **Status:** complete
- Ticket 01 RED：新增共享厂别文件、增量覆盖、月修剪、降级与 option 派生测试，首轮 4 failed / 4 passed。
- Ticket 01 GREEN：重构 `QTimeSnapshotStore` 为每厂别单文件 + 覆盖元数据；`QTimeRepository` 改为无筛选事实查询、内存过滤与 2 日覆盖增量合并。
- Ticket 01 完成：application `refresh_snapshots` 可观测 interface 通过测试。
- Ticket 02 完成：新增 Python CLI 和 Windows Task Scheduler 注册 adapter，3 tests passed，PowerShell 语法通过。
- Ticket 03：隔离 CLI → 三厂快照 → 共享 application 读取 E2E 1 passed；集中回归 147 passed。
- 浏览器 E2E 首轮失败：测试假定默认站点已选中，实际干净会话中为空，导致“查询” disabled；修正测试选择步骤后，Q-Time 页面与自动预警矩阵两条浏览器旅程均通过。
- 交付前评审发现并修复：数据/元数据双文件短暂不一致，改为 Parquet 内嵌元数据并单文件原子发布；旧 L1 清理提前，改为三个厂别替代快照全部存在后清理。
- 更新页面单元测试以匹配已批准的 Q-Time/Inline 模块标题和纯文本“查询”按钮。

### Phase 4: Project record

- **Status:** complete
- 更新 `ARCHITECTURE.md` 的 Q-Time 运行流。
- 修订 ADR-0023，记录统一 L1、两日覆盖、月窗口、降级与外部调度边界。

## Test Results

| Test | Expected | Actual | Status |
|---|---|---|---|
| Q-Time repository focused | 9 tests | 9 passed | pass |
| indicator_domain unit suite | 63 tests | 63 passed | pass |
| CLI adapter unit | 3 tests | 3 passed | pass |
| isolated CLI-to-shared-read E2E | 1 flow | 1 passed | pass |
| related regression suite | 147 tests | 147 passed; existing Excel COM fatal diagnostic printed | pass with warning |
| final changed-scope regression | 159 tests | 159 passed; existing Excel COM diagnostic printed | pass with warning |
| Q-Time browser E2E | full interaction | station selection, query, chart/table | pass |
| Alert matrix browser E2E | full interaction | matrix states, Q-Time detail, refresh token | pass |
| repository-wide `pytest tests` | 987 tests | 974 passed, 13 unrelated pre-existing failures | scoped changes clean |
| compileall + PowerShell parse + diff check | all changed code | pass | pass |

## Error Log

| Error | Attempt | Resolution |
|---|---:|---|
| RTK `README*` Windows glob 报错 | 1 | 改用明确路径，不再重试该命令 |
| `uv run ruff` program not found | 1 | 不安装新工具，改用现有验证门 |
| 集中测试输出 Excel COM 0x80010108 | 1 | pytest 继续并 147 passed；记为既有环境警告 |
| Q-Time browser E2E 按钮 disabled 超时 | 1 | 改为测试显式选择站点后重跑 |
| 全仓 pytest 误收集 `output/tmp` | 1 | 改为正式测试根 `pytest tests`；临时目录中的外部技能测试不属于仓库套件 |
| 全仓正式测试 13 个无关失败 | 1 | 核对为加密 Excel 诊断、其他页面/配置陈旧断言等既存问题；本需求相关 159 项全通过 |
| compileall 写入运行中页面 pyc 被拒绝 | 1 | 不停止用户的 8503 Streamlit 服务；改为编译本次变更模块，全部通过 |

## 5-Question Reboot Check

| Question | Answer |
|---|---|
| Where am I? | Phase 4 complete |
| Where am I going? | Delivery handoff |
| What's the goal? | 每日 07:00 共享增量 Q-Time L1 快照并通过 E2E |
| What have I learned? | 旧 L1 按查询与 options 分割，必须收敛为厂别统一事实快照 |
| What have I done? | 实现、评审、单元/集成/E2E 验证和架构沉淀均完成 |
