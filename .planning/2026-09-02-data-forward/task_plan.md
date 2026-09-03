# Task Plan: 报表制造事实日期前推

## Goal

在不改写原始快照和质量算法的前提下，为四个 domain 建立可配置的四天显示时间平移，并让定向回归、smoke 与关键页面 E2E 通过。

## Source and Approval

- Issue: `D:/wzy/Python/vivo-project-data-forward/.scratch/data-forward/issues/01-report-data-forward.md`
- PRD: `D:/wzy/Python/vivo-project-data-forward/docs/PRD/PRD-2026-09-02-报表数据日期前推.md`
- User approval: 2026-09-02 用户要求“分析并完成开发”，原始任务明确要求调用 development-flow 并“不断迭代优化直至 E2E 测试通过”；该授权视为对以下计划、公开接口和测试优先级的预先批准。

## Current Phase

Phase 5

## Phases

### Phase 1: Requirements and Triage

- [x] 读取任务、架构、领域词汇、配置、仓储与快照代码。
- [x] 生成 PRD 并用 grill-with-docs 对窗口、快照、缓存、新鲜度和内部服务方案做压力测试。
- [x] 创建 Local Markdown Issue 并 triage 为 `enhancement / ready-for-agent`。
- **Status:** complete

### Phase 2: Plan and Acceptance Mapping

- [x] 建立隔离 plan、findings 和 progress 文件并记录 issue 主键。
- [x] 将 9 条 acceptance criteria 映射到 TDD 切片、回归、失败路径与 E2E 证据。
- [x] 记录用户对 development-flow、接口变化与 E2E 优先级的预先批准。
- **Status:** complete

### Phase 3: TDD Vertical Slices

- [x] Tracer RED：共享不可变策略覆盖缺省关闭、开启四天、签名、窗口反向换算、自然月月初与 DataFrame 非原地平移；验证：11 passed，关键行为均先 RED 后 GREEN。
- [x] Slice 1：`ConfigLoader` 暴露经严格校验的统一策略，`global.yaml` 开启四天；验证：缺失、关闭、非法 bool、签名变化测试通过。（AC1、AC2、AC8）
- [x] Slice 2：共享 Inline 与 AOI_RS 原始快照窗口使用第三个月月初，读取边界平移、显示窗口过滤且 Parquet 保持源时间；验证：7 + 12 passed。（AC2、AC4、AC5、AC6）
- [x] Slice 3：Yield 原始增量快照按源时间维护，返回前平移 `warehousing_time`；数组投入时间同步平移；验证：4 passed。（AC3、AC4、AC6）
- [x] Slice 4：Q-Time 与 IJP 把显示窗口反向换算为源窗口并平移 `timekey`、`print_time/day`；验证：16 passed。（AC3、AC4）
- [x] Slice 5：Equipment 先按源时间执行三天新鲜度，再对真实/仿造结果统一平移；验证：定向集成 + 设备单元回归通过。（AC4、AC7）
- [x] Slice 6：产品页、关键备件与全产品预警派生缓存签名纳入策略签名；验证：签名单测及页面调用检查。（AC8）
- **Status:** complete

### Phase 4: Regression and E2E

- [x] 静态验证：`uv run python -m compileall -q src app tests` 与 `git diff --check` 通过；Ruff/Pyright 未配置，不静默安装工具。
- [x] 定向 pytest：共享策略、Yield、Inline/AOI_RS、Indicator、Equipment 新增与受影响测试全绿（354 passed）。
- [x] Domain smoke：SPC 334 passed；Equipment 43 passed；Yield 145 passed，3 个失败在原分支复现。
- [x] 全量 `uv run pytest -q`：826 passed、9 failed、3 skipped；失败均通过主工作区复现或确认来自隔离基线资源差异，无任务内新增失败。
- [x] 浏览器功能 E2E：快照型和直接查询型均验证开启四天前推、关闭真实日期；脚本和截图落 `output/test-results/data-forward/`。
- [x] 浏览器视觉/viewport-fit：1365×768 截图无横向溢出或错误态；验证月初快照起点与切换模式后的 rerun。
- **Status:** complete

### Phase 5: Documentation and Delivery

- [x] 更新稳定领域术语/运行边界文档，记录源时间、显示时间和原始快照不平移契约。
- [ ] development-flow checklist 全部以证据关闭后创建 ADR；测试未通过不得写 ADR。
- [ ] 复核 worktree diff/status，保留主工作区用户改动，报告有意排除项和所有验证证据。
- **Status:** in_progress

## Acceptance Criteria Checklist

- [x] AC1 配置缺失/关闭兼容；开启与非法值行为可观察。【验证：11 个配置/策略测试】
- [x] AC2 四天平移、非原地、重复读取不累加。【验证：共享策略 + 快照重复读取测试】
- [x] AC3 Q-Time/IJP 源窗口换算与显示列平移，区间语义不变。【验证：SQL 参数契约 + 结果测试】
- [x] AC4 四域规定时间列统一策略、其他字段不变。【验证：各域参数化测试】
- [x] AC5 9/2→6/1、10/1→7/1 自然月快照起点。【验证：固定日期快照测试】
- [x] AC6 原始 Parquet 保持源时间、未来显示记录过滤、降级不回归。【验证：快照集成测试】
- [x] AC7 Equipment 源时间新鲜度在平移前判断、真实/仿造一致。【验证：设备服务测试】
- [x] AC8 策略签名隔离派生缓存。【验证：缓存签名单测 + 页面签名接入】
- [x] AC9 单元/集成/E2E 与回归证据齐全。【验证：Phase 4 命令和浏览器产物】

## Public Interfaces

- `DataForwardPolicy`：不可变对象，字段 `enabled`、`offset_days`，提供 `effective_days` 和稳定 `signature`。
- `ConfigLoader.get_data_forward_policy()`：读取全局策略；缺失时关闭，非法启用配置抛出明确 `ValueError`。
- 共享时间 API：显示窗口反向换算、第三个月月初计算、DataFrame 指定时间列复制平移；不暴露 Streamlit 或数据库依赖。
- 各 domain 现有 repository/application 公共方法签名保持兼容，调用方继续传显示时间。

## Decisions Made

| Decision | Rationale |
|---|---|
| 原始 Parquet 永久保存源时间，只在仓储返回边界平移 | 可逆、无历史迁移、避免重复平移，符合 ADR-0001/0012/0015。 |
| 直接查询窗口反向平移；快照结果平移后按显示窗口过滤 | 同时保证窗口头部完整和截止日后记录不泄漏。 |
| 三个月起点使用自然月月初 | 满足 9/2→6/1 的明确业务示例。 |
| Equipment 先判源时间新鲜度再平移 | 避免四天偏移把陈旧记录误判为新鲜或把近期记录变成未来。 |
| 内部服务复用同一代码、使用关闭策略的独立部署配置 | 避免复制源码漂移；端口/部署不属于本 issue。 |
| 计划视为已批准 | 用户明确授权完整开发流并要求持续到 E2E 通过。 |

## Scope Guard

- 不修改测量值、缺陷/良率/寿命/告警算法、日志/TTL/审计时钟。
- 不复制源码、不增加部署端口、不迁移或重写历史原始快照。
- 不重构 Yield Mapping、DatabaseManager 或既有数据库失败降级策略。

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| PowerShell 首次读取 UTF-8 中文按错误编码显示 | 1 | 后续读取统一显式 `-Encoding utf8`；源文件未改写。 |
| 首次宽泛 `rg` 输出被截断 | 1 | 改用按仓储/函数的定向读取并把关键发现写入 findings。 |
