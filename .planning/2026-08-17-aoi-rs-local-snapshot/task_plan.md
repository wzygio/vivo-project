# Task Plan: AOI_RS 产品级本地快照

## Goal

在不改变共享 measurement 连续量测契约和 AOI_RS 统计口径的前提下，为 AOI_RS 的 RS 明细与过货分母提供产品级 Parquet 快照、显式刷新和数据库失败降级，并完成自动化及浏览器验证与 ADR 沉淀。

## Source and approval

- Issue: `.scratch/aoi-rs-local-snapshot/issues/01-add-aoi-rs-local-snapshot.md`
- Issue state: `ready-for-agent`
- User approval: 2026-08-17，用户批准 AOI_RS 专属 infrastructure 快照方案，并明确“生成计划后直接开发即可”。
- Public interface change: AOI_RS application service 从具体 DB manager/loader 依赖改为 AOI_RS-owned data port；composition root 新增仓储构造和强制刷新入口；页面向统一页头注册底层刷新 handler。

## Current Phase

Complete

## Phases

### Phase 1: Requirements, architecture and plan

- [x] 将原始需求转换为 `enhancement` issue，并通过 triage 转为 `ready-for-agent`。
- [x] 证实 AOI_RS 与共享 measurement 只有查询形态相似，事实源与值语义不同，采用 AOI_RS 专属 infrastructure 快照。
- [x] 将用户对方案、接口方向、测试重点和“计划后直接开发”的批准记录在计划中。
- [x] 将全部 acceptance criteria、失败路径、回归与 UI 验证映射到 checklist。
- **Status:** complete

### Phase 2: TDD tracer — 端口到首个快照命中切片

- [x] RED：添加 AOI_RS data port/application 边界测试，证明 service 不再导入具体 infrastructure，并由 fake port 返回现有 payload。【验证：定向 pytest，`_data_port` TypeError】
- [x] RED：添加最小快照测试，覆盖首次数据库加载写入、第二次读取命中 Parquet 且不再调用 loader。【验证：定向 pytest，先失败；收集期 `ModuleNotFoundError`】
- [x] GREEN：实现最小 AOI_RS-owned port、专属 snapshot repository 与 composition 装配，使 tracer tests 通过，同时保持现有 ViewModel/payload 契约。【验证：snapshot 5 passed、service 7 passed、page 1 passed】
- **Status:** complete

### Phase 3: 快照完整语义与页面刷新

- [x] 实现 RS 明细和过货分母两个产品级快照，稳定字段分别保持现有逻辑模型；读取后按请求开始/结束日期过滤。【验证：snapshot/DAO 定向测试】
- [x] 实现 8 小时 TTL、三个月滚动数据库提取、策略版本和请求结束日期覆盖元数据；版本/TTL/覆盖不足触发刷新。【验证：mtime/sidecar/loader query 测试】
- [x] 实现同产品同数据集并发冷启动合并、临时文件同目录原子替换，以及明细/分母互不覆盖。【验证：ThreadPool loader=1、双路径、无 `.tmp` 残留】
- [x] 实现强制刷新；数据库异常或空结果不覆盖有效快照，有可读历史快照时降级，无可读快照时返回契约空表并记录诊断。【验证：异常、空、损坏、schema、无快照测试】
- [x] 规格继续独立查询；应用服务通过 port 获取明细、分母和规格，既有 type_flag、code_desc、修饰与图表 payload 不变。【验证：service 7 passed】
- [x] 页面注册当前产品 AOI_RS 快照刷新 handler；成功刷新不清除 `st.cache_data`，产品 revision 仍单独控制页面缓存。【验证：页面 callback 接线测试】
- **Status:** complete

### Phase 4: 回归、静态边界与浏览器 QA

- [x] 运行格式/静态检查（以仓库现有配置为准）并修复本任务引入的问题。【验证：compileall exit 0；AST boundary 8 passed；Ruff/Pyright 未安装且未配置】
- [x] 运行 AOI_RS infrastructure/application/core/page 定向测试，确认快照、数据提取、计算、规格、修饰、筛选和渲染全部通过。【验证：45 passed】
- [x] 运行 SPC、CTQ、AOI_TT 与 measurement 定向回归，确认共享 measurement 字段、TTL 和行为未变化。【验证：inline_domain 173 passed；相邻页面 8 passed】
- [x] 运行仓库全量自动化测试；任何既有失败必须以基线证据区分，不能把新增失败标为既有。【验证：原始 451 passed/8 failed；精确 deselect 8 个跨域基线后 451 passed】
- [x] 启动 Streamlit 并执行 AOI_RS 浏览器功能烟测：页面打开、产品/厂别/站点/Code 筛选、查询门控与图表渲染。【验证：8504 冷启动，M626/ARRAY/11629，4 Code、12 图，无 traceback】
- [x] 执行管理员刷新探索性烟测：分别检查“刷新数据”和“刷新缓存”的提示及分层行为；检查常用与窄 viewport 无新增布局溢出或遮挡。【验证：mtime/revision 更新；768px scrollWidth=innerWidth；视觉截图 signoff】
- **Status:** complete

### Phase 5: 文档沉淀与交付

- [x] 根据已通过实现新增 ADR，记录专属 AOI_RS 快照、端口/组合根、刷新/降级语义及对 ADR-0007 的部分取代；明确 ADR-0012 不变。【验证：ADR-0015 与实现/测试证据交叉核对通过】
- [x] 更新 `ARCHITECTURE.md` 中 AOI_RS 数据流、快照与刷新边界，不改动无关领域文档。【验证：文档路径与术语审阅通过】
- [x] 用测试证据逐项关闭 issue 和本计划 checklist，更新 progress/findings，并审计四个用户维护工作簿。【验证：其余三个工作簿未被本任务触碰；AOI_RS 工作簿在实机 QA 中被企业加密组件重新封装，但与 HEAD 解密后的 sheet、字段及两行业务数据完全一致；临时审计副本已清理】
- **Status:** complete

## Acceptance criteria mapping

| Issue criterion | Plan evidence |
|---|---|
| 明细与分母产品级快照及字段契约 | Phase 2 tracer；Phase 3 双快照测试 |
| 版本、TTL、滚动窗口、并发、原子写入、日期覆盖 | Phase 3 freshness/concurrency/atomic tests |
| 命中不查 SQL且按请求窗口返回 | Phase 2 hit test；Phase 3 filtering test |
| 异常/空/损坏安全降级 | Phase 3 failure-path matrix |
| 规格和计算/修饰兼容 | Phase 3 service tests；Phase 4 AOI_RS regressions |
| application port 与 composition root | Phase 2 AST/boundary + tracer tests |
| 页面底层刷新与页面缓存分层 | Phase 3 page tests；Phase 4 admin browser smoke |
| 全部自动化和跨域回归 | Phase 4 test suites |
| UI 功能、视觉、viewport 与探索烟测 | Phase 4 browser QA |
| ADR 取代范围与 measurement 不变 | Phase 5 ADR/architecture review |

## Technical decisions

| Decision | Rationale |
|---|---|
| AOI_RS 快照归 `infrastructure/aoi_rs`，不并入共享 `measurement` | RS Code 计数和过货分母不是连续量测事实；保护 ADR-0012 稳定字段与消费者边界 |
| 快照 RS 明细与过货分母，规格保持直接元数据查询 | 两个原始输入决定报表可用性；规格体量小且 AOI_TT 同样不将规格混入原始快照 |
| application 定义/拥有 AOI_RS data port | 消除 application → infrastructure 反向依赖，便于 fake port 和独立测试 |
| 每个产品、每类事实独立 Parquet + sidecar 元数据 | 避免异构 schema 混装；可分别验证 TTL、覆盖日期和故障降级 |
| TTL 8 小时、数据库滚动提取 3 个月 | 与 AOI_TT/共享 Inline 快照运维语义一致，并覆盖页面固定窗口 |
| 空数据库结果不替换已有快照 | 避免暂时性查询/连接问题导致持久化数据丢失 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| 领域 spec 旧路径不存在 | 1 | 通过 references 路由定位到 `references/domain/Inline_domain/spec-data_source-aoi_rs.md` |
| `references/dev_references/coding_spec` 不存在 | 1 | 使用项目 AGENTS、ADR、现有 tooling 与 ECC common+python 规则作为约束，并在交付证据中注明 |
| 全量 pytest 存在跨域既有失败并触发 Excel COM `0x80010108` 噪声 | 1 | AOI_RS/Inline 定向套件均全绿；用 `--tb=no` 复取精确计数，并将加密 Excel、专项页头、Code selector、Yield 配置失败登记为任务外基线，不修改无关模块 |

## Scope guard

- 不修改共享 measurement 数据 schema/TTL/仓储实现。
- 不改变 AOI_RS 计算、规格类型、修饰、图表或视觉设计。
- 不快照规格表，不修改数据库结构。
- 不覆盖或回退 `resources/*_decoration.xlsx` 的现有用户改动。
