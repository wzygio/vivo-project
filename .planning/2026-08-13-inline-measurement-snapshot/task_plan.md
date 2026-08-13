# Task Plan: 统一 Inline 测量提取与报表端口

## Goal

将 SPC、CTQ、AOI_TT 的同源数据库提取和产品快照集中到共享 infrastructure 适配器，通过应用层出站端口与三个报表适配器消费，并保持现有业务口径直到定向 E2E 全部通过。

## Source and approval

- Issue: `D:\wzy\Python\vivo-project\.scratch\inline-measurement-snapshot\issues\01-centralize-inline-measurement-snapshot.md`
- Issue state: `ready-for-agent`
- Approval: 用户于 2026-08-13 明确要求“若设计合理则按步骤完成开发，并不断迭代直至 E2E 通过”；架构评估成立，视为对本计划、接口边界和测试重点的明确批准。
- Execution mode: inline；不使用 sub-agent。

## Current Phase

Phase 5 — Architecture record and delivery gate

## Phases

### Phase 1: Requirements and architecture assessment

- [x] 验证同源表、字段、参数识别、快照及降级现状。
- [x] 形成 `ready-for-agent` issue、Agent Brief、边界和验收标准。
- [x] 确认六边形依赖方向：app → application use case → outbound port ← infrastructure adapter。
- **Status:** complete

### Phase 2: Approved implementation plan

- [x] 记录共享原始字段超集、三个派生适配器和 composition 策略。
- [x] 映射所有 acceptance criteria 到可观察验证项。
- [x] 记录用户的条件式预批准与 E2E 完成门。
- **Status:** complete

### Phase 3: TDD tracer bullet and vertical migrations

- [x] RED：共享 DAO 字段超集、一次 UNION、时间/产品过滤及数值清洗测试。
- [x] GREEN：实现共享测量 DAO 与产品级快照适配器，包含 TTL、策略、强刷、原子写和快照降级。
- [x] RED→GREEN：为应用层定义小型 outbound ports，并通过显式 composition 注入具体 adapters。
- [x] RED→GREEN：SPC 适配器承接白名单、异常值、维度过滤与主制程追溯；保持原服务 facade。
- [x] RED→GREEN：CTQ 独立适配器复用共享事实集且保持 CTQ 输出契约。
- [x] RED→GREEN：AOI_TT 适配器按 step+param 识别、字段映射、规格与查询过滤，保留 lot/sheet/period 口径。
- [x] 清除 app 对 Parquet/具体 repository 的直接依赖，保持查询 DTO、缓存 payload 与产品签名行为。
- **Status:** complete

### Phase 4: Regression, integration and E2E verification

- [x] 静态边界检查：app 不导入 snapshot/repository；application 不直接构造 SQL/Parquet adapter。
- [x] 定向 unit/integration：monitor snapshot、SPC、CTQ、AOI_TT、自动预警兼容测试全部通过。
- [x] 项目 smoke：`uv run python tools/smoke.py spc` 通过。
- [x] 相关 E2E：SPC、CTQ、AOI_TT 页面加载、筛选、图表、产品级刷新和缓存行为通过。
- [x] 全量 pytest（或仓库可执行的最大安全测试集）运行并记录所有非本任务失败。
- **Status:** complete

### Phase 5: Architecture record and delivery gate

- [x] 更新 `ARCHITECTURE.md` 的共享测量所有权、依赖方向和运行流。
- [x] 写入 ADR，记录共享原始快照、独立派生适配器、兼容迁移和后果。
- [x] 逐条关闭 issue 与本计划 checklist，并审查 diff 未覆盖用户已有改动。
- [x] 定向自动化与 E2E 全绿后完成 development-flow project-record 阶段；全量基线失败已记录。
- **Status:** complete

## Acceptance checklist

- [x] AC1：共享 DAO 单次 UNION 输出完整稳定字段超集；由 DAO 单测检查 SQL 与列。
- [x] AC2：共享快照窗口/TTL/版本/强刷/原子写/失败降级；由 adapter 单测覆盖每条分支。
- [x] AC3：三个报表只从共享事实获取测量数据，专属规则隔离；由 fake port 单测和导入边界检查证明。
- [x] AC4：AOI_TT 聚合、规格及降级行为不变；由既有 calculator/service 与新增 adapter 回归证明。
- [x] AC5：app 只调用应用服务；由源码边界扫描和页面测试证明。
- [x] AC6：ADR-0001 payload 与产品签名兼容；由缓存发现/热重载/页面回归证明。
- [x] AC7：定向 unit/integration/E2E 全绿；测试日志记录在 progress.md。
- [x] AC8：Architecture 与 ADR 完成；文件内容审查证明。

## Decisions Made

| Decision | Rationale |
|---|---|
| `infrastructure/monitor` 归属共享 DAO 与 snapshot adapter | 满足用户目录要求，同时不让 monitor 应用/核心业务成为跨报表依赖。 |
| application 定义消费方拥有的 Protocol ports | 依赖倒置；应用用例可用 fake ports 测试。 |
| 共享快照保存预处理前字段超集 | 防止 SPC 的异常值、LOSS、追溯策略污染 CTQ/AOI_TT。 |
| SPC/CTQ/AOI_TT infrastructure 各自组合共享 adapter | 保留每个报表的数据契约与独立演进能力。 |
| 渐进 facade 迁移 | 保持页面、缓存发现和既有测试兼容，降低重构爆炸半径。 |
| 保留工作树已有 monitor 合规改动 | 用户改动与本任务无关，不覆盖或还原。 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| 首次读取旧文档路径 `references/domain/aoi_tt/spec-data_source.md` 不存在 | 1 | 用 `rg --files` 定位为 `references/domain/Inline_domain/spec-data_source-aoi_tt.md`，不重复旧路径。 |

## Scope guard

- 不改报表公式、视觉设计、数据库结构、AOI_RS/Yield/Equipment。
- 不重构用户正在修改的自动预警合规规则。
- 不复制 DataFrame 业务规则到 app。
