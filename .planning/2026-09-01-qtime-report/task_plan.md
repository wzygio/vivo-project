# Task Plan: Q-Time 报表开发

## Goal

完成独立 Q-Time DDD 后端与 Streamlit 页面，并让隔离数据驱动的浏览器 E2E、聚焦测试和相关回归全部通过。

## Source

- Issue: `D:/wzy/Python/vivo-project/.scratch/qtime-report/issues/01-create-qtime-report.md`
- User approval: 2026-09-01 用户要求“不断迭代优化，直至 E2E 测试通过”，并授权无业务阻塞时自动执行。

## Current Phase

Phase 5

## Phases

### Phase 1: Requirements & Data Discovery

- [x] 解析任务、参考图、架构、领域词汇和既有页面模式。
- [x] 只读探查目标表、schema、字段类型、权限和潜在替代源。
- [x] 创建并 triage Local Markdown Issue 到 `ready-for-agent`。
- **Status:** complete

### Phase 2: Plan & Acceptance Mapping

- [x] 建立隔离计划、findings 和 progress 文件。
- [x] 将全部 issue acceptance criteria 映射到实现切片与验证证据。
- [x] 记录用户对自动开发、接口与测试优先级的预先批准。
- **Status:** complete

### Phase 3: TDD Vertical Slices

- [x] Tracer bullet：失败测试定义查询配置、厂别映射和参数化仓储契约；验证：聚焦 pytest 首次 RED，随后 GREEN。
- [x] Slice 1：产品/路径选项和明细仓储可通过端口查询，空产品不过滤且日期为 `[start,end)`；验证：仓储单元/SQLite 契约测试。
- [x] Slice 2：应用服务返回原生可缓存 payload，权限/连接异常保留诊断并转为稳定 UI 错误；验证：service 测试。
- [x] Slice 3：确定性柱线图与双语表格模型处理正常、空、缺规格和非数值数据；验证：chart/section 测试。
- [x] Slice 4：薄页面入口组装时间→厂别→路径→产品筛选和查询门控；验证：页面静态/Streamlit AppTest 测试。
- [x] 输出数据源分析文档，覆盖表职责、字段映射、权限与不采用替代视图的理由；验证：文档人工核对。
- **Status:** complete

### Phase 4: Regression & E2E

- [x] 运行 Q-Time 聚焦单元/集成测试；验证：22 passed。
- [x] 运行受影响 app/shared 回归；验证：219 passed / 2 个已确认既有基线 deselected。
- [x] 运行项目配置的静态检查（若仓库可用）和全量 pytest，区分既有失败；验证：compileall 通过，759 passed / 8 个既有失败，Ruff 未安装。
- [x] 启动 Streamlit，使用隔离 Q-Time 数据执行浏览器功能烟测：页面打开、级联筛选、查询、图表、表格；验证：Playwright 全流程通过，证据在 `output/test-results/qtime/`。
- [x] 视觉/viewport-fit：1365×768 与 900×900 检查筛选条、图表、表格不产生页面级横向溢出；验证：两视口均 `scrollWidth == clientWidth`。
- [x] 探索性烟测：非法日期、空结果、缺规格、重复 rerun 后状态稳定；验证：AppTest/浏览器记录。
- **Status:** complete

### Phase 5: Documentation & Delivery

- [x] 根据稳定领域边界更新 `CONTEXT.md`、`ARCHITECTURE.md` 和领域词汇表。
- [x] development-flow checklist 全部以证据关闭，写入 Q-Time ADR。
- [x] 复核 git diff，确认未覆盖用户已有 task 文档、Q-Time 图片、IJP task 和 SPC Excel 改动；测试改写的 AOI_RS 工作簿已恢复。
- [x] 更新 issue、计划与进度证据，完成交付。
- **Status:** complete

## Acceptance Criteria Checklist

- [x] AC1 页面自动发现、薄入口、正确标题；证据：页面静态测试 + 浏览器标题/heading。
- [x] AC2 时间→厂别→路径→产品级联、排他结束时间、查询门控；证据：核心/section 测试 + 浏览器操作。
- [x] AC3 厂别映射 ARRAY/OLED/TP；证据：参数化单测。
- [x] AC4 `mdw.qtime_tzbjx` 参数化 SQL、空产品语义、稳定排序；证据：仓储 SQL 捕获和 SQLite 集成测试。
- [x] AC5 柱状 wait_time + 红色 q_spec 线及缺失规格安全态；证据：Plotly figure 单测 + 视觉截图。
- [x] AC6 双语九列表格与表头/斑马纹语义；证据：table model 单测 + 视觉截图。
- [x] AC7 数据源分析文档与权限边界；证据：`Q-Time数据源分析.md`。
- [x] AC8 单元、集成、E2E 覆盖；证据：pytest 与 Playwright 日志。
- [x] AC9 无相关回归、不触碰用户资源改动；证据：回归结果 + 最终 diff/status。

## Public Interfaces

- `QTimeQuery`：开始/结束时间、shop、step_desc、产品集合。
- `QTimeDataPort`：`list_products`、`list_step_descriptions`、`fetch_details`。
- `QTimeReportService`：把端口 payload 交付给页面，不让展示层访问数据库。
- 页面筛选与图表/表格 helpers 为可测试的纯展示接口。

## Decisions Made

| Decision | Rationale |
|---|---|
| 独立 `qtime_domain`，不并入 inline domain | 任务要求独立 domain，且 Q-Time 是站点间过货事实而非测量参数分析。 |
| 生产查询只使用目标成品表契约 | 替代 view 仅 ARRAY、字段不完整且超时，静默替代会改变业务语义。 |
| E2E 使用隔离数据注入 | 当前账号缺少目标表 SELECT 权限；隔离数据可验证完整 UI 行为但不伪称 live DB 已验证。 |
| 时间窗采用 `[start,end)` | 与 FineReport SQL 完全一致，避免结束边界重复计数。 |
| 计划视为已批准 | 用户明确授权 development-flow 自动执行到 E2E，无需逐阶段确认。 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| PowerShell 初次读取中文出现乱码 | 1 | 固定 UTF-8 输出与 `Get-Content -Encoding utf8`。 |
| 两次复杂 `python -c` 引号解析失败 | 1-2 | 改为 `output/tmp/` 诊断脚本，避免重复脆弱 quoting。 |
| 目标表查询 `InsufficientPrivilege` | 1 | 通过 pg_catalog 确认字段与权限；按目标契约实现并记录部署前置条件。 |
| 替代 view 全表聚合被 WLM/timeout 取消 | 1-2 | 读取 view definition，确认其仅 ARRAY 且不等价，停止继续重试。 |

## Scope Guard

- 不修改数据库权限或 FineReport。
- 不复刻 FineReport 通用工具栏。
- 不改动现有 AOI/SPC/CTQ/Yield 业务语义。
- 不修改用户已有 Excel 与任务文档改动。
