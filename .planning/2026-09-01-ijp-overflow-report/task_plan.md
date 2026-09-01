# Task Plan: IJP 溢流报表开发

## Goal

在 `src/qtime_domain/` 内完成 IJP 溢流监控 DDD 后端与 Streamlit 页面（复刻 FineReport 样式），并让单元/集成测试与隔离数据驱动的浏览器 E2E 全部通过。

## Source

- Issue: `D:/wzy/Python/vivo-project/.scratch/ijp-overflow-report/issues/01-create-ijp-overflow-report.md`（ready-for-agent）
- 任务书: `docs/dev_docs/dev_spec/qtime_domain/task-IJP溢流报表开发.md`
- 数据源分析: `docs/dev_docs/dev_spec/qtime_domain/datasource-IJP溢流报表分析.md`
- User approval: 2026-09-01 用户要求"不断迭代优化，直至 E2E 测试通过"，并授权无业务阻塞时自动执行、无需逐阶段确认（含计划批准）。

## Current Phase

Phase 3

## Phases

### Phase 1: Requirements & Data Discovery

- [x] 解析任务书、样式截图、13 组数据集 SQL、架构与既有 Q-Time 页面模式。
- [x] 只读探查全部数据源（7 项验证通过，证据 `output/tmp/ijp_db_probe_result.md`）。
- [x] 输出数据源分析文档，记录字段映射、类型偏差与派生选项。
- [x] 创建并 triage issue 至 `ready-for-agent`。
- **Status:** complete

### Phase 2: Plan & Acceptance Mapping

- [x] 建立隔离计划目录（task_plan/findings/progress）。
- [x] 将 issue acceptance criteria 映射到实现切片与验证证据（见文末 checklist）。
- **Status:** complete

### Phase 3: TDD Vertical Slices

- [x] Tracer bullet：失败测试定义 `IjpQuery` DTO、PANEL_LOCATION 映射、参数化仓储契约；验证：聚焦 pytest 先 RED 后 GREEN。
- [x] Slice 1（infrastructure）：`IjpRepository` 筛选项查询（PRODCODE/PRODUCT/SUB_PROD_TYPE/PICI/CYCLE）与 By天 占比、明细查询，绑定参数 + 白名单常量；验证：SQLite ATTACH 契约测试。
- [x] Slice 2（application）：`IjpReportService` 只依赖 `IjpDataPort`，返回原生可缓存 payload；DB 异常包装为 `IjpDataAccessError` 稳定文案；验证：service 单测（Fake port）。
- [x] Slice 3（charts/sections）：By天 堆叠占比图（Target 参考线）与明细表模型（含 Total 行、原图链接列），处理空数据/非数值；验证：chart/section 单测。
- [x] Slice 4（page）：薄入口 `app/pages/IJP溢流监控报表.py` 组装页头与 dashboard section，查询按钮门控；验证：页面静态测试。
- **Status:** complete（证据见 `progress.md` 2026-09-01 条目）

### Phase 4: Regression & E2E

- [x] IJP 聚焦单元/集成测试全部通过（36 passed）。
- [x] 全量 pytest 回归，区分既有失败基线（806 passed / 8 failed；7 项既有基线 + 1 项页面清单测试，计数已更新为 13，仍因既有 `专项资料-*` 页面缺页头失败）。
- [x] 隔离 fixture 页面 + playwright-cli 浏览器 E2E：页面打开、筛选、查询门控、图表、表格、空/错误分支；产物落 `output/test-results/ijp/`。
- [x] viewport-fit 与探索性烟测（非法日期、空结果、重复 rerun）。
- **Status:** complete（证据见 `progress.md` 2026-09-01 条目）

### Phase 5: Documentation & Delivery

- [x] 更新 `ARCHITECTURE.md`（qtime_domain 增加 IJP 子域描述）。
- [ ] 写 ADR 并关闭 issue/plan 证据。
- **Status:** pending

## Acceptance Criteria Checklist

- [x] AC1 页面自动发现、薄入口；验证：`tests/unit/app/pages/test_ijp_page.py` 静态组合测试 + 浏览器标题 `IJP溢流监控 E2E`（fixture）/页面 `page_title=IJP溢流监控报表`。
- [x] AC2 空筛选=不过滤、闭区间时间、查询按钮门控；验证：DTO 单测 + section AppTest（门控/非法时间窗）+ 仓储契约测试（G6/G7 边界行）。
- [x] AC3 设备/RS_CODE 白名单、线体前 6 位派生；验证：`test_ijp_repository_sql.py`（G3 白名单外设备、G4 白名单外 CODE 被排除；lines 筛选命中 G2）。
- [x] AC4 PANEL_ID 子串提取、PANEL_LOCATION 映射（含 BOTTOM0~9/KONG*）、原图 URL 拼接；验证：`test_ijp_overflow_core.py` + SQLite 集成测试（G1 BOTTOM/BOTTOM0 展开、KONGTOP）。
- [x] AC5 By天 堆叠占比图 + Target 参考线、空/错误安全态；验证：`test_ijp_chart.py` + 视觉截图 `output/test-results/ijp/ijp_report_main.png`。
- [x] AC6 明细表全列 + CODE_RATIO（GLASS_ID 内占比，ROUND 3）+ Total 行；验证：table model 单测 + 截图 `ijp_report_table.png`（Total=3.667）。
- [x] AC7 全部 SQL 绑定参数、错误不含 SQL/凭据；验证：`test_ijp_repository.py`（注入串仅出现在 params、语句中无拼接）+ service 错误路径测试。
- [x] AC8 单元/集成/E2E 覆盖；验证：聚焦 36 passed + E2E `IJP E2E 通过：bar traces=4，viewport 无横向滚动`。
- [x] AC9 无既有回归、不触碰用户资源；验证：全量回归 806 passed / 8 failed（均为既有基线，见 progress.md）+ 最终 git status 仅新增 IJP 文件与计划目录。

## Public Interfaces

- `IjpQuery`（frozen pydantic DTO）：start/end（必填闭区间）、可选筛选集合、target 可选数值。
- `IjpDataPort`（Protocol）：`list_filter_options`、`fetch_daily_ratios`、`fetch_details`。
- `IjpReportService`：薄应用服务，只依赖 port。
- `IjpDataAccessError`：稳定 UI 错误。
- `build_ijp_repository(db_manager)`：组合根。

## Decisions Made

| Decision | Rationale |
|---|---|
| IJP 作为 `qtime_domain` 内子域（`application/ijp/` 等）而非新建顶层 domain | IJP 属 OLED 段质量监控族且任务书归 qtime_domain 目录；复用既有分层/组合根模式，避免顶层 domain 膨胀。 |
| 明细 JOIN 用 `DWR_MES_PRODUCTSPEC` 基础表、筛选项用 `_V` 视图 | 两表列集不同（探查验证），与原 SQL 一致。 |
| timestamp 列删除 `<> 'NaT'` | 探查实证该判断在 timestamp 列上直接报错。 |
| By天 图起始时间向前扩 7 天 | 与原 `SERACH_BYDAY` SQL 一致。 |
| E2E 使用隔离 fixture 数据 | 与 Q-Time 一致：可验证完整 UI 行为，不依赖生产库状态。 |
| 计划视为已批准 | 用户明确授权 development-flow 自动执行至 E2E，无需逐阶段确认。 |
| PANEL_LOCATION/BOTTOM 展开在 core 纯 Python 实现，仓储只查原始列 | split_part 为 PG 专属；仓储保持双方言可执行（SQLite 契约测试）。 |
| KONG* 映射统一取 3 位后缀 | 原 SQL KONGLEFT 分支用 2 位后缀比 3 位边界（永不命中），按 brief 意图实现。 |
| `EVENT_TIME` 按方言分支（PG: `::TIMESTAMP`+`<> 'NaT'`；SQLite: 文本比较） | 最简单可靠的双方言方案；按 `engine.dialect.name` 判定。 |
| By天 聚合 SQL 侧 GROUP BY + pandas 侧占比；不含边框筛选 | 与原 SERACH_BYDAY 语义一致；日期截断用 `SUBSTR(CAST(... AS TEXT),1,10)` 双方言可执行。 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| `<> 'NaT'` 作用于 timestamp 列报 invalid input syntax | 1 | 仅保留在 varchar 的 EVENT_TIME 上，timestamp 列改用绑定参数比较。 |
| 契约测试边界行 G6 缺失 | 1 | H.CUT_START_TIME 同样受时间窗约束（原 SQL 语义），夹具 cut_start 调整到窗口内；实现无问题。 |
| Streamlit 1.60 多选下拉为虚拟列表，仅渲染前 10 项，C3BH2 点选超时 | 2 | E2E 中先向 combobox 键入过滤文本再点选 option。 |
| 选中值后 combobox aria-label 变为 "Selected X. CODE"，按名称匹配失败 | 2 | 改用 `[role="combobox"][aria-label*="CODE"]` 属性包含匹配；取消选择用 Baseweb 的 Backspace 语义。 |
| `test_hot_reload.py` 页面清单硬编码 12 | 1 | 计数更新为 13；该测试仍因两个既有 `专项资料-*` 页面缺共享页头失败（HEAD 上即存在的基线失败，未越权修改页面）。 |

## Scope Guard

- 不修改数据库结构/权限或 FineReport 原报表。
- 不复刻 FineReport 分页/打印/导出工具栏。
- 不做缺陷原图代理/鉴权服务，仅渲染 IMG_WEB 链接。
- 不引入快照降级契约（与 Q-Time 域现状一致）。
- 不改动现有 AOI/SPC/CTQ/Yield/Q-Time 业务语义与用户资源文件。
