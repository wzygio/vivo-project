# SPC 主制程设备/腔室追溯 — 实现计划

- Plan ID: `2026-08-10-spc-main-process-chamber`
- Issue: `D:/wzy/Python/vivo-project/.scratch/spc-main-process-chamber/issues/01-trace-main-process-equipment-chamber.md`（`ready-for-agent`）
- Source: `docs/dev_docs/dev_prompt/feat-SPC_CPM.md` Task2
- Created: 2026-08-10

## Goal

在不改变 SPC 点位粒度、筛选、能力计算和其他图表的前提下，让第二幅图按参数规格指定的主制程设备/腔室 OUT 履历分组，并通过数据源文档、TDD、SPC smoke 和浏览器 E2E 证明。

## Current phase

Phase 5 — Traceable closeout complete

## Approved decisions

- 用户指令“请分析需求并完成 Task2”以及需求文档“不断完善直至 E2E 测试通过”构成对本计划、公开 DataFrame 字段扩展和测试重点的预先批准。
- 每条量测选择不晚于量测时刻的最近 OUT 履历，解决北极星原 SQL 已知的多对多放大风险。
- 履历查询窗口为报表开始日前推一个月至报表结束日；以 `event_timekey` 前 14 位作为排序时间。
- 路由遵循规格 `main_eqp_type`：EQP 展示主设备，CHAMBER 展示主腔室；缺失分别默认当前站点和 EQP。
- EQP 履历缺失时回退量测 `unit_id`；CHAMBER 履历缺失时为 `UNKNOWN`，不把检测站点 `unit_id` 冒充主腔室。
- 主追溯字段进入原生 DataFrame payload 与 Parquet 快照；保持 ADR-0001 的缓存边界，不缓存自定义 ViewModel。

## Phases and evidence checklist

### Phase 0 — Contract and regression baseline

- [x] Issue 为单一 `enhancement + ready-for-agent`，Agent Brief、验收条件与 out-of-scope 完整。验证：读取 issue 状态与章节。
- [x] 数据源存在且字段可用。验证：只读 `information_schema`、规格唯一性与路由聚合探查。
- [x] 记录修改前 SPC 定向回归基线。验证：`python tools/smoke.py spc` → 138 passed。
- **Status:** complete

### Phase 1 — TDD tracer bullet: specification and one-route enrichment

- [x] RED→GREEN：规格加载返回 `main_step_id/main_eqp_type`，空值归一为当前站点/EQP，三元组仍不放大。验证：`test_spc_data_loader.py` 2 passed；实库三元组 1352/1352 唯一。
- [x] RED→GREEN：纯函数从候选历史中为量测选择量测前最近 OUT，历史多行不放大量测；无匹配执行已批准回退。验证：`test_main_process_trace.py`。
- [x] Tracer route：ARRAY EQP 从主站点 Sheet OUT 履历得到主设备和来源字段。验证：DAO SQL capture + enrichment integration test。
- **Status:** complete

### Phase 2 — Complete three-factory routing

- [x] ARRAY CHAMBER 优先 `SUB_UNIT_SHT`、回退 `UNIT_SHT`，且名称过滤符合北极星契约。验证：route-specific tests。
- [x] OLED/TP EQP 使用 `INOUT_GLS`，缺失历史回退量测设备。验证：route-specific tests。
- [x] TP CHAMBER 使用 `SUB_UNIT_GLS`；OLED CHAMBER 连接 OLED 工序映射并兼容 CVD1~4→CVD。验证：route-specific tests。
- [x] 查询只拉取目标物料、主站点和时间窗内 OUT 履历；参数绑定且不泄露动态 SQL 值。验证：SQL capture tests + code review。
- [x] DataFrame 输出保留原列并新增 `main_step_id/main_eqp_type/main_process_unit_id/main_process_trace_source/main_process_event_time`。验证：service contract tests + 实库 72 行六路由样本。
- **Status:** complete

### Phase 3 — Snapshot, service, and dashboard integration

- [x] 快照策略升级；旧快照缺主追溯字段触发刷新，完整快照仍命中，DB 失败仍按现有边界降级。验证：repository tests 4 passed。
- [x] 第二幅图只按 `main_process_unit_id` 分组排序，标题明确“主站点设备/腔室”；第一、第三幅图不改变数据口径。验证：dashboard chart tests 34 passed。
- [x] 预警区与普通报表区均消费同一已追溯 payload，图表 key 继续唯一。验证：现有及新增 dashboard tests。
- [x] 固定时间窗和厂别/站点/参数筛选行为保持一致。验证：SPC smoke 150 passed；Playwright 使用 OLED/21200 筛选查询成功。
- [x] 完成 `references/domain/spc/spec-data_source.md`：展示字段、规格、每厂路由、时间/去重/回退、探查证据。验证：人工契约检查。
- **Status:** complete

### Phase 4 — Regression and browser QA

- [x] 静态验证通过。验证：`compileall`、任务文件 scoped whitespace/diff check；ruff/black 未安装，已记录工具缺失。
- [x] SPC 单元/集成回归通过。验证：定向 55 passed；`python tools/smoke.py spc` 150 passed。
- [x] 功能 E2E：打开 `/SPC监控报表`、筛选并查询，第二幅图存在且标题/分组使用主站点设备/腔室。验证：`tests/e2e/spc_main_process_chamber.js`。
- [x] 视觉 QA：1440px/900px 无横向溢出；截图已人工检查；无业务控制台异常。验证：browser screenshot + console。
- [x] 探索性烟测：OLED/21200 筛选查询、多个参数主站点标签、第三幅时间排序均出现；M626 快照含 9 条 `unmatched_chamber` 且页面不崩溃。
- **Status:** complete

### Phase 5 — Traceable closeout

- [x] 对照 issue 每条 acceptance criterion 更新 checkbox 与交付证据。
- [x] 更新 planning progress/findings，所有 checklist 有实际证据。
- [x] 创建 `docs/ADR/0009-spc-main-process-equipment-chamber-trace.md`，记录主制程追溯、最近 OUT 和回退决策。
- **Status:** complete

## Errors encountered

| Error | Attempt | Resolution |
|---|---:|---|
| 初次并行读取包含不存在的 `spc_repository.py` 路径 | 1 | 使用 `rg --files` 定位实际 `repositories/spc_repository.py` 后读取。 |
| 初次 `rg` 组合正则引号不完整 | 1 | 改用单引号简化模式并重新执行。 |
| 首次 E2E 图表元素截图命中隐藏 Plotly 节点并超时 | 1 | 保留独立已验证图表截图，正式 E2E 使用稳定的全页截图与 DOM 图表契约断言。 |
| 全量测试收集 `test_shadow_ema.py` 失败 | 1 | 确认为无关 yield_domain 既有导入漂移；排除该文件后 331 passed / 5 个无关既有失败。 |
| `ruff`、`black`、`pytest-cov` 未安装 | 1 | 未擅自安装依赖；使用 compileall、定向测试、SPC smoke、E2E 和 scoped diff check 完成质量门。 |

## Scope guard

- 不修改统计公式、周期窗口、人工修饰、筛选器、第一/第三幅图口径或其他报表。
- 不新增时间筛选，不迁移北极星其他 SQL，不修改数据库源表。
