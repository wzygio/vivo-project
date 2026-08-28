# Progress: Inline APP 层 Shared 公共管线重构

## 2026-08-19 — Session 1

- 分析现状：四个 dashboard 全部通读，重复点与 CTQ chart_type 缺陷核实（详见 findings.md）。
- 产出 PRD：`docs/PRD/PRD-2026-08-19-Inline-APP层Shared公共管线重构.md`。
- development-flow 模块 1 完成：issue `.scratch/inline-app-shared/issues/01-inline-app-shared-pipeline.md`，Status: ready-for-agent，Agent Brief 已填。
- development-flow 模块 2：创建本计划（task_plan / findings / progress），批准记录见 task_plan.md。

### Test Runs

| 时间 | 命令 | 结果 |
|------|------|------|
| 2026-08-19 | `pytest tests/unit -q`（基线） | 7 failed / 450 passed（既有基线：hot_reload、aoi_rs portal nav、code_selector×2、yield×3） |
| 2026-08-19 | `pytest tests/unit/app/sections/shared -q`（chart_type RED→GREEN） | 5 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/shared -q`（+spec_lines） | 12 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/shared -q`（+filters） | 20 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/shared -q`（+sheet_charts） | 26 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/spc tests/unit/app/pages/test_spc_page_alerts.py tests/unit/app/sections/shared -q`（spc 切换 shared） | 63 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/ctq tests/unit/app/pages/test_ctq_page.py -q`（ctq 切换 + chart_type 修复） | 8 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/shared -q`（+aoi_charts） | 32 passed |
| 2026-08-19 | `pytest tests/unit/app/sections/{aoi_rs,aoi_tt} + pages aoi 用例 -q`（aoi 切换） | 23 passed / 1 failed（=既有基线 aoi_rs portal nav） |
| 2026-08-19 | `pytest tests/unit -q`（全量回归） | 7 failed / 482 passed（失败集=既有基线，无新增；通过数 450→482，+32 条 shared 新测） |
| 2026-08-19 | grep 验收：ctq 无 spc_dashboard 导入；四 dashboard 无重复私有函数/常量 | 通过 |
| 2026-08-19 | import smoke：shared + 四 section + 四页面模块 | 通过（shared exports: 37） |

## 进度记录

- Phase 1 完成：shared 包（constants / chart_type / spec_lines / filters / decoration_admin）+ 26 条 shared 单测。
- Phase 2 完成：spc_dashboard、ctq_dashboard 重写为委托层；ctq 不再导入 spc_dashboard；CTQ chart_type 修复为配置驱动（既有 ctq 测试不修改即通过）。
- Phase 3 完成：shared/aoi_charts.py（AoiSpecLine 注入规格线：RS 单值 / TT USL+UCL），aoi_rs/aoi_tt 重写为委托层。
- Phase 4 完成：全量回归与基线一致；grep 验收与 import smoke 通过；ARCHITECTURE.md 前端结构描述已更新。
- UI smoke: not applicable —— 用户明确"无需 E2E 测试，自行完成"；本次为纯重构 + 单测覆盖。
- 分支策略偏差：系统约束禁止未经确认的 git 变更，未建开发分支，改动保留在工作区待用户审查/提交。
- 过程修正：spc 重写时一度把 `render_cpk_decoration_admin` 的 `nullcontext()` 误写为 `st.container()` 并漏导 `BytesIO`，当场修复后测试通过。

## 2026-08-19 — Session 2（结构调整：section = 组装层）

- 维护者指示：图表绘制下沉 `app/charts/`，section 作为组装层（对齐后端 application 层）。
- 执行：`chart_type` / `spec_lines` / `sheet_charts` / `aoi_charts` + 图表常量迁入
  `app/charts/inline/`；`sections/inline_domain/shared/` 精简为 constants（厂别）/
  filters / decoration_admin；四个 dashboard 与测试导入同步更新；
  图表测试迁至 `tests/unit/app/charts/inline/`。
- 可复用 UI 组件评估：无新组件需进 `app/components/`（trace 工厂
  `create_box_distribution_trace` / `create_point_line_trace` 本就在其中）。
- 验证：`pytest tests/unit -q` = 7 failed（=既有基线）/ 482 passed；无旧路径残留引用；
  import smoke 通过（charts/inline 导出 26，sections/shared 导出 11）。
- 文档同步：ARCHITECTURE.md、ADR-0016（结构调整注记）、PRD 4.1 目标结构。
