# Task Plan: Inline APP 层 Shared 公共管线重构

- Plan ID: `2026-08-19-inline-app-shared`
- Issue: `d:/wzy/Python/vivo-project/.scratch/inline-app-shared/issues/01-inline-app-shared-pipeline.md`（Status: ready-for-agent）
- PRD: `docs/PRD/PRD-2026-08-19-Inline-APP层Shared公共管线重构.md`
- Created: 2026-08-19
- 模式：inline 执行；无 E2E（用户明确自行完成）

## Goal

新建 `app/sections/inline_domain/shared/` 公共管线包，spc/ctq/aoi_rs/aoi_tt 四个 dashboard 委托复用；修复 CTQ chart_type 签名错配缺陷；公开 API、session key、图表行为不变；`pytest tests/unit -q` 不引入新失败。

## 批准记录

- 2026-08-19：用户在 `docs/dev_docs/dev_spec/Inline_domain/refactor-app_shared.md` Workflow 中明确要求"调用 development-flow 完成开发，无需 E2E 测试"，PRD 按用户指定产出至 `docs/PRD/`；视为对计划、公开接口保持策略与单元测试优先策略的批准（同 decoration-unify 先例）。

## Phases

### Phase 1 — shared 骨架与纯函数下沉（filters / chart_type / spec_lines / constants）

- [x] 1.1 创建 `shared/` 包：`constants.py`、`filters.py`、`chart_type.py`、`spec_lines.py`、`__init__.py`（显式 re-export）。验证：`python -c "from app.sections.inline_domain import shared"` 成功。
- [x] 1.2 新增 `tests/unit/app/sections/shared/` 单测：chart_type（token 命中/未命中/大小写/空配置）、spec_lines（LSL 空/0 仅上限、极小值格式化、y 轴范围）、filters（级联推导、normalise、signature）。验证：`pytest tests/unit/app/sections/shared -q` 绿。
- [x] 1.3 spc/ctq 切换到 shared.filters / chart_type / spec_lines（保留公开 API 薄封装）。验证：`pytest tests/unit/app/sections/spc tests/unit/app/sections/ctq -q` 绿。
- [x] 1.4 CTQ chart_type 修复：`resolve_chart_type(param_name, ConfigLoader.get_spc_line_chart_param_name_contains())`。验证：ctq 既有测试（含 `SE_L1T_UNI` → line 断言）不修改即通过；新增 shared 级断言"不含 token → box"。

### Phase 2 — sheet_charts 下沉（spc/ctq 共用图）

- [x] 2.1 迁移月周天分布图与 Sheet 点位图（By 腔室 / By 过货时间，时间轴 `type="date"`）到 `shared/sheet_charts.py`。验证：新增 shared 级测试断言过货时间 line 图 `xaxis.type == "date"`、箱线按腔室着色。
- [x] 2.2 spc 删除已下沉私有函数改为 shared 导入；ctq 删除对 spc_dashboard 的全部导入。验证：`grep -n "spc_dashboard" app/sections/inline_domain/ctq/` 无结果；spc/ctq/pages 测试绿。

### Phase 3 — aoi_charts 下沉（aoi_rs/aoi_tt 共用图）

- [x] 3.1 迁移 `_add_spec_trace`、月周天趋势图、By Lot/By Sheet 点线图到 `shared/aoi_charts.py`，规格线经 spec_provider 回调注入（RS 单值 / TT USL+UCL）；code 列名、y 轴文案、柱名参数化。验证：新增 shared 级测试（分组 x 轴分隔位、次 Y 轴柱、双/单规格线注入）。
- [x] 3.2 aoi_rs、aoi_tt 切换到 shared（filters + aoi_charts），保留各自 spec 装配与 render 编排。验证：`pytest tests/unit/app/sections/aoi_rs tests/unit/app/sections/aoi_tt -q` 绿。

### Phase 4 — 全量回归与收尾

- [x] 4.1 `pytest tests/unit -q` 全量运行，与既有失败基线对比不新增失败。验证：测试输出记录到 progress.md。
- [x] 4.2 重复消除验证：grep 确认四个 dashboard 不再各自定义 `_unique_sorted`、`_normalise_selection`、`_filter_signature`、`_add_spec_trace`、PERIOD 常量。
- [x] 4.3 四个页面 import smoke：`python -c` 逐个 import 页面模块对应 section，无循环依赖。
- [x] 4.4 issue 勾选验收标准并记录完成 comment；更新 ARCHITECTURE.md 前端结构描述（如涉及）。

## 范围守卫（Out of scope）

- monitor/equipment/yield 共享化、E2E、性能优化、`src/` 后端改动、布局/配色/文案变更。

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
