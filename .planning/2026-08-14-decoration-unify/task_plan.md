# Task Plan：修饰逻辑统一（decoration-unify）

- Issue：`.scratch/decoration-unify/issues/01-unify-decoration-into-shared.md`（ready-for-agent）
- PRD：`.scratch/decoration-unify/PRD.md`
- 方案：`docs/dev_docs/generated/Inline_domain/decoration-unify-proposal.md`
- 批准记录：用户 2026-08-14 明确"同意方案，跳过计划评审，直接按默认 plan 完成开发，直至 E2E 通过"（D1–D5 已确认；D6 已核实解决）。

## Goal

修饰算法收敛 shared（引擎一份 + 应用层入口一份），aoi_tt/aoi_rs 对齐 spc/ctq
（自动截断默认 + 工作簿三态 flag 释放/删除），aoi_rs 截断下移 service 层；
重构段零行为变化，行为段仅新增 aoi 工作簿能力；E2E 全部通过。

## 已确认设计（计划阶段定案）

- **D6 核实结果**：aoi_tt 明细含 `sheet_id/lot_id`（`aoi_tt_repository.py:12-15`），
  aoi_rs 明细含 `sheet_id/lot_id`（`data_loader.py:51-60`）。键列定为：
  - aoi_tt 工作簿：`[prod_code, step_id, tt_name, sheet_id]`，工作簿
    `resources/aoi_tt_sheet_oos_decoration.xlsx`（每产品一个 sheet）；
  - aoi_rs 工作簿：`[prod_code, factory, step_id, rs_code, chart_key]` +
    `chart_kind` 列（`lot`→chart_key=lot_id，`sheet`→chart_key=sheet_id），工作簿
    `resources/aoi_rs_sheet_oos_decoration.xlsx`。
- **引擎泛化方式**：迁入 `core/shared/sheet_oos_decoration.py` 后，
  将 `OOS_KEY_COLUMNS` 及相关列常量设为默认参数的模块常量，
  load/merge/persist/apply 函数增加 `key_columns`/`detail_columns` 参数（默认=现状），
  spc/ctq 调用零改动；aoi 复用同一批函数传入自己的列集。
- **应用层统一入口**：`application/shared/decorated_data.py::prepare_decorated_data(
  raw_measurements_df, spec_df, prod_code, scope, persist=True)`，
  `scope → 工作簿文件名` 映射表驱动；`_preprocess_sheet_features_by_type` 与
  资源目录解析各保留一份于此。返回 dataclass 不含修饰前特征。
- **payload 精简**：`fetch_decorated_features` 移除 `original_sheet_features_df` /
  `original_raw_measurements_df`；spc_service 空数据判断改用修饰后
  `raw_measurements_df`（已验证：两种判断最终都导向空 payload，行为等价）。
- **aoi_rs 截断下移**：service 内 `build_lot_point_df`/`build_sheet_point_df` →
  `attach_spec_values` → 工作簿修饰/截断 → payload 新增 `lot_points_df`/`sheet_points_df`；
  section 删除对应计算与 clip 调用，仅渲染。月周天趋势/过货量本就无修饰，维持现状。

## Checklist（TDD：每切片先写/改测试）

### Phase 0 — 安全网基线
- [x] 0.1 记录基线：`.venv/Scripts/python -m pytest tests/unit -q`（预期 409 passed /
  4 failed 既有基线：yield_global_data_policy ×2、code_selector_filter ×2）；
  验证方式：命令输出存档于 progress.md。

### Phase 1 — 引擎归位 core/shared（纯重构，tracer bullet）
- [x] 1.1 `core/spc/spc_sheet_oos_decoration.py` → `core/shared/sheet_oos_decoration.py`，
  全部引用点更新（spc/ctq wrapper、auto_decoration、spc_service、ctq_service、tests）；
  验证：`pytest tests/unit/inline_domain -q` 全绿。
- [x] 1.2 引擎函数增加 `key_columns` 等参数（默认=现状常量）；新增单元测试：
  自定义键列的工作簿 merge/persist/apply 行为；验证：新测试 + 既有测试全绿。

### Phase 2 — 应用层 wrapper 合并 + original_* 清理（纯重构）
- [x] 2.1 新增 `application/shared/decorated_data.py`（统一入口 + 单份
  `_preprocess_sheet_features_by_type` + 资源目录解析）；先加特征化测试锁定
  spc/ctq 两 scope 的输出契约；验证：新测试绿。
- [x] 2.2 `decorated_features.py` 单分支化、payload 移除 original_*、ctq 延迟导入消除；
  更新 `test_decorated_features.py`、`test_monitor_decoration_scope_routing.py`；
  验证：`tests/unit/inline_domain/application` 全绿。
- [x] 2.3 删除 `spc_data_decoration.py`/`ctq_data_decoration.py` 及其测试
  （特征化用例迁入 2.1）；`spc_service`/`ctq_service`/`monitor_service` 引用更新，
  spc 空判断改修饰后数据；验证：`tests/unit/inline_domain` 全绿 + 全量 pytest 无新失败。

### Phase 3 — aoi_rs 截断下移 service（D4）
- [x] 3.1 先写测试：service payload 中 lot/sheet 点帧为修饰后值（超规点被截断）、
  spec 列不泄漏；验证：测试先红。
- [x] 3.2 `aoi_rs_service` 内聚 lot/sheet 点帧构建 + attach_spec + clip；
  `aoi_rs_dashboard.py` 删除 `clip_over_spec_column`/`attach_spec_values` 调用改用 payload；
  验证：测试转绿 + `app/sections/aoi_rs/` grep 无修饰调用。

### Phase 4 — aoi_tt 工作簿三态（新能力）
- [x] 4.1 测试先行：默认（无工作簿）= 现状自动截断；flag=False 释放真实值；
  flag=Delete 行消失；缺 sheet=空修饰。验证：先红。
- [x] 4.2 `core/aoi_tt/aoi_tt_decoration.py`（detail 构建 + 共享引擎 load/merge/persist +
  截断应用），`aoi_tt_service` 接入；验证：转绿 + aoi_tt 既有测试不红。

### Phase 5 — aoi_rs 工作簿三态（新能力）
- [x] 5.1 测试先行：lot/sheet 两种 chart_kind 的三态行为 + 默认兼容；验证：先红。
- [x] 5.2 `core/aoi_rs/aoi_rs_decoration.py` + service 接入（在 Phase 3 下移后的位置）；
  验证：转绿。

### Phase 6 — 文档与全量回归
- [x] 6.1 更新 `docs/dev_docs/generated/Inline_domain/` 三份文档至最终态；
  固化"改工作簿→刷新缓存"操作契约；验证：文档与代码引用一致。
- [x] 6.2 全量 `pytest tests/unit -q`：无新失败（对照 0.1 基线）。

### Phase 7 — E2E（目标：全部通过）
- [x] 7.1 启动应用（localhost:8503），跑既有 `tests/e2e/spc_cpk_cpm_decoration.js`、
  `spc_cpk_alert.js`、`aoi_tt_report.js`、`monitor_compliance_config.js` 等；验证：无抛错。
- [x] 7.2 新增 aoi_rs 修饰 E2E（工作簿 flag=False/Delete 行为）+ CTQ 浏览器烟测
  （截图存 `output/test-results/`）；验证：断言通过、无 traceback。

### Phase 8 — 沉淀（模块 4）
- [x] 8.1 `docs/ADR/` 新增 ADR（修饰统一架构决策）；issue 状态 → complete。

## 非目标（与 issue 一致）

monitor AOI scope 切换、合规洗白、CPK flag 语义、历史 flag 数据迁移、
aoi 参数识别集统一。

## Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| （暂无） | | |
