# ADR-0016：入库良率修饰改为指定良损驱动（删除 EMA/人工覆盖链路）

- Status: Accepted
- Date: 2026-08-18
- Scope: `src/yield_domain/core/mwd_trend/`、`src/yield_domain/core/mapping/mapping_processor.py`、`src/yield_domain/core/defect_modifier.py`、`src/yield_domain/application/{yield_service,excel_service}.py`、`app/pages/入库不良率分析看板.py`、`config/products/*.yaml`、`tools/update_yield_modifier_table.py`
- Trace: Issue `.scratch/mwd-processor-opt/issues/01-simplify-yield-modifier-pipeline.md`、
  Plan `.planning/2026-08-18-mwd-processor-opt/`、
  PRD `docs/PRD/PRD-2026-08-18-入库良率修饰逻辑简化.md`

## Context

入库良率模块的数据修饰链路为
"Codebaseline → EMA → 趋势调节 → 月度校准 → 人工覆盖（趋势图人工修正.xlsx）"，
只有开发人员能理解；报表需交接业务人员维护，需求方要求全部改为由
`resources/入库良率修饰表.xlsx` 的人工指定良损驱动，并要求日度生成确定性、
无周期震荡、跨月平滑，且月周日趋势不良数与 Mapping 不良数水准一致。
Mapping 的级联衰减为业务红线，禁止静态重构。

## Decision

1. **删除旧链路**：`code_baseline.py`、`ema.py`、`trend_regulator.py`、
   `manual_overrides.py`、`pipeline.py` 及 `趋势图人工修正.xlsx` 的消费代码
   （`inject_excel_overrides_to_config`，页面改调 `inject_mapping_config_to_config`）
   全部删除；`趋势图人工修正.xlsx` 文件保留作参考。
2. **修饰表为唯一修饰来源**（`mwd_trend/modifier_table.py`）：读取
   `<产品>_Group级`/`Code级` Sheet（COM 回退）；每次计算只回写当月行的
   `当月良损`；`指定良损` 签名（blake2b，存 `<表名>.sig.json`，按 `<产品>:<级别>`
   分键）变化时重算并回写 `缩放倍数 = round(回退后指定良损/当月良损, 2)`；
   写回失败仅记日志。目标良损回退链：当月指定 → 最近上月指定 → 当月原始 →
   无目标（保持原始日度不良数，即当月良损水准，与 Mapping 倍数 1.0 一致）。
3. **日度生成器**（`mwd_trend/daily_generator.py`）：月中（15 日）锚点线性插值
   基线（跨月平滑无阶梯）+ blake2b 哈希白噪声（无周期、跨进程确定）+ 月内
   权重整数分配（复用 `allocate_integer_counts`，单日 ≤ 当日投入，月合计精确
   等于 `round(目标良损 × 当月投入)`）。未指定的缺陷保持原始日度不良数。
4. **Group 由 Code 汇总**（评审对 PRD-D1 的调整）：Group 趋势不独立生成，
   仍由 Code 日度按 Group 汇总；Group 级 Sheet 照常回写仅作展示。
5. **Mapping 月度缩放为级联前的前置步骤**：`prepare_mapping_data` 新增可选参数
   `monthly_factors`，在位置修饰之后、级联衰减之前按 `(defect_desc, 批次月份)`
   确定性抽样/复制（`_SIM_M` 后缀）。级联代码零改动。Mapping 不良数 =
   defect_modifier（全局）× 缩放倍数（月度）× 级联衰减。
6. **当月良损口径 = 趋势图同款 panel 明细**（D5）：即 `defect_multipliers`
   修饰后的展示数据，使 Mapping 数学严格成立（展示原始 × 指定/展示原始 = 指定）。
7. **缓存 key 含修饰表签名**：页面以 `compute_snapshot_signature(修饰表路径)`
   传入趋势/Mapping 缓存方法，业务改表后下次刷新即生效；写回发生在 cache miss 时。
8. `load_static_warning_lines`（入库不良率规格.xlsx）保留：其消费方还有
   sheet_lot/capping、页面警戒线展示、alert_center（D4）；仅趋势侧入参随
   TrendRegulator 删除。Lot/Sheet 级良损生成逻辑（sheet_lot 链路）不变（C1）。

## Alternatives considered

- **Group/Code 两级独立生成（PRD 初稿 D1）**：被评审否决——放弃 Group=ΣCode
  的内部一致性收益不大，业务更习惯现有汇总口径。
- **缩放倍数沿用"仅当月指定/当月原始"口径**：被否决——当月未指定但上月指定
  时趋势按回退生成而 Mapping 倍数为 1.0，两者水准不一致；改为回退口径后一致。
- **签名存 `data/<prod>/` sidecar**：改为修饰表旁 `<表名>.sig.json`——多产品
  共享同一工作簿时按 `<产品>:<级别>` 分键即可，且测试天然隔离。

## Consequences

- 正面：修饰逻辑对业务可读（填"指定良损"即可）；日度生成确定性可复现；
  趋势与 Mapping 共享同一水准；删除 5 个旧模块与 4 个旧测试文件。
- 负面/约束：
  - `当月良损 = 0` 且指定 > 0 时倍数记 1.0（Mapping 无法从 0 放大），
    趋势与 Mapping 在该口径下存在已知差异；
  - 企业加密工作簿写回走 `replace_workbook_sheet` 整体重写明文（与
    codebaseline 既有行为一致），文件被 Excel 占用时跳过写回；
  - Mapping 级联天花板绑定时上调倍数不再放大该批次（设计内行为）；
  - `resources/codebaseline.xlsx` 不再被读取，文件留待业务确认后清理。

## Verification

- 单元/集成：`tests/unit` 461 passed / 5 failed（= 既有基线：hot_reload ×1、
  code_selector ×2、yield_global_data_policy ×2）；新增
  `test_modifier_table.py`（27）、`test_daily_generator.py`（9）、
  `test_mapping_monthly_factor.py`（4）、`test_yield_service_modifier_wiring.py`（5）、
  重写 `test_defect_panel_count_alignment.py`（5）。
- 数值 E2E（真实 M678 快照，`output/tmp/verify_modifier_e2e.py`）：7/7 通过——
  当月行写回、指定解析、倍数两位小数、Code 月趋势 == 指定水准、Mapping 上/下调
  方向正确（上调遇级联天花板为设计内）、清空指定回落原始。
- 浏览器 E2E（playwright-cli，`tests/e2e/yield_modifier_dashboard.js`，worktree
  :8510）：页面无异常渲染完成，截图 `output/test-results/yield_modifier_dashboard.png`。
- 级联红线：`git diff` 核对 `mapping_processor.py` 级联段全部为新增行。
- CLI：`tools/update_yield_modifier_table.py --product M678` 实跑写回正确。
