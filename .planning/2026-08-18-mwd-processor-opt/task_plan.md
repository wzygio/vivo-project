# Task Plan: 入库良率修饰逻辑简化（指定良损驱动）

**Plan ID:** 2026-08-18-mwd-processor-opt
**Issue:** `.scratch/mwd-processor-opt/issues/01-simplify-yield-modifier-pipeline.md`（ready-for-agent）
**PRD:** `docs/PRD/PRD-2026-08-18-入库良率修饰逻辑简化.md`
**Created:** 2026-08-18
**Status:** approved

## Goal

删除入库良率 "Codebaseline→EMA→趋势调节→月度校准→人工覆盖" 修饰链路，改为
`resources/入库良率修饰表.xlsx` 的"指定良损"驱动：确定性日度生成 + 周/月聚合，
并把月度缩放倍数接入 Mapping 不良数（级联衰减红线不动），直至 E2E 通过。

## Confirmed Decisions

- D1（2026-08-18 评审调整）：Group 趋势继续由 Code 日度按 Group 汇总；
  Group 级 Sheet 照常回写当月良损/缩放倍数，仅作展示，不驱动生成。
- D2：写回双通道（页面 cache miss 顺带写回 + CLI）；写回失败仅记日志。
- D3：`趋势图人工修正.xlsx` 停止消费、文件保留。
- D4（计划阶段已核实）：`load_static_warning_lines`（入库不良率规格.xlsx）消费方包括
  sheet_lot/capping、页面警戒线展示、alert_center —— **保留**；仅删除趋势侧
  `warning_lines` 入参（随 TrendRegulator 删除）。

## Phases & Checklist

### Phase 1 — 测试基线与 TDD 锚点
- [ ] 1.1 记录全量 pytest 基线（既有失败清单写入 findings.md）【验证：`pytest tests/unit -x -q` 输出】
- [ ] 1.2 新测试骨架先行（红）：`test_modifier_table.py`、`test_daily_generator.py`、`test_mapping_monthly_factor.py`【验证：新增测试全部 FAIL】

### Phase 2 — 修饰表管理器 `core/mwd_trend/modifier_table.py`（对应 AC-1/AC-2）
- [x] 2.1 `read_modifier_table`：读 `<prod>_Group级`/`<prod>_Code级`，COM 回退，缺文件/缺 Sheet/缺列 → 空表语义【验证：单测 10 项通过】
- [x] 2.2 `compute_current_month_loss`：按 level（group/code）算当月原始良损【验证：单测 3 项通过】
- [x] 2.3 `resolve_monthly_targets`：当月指定 → 最近上月指定 → 当月原始 的回退链【验证：单测 5 项通过】
- [x] 2.4 `compute_scale_factors`：两位小数、除零/缺失 1.0、百分比字符串与 >1 防呆解析、上月回退口径【验证：单测 4 项通过】
- [x] 2.5 `signature` + `sync_modifier_table`：仅更新当月行（缺失则追加）、签名变化才回写缩放倍数、写回异常仅记日志【验证：单测 5 项通过（mock replace_workbook_sheet）】

### Phase 3 — 日度生成器 `core/mwd_trend/daily_generator.py`（对应 AC-3）
- [x] 3.1 确定性：同输入两次输出逐行一致（blake2b 哈希，非内置 hash）【验证：`test_daily_generator.py` 通过】
- [x] 3.2 月度合计 == `round(rate × 当月投入)`，单日 ≤ 当日投入（复用 `allocation.allocate_integer_counts`）【验证：单测】
- [x] 3.3 跨月平滑：月中锚点线性插值基线，月末日→次月首日基线率连续无阶梯【验证：单测】
- [x] 3.4 无周期震荡：白噪声自相关检验 + 全空输入回落原始【验证：单测 9 项通过】

### Phase 4 — facade 重写与旧链路删除（对应 AC-4/AC-8）
- [x] 4.1 facade 重写：Code 指定良损驱动；Group 保留 `mwd_code_data` 汇总（D1 调整）；输出契约不变【验证：重写后 `test_defect_panel_count_alignment.py` 5 项通过】
- [x] 4.2 删除 `code_baseline.py`/`ema.py`/`trend_regulator.py`/`manual_overrides.py`/`pipeline.py`/模块级兼容入口/`inject_excel_overrides_to_config`（页面改调 `inject_mapping_config_to_config`）/defect_modifier 死代码/service 的 ema/scale 类属性/`allocation.reconcile_code_daily_counts` 保留（注：该函数仍在 allocation.py，无调用方，作为公开工具保留）【验证：grep 无残留引用】
- [x] 4.3 删除旧测试 4 个文件【验证：pytest 收集无 error】

### Phase 5 — Service / 配置 / CLI 接线（对应 AC-9/AC-10）
- [x] 5.1 `config/products/*.yaml`（5 个产品）`paths` 增加 `yield_modifier_config`【验证：脚本写入 + 接线测试】
- [x] 5.2 `YieldAnalysisService` 趋势方法接 modifier_targets，缓存 key 含修饰表签名（页面传 `compute_snapshot_signature(修饰表路径)`）；`get_mapping_data` 传 Code 级 `monthly_factors`【验证：`test_yield_service_modifier_wiring.py` 5 项通过】
- [x] 5.3 CLI `tools/update_yield_modifier_table.py --product <code>`【验证：M678 实跑写回正确（group 6 行/code 57 行 2026-08）】

### Phase 6 — Mapping 月度缩放（对应 AC-5/AC-6）
- [x] 6.1 `prepare_mapping_data` 新增 `monthly_factors`，级联衰减之前按 `(defect_desc, 批次月份)` 缩放；缺省 1.0 结果与现状一致；级联代码零改动【验证：`test_mapping_monthly_factor.py` 4 项通过；级联段 diff 为空】
- [x] 6.2 同 Code 月趋势不良数 ≈ Mapping 同月不良数（指定良损口径）【验证：数值 E2E 7/7（趋势==指定水准；Mapping 上/下调方向正确，上调遇级联天花板为设计内行为）】

### Phase 7 — 回归、E2E 与文档沉淀
- [x] 7.1 全量 pytest 不引入新失败【验证：461 passed / 5 failed = 基线（hot_reload×1、code_selector×2、yield_global_data_policy×2），两轮一致】
- [x] 7.2 E2E【验证：数值 E2E `output/tmp/verify_modifier_e2e.py` 7/7；浏览器烟测 `tests/e2e/yield_modifier_dashboard.js` 通过（无异常、页面完整渲染），截图 `output/test-results/yield_modifier_dashboard.png`】
- [x] 7.3 文档：`references/domain/yield_domian/mwd_trend_processor_algorithm.md` 重写、`ARCHITECTURE.md` 两处更新【验证：diff】
- [x] 7.4 ADR【验证：`docs/ADR/0016-yield-modifier-specified-rate-driven.md`】

## Out of Scope（与 issue 一致）

级联衰减算法、defect_multipliers 语义、mapping 热点脚本、趋势图人工修正.xlsx 迁移、
codebaseline.xlsx 文件清理、周/日粒度指定、其他域修饰逻辑。

## Errors Encountered

（暂无）

## Approval

- 2026-08-18 用户批准计划，并调整 D1：Group 趋势继续由 Code 日度汇总
  （放弃两级独立生成；Group 级 Sheet 仅作展示）。PRD / issue / 计划已同步。
