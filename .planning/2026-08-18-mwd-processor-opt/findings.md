# Findings: 入库良率修饰逻辑简化

## 代码事实（2026-08-18 调研）

- 趋势调用链：`app/pages/入库不良率分析看板.py:77-88` → `yield_service.py:171-233`
  → `MWDTrendProcessor`；`ema_span=120/scale=1.0/volatility=0.1` 为 service 类属性硬编码。
- 旧链路模块：`code_baseline.py`(523行)、`ema.py`(231行)、`trend_regulator.py`(92行)、
  `manual_overrides.py`(431行)、`pipeline.py`(44行)；保留：`data_preparation.py`、
  `aggregation.py`、`formatting.py`、`allocation.py`（整数分配复用）。
- 模块级兼容入口 `create_mwd_trend_data`（mwd_trend_processor.py:268）唯一调用方：
  `tests/unit/test_override_logic.py`（stale）。
- `defect_modifier.py:69-119` `_build_weight_maps`/`_get_dispersion_target` 全仓无调用（死代码）。
- Mapping：`mapping_processor.py:22` `prepare_mapping_data`；只取最新 5 批次；
  批次日期解析自 `batch_no`（:62-85）；级联衰减 :137-189（红线，禁止改动）。
- Mapping 不良数 = 每 `(batch_no, defect_desc)` 行数（:150），抽样 `random_state=42`（:180）。
- 趋势覆盖注入：`excel_service.inject_excel_overrides_to_config:230-252`；
  `趋势图人工修正.xlsx` sheet `<prod>_Group级`/`<prod>_Code级`；
  解析惯例：百分比字符串、`>1` 防呆（`_parse_override_excel:168-227`）。
- 加密 Excel：读 `_read_encrypted_xlsx_via_com`（COM）；写 `replace_workbook_sheet`
  （openpyxl 失败 → COM 全读 → 整体重写明文；PermissionError 仅告警）。
- D4 核实（2026-08-18）：`load_static_warning_lines`（yield_service.py:368，入库不良率规格.xlsx）
  消费方 = sheet_lot/capping.py:34、入库不良率分析看板.py:106/141/199（页面警戒线展示）、
  alert_center.py:14 → **保留**；仅趋势侧 `warning_lines` 入参随 TrendRegulator 删除。

## 修饰表实测结构

- 10 sheets：M626/M678/Z571/M673/Z517 × Group级/Code级。
- 列：`不良类型 | 周期类型 | 时间标签 | 当月良损 | 指定良损 | 缩放倍数`。
- `周期类型` 仅"月度"；`时间标签` = `2026-06/07/08`；`指定良损`/`缩放倍数` 全空。
- M678：Code级 171 行（3月 × 57 Code），Group级 18 行（3月 × 6 Group）。
- 全仓无代码读写该表（仅需求文档命中）。

## 约束补充（2026-08-18 用户强调）

- C1：Lot/Sheet级良损生成逻辑（`core/sheet_lot/` 链路）保持不变，也无需考虑其
  Group 级良损（本来无需展示）——本计划本就不触碰 sheet_lot，`load_static_warning_lines`
  保留（D4）。
- C2：开发在新分支进行，不影响原始代码 → 已创建分支 `feat/mwd-processor-opt`
  （master 引用不变；工作区中用户既有改动原样保留）。
- C3：`resources/入库良率修饰表.xlsx` 于 2026-08-18 从工作区移除（git 状态 ` D`，
  用户侧操作）。设计本就将该文件视为可选（缺失 → 空表语义）；开发/单测用 tmp
  文件，E2E 时用 CLI 生成样例表。

## pytest 基线

- 2026-08-18 `pytest tests/unit -q`：**436 passed / 5 failed**（32.89s）。
- 既有失败基线（与本任务无关，不得新增）：
  - `tests/unit/app/components/test_hot_reload.py::test_every_streamlit_page_uses_the_shared_page_header`
  - `tests/unit/test_code_selector_filter.py` ×2
  - `tests/unit/test_yield_global_data_policy.py` ×2
- 备注：`references/dev_references/coding_spec` 不存在（模块 3 提到的编码规范路径），
  以仓库既有代码风格为准。
