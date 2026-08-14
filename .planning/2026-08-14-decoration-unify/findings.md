# Findings：decoration-unify

## 代码事实（2026-08-14 核实）

### aoi 数据键列（D6 定案依据）
- aoi_tt 明细列：`factory, prod_code, start_time, sheet_id, lot_id, step_id, tt_name, tt_qty`
  （`src/inline_domain/infrastructure/aoi_tt/aoi_tt_repository.py:12-15`）；
  规格：`prod_code, step_id, tt_name, usl, ucl`（param_type IS NULL 投影）。
  现截断：service 层 `auto_clip_over_spec(value_col=tt_qty, join_keys=[step_id, tt_name], upper=usl)`。
- aoi_rs 明细列：`factory, prod_code, start_time, sheet_id, lot_id, step_id, rs_code, code_qty`
  （`src/inline_domain/infrastructure/aoi_rs/data_loader.py:51-60`）；
  规格：`prod_code, factory, type_flag, step_id, rs_code, code_desc, spec`（单边上限），
  type_flag ∈ MWD_RATIO / LOT_RATIO / SHEET_ID / GLASS_ID。
- aoi_rs 图表粒度（`core/aoi_rs/aoi_rs_calculator.py`）：
  By Lot = 每 lot `Σcode_qty ÷ distinct 过货 sheet 数`（键含 lot_id）；
  By Sheet = 每 sheet `Σcode_qty`（键含 sheet_id）；
  指标键 = `[factory, step_id, rs_code]`；`attach_spec_values(chart_kind)` 按
  type_flag 附着 spec（`SPEC_TYPE_BY_CHART`，:20-24）。
- aoi_rs 现截断位置：`app/sections/aoi_rs/aoi_rs_dashboard.py:416-425`
  （lot_df 用 chart_kind="lot"、sheet_df 用 "sheet"，各 clip 后 drop spec 列）。

### 引擎与应用层
- 引擎 `core/spc/spc_sheet_oos_decoration.py` 已支持 `decoration_file_name` /
  `decoration_sheet_name` 参数；键列硬编码 `OOS_KEY_COLUMNS`（:19）。
- flag 解析：`_parse_flag`（空默认 True）、`_is_delete_action`（大小写不敏感）、
  `_normalize_flag_action`；截断 `_clip_inside_spec` margin 5%~15% span，
  `_stable_fraction` SHA-256 稳定伪随机；企业加密回退 `_read_encrypted_xlsx_via_com`。
- 引用引擎的文件（迁移时全改）：
  `application/spc/spc_data_decoration.py`、`application/ctq/ctq_data_decoration.py`、
  `application/spc/spc_service.py`、`application/ctq/ctq_service.py`、
  `core/shared/auto_decoration.py`（import `_stable_fraction`）、
  `app/sections/spc/spc_dashboard.py`（SheetOosDecorationResult 等）、
  相关 tests。
- CPK 已单轨（2026-08-14）：`cpk_decoration.py` 单输入，`cpk_actual` 已删。

### original_* payload 消费点（清理时同步改）
- `spc_service.py:205` 空判断（改用修饰后 raw，已验证等价）；
- `tests/unit/inline_domain/application/shared/test_decorated_features.py:144-145,213`；
- `tests/unit/inline_domain/application/monitor/test_monitor_decoration_scope_routing.py:101-103`（mock payload）。

### E2E 现状
- `tests/e2e/*.js` 为 playwright 脚本（page => {}），打 localhost:8503 的 Streamlit
  （如 `spc_cpk_decoration.js` 用 `?admin=true`）。既有文件：spc_cpk_alert /
  spc_cpk_decoration / spc_filter_layout_mt_ch / spc_main_process_chamber /
  aoi_tt_report / monitor_compliance_config。无 ctq、aoi_rs 的 e2e。
- 上一计划（2026-08-13-inline-pipeline-reuse）E2E 方式：起应用 + playwright-cli
  跑脚本/截图签核，产物存 `output/test-results/`（AGENTS.md 安全边界要求）。

### 测试基线（2026-08-14 CPK 单轨后）
- `tests/unit`：409 passed / 4 failed（yield_global_data_policy ×2、
  code_selector_filter ×2，与 inline 域无关，为既有基线）。
