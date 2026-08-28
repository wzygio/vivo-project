# Findings: Inline APP 层 Shared 公共管线重构

## 代码事实（2026-08-19 核实）

- `spc_dashboard.py` 1215 行；`ctq_dashboard.py` 283 行；`aoi_rs_dashboard.py` 502 行；`aoi_tt_dashboard.py` 499 行。
- `ctq_dashboard.py:13-18` 从 `spc_dashboard` 导入 `_create_period_overview_chart`、`_create_sheet_points_box_charts`、`_resolve_chart_type`、`render_sheet_oos_decoration_admin`。
- **缺陷**：`ctq_dashboard.py:270` 调用 `_resolve_chart_type(indicator_features_df, indicator_raw_df)`；spc 定义 `_resolve_chart_type(param_name, line_param_name_contains)`（`spc_dashboard.py:563-573`）。DataFrame 迭代得列名，列名必在 `str(df)` 中 → CTQ 恒 line。配置 `spc.chart.line_param_name_contains=["UNI"]`（`config/inline_config.yaml:13-14`）在 CTQ 路径未生效。
- CTQ 测试参数 `SE_L1T_UNI` 含 token `UNI` → 修复后断言 `== "line"` 仍成立，测试无需修改。
- 规格线规则（`spc_dashboard.py:655-678` `_apply_measurement_spec_lines`）：LSL 为 NaN 或 0 → 仅 USL+UCL；否则 USL/LSL/UCL/LCL/Target/CL。y 轴范围 `_resolve_measurement_y_range`（`681-710`）。
- By 过货时间（`spc_dashboard.py:865-969`）：`sort_mode="按过货时间排序"` 且 chart_type=line → 横轴 `sheet_start_time` 真实时间，`type="date"`、`tickformat="%m-%d\n%H:%M"`、xaxis_title="过货时间"。
- 月周天分布（`_create_period_overview_chart` `772-831`）：period 轴（`build_available_period_axis`/`build_period_axis` 来自 `src/inline_domain/core/spc/spc_calculator`）+ display label「月/周/日 | label」+ PERIOD_COLORS/FILL 箱线。
- AOI 趋势图：x 轴 period 分组 + 零宽分隔符 `_PERIOD_SEPARATORS=["​","​​"]` + 去年份前缀显示 `re.sub(r"^\d{4}-","",label)`；次 Y 轴过货量/检测片数柱（PERIOD_BAR_COLORS）；主 Y 轴单 code 比值线（`CODE_PALETTE[0]`，不进图注）+ 规格线。
- AOI 点线图：x 按 `first_start_time` 排序的 id 类别轴；每 code 一条 `create_point_line_trace` + 规格线；RS 规格为单值（`attach_spec_values(chart_kind=...)`，区分 mwd/lot/sheet 口径），TT 为 (usl, ucl) 双上限。
- RS code 显示名：`rs_code（code_desc）`；TT 直接用 tt_name。
- trace 工厂已存在于 `app/components/distribution_charts.py`（`create_box_distribution_trace`、`create_point_line_trace`），shared 复用，不动。
- 筛选面板结构四者一致：`st.columns([1.1,2.5,3.4,0.9])`；厂别 selectbox、站点 multiselect（`format_step_label`）、第三级 multiselect（disabled=未选站点）、查询按钮签名门控；厂别切换清空下级；站点签名变化时第三级自动全选。
- session key 前缀：`spc_`、`ctq_`、`aoi_rs_`、`aoi_tt_`，第三级标签分别为「参数名称」/「参数名称」/「Code名称」/「Code名称」，第三级列为 `param_name`/`param_name`/`rs_code`/`tt_name`。
- 页面消费：`app/pages/{SPC,CTQ,AOI_RS,AOI_TT}监控报表.py` 按公开函数名导入对应 dashboard。
- 既有测试基线：tests/unit 有少量既有失败（yield_global_data_policy ×2、code_selector_filter ×2 等，引自 decoration-unify 完成记录）。

## 设计决策

- D1：shared 位于 `app/sections/inline_domain/shared/`，对齐后端 `src/inline_domain/{core,application}/shared/` 先例。
- D2：CTQ 与 SPC 共用 `spc.chart.line_param_name_contains` 配置（PRD OPEN QUESTION 1 的本期结论）。
- D3：`render_sheet_oos_decoration_admin` 属于"修饰后台 UI"而非绘图/筛选公共管线，且已被 ctq 以参数化方式复用——迁移到 shared（作为 filters 之外的 admin 组件）或保留 spc 由 ctq 继续导入？→ 决策：随 `_excel_bytes` 一起下沉 shared（消除 ctq→spc 导入的最后残留），key_prefix/report_name 参数化保持。
- D4：shared 纯函数不读 `ConfigLoader`、不碰 `st.session_state`；配置与 session key 前缀由调用方注入。
