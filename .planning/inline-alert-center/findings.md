# Findings: Inline 自动预警中心

## 2026-08-25 调研结论（两个 explore 子代理）

### 后端事实

- 共享修饰引擎：`src/inline_domain/core/shared/sheet_oos_decoration.py`
  - `build_sheet_oos_detail()` 筛 `sheet_max>usl or sheet_min<lsl` → **工作簿每行都是超规片**
  - `_parse_flag`：False/"false"/"0"/"否"/"不修饰" → False（释放真实值）；空/其他 → True（默认截断）；`"Delete"` → 删除行
  - `load_sheet_oos_decoration()` 内置加密 COM 回退（`src/shared_kernel/utils/excel_tools.py`）
- 工作簿实测（.venv + pandas openpyxl）：
  - spc：列 `factory,prod_code,step_id,param_name,sheet_id,sheet_start_time,sheet_max,sheet_min,sheet_mean,usl,lsl,oos_type,flag`；`sheet_start_time` 为 datetime64；flag=False 记录合计约 73 条
  - ctq：同 spc 列结构；当前仅 M626 有 2 行（均 True）
  - aoi_tt：时间列名 `start_time`；所有 sheet 当前 0 行
  - aoi_rs：企业加密（须 COM）；列 `prod_code,factory,step_id,rs_code,chart_kind,point_id,value,spec,flag`；**无时间列**
- spc_cpk_decoration.xlsx 的 flag 语义相反（opt-in 修饰），趋势波动预警不读它，直接用 `build_weekly_cpk_alerts` 既有判据。
- ISO 上周范式：`tools/generate_ppa_oos_weekly_summary.py:previous_calendar_week` 返回 `[上周一, 本周一)` 半开区间；项目无共享工具函数。
- Service/ViewModel：spc/ctq 的 ViewModel 已含 `sheet_oos_decoration_result`；aoi_tt/aoi_rs 不含，需读工作簿。

### 前端事实

- SPC 模板（`app/sections/inline_domain/spc/spc_dashboard.py`）：
  - `build_weekly_cpk_alerts` :67（week + cpk<1.33 + 上一周标签 + 未修饰）
  - `render_cpk_alert_center` :120（Expander 有警自动展开）
  - `filter_spc_report_by_alerts` :153（中文键→英文列，MultiIndex.isin）
  - `render_cpk_alert_indicator_sections` :556（`🚨 自动预警指标图像` Expander + RenderGate.collect_memoized + `_alert_charts_signature` + `chart_key_prefix="spc_alert"`）
- RenderGate（`app/manager/render_gate.py`）：stage 纯计算 / collect / collect_memoized(state_key, signature)；签名须含 `build_product_cache_signature`（`app/components/page_header.py:53`）
- Yield（`app/components/alert_center.py` + `app/sections/yield_domain/yield_dashboard.py`）：
  - `AlertService.get_dashboard_alerts` 返回 `List[str]`（无结构化字段）
  - `AbnormalDetector` 阈值：环比翻倍且>0.1%，或绝对激增>0.2%
  - `compute_lot_oos_records`：近 30 天 lot 超规
  - 按 code 出图入口：`_build_compact_render_payload`（:935-981）、`render_code_compact_expanders`（:1055+）
- 无通用 alert 组件；`app/sections/inline_domain/shared/` 现有 filters/constants/decoration_admin。

## 需求方已确认决策

- D1：单片异常仅报 flag=FALSE（2026-08-25 AskUserQuestion）
- D2：aoi_rs 改造写入加时间列（2026-08-25 AskUserQuestion）
