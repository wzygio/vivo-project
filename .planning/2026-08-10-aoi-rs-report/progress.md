# Progress — AOI_RS 报表

## 2026-08-10 Session 1

- development-flow 模块 1 完成：issue `.scratch/aoi-rs-report/issues/01-create-aoi-rs-report.md` 创建并 triage 至 `ready-for-agent`（Agent Brief 已补全）。
- 模块 2：创建本 plan（task_plan / findings / progress）。计划要点：镜像 SPC/CTQ 链路的五层模块布局；复用 period 切分；st.cache_data 直查库；用户已预先批准全流程。
- Phase 0 完成：回归基线 `pytest tests/unit/inline_domain -q` → **56 passed in 11.56s**（2026-08-10）。`references/dev_references/coding_spec` 不存在，跳过。
- Phase 1 完成（TDD 3 切片）：`src/inline_domain/infrastructure/aoi_rs/data_loader.py`（AoiRsQueryConfig + load_rs_details/load_pass_through/load_rs_spec_limits，sqlite ATTACH 集成测试）+ `application/aoi_rs/aoi_rs_service.py`（缓存 payload→ViewModel，ADR-0001）。证据：infrastructure 4 + application 3 = 7 passed。
- Phase 2 完成（TDD 1 文件 6 测试）：`core/aoi_rs/aoi_rs_calculator.py`（build_period_trend_df 分子/分母比值、分母 0→NaN；build_lot_point_df/build_sheet_point_df；attach_spec_values type_flag 映射）。inline_domain 全量 69 passed。
- Phase 3 完成：`app/sections/aoi_rs/aoi_rs_dashboard.py`（筛选级联+查询门控+三图渲染）、`app/pages/AOI_RS监控报表.py`（固定窗口、缓存签名 aoi_rs_report_v1）、`app/static/config.js` 门户注册（AOI_RS_REPORT）。dashboard 6 + page 2 = 8 passed。
- Phase 4a 完成：全量单测 283 passed / 5 failed（5 个均为既有跨域失败：code_selector×2、compliance_xlsx×1、yield_global_data_policy×2；另有既有 test_shadow_ema 收集错误按 CTQ 先例排除）。
- Phase 4b 完成：Streamlit(8503) + playwright-cli 烟测全项通过；修复 1 个视觉缺陷（图例压标题→图例移至图下）；截图证据在 `output/screenshots/aoi_rs_*.png`。
- 下一步：development-flow 模块 4（ADR 沉淀）。

## 2026-08-10 Session 2 — Task1 报表优化（issue 02）

- 需求：趋势图加过货量柱状（双轴）、月周天视觉分组、按 Code 拆 Expander 三图并列。
- 交付：`build_period_throughput_df`（core，全 period 轴分母 0 填充）；`create_aoi_rs_trend_chart` 重构（make_subplots 双 Y 轴、组间零宽空格留白、柱按粒度配色）；`render_aoi_rs_indicator_sections` 重构（每站点+Code 一个 expanded Expander，columns(3) 并列）。
- 证据：相关单测 85 passed；全量 288 passed / 5 failed（同一批既有跨域失败，无新增）；E2E 复验通过（4 Code → 12 图，截图 output/screenshots/aoi_rs_task1_expander.png）。
- 口径未变，ADR-0007 继续有效，未新增 ADR。

## 2026-08-11 Session 3 — Task1 样式微调

- 需求（用户截图标注）：①趋势图横轴去年份（2026-07→07、2026-W31→W31、2026-08-10→08-10）；②趋势图去掉折线/规格线图注（仅保留过货量柱图注）；③By Lot/By Sheet 图注下移避免遮挡竖排 ID。
- 交付：`create_aoi_rs_trend_chart` 显示标签去年份（值映射仍用原始 period_label）、折线/规格 `showlegend=False`；`create_aoi_rs_point_chart` 图例 y=-0.5、底边距 200、图高 520。
- 证据：dashboard 测试 7 passed（RED→GREEN）；E2E 复验截图 `output/screenshots/aoi_rs_style_fix.png`；`tests/unit/app + inline_domain` 189 passed。
- 冲突处理：并行会话（AOI_TT 页面开发）在 `app/static/config.js` 中把 AOI_RS 导航改回 FineReport 旧链接、AOI_TT 指向 `LINKS.AOI` 占位，导致两个门户测试红。已按双方测试意图修复：AOI_RS→`AOI_RS_REPORT`、AOI_TT→`AOI_TT_REPORT`。

## 2026-08-11 Session 4 — By Lot 口径修正（Lot 内平均每片）

- 需求（用户截图指出全部超规异常）：By Lot 图应为 Lot 内 By Sheet/Glass 平均数 = Σcode_qty ÷ 该 Lot 同站点过货 distinct sheet/glass 数，与 LOT_RATIO 规格的比率语义对齐。
- 交付：`load_pass_through` 增选 `lot_id`；`build_lot_point_df(rs_details_df, pass_through_df)` 产出 rs_qty/sheet_qty/value（分母 0→NaN）；点线图新增 `y_col` 参数，Lot 图用 `value`，标题/y 轴/hover 改为"平均每片 RS 个数"。
- 证据：新增/改写 4 项测试 RED→GREEN；`tests/unit/app + inline_domain` 191 passed；E2E 复验 By Lot 值域 0.5~1.5 vs 规格线 4（截图 `output/screenshots/aoi_rs_bylot_avg2.png`）。
- 排障：TaskStop 只杀包装进程导致旧 uvicorn 子进程残留占用 8503（IPv4），E2E 首轮打到旧服务；按 PID 清掉残留后复验通过。

## 2026-08-11 Session 5 — 优化同步至 AOI_TT

- 应用户要求，将样式优化与 By Lot 口径修正同步到并行会话开发的 AOI_TT 报表（其已自建 Task1 双轴柱状+Expander 结构，缺样式三轮与 lot 均值）。
- 交付：`aoi_tt_calculator.build_lot_point_df` 改为 Lot 内平均每片（分母=TT 表自身 distinct sheet，每片必测无除零）；`aoi_tt_dashboard` 趋势图去年份+折线/USL/UCL 隐藏图注、点线图图注下移+y_col="value"+标题改"平均每片 TT 个数"。
- 证据：4 项测试 RED→GREEN，`tests/unit/app + inline_domain` 192 passed；E2E 复验截图 `output/screenshots/aoi_tt_synced.png`。
- 已在 `.scratch/aoi-tt-report/issues/01-create-aoi-tt-report.md` 追加同步交付记录。
- 重启教训复用：本次按 `Get-NetTCPConnection -LocalPort 8503` 清理监听进程，避免了上轮的残留服务问题。
