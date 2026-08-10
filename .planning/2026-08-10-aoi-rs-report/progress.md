# Progress — AOI_RS 报表

## 2026-08-10 Session 1

- development-flow 模块 1 完成：issue `.scratch/aoi-rs-report/issues/01-create-aoi-rs-report.md` 创建并 triage 至 `ready-for-agent`（Agent Brief 已补全）。
- 模块 2：创建本 plan（task_plan / findings / progress）。计划要点：镜像 SPC/CTQ 链路的五层模块布局；复用 period 切分；st.cache_data 直查库；用户已预先批准全流程。
- Phase 0 完成：回归基线 `pytest tests/unit/inline_domain -q` → **56 passed in 11.56s**（2026-08-10）。`references/dev_references/coding_spec` 不存在，跳过。
- Phase 1 完成（TDD 3 切片）：`src/inline_domain/infrastructure/aoi_rs/data_loader.py`（AoiRsQueryConfig + load_rs_details/load_pass_through/load_rs_spec_limits，sqlite ATTACH 集成测试）+ `application/aoi_rs/aoi_rs_service.py`（缓存 payload→ViewModel，ADR-0001）。证据：infrastructure 4 + application 3 = 7 passed。
- Phase 2 完成（TDD 1 文件 6 测试）：`core/aoi_rs/aoi_rs_calculator.py`（build_period_trend_df 分子/分母比值、分母 0→NaN；build_lot_point_df/build_sheet_point_df；attach_spec_values type_flag 映射）。inline_domain 全量 69 passed。
- Phase 3 完成：`app/sections/aoi_rs/aoi_rs_dashboard.py`（筛选级联+查询门控+三图渲染）、`app/pages/AOI_RS监控报表.py`（固定窗口、缓存签名 aoi_rs_report_v1）、`resources/static/config.js` 门户注册（AOI_RS_REPORT）。dashboard 6 + page 2 = 8 passed。
- Phase 4a 完成：全量单测 283 passed / 5 failed（5 个均为既有跨域失败：code_selector×2、compliance_xlsx×1、yield_global_data_policy×2；另有既有 test_shadow_ema 收集错误按 CTQ 先例排除）。
- Phase 4b 完成：Streamlit(8503) + playwright-cli 烟测全项通过；修复 1 个视觉缺陷（图例压标题→图例移至图下）；截图证据在 `output/screenshots/aoi_rs_*.png`。
- 下一步：development-flow 模块 4（ADR 沉淀）。
