# Task Plan — AOI_TT 报表

> Issue：`.scratch/aoi-tt-report/issues/01-create-aoi-tt-report.md`
> Spec：`references/domain/aoi_tt/spec-data_source.md`（数据源已全部确认）
> 模板：`.planning/2026-08-10-aoi-rs-report/`（同日 AOI_RS 交付先例）

## 交付切片（TDD，镜像 aoi_rs 五层布局）

1. **Phase 0 回归基线**：`pytest tests/unit/inline_domain -q` 记录基线。
2. **Phase 1 infrastructure**：`src/inline_domain/infrastructure/aoi_tt/data_loader.py`
   - `AoiTtQueryConfig`；`load_tt_param_set`（规格表 param_type IS NULL）；`load_tt_details`（三厂 UNION ALL + 字典 join + (step,param) 过滤）；`load_tt_spec_limits`（usl/ucl）。
   - 测试：sqlite ATTACH 契约测试（镜像 `test_aoi_rs_data_loader.py`）。
3. **Phase 2 core**：`src/inline_domain/core/aoi_tt/aoi_tt_calculator.py`
   - `build_period_trend_df`（值=Σtt_qty÷distinct sheet，分母 0→NaN）；`build_period_throughput_df`（检测片数柱状，全 period 0 填充）；`build_lot_point_df`；`build_sheet_point_df`；`attach_spec_values`（usl/ucl 双列）。
4. **Phase 3 application + 页面**：
   - `application/aoi_tt/aoi_tt_service.py`（缓存 payload→ViewModel，ADR-0001）；
   - `app/sections/aoi_tt/aoi_tt_dashboard.py`（筛选级联+查询门控+三图，USL/UCL 双虚线）；
   - `app/pages/AOI_TT监控报表.py`（固定窗口，缓存签名 `aoi_tt_report_v1`）；
   - `resources/static/config.js` 门户注册 `AOI_TT_REPORT`。
5. **Phase 4 验证**：inline_domain 回归 + 全量单测对照基线；Streamlit(8503) + playwright-cli E2E 烟测（页面发现/筛选门控/三图渲染/产品切换），截图存 `output/screenshots/aoi_tt_*.png`。
6. **Phase 5 沉淀**：ADR（数据源与口径决策：TT 识别规则、分母口径）。

## 关键口径（来自 spec，不再重新推断）

- TT 参数 = 规格表 `param_type IS NULL` 组合；明细按 (step_id, param_name) 过滤。
- 分母 = 明细表自身 distinct sheet/glass（过货视图无 AOI 站点）。
- 规格 = usl/ucl，(prod,step,param) 唯一，三图共用。
- 逻辑模型列：factory / prod_code / start_time / sheet_id / lot_id / step_id / tt_name / tt_qty。
