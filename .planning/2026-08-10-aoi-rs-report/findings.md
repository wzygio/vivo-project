# Findings — AOI_RS 报表

## 数据源（实库探查 2026-08-10，详见 references/domain/aoi_rs/spec-data_source.md）

- RS 明细：`eda.spc_tzbjx_rs_{array,oled,tsp}`，15 列同构；关键列 `product_spec/step_id/lot_id/sheet_id|glass_id/*_start_time/rs_code/code_qty`。
- 分母：`eda.spot_eda_array_view_sht_v`、`eda.spot_eda_oled_view_gls_v`、`eda.spot_eda_tp_view_gls_v`（**TP 非 tsp**，文档笔误）。
- 规格：`mdw.dwd_imp_rs_code_xishu_fo_tzsbjx`（555 行），type_flag ∈ {MWD_RATIO, LOT_RATIO, SHEET_ID, GLASS_ID}，`code_desc` 中文名。
- 产品字典：`mdw.dwr_mes_productspec`（productspecname→productcode）。

## 偏差与坑

1. `code_qty` 非恒 1（0~65），OLED ~40% 为 0 → 加和口径不变。
2. array RS 表 `productcode` 全空 → 必须 product_spec join 字典。
3. RS product_spec 有脏值（'1','55'）→ join 不上即排除。
4. `pd.read_sql` 直传含 `%` 的 SQL 触发 psycopg2 格式化 TypeError → 用 `sqlalchemy.text()`。
5. rs_code 存在 6 位个体（C4PCP3）→ 不做长度假设。

## SPC 链路可复用件

- `build_available_period_axis`（`src/inline_domain/core/spc/spc_calculator.py:142-197`）：跳过空值向前补全，2 月/3 周/7 天。
- 页面模式：`app/pages/CTQ监控报表.py` 是最近的镜像模板（缓存签名、query_config、render_page_header、filter 门控）。
- 筛选交互：`app/sections/spc/spc_dashboard.py:217-275` render_spc_filters。
- 图表元件：`app/components/distribution_charts.py` 的 create_point_line_trace / create_box_distribution_trace。
- 缓存边界：ADR-0001，payload 仅原生类型，ViewModel 缓存外构造。

## 测试约定

- 单测布局：`tests/unit/inline_domain/{application,core,infrastructure}`、`tests/unit/app/...`。
- E2E：`tests/e2e/*.js`（Playwright，打 localhost:8503）。
- 集成 DB 测试：`tests/integration/`。
