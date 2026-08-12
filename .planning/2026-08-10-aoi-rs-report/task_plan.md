# AOI_RS 监控报表 — 实现计划

- Plan ID: `2026-08-10-aoi-rs-report`
- Issue: `D:/wzy/Python/vivo-project/.scratch/aoi-rs-report/issues/01-create-aoi-rs-report.md`（`ready-for-agent`）
- 数据源 Spec: `D:/wzy/Python/vivo-project/references/domain/aoi_rs/spec-data_source.md`
- 创建: 2026-08-10

## Goal

新增独立 AOI_RS 监控报表页面（月周天趋势 / By Lot / By Sheet 三张图，厂别+站点+Code 筛选，固定时间窗），TDD 交付直至 E2E 烟测通过，不破坏 SPC/CTQ 既有行为。

## 已批准的设计决定

- **模块布局**（镜像 SPC/CTQ 链路）：
  - `src/inline_domain/infrastructure/aoi_rs/data_loader.py` — SQL 加载（RS 明细 UNION ALL、过货分母、规格表）
  - `src/inline_domain/application/aoi_rs/aoi_rs_service.py` — 缓存 payload + ViewModel（ADR-0001）
  - `src/inline_domain/core/aoi_rs/aoi_rs_calculator.py` — period/lot/sheet 聚合与规格匹配
  - `app/sections/aoi_rs/aoi_rs_dashboard.py` — 筛选 + 三图渲染
  - `app/pages/AOI_RS监控报表.py` — 页面入口
- **统一逻辑列**：`factory, prod_code, sheet_id, lot_id, step_id, rs_code, code_desc, code_qty, start_time`（glass 归一为 sheet 命名）。
- **产品过滤**：三厂统一 `product_spec` LEFT JOIN `mdw.dwr_mes_productspec` 按 `productcode` 过滤；不使用 RS 表 productcode 列（array 全空）。
- **分母**：`eda.spot_eda_array_view_sht_v` / `spot_eda_oled_view_gls_v` / `spot_eda_tp_view_gls_v`，同站点同时间窗 COUNT(DISTINCT id)。
- **规格线**：`mdw.dwd_imp_rs_code_xishu_fo_tzsbjx`，type_flag 映射：月周天→`MWD_RATIO`，By Lot→`LOT_RATIO`，By Sheet→`SHEET_ID`（ARRAY）/`GLASS_ID`（OLED/TP），单边上限。
- **period 切分**：复用 `build_available_period_axis`（`spc_calculator.py:142-197`），不重写。
- **缓存策略**：数据量小（月级 ≤10 万行），不做 parquet 快照；`st.cache_data` 直接查库，payload 仅原生类型。
- **计划批准**：用户指令"按 development-flow 完成全栈开发，直至E2E测试通过"构成对计划与全流程的预先批准（2026-08-10，记录于此满足 TDD 计划门）。

## Phases

### Phase 0 — 契约冻结与测试基线
- [ ] 记录当前 SPC/CTQ 定向回归基线（`pytest tests/unit/inline_domain -q` 等）到 progress.md，作为后续回归对照。

### Phase 1 — Tracer bullet：数据加载 + 最小可打开页面（TDD）
- [x] RED→GREEN：`AoiRsQueryConfig` 与 data_loader 三表 UNION ALL 加载的单元测试（sqlite ATTACH 模拟 eda/mdw，验证字段映射与产品 join）。证据：`test_aoi_rs_data_loader.py` 2 passed。
- [x] RED→GREEN：规格表与过货分母 loader 测试（type_flag、TP 视图名 `spot_eda_tp_view_gls_v`）。证据：`test_aoi_rs_loaders_pass_spec.py` 2 passed。
- [x] service（缓存 payload → ViewModel）测试 3 passed；页面并入 Phase 3 一次性交付（含页面测试），避免二次改写。

### Phase 2 — Core 聚合（TDD）
- [x] RED→GREEN：period 聚合——分子 Σcode_qty、分母 distinct sheet 数、比值；分母为 0 → NaN 不除零（AC2）。
- [x] RED→GREEN：period 轴复用 `build_available_period_axis`，最近 2 月/3 周/7 天非空补全（AC2）。
- [x] RED→GREEN：By Lot / By Sheet 聚合（AC3）。
- [x] RED→GREEN：规格匹配——按 factory+step_id+rs_code+type_flag 取线；无规格 NaN 降级（AC6）。
- 验证：`test_aoi_rs_calculator.py` 6 passed；inline_domain 全量 69 passed（基线 56 + 新增 13）。

### Phase 3 — Dashboard 与页面集成
- [x] 筛选框：厂别 selectbox、站点 multiselect、Code 名称 multiselect（rs_code，图例带 code_desc），查询按钮门控与 SPC 一致（AC4）。
- [x] 三图渲染：按（厂别+站点）分组，月周天趋势（按 Code 分线 + MWD_RATIO 规格线）、By Lot、By Sheet 点线图（AC2/AC3）。
- [x] 页面集成：`app/pages/AOI_RS监控报表.py`，Header/产品切换/缓存签名/空态（AC1/AC6/AC7）；固定时间窗=上一自然月1日~当前（AC1，页面测试断言 2026-07-01~2026-08-10 且无 date_input）。
- [x] 门户注册：`app/static/config.js` 新增 `AOI_RS_REPORT` 并把侧边栏/技能树 AOI_RS 指向 Streamlit 页面（CTQ 先例）。
- 验证：`test_aoi_rs_dashboard.py` 6 passed、`test_aoi_rs_page.py` 2 passed。

### Phase 4 — 回归与 E2E
- [x] SPC/CTQ 定向回归与 Phase 0 基线一致（AC8）：`pytest tests/unit/inline_domain -q` 69 passed（基线 56 + 新增 13，无回归）。
- [x] 新增单测全绿；`pytest tests/unit -q --ignore=tests/unit/test_shadow_ema.py` → **283 passed, 5 failed**，5 个失败均为既有跨域问题（test_code_selector_filter×2、test_compliance_xlsx_config×1、test_yield_global_data_policy×2），与本次改动文件无交集；`test_shadow_ema.py` 收集错误亦为既有问题（CTQ 交付记录中已登记的同类遗留）。
- [x] Streamlit + Playwright E2E 烟测全项通过（证据截图 `output/screenshots/aoi_rs_*.png`）：
  - 页面被 Streamlit 发现并打开；Header/产品切换（M626→M673）/刷新按钮正常；控制台仅基础设施噪音。
  - 筛选级联（厂别→站点→Code 自动全选、未选站点时 Code disabled、查询按钮 disabled 规则）符合 SPC 门控。
  - ARRAY 两站点 × 三图 = 6 张 plotly 图渲染；OLED（glass 分支，M673/21329）3 张渲染。
  - 月周天组合轴（2026-07/2026-08 → W31-W33 → 08-03~08-10）跳过空值补全生效；规格线（MWD_RATIO/LOT_RATIO/SHEET_ID）按 Code 叠加；Code 图例带中文描述。
  - 查询门控：清空 Code 后报表隐藏并提示"尚未查询"。
  - 窄视口（900px）viewport-fit 通过。
  - 缺陷修复：图表标题与图例重叠 → 图例移至图下方（margin/legend 调整），复验通过。

### Phase 5 — 收尾
- [x] 更新 issue checklist 与 Comments（交付证据，2026-08-10）。
- [x] 移交 development-flow 模块 4（ADR）：`docs/ADR/0007-aoi-rs-report-data-source-and-counting.md` 已创建。

### Phase 6 — Task1 报表优化（issue 02，2026-08-10）
- [x] RED→GREEN：core 新增 `build_period_throughput_df`（全 period 轴过货量，0 填充）。证据：`test_aoi_rs_throughput.py` 3 passed。
- [x] RED→GREEN：趋势图重构——双 Y 轴（比值线+规格 / 过货量柱），月周天组间零宽空格留白+柱按组配色（月蓝/周绿/天橙）。证据：`test_trend_chart_has_bars_line_spec_and_grouped_axis`、`test_trend_chart_without_spec_draws_no_spec_line` passed。
- [x] RED→GREEN：渲染结构重构——每（站点+Code）一个默认展开 Expander，`st.columns(3)` 并列三图。证据：`test_render_sections_expander_per_code_with_three_side_by_side_charts` passed（3 Code → 3 expander × 3 图）。
- [x] 回归：`tests/unit/inline_domain + app/sections + app/pages` 85 passed；全量 `tests/unit`（排除既有 test_shadow_ema 收集错误）**288 passed / 5 failed**，5 个失败与 Phase 4 登记的既有跨域失败完全相同，无新增。
- [x] E2E 复验（playwright-cli @ 8503，M626/ARRAY/11629 多 Code）：4 个 Code → 12 张图（每 Code 一 Expander 三图并列）；趋势图柱（分组配色）+双轴+规格线正确；控制台仅基础设施噪音。截图 `output/screenshots/aoi_rs_task1_expander.png`。
- [x] 更新 issue 02 checklist 与交付证据。

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
| （暂无） | | |
