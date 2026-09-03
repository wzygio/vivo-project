# Task Plan: 自动预警看板矩阵化（alert-matrix-board）

- Plan ID: `2026-09-02-alert-matrix-board`
- Issue: `.scratch/alert-matrix-board/issues/01-alert-matrix-board.md`（Status: ready-for-agent）
- PRD: `docs/PRD/PRD-2026-09-02-自动预警看板矩阵化.md`
- 可行性分析: `docs/dev_docs/generated/others/alert-center-matrix-board-feasibility.md`
- 分支: `feat/alert-matrix-board`（模块 3 开始时新建）
- Created: 2026-09-02

## Goal

在 `app/pages/自动预警看板.py` 页首新增"产品 × 监控参数"四态预警矩阵（8 行 × 7 产品列），点击红点懒加载预警明细与图像；qtime 补缓存层后接入矩阵；`fetch_decorated_features` max_entries 12 → 32。直至全部单元/集成/E2E 测试通过。

## 已确认决策（来自 PRD §2.2，不再变更）

- D1：矩阵本体只渲染指示灯，不自动渲染图像；详情点击后懒加载。
- D2：qtime 相关风险全部按可行性报告修正（补 `@st.cache_data` 缓存层，键含产品集合+时间窗+决策签名，TTL 12h）。
- D3：矩阵页绕开 Header 单产品筛选，不改 SessionManager。

## 设计决策（2026-09-02 Phase 3 前定点调研后锁定）

- **落点**：矩阵计算与 UI 落 app 层（`app/sections/inline_domain/monitor/alert_matrix*.py`）。理由：矩阵是跨域组装（inline+yield+indicator），src 域间互导会破坏分层；app 层可自由 import 各 src 域（先例：SPC 页同时 import monitor+spc）；`tests/unit/app/` 已有测试组织先例。
- **矩阵列**：`ConfigLoader.get_enabled_products()`（7 个，config/global.yaml:29-36），不用 data/ 目录发现。
- **行数据源**（全部复用既有判据，零算法改动）：
  - 4 个 sheet OOS 行：只读 scope 工作簿（`load_sheet_oos_decoration` + scope 键列 + COM 回退），`build_sheet_oos_alerts`（time_column：spc/ctq/aoi_rs=`sheet_start_time`，aoi_tt=`start_time`）；file_stat 门控 + st.cache_data；**只读，绝不走 prepare_*（会写盘）**。
  - spc CPK 行：`spc_cpk_cpm_decoration.xlsx` 是修饰台账、不含判据，不能直接读——必须经 `SpcReportService.fetch_spc_report_payload`（缓存 4h）拿 period_capability_df，再过 `build_weekly_cpk_alerts`（app/sections/inline_domain/spc/spc_dashboard.py:143）。
  - yield lot 超规：`get_lot_defect_rates` + `load_static_warning_lines` + `compute_lot_oos_records`（app/components/alert_center.py:14，近 30 天口径），适配层按 `warehousing_time` 过滤到上一 ISO 周（呈现层过滤，不改算法）。
  - yield 良率波动：`get_mwd_trend_data` + `get_code_level_trend_data` + `AlertService.get_dashboard_alert_records`，记录非空即红（period 制，不做 ISO 周过滤）。注意 `get_mwd_trend_data` 会回写良损修饰表（yield_service.py:185-217）——矩阵侧必须只读，给服务加可选只读开关（默认保持现行为，矩阵传只读）。
  - qtime：按 shop（ARRAY/OLED/TP）各调一次 `get_cached_monitoring`（step_descriptions=该 shop 全部站点，products=() 全产品），union 后按 prodcode 分列，适配层按 `timekey` 过滤上一 ISO 周。空 step_descriptions 会 ValidationError（dtos.py:34 min_length=1），必须显式取全站点。
- **单元格语义**：统一 = "上一 ISO 周有预警"；yield 良率波动为例外（period 制口径，探测记录非空即红），图例中注明各行时间口径。
- **降级**：每单元格 try/except → error 态（⬜ + message）；工作簿不存在 → no_data（⚪）；读取失败（SheetOosDecorationReadError 等）→ error。

### Phase 3 落地补充决定（2026-09-02 实施时锁定）

- **yield 只读开关实现**：`sync_modifier_table` 增加 `read_only=False`（内存合并当月良损/回退口径完全一致，仅跳过工作簿写回与 `.sig.json` 签名写入），经 `get_modifier_context` / `get_code_level_trend_data` / `get_mwd_trend_data` / `get_lot_defect_rates` 逐层穿透；默认 False，页面现行为不变，矩阵传 True。
- **load_sheet_oos_decoration 缺失行为已确认**：工作簿或产品 sheet 不存在返回空 DataFrame（不抛异常）→ 映射 no_data；工作簿存在但不可读（openpyxl+COM 均失败）抛 `SheetOosDecorationReadError` → error；已读入但缺时间列 → no_data（message 注明缺列）。
- **qtime 产品无数据判定**：details 与 alerts 中均无该 prodcode → no_data；有数据但上一 ISO 周无 flag=False 记录 → ok。
- **签名采集失败**：逐分量 try/except 降级为确定性 `"unavailable"` 标记（不产生每次 rerun 都变化的脏键；对应域数据加载大概率同样失败并落入 error 单元格）。
- **模块拆分**：纯计算（注册表/evaluator/payload/签名摘要）在 `alert_matrix_service.py`（不 import streamlit）；缓存入口 + 生产装配在 `alert_matrix_cache.py`（st.cache_data，ADR-0001 下划线参数）。

## Phases 与 Checklist

### Phase 1 — qtime 缓存层补齐（PRD §4.3）

- [x] 1.1 qtime 查询入口增加 `@st.cache_data` 缓存包装：键 = (products, shop, step_options, 时间窗, 修饰决策签名)，TTL 12h；决策签名按 `qtime_oos_decoration.xlsx` (mtime_ns, size) 门控。验证：单元测试 —— 相同键命中（mock 库查询计数不增）、任一维度变化触发重算。【证据：`tests/unit/indicator_domain/application/qtime/test_cached_monitoring.py` 10 passed】
- [x] 1.2 qtime 页既有交互无回归（查询按钮、session_state 签名、预警中心）。验证：既有 qtime 相关测试通过 + E2E 回归脚本。【证据：`tests/unit/indicator_domain` + qtime section/page 测试 67 passed；E2E 在 Phase 6 执行】

### Phase 2 — 缓存容量调整（PRD §4.4）

- [x] 2.1 `src/inline_domain/application/decorated_features.py:79` `max_entries` 12 → 32。验证：单元测试 —— 21 个不同 (prod, scope) 键写入后最早条目不淘汰。【证据：`tests/unit/inline_domain/application/shared/test_decorated_features.py::test_max_entries_keeps_all_matrix_product_scope_entries`，RED(assert 22==21)→GREEN；实际文件为 `src/inline_domain/application/shared/decorated_features.py:79`】

### Phase 3 — 矩阵数据服务（PRD §4.1，tracer bullet）

- [x] 3.1 新增矩阵 payload 构建服务（纯计算、无 st.*）：`build_alert_matrix_payload(reference_date) -> {products, rows, cells, signature}`；四态契约 `ok|alert|no_data|error`。验证：单元测试 —— schema、四态映射全分支、空数据。【证据：`app/sections/inline_domain/monitor/alert_matrix_service.py`；`tests/unit/app/sections/monitor/test_alert_matrix_service.py` schema/注册表/四态用例通过】
- [x] 3.2 各域适配器（复用既有判据，不新增算法）：aoi_rs/aoi_tt/spc/ctq sheet OOS（`build_sheet_oos_alerts`）、spc CPK、yield lot 超规 + 良率波动结构化记录、qtime `build_qtime_alerts`；任一域/产品异常 → `error` 态降级。验证：单元测试 —— 单域异常仅影响对应单元格；单产品缺失整列降级。【证据：同测试文件 28 项（含 aoi_tt start_time、qtime timekey 上周/本周/上上周过滤）；yield 只读开关 `read_only` 穿透 yield_service→sync_modifier_table（`tests/unit/test_yield_service_readonly.py` 3 项 + `tests/unit/test_modifier_table.py::TestSyncModifierTableReadOnly` 2 项）】
- [x] 3.3 矩阵缓存键组装集中一处：逐产品 revision + 逐 (prod, scope) 决策签名 + 上一 ISO 周时间窗。验证：单元测试 —— 签名任一维度变化 → 键变化。【证据：`alert_matrix_cache.build_default_signature_components` + `get_cached_alert_matrix`（周归一 + 签名敏感性 + TTL=12h 用例通过）；`config/global.yaml` 新增 `service_cache.ttl_hours.alert_matrix_payload: 12`】
- [x] 3.4 集成测试：多产品模拟数据下矩阵口径与单产品页预警结果一致。验证：`pytest tests/integration` 对应用例。【证据：`tests/integration/test_alert_matrix_integration.py` 2 项通过（含只读字节校验）；`tests/integration` 全部 23 项通过】

### Phase 4 — 矩阵 UI 与页面接线（PRD §4.2）

- [x] 4.1 矩阵 UI 组件（四态色点、行分组、⬜ tooltip），落 `app/sections/inline_domain/monitor/alert_matrix.py`（与矩阵服务/缓存同目录）。交互方式：st.button 网格（原生 help tooltip，AppTest/Playwright 可稳定点击）。验证：AppTest —— 标题/图例/模块分组/四态字符/⬜ tooltip。【证据：`tests/unit/app/sections/monitor/test_alert_matrix_ui.py::test_matrix_renders_title_legend_groups_and_four_states` + `test_error_cell_tooltip_carries_message` 通过】
- [x] 4.2 `app/pages/自动预警看板.py` 页首接入矩阵（页头之后、控制台之前），既有汇总图/Top10/明细表保留下方不动；payload 经 `get_cached_alert_matrix` 集中计算后一次性渲染；矩阵整体失败降级 info 不阻断页面。验证：AppTest 降级用例 + 页面 py_compile。【证据：`test_board_degrades_to_info_when_payload_fails` 通过】
- [x] 4.3 矩阵页不参与 Header 单产品筛选（沿用预警看板先例，未改 SessionManager/Header）。验证：E2E 留 Phase 6；代码层面矩阵接线不读取 Header 产品状态。

### Phase 5 — 点击详情懒加载（PRD §4.2，D1）

- [x] 5.1 点击红点单元格 → 矩阵下方展开该 (产品, 行) 详情：预警明细表 + 图像，复用各域既有渲染函数（spc/ctq/aoi 指标 sections、spc CPK payload 模式、yield `render_alert_code_expanders`、qtime 预警中心 + `build_qtime_figure`）；数据包经 `get_cached_matrix_detail`（st.cache_data，键 = detail_key + 参考周 + 矩阵签名，ADR-0001 原生载荷）；SPC/CTQ/Yield/Q-Time 图像走 `RenderGate.collect_memoized`（签名含产品 revision + 预警内容指纹 + 矩阵 generated_at），chart key 前缀 `matrix_detail`。验证：AppTest —— 点击后明细出现、不点击不产生详情计算、再开命中缓存、切换单元格跟随更新、加载失败降级。【证据：`tests/unit/app/sections/monitor/test_alert_matrix_ui.py` 15 项通过】
- [x] 5.2 点击 🟢/⚪/⬜ 单元格只显示说明文案（达标/无数据/失败原因），不产生详情计算。验证：AppTest。【证据：`test_click_ok_cell_shows_explanation_without_loading`、`test_click_error_cell_shows_message_without_loading` 通过】

### Phase 6 — 验收与回归

- [x] 6.1 全部单元测试通过（`pytest tests/unit`），既有 555+ 项无新增失败。【证据：2026-09-03 `pytest tests/unit` 855 passed, 5 failed —— 5 项均为基线预存在失败（test_hot_reload 页头、aoi_rs portal、yield_global_data_policy ×2、yield_dashboard_plotly_keys），逐项核对无新增】
- [x] 6.2 集成测试通过（`pytest tests/integration`）。【证据：2026-09-03 `pytest tests/integration` 23 passed】
- [x] 6.3 E2E 全部通过（Playwright，产物落 `output/test-results/`）：矩阵四态、点击懒加载、刷新缓存重建、qtime 页回归。【证据：新增 `tests/e2e/fixtures/alert_matrix_app.py`（假数据隔离 harness，默认模式四态 payload + ?mode=cache 真实 st.cache_data 重建验证）与 `tests/e2e/alert_matrix_board.js`、`tests/e2e/qtime_report.js`，playwright-cli run-code 全部通过；截图 `output/test-results/alert_matrix_board_e2e.png`、`alert_matrix_detail_qtime_e2e.png`、`alert_matrix_error_cell_e2e.png`、`alert_matrix_cache_rebuild_e2e.png`、`qtime_report_e2e.png`、`qtime_report_tp_error_e2e.png`】
- [x] 6.4 手工抽查：矩阵单元格状态与同期单产品页预警结果一致（至少 2 产品 × 3 行）。【用户决定跳过真实数据浏览器抽查（2026-09-03）；以集成测试口径一致性证据替代：`tests/integration/test_alert_matrix_integration.py` 2 项通过（多产品模拟数据下矩阵口径与单产品页预警结果一致，含只读字节校验）】
- [x] 6.5 issue 验收标准全部勾选并附证据；更新 issue Comments。【证据：`.scratch/alert-matrix-board/issues/01-alert-matrix-board.md` 9 条验收标准全部勾选附证据，Comments 含 2026-09-03 交付记录；主 agent 复验 `pytest tests/unit` 855 passed / 5 failed（均为基线预存在）、`tests/integration` 23 passed（2026-09-03）】

### Phase 6 顺带修复（2026-09-03）

- 验收过程中发现：`app/pages/自动预警看板.py` 缺少其它页面都有的 sys.path bootstrap（pyproject 锚定 + root/src 注入），矩阵引入 `yield_domain` 顶层导入后，在"新进程直接访问该页 URL（未先过 Home）"场景下报 `ModuleNotFoundError: No module named 'yield_domain'`（既有 yield 页面各有 bootstrap 所以从未暴露）。已按 `AOI_RS监控报表.py:1-16` 同款惯例补齐页面头部 bootstrap；`py_compile` 通过，`tests/unit/app` 286 passed / 2 failed（均为基线预存在）。

## 范围守卫（Out of scope，不得纳入）

- 不修改任何预警判定算法/阈值；不改 Header 筛选与 SessionManager；不新增推送/通知；不重排既有汇总图/Top10；不为 qtime 建本地快照。

## 风险与对策（详见 PRD §6）

- 冷启动慢 → L1/L2 缓存 + RenderGate 单 spinner；缓存淘汰 → max_entries=32；qtime 打库 → 缓存层 + error 降级；单产品失败 → 单元格级灰点降级。

## Approval

- 2026-09-02 计划已呈现需求方并获明确批准（批准范围：六个 Phase、接口变化、测试重点），进入模块 3（TDD）。
