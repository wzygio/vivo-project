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

### Phase 7 — qtime 全产品公共管线 + 页面全产品化 + 矩阵按钮门控（2026-09-03）

- [x] 7.1 qtime 公共管线：`cached_monitoring.py` 新增 `get_cached_shop_monitoring`（厂别全部站点 + products=()，复用 `_cached_monitoring` 同一组缓存键维度）与 `get_qtime_cached_funcs()` 清理清单；矩阵 `load_all_product_qtime_monitoring` 统一走该入口（厂别无站点 ValidationError → 维持 skip）。验证：单元测试 —— 矩阵式调用与页面式调用共享缓存条目（fetch 计数不增）、as_of=None 归一、无站点上抛、取站点失败上抛、清理清单可 clear。【证据：`tests/unit/indicator_domain/application/qtime/test_cached_monitoring.py` 新增 6 项，全文件 16 passed】
- [x] 7.2 Q-Time 页面全产品化：`render_page_header` 新增 `show_product_filter=True` 参数（其余 12 页默认行为不变）；`Q_Time监控报表.py` 传 `show_product_filter=False` + `cached_funcs=get_qtime_cached_funcs()`；`dashboard.py` 筛选区改为 产品 multiselect（默认全选）→ 厂别 → 站点 → 查询，`_run_query` 走 `get_cached_shop_monitoring` 后按产品/站点内存过滤 details/alerts/decoration 三帧（`dataclasses.replace`），session 签名含产品选择，空产品禁用查询。验证：AppTest —— 产品框默认全选且顺序为产品→厂别→站点、单产品内存过滤（三帧 prodcode 唯一 + 预警数收缩）、空产品禁用、页头无产品筛选框（stub 断言）。【证据：`test_qtime_dashboard.py` 10 passed（新增 3 项 + 更新 2 项）；`test_qtime_page.py` 2 passed；`test_page_header_product_cache.py` 新增 4 项 passed】
- [x] 7.3 矩阵渲染按钮门控：`自动预警看板.py` 页首默认显示说明文案 +「🚦 加载预警矩阵」主按钮（on_click 置 `alert_matrix_board_loaded`），已加载后渲染矩阵 +「收起预警矩阵」；`perform_hard_reset` 阶段 4 与 `_refresh_data_callback` 清除该 session key；文案明确按钮仅读取缓存 payload。验证：单元测试 —— 刷新缓存/刷新数据后已加载状态清除。【证据：`test_hard_reset_clears_alert_matrix_loaded_state`、`test_refresh_data_clears_alert_matrix_loaded_state` passed】
- [x] 7.4 E2E 回归：fixture `qtime_app.py` 改为多产品假数据（M626+M678，products=() 语义）。【证据：`tests/e2e/qtime_report.js` {"ok":true}、`tests/e2e/alert_matrix_board.js` counts 🔴×4 ⚪×1 ⬜×1 🟢×10 + 缓存重建令牌 10:31:15→10:31:19；截图 `output/test-results/qtime_report_e2e.png`、`qtime_report_tp_error_e2e.png`、`alert_matrix_board_e2e.png`、`alert_matrix_detail_qtime_e2e.png`、`alert_matrix_cache_rebuild_e2e.png`（2026-09-03 10:30-10:31）】

### Phase 8 — 页头产品筛选移除 + 超规片预警查询门控 + 矩阵筛选条（2026-09-03 需求轮次）

- [x] 8.1 页头产品筛选移除：`自动预警看板.py` 传 `show_product_filter=False`（参数为 Phase 7.2 既有能力，本页为全产品视图）。验证：页面组合层测试断言 header kwargs。【证据：`tests/unit/app/pages/test_auto_warning_page.py::test_page_hides_header_product_filter_and_gates_data_loading` passed】
- [x] 8.2 「超规片自动预警」查询门控：`monitor_dashboard.py` 新增 `render_monitor_query_gate`（筛选签名 = 监控类型+产品+厂别排序元组，session key `monitor_query_signature`，on_click 提交；签名过期回到未提交态并提示）；`自动预警看板.py` 签名预算 + `get_monitor_dashboard_data` + 汇总图 + Top10 + admin 明细表全部移入门控块；`perform_hard_reset` 阶段 4 与 `_refresh_data_callback` 清除该 key。验证：AppTest 门控 harness（未点击不加载/点击加载/rerun 保持/三维度签名变更回到未提交态）+ 页面组合层 monkeypatch 计数 + 页头清除用例。【证据：`tests/unit/app/sections/monitor/test_monitor_query_gate.py` 6 项 + fixture `fixtures/monitor_query_gate_app.py`；`test_auto_warning_page.py` 2 项（未点击 load_calls==0 且 decision_calls==0；点击后 14 次签名预算 + 1 次加载）；`test_page_header_product_cache.py::test_hard_reset_clears_monitor_query_state` / `test_refresh_data_clears_monitor_query_state` passed】
- [x] 8.3 矩阵区筛选条（与下方控制台同观感，key 前缀 `alert_matrix_`）：监控类型 selectbox（ALL/SPC/CTQ/AOI(含两行)/Yield/Q-Time，客户端切行）、产品型号 multiselect（默认全选，客户端切列）、厂别 multiselect（ARRAY/OLED/TP 默认全选，单元格状态 = 选中厂别 ∩ `alert_factories` 非空则 🔴，纯客户端切片）。验证：AppTest 筛选条默认值/切列/切行/切单元格/空产品提示。【证据：`test_alert_matrix_ui.py` 新增 6 项 passed；fixture `alert_matrix_app.py` 单元格补 `alert_factories`、行补 `factory_filter_supported`】
- [x] 8.4 单元格 payload 扩展 `alert_factories`：sheet OOS 四行取记录 `factory` 列（排序去重、大小写归一）；spc CPK 行取 `厂别` 列；qtime 行按 shop 打标（`load_all_product_qtime_monitoring` union 时 `assign(shop=shop)` 返回新帧，不污染共享 L2 缓存对象）；yield 两行记录结构无厂别列（`compute_lot_oos_records` / `get_dashboard_alert_records` 已核实）→ 行声明 `supports_factory_filter=False`，厂别筛选保持原状态并在图例注明。验证：服务层提取用例 + shop 打标用例 + 行标记用例。【证据：`test_alert_matrix_service.py` 新增 6 项 passed（含 tp/TP 大小写归一、已修饰/本周记录不计入、qtime 本周 ARRAY 不计入、yield 行回退）；矩阵服务既有 36 项零改动全部通过】
- [x] 8.5 回归：`pytest tests/unit tests/integration -q --no-header -p no:cacheprovider`。【证据：913 passed / 5 failed（51.61s）——5 项均为基线预存在失败（test_hot_reload 页头仅列专项资料两页、aoi_rs portal、yield_global_data_policy ×2、yield_dashboard_plotly_keys），逐项核对无新增；基线 891 → 913，新增 22 项全绿】
- 本页面改动按需求方禁令只用单元测试 + AppTest 验证，未跑任何触及自动预警看板的 E2E（含 `tests/e2e/alert_matrix_board.js` fixture harness）。

### Phase 9 — UI 优化：去除 st.info 提醒条 + 模块化标题/Expander 收纳（2026-09-03 需求轮次）

- [x] 9.1 渲染面禁用 st.info：门控提示类（矩阵「加载」说明、查询门控两条签名文案）直接删除，门控语义由按钮承担；空态/无数据类一律改 `st.caption` 灰字（矩阵空数据/空产品/空行、详情区无数据/不支持/状态说明、admin 明细表三处空态）；错误类保留并升级——矩阵整板加载失败由 info 升为 `st.warning`（错误必须可见），其余 st.error/st.warning 不动。共享排查：grep 确认 `monitor_dashboard.py` 仅被 `自动预警看板.py` 引用（无其他页面共享），直接改行为，无需参数开关；`render_monitor_detail_section`/`show_drilldown_modal` 不在该页渲染路径（页面早已不调用），未动。【证据：`grep -rn monitor_dashboard app/` 仅页面一处 import；`test_monitor_query_gate.py::_assert_no_info` 6 项用例、`test_alert_matrix_ui.py` 默认渲染 `len(app.info)==0` 断言通过】
- [x] 9.2 模块化结构：模块一 `st.subheader("🚦 预警矩阵")` + `st.expander("产品 × 监控参数 · 上一周期预警状态", expanded=True)`（未加载仅按钮；已加载矩阵本体+收起按钮+详情区）；模块二 `st.subheader("⚠️ 超规片自动预警")` + `st.expander("筛选控制台与预警结果", expanded=True)`（控制台 + admin 修饰面板原位 + 查询按钮 + 门控内容）；subheader 与 expander 文案不重复；矩阵内部 `#### 🚦 预警矩阵` 标题移除（避免三层重复，周次信息由图例 caption 承担）。嵌套 expander（模块 expander 包详情/明细 expander）经 AppTest 探针确认 Streamlit 1.60 支持。【证据：`test_auto_warning_page.py::_assert_module_structure`（两模块 subheader + expanded=True expander + 文案不重复 + infos==[]）2 项通过；嵌套探针 AppTest `expanders: ['outer','inner']` 无异常】
- [x] 9.3 测试更新与回归：fixture/断言同步（info→caption/warning、矩阵内部标题移除）；门控行为（未点击不加载、签名变更回未提交态）不变。用户在门控块内新增的 `data_forward_signature`（`ConfigLoader.get_data_forward_policy().signature` 进入两处 snapshot_signature）原样保留。【证据：聚焦套件 92 passed；全量回归见 progress.md Session 7】

### Phase 10 — 矩阵筛选条常驻（需求方反馈修正，2026-09-03）

- [x] 10.1 筛选条移出"已加载"分支：`_render_matrix_filter_bar` 提升为公开 `render_alert_matrix_filter_bar`，由页面在模块一 Expander 内、加载/收起按钮上方常驻渲染一次（未加载时 Expander 内 = 筛选条 + 加载按钮，无其他文案）；`render_alert_matrix_section`/`render_alert_matrix_board` 新增 `filter_selection` 参数——传入时 section 不再渲染筛选条（widget key 只渲染一处，无冲突），缺省时自行渲染（测试/独立场景兼容）。选择存 session_state（`alert_matrix_` 前缀 key 不变），加载后按当前选择客户端切片，已加载后改筛选即时生效（逻辑未动）。`data_forward_signature` 用户逻辑原样保留。【证据：`test_auto_warning_page.py` 未加载态断言筛选条三 key 各渲染一次 + 加载按钮在、收起按钮不在；已加载态断言筛选条仍只一处 + board 收到 `filter_selection` 三元组（ALL/全产品/全厂别默认值）+ 收起按钮在；`test_alert_matrix_ui.py::test_section_with_external_filter_selection_skips_bar_and_slices`（外部选择下 section 不渲染筛选条 widget、AOI 切行 + M678 切列 + 厂别切片照常）】
- [x] 10.2 回归：聚焦套件 94 passed；全量见 progress.md Session 8。

### Phase 11 — 筛选与操作按钮同行布局（2026-09-03 需求轮次）

- [x] 11.1 模块一：`render_alert_matrix_filter_bar(products, *, action_renderer=None)` 新增按钮渲染回调参数——传回调时四列布局 `[1.0, 2.6, 1.6, 0.9]` + `vertical_alignment="bottom"`（产品型号最宽，按钮最右），缺省保持三列旧布局；页面把「🚦 加载预警矩阵」/「收起预警矩阵」收进 `_render_matrix_action_button` 传入，按钮从矩阵下方移入筛选行最右列，widget key 唯一性与"筛选条只渲染一处"契约不变。【证据：`test_auto_warning_page.py::_assert_row_layout`（两处 4 列布局 + bottom 对齐 + 模块一连续 widget 序列 `alert_matrix_data_type → alert_matrix_products → alert_matrix_factories → 按钮key`）】
- [x] 11.2 模块二：`render_monitor_control_panel(..., *, action_renderer=None)` 同款四列布局 `[1.0, 2.6, 1.6, 0.8]`；门控拆为 `render_monitor_query_button()`（渲染按钮并返回点击状态，key 不变）+ `render_monitor_query_gate(filter_state, *, clicked=None)`（clicked 覆盖支持按钮先于签名渲染的行内布局；on_click 回调改为返回值式提交）；控制台两个 multiselect 补显式 key（`monitor_products`/`monitor_factories`，label 不变）。【证据：fixture `monitor_query_gate_app.py` 新增 `row_layout` 模式 + `test_monitor_query_gate.py` 新增 2 项行内布局门控用例（未点击不加载/点击加载/签名过期回未提交态）；既有 6 项独立路径用例零改动通过；模块二连续 widget 序列 `spc_data_type_filter → monitor_products → monitor_factories → btn_monitor_query_submit` 断言通过】
- [x] 11.3 回归：聚焦套件 96 passed；全量 `pytest tests/unit tests/integration` 935 passed / 5 failed（50.47s），5 项均为基线预存在，无新增失败。`data_forward_signature` 用户逻辑原样保留（3 处引用核实）。【证据：progress.md Session 9】

## 范围守卫（Out of scope，不得纳入）

- 不修改任何预警判定算法/阈值；不改 Header 筛选与 SessionManager；不新增推送/通知；不重排既有汇总图/Top10；不为 qtime 建本地快照。
- **2026-09-03 需求方指令：禁止对"自动预警界面"（`app/pages/自动预警看板.py` 真实页面）进行任何 E2E 测试。** 既往矩阵 E2E 均为隔离 fixture harness（`tests/e2e/fixtures/alert_matrix_app.py`，不触真实页面/数据/工作簿）；此后连该 fixture 脚本也暂停执行，矩阵改动以单元测试 + AppTest 验证。

## 风险与对策（详见 PRD §6）

- 冷启动慢 → L1/L2 缓存 + RenderGate 单 spinner；缓存淘汰 → max_entries=32；qtime 打库 → 缓存层 + error 降级；单产品失败 → 单元格级灰点降级。

## Approval

- 2026-09-02 计划已呈现需求方并获明确批准（批准范围：六个 Phase、接口变化、测试重点），进入模块 3（TDD）。
