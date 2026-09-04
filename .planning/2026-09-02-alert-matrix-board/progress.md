# Progress: alert-matrix-board

## 2026-09-02 — Session 1

- 完成 Task-1 可行性分析，报告写入 `docs/dev_docs/generated/others/alert-center-matrix-board-feasibility.md`。
- 输出 PRD：`docs/PRD/PRD-2026-09-02-自动预警看板矩阵化.md`。
- development-flow 模块 1（需求制定）：issue `.scratch/alert-matrix-board/issues/01-alert-matrix-board.md` 创建并 triage 至 `ready-for-agent`（Agent Brief 已补全）。
- development-flow 模块 2（计划制定）：本计划创建，用户已批准。
- 分支 `feat/alert-matrix-board` 已建（从 master，工作区用户未提交改动保留不带入提交）。
- 模块 3 Phase 1+2 完成（coder subagent，TDD）：
  - 新增 `src/indicator_domain/application/qtime/cached_monitoring.py`（`get_cached_monitoring`，键=shop+step_descriptions+products+as_of 归一+决策文件 (mtime_ns,size) 哨兵 (-1,-1)，max_entries=32，TTL 读 `qtime_monitoring: 12`）；`QTimeReportService` 新增只读 `decoration_path` 属性；`dashboard.py::_run_query` 改走缓存包装。
  - `config/global.yaml` 增加 `service_cache.ttl_hours.qtime_monitoring: 12`。
  - `decorated_features.py:79` max_entries 12→32；新增 21 条目淘汰测试（RED: assert 22==21 → GREEN）。
  - 证据：`test_cached_monitoring.py` 10 passed；qtime 相关 67 passed；`tests/unit/indicator_domain + tests/unit/inline_domain` 315 passed。
- 全量单测回归：799 passed, 5 failed —— 5 项均核实为既有失败（yield_global_data_policy ×2：config 已更新但断言旧值；yield_dashboard_plotly_keys ×1；hot_reload 页头 ×1：专项资料页；aoi_rs portal ×1），与本改动无关。

## 2026-09-03 — Session 3（Phase 6：E2E 与整体验收，coder subagent）

- 全量回归：`pytest tests/unit` 855 passed / 5 failed（5 项逐一核对均为基线预存在：test_hot_reload 页头、aoi_rs portal、yield_global_data_policy ×2、yield_dashboard_plotly_keys，无新增）；`pytest tests/integration` 23 passed。
- 新增 E2E（沿用 `tests/e2e/fixtures/qtime_app.py` 隔离 harness 风格，playwright-cli run-code 执行，全部通过）：
  - `tests/e2e/fixtures/alert_matrix_app.py`：假数据隔离 harness。默认模式直接构造 2 产品 × 8 行四态 payload（🔴×4 ⚪×1 ⬜×1 🟢×10）+ 假 qtime 详情 loader；`?mode=cache` 走真实 `_cached_alert_matrix_payload`（st.cache_data）+ 假 context，页面展示构建令牌并提供「刷新缓存并重建矩阵」按钮。
  - `tests/e2e/alert_matrix_board.js`：渲染存在性（标题/图例/8 行标签/产品列头）、四态数量断言、点击 🔴（Q-Time×M678）懒加载详情（明细文案 + stPlotlyChart 容器）、点击 🟢/⚪/⬜ 说明文案、普通 rerun 缓存命中（令牌不变）+ 刷新后重建（令牌 08:56:27→08:56:41）。定位锚点：单元格容器 `st-key-matrix_cell_<row>_<prod>` class（help tooltip 会把 button 渲染两份，不能按 button 文本计数）。
  - `tests/e2e/qtime_report.js`：qtime 页回归（fixture 8513）——初始门控提示、查询交互、预警中心 + 图像渲染、重复查询稳定、TP 厂别数据访问失败降级文案。
  - 产物：`output/test-results/alert_matrix_board_e2e.png`、`alert_matrix_detail_qtime_e2e.png`、`alert_matrix_error_cell_e2e.png`、`alert_matrix_cache_rebuild_e2e.png`、`qtime_report_e2e.png`、`qtime_report_tp_error_e2e.png`。
- 顺带发现并修复：`app/pages/自动预警看板.py` 缺 sys.path bootstrap，矩阵引入 `yield_domain` 顶层导入后新进程直接访问该页报 ModuleNotFoundError；已按其它页面同款惯例补齐（py_compile 通过，`tests/unit/app` 286 passed / 2 基线预存在 failed）。
- 6.4 真实数据手工抽查：用户决定跳过（2026-09-03），以集成测试口径一致性证据替代；6.5 留主 agent 统一核验。
- 环境备注：验收期间将 8503 真实 app 重启为当前分支代码（原进程 runOnSave=false 不会加载新代码），进程保持运行。

## 2026-09-03 — Session 4（合并 + 模块 4 项目沉淀，主 agent）

- 主 agent 复验：`pytest tests/unit` 855 passed / 5 failed（均为基线预存在）、`tests/integration` 23 passed；6.5 勾选。
- 需求方确认合并：工作区清理（仅暂存本任务文件；`tests/e2e/fixtures/qtime_app.py` 用 `git add -p` 只暂存本任务 2 个 hunk，用户路径修正 hunk 经单文件 stash 保留）→ feat 分支提交 `d8141a3` → checkout master → merge（fast-forward，d8141a3）→ stash pop 恢复用户改动，全部保留。
- 合并后 master 复验：unit 855 passed / 5 failed（基线预存在）、integration 23 passed。
- 8503 端口 Streamlit 进程按需求方决定保留运行（当前分支代码）。
- 模块 4：ADR `docs/ADR/0022-alert-matrix-board-and-qtime-cache.md` 创建（编号接续 0021，遵循 docs/ADR 模板）。

## 2026-09-03 — Session 5（Phase 7：qtime 全产品公共管线 + 页面全产品化 + 矩阵按钮门控，coder subagent）

- TDD：先写失败测试（9 failed + 1 collection error），再实现转绿。
- 改动 1：`src/indicator_domain/application/qtime/cached_monitoring.py` 新增 `get_cached_shop_monitoring`（内部 `get_filter_options(shop)` 取全站点后以 products=() 调既有 `_cached_monitoring`，不新增缓存键维度）与 `get_qtime_cached_funcs()`；矩阵 `load_all_product_qtime_monitoring` 统一入口（ValidationError → 维持厂别 skip）。
- 改动 2：`page_header.render_page_header` 新增 `show_product_filter=True`；`Q_Time监控报表.py` 传 `show_product_filter=False` + qtime 缓存清理清单；`dashboard.py` 筛选区四列（产品 multiselect 默认全选 → 厂别 → 站点 → 查询），`_run_query` 厂别全量取数后内存过滤三帧（`dataclasses.replace`，decisions 台账不过滤），签名含产品选择，空产品禁用查询；`render_qtime_dashboard(service)` 不再消费 `selected_product`。
- 改动 3：`自动预警看板.py` 页首矩阵改按钮门控（session key `alert_matrix_board_loaded`，on_click 置位/收起）；`perform_hard_reset` 阶段 4 与 `_refresh_data_callback` 清除该 key。
- 测试：`test_cached_monitoring.py` +6 项（共享缓存条目 fetch 计数、无站点上抛、取站点失败上抛、清理清单）；`test_qtime_dashboard.py` +3 项更新 2 项（产品框默认全选/顺序/内存过滤/空产品禁用；全选时预警数 1→2 因 fixture 双产品）；`test_qtime_page.py` 更新组合层断言；`test_page_header_product_cache.py` +4 项（show_product_filter 开/关、硬重置与刷新数据清矩阵已加载状态）。
- E2E fixture `qtime_app.py`：接受 products==() 并按调用方过滤语义返回（FIXTURE_PRODUCTS = M626/M678 双产品假数据），用户 decoration_path hunk 未动。
- E2E：`qtime_report.js` {"ok":true}；`alert_matrix_board.js` 四态计数 🔴×4 ⚪×1 ⬜×1 🟢×10 + 刷新重建令牌变化；截图落 `output/test-results/`（见 task_plan Phase 7.4）。
- 注意：组合跑批时 `test_ijp_dashboard_gates_results_until_the_user_queries` 曾出现一次偶发失败，同命令立即重跑全绿，疑似 AppTest 顺序/时序 flake，回归时持续关注。

## Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| 基线测试与 coder 并发运行导致基线不准 | 1 | Phase 1+2 完成后重跑全量，逐项核实失败为既有问题 |
| E2E 首跑四态计数翻倍/为零 | 2 | help tooltip 使每个 st.button 渲染两个 button 节点；改用 `st-key-matrix_cell_*` 容器锚点定位与计数 |
| 真实 app 新进程直访页面报 `No module named 'yield_domain'` | 1 | 定位为页面缺 sys.path bootstrap（预存在惯例缺陷，矩阵首次触发）；补齐 bootstrap，非算法改动 |
| Edit 工具无法表达混合行尾文件的 CR 字节（两次 old_string not found） | 2 | 改用 python 字节级替换；其中一次 `open(path,"wb")` 在参数求值前截断了 test_qtime_dashboard.py，用 `git show HEAD:` 恢复后重放编辑（内容经 git diff 校验一致） |

## 2026-09-03 — Session 6（Phase 8：页头产品筛选移除 + 查询门控 + 矩阵筛选条，coder subagent）

- TDD：先写失败测试（35 failed：服务 6 + UI 6+既有 13（fixture 先引用新行字段）+ 门控 6 + 页面组合 2 + 页头 2），再实现转绿（73 passed）。
- 改动 1：`app/pages/自动预警看板.py` 传 `show_product_filter=False`；签名预算（product_revisions/decision_signatures）与数据加载、汇总图、Top10、admin 明细表整体移入 `if render_monitor_query_gate(filter_state):` 块（bare 模式 st.stop() 不中断脚本，故用 if 块而非 st.stop 早退）。
- 改动 2：`monitor_dashboard.py` 新增 `MONITOR_QUERY_SIGNATURE_KEY` / `monitor_filter_signature` / `render_monitor_query_gate`（on_click 提交签名；未提交/签名过期分别给出说明文案，仿 Q-Time 页签名过期模式）。
- 改动 3：`page_header.py` 硬重置阶段 4 与 `_refresh_data_callback` 清除 `monitor_query_signature`（仿 `alert_matrix_board_loaded` 先例）。
- 改动 4：矩阵筛选条落 `alert_matrix.py::render_alert_matrix_section`（监控类型切行/产品切列/厂别切单元格状态，全客户端切片；key 前缀 `alert_matrix_`；空产品/空行给 info；图例注明不支持厂别细分的行）。
- 改动 5：`alert_matrix_service.py` 单元格 payload 扩展 `alert_factories`（`_extract_factories` 排序去重大小写归一）：sheet OOS 四行取 `factory` 列、spc CPK 行取 `厂别` 列、qtime 行取 `shop` 打标列；`AlertMatrixRow` 新增 `supports_factory_filter`（yield 两行 False，记录结构核实无厂别列），payload rows 带出 `factory_filter_supported`。
- 改动 6：`alert_matrix_cache.py::load_all_product_qtime_monitoring` union 时 `alerts.assign(shop=shop)`（新帧，不污染与 Q-Time 页共享的 L2 缓存对象）；details 帧不打标（仅 alerts 参与厂别提取）。
- 测试：`test_alert_matrix_service.py` +6；`test_alert_matrix_ui.py` +6（fixture `alert_matrix_app.py` 同步扩展，既有 15 项 UI 用例零改动转绿）；新增 `test_monitor_query_gate.py` 6 项 + fixture `monitor_query_gate_app.py`；新增 `tests/unit/app/pages/test_auto_warning_page.py` 2 项（runpy 组合层，monkeypatch 计数）；`test_page_header_product_cache.py` +2（CRLF 文件，按字节保行尾追加）。
- 环境备注：monitor_dashboard 在 bare/收集阶段无法真实注册 streamlit_echarts/st_aggrid，测试沿用 `test_monitor_dashboard_type_rollup.py` 的 sys.modules stub 惯例。
- 未跑任何触及自动预警看板的 E2E（需求方禁令）。

### Session 6 补记（中断后复核）

- 配额中断后复核：`git status` 确认 Phase 8 全部改动与测试、计划文档均已落盘，无半成品状态。
- 复跑证据：聚焦套件 `tests/unit/app/sections/monitor + test_auto_warning_page + test_page_header_product_cache` 91 passed；全量 `pytest tests/unit tests/integration` 912 passed / 6 failed —— 其中 5 项为基线预存在（hot_reload 页头仅列专项资料两页、aoi_rs portal、yield_global_data_policy ×2、yield_dashboard_plotly_keys），另 1 项 `test_ijp_dashboard_gates_results_until_the_user_queries` 为 Session 5 已记录的 AppTest 顺序/时序 flake（单跑 7 passed、与本轮套件组合跑 83 passed，本轮未触及 ijp 任何代码与其 session key）。

## 2026-09-03 — Session 7（Phase 9：UI 优化——去 st.info + 模块化 Expander 收纳，coder subagent）

- TDD：先更新测试（9 failed：门控 4 + 矩阵 UI 3 + 页面组合 2），再实现转绿。
- 改动 1（去 info）：`monitor_dashboard.py` 查询门控两条 st.info 删除（返回值简化为 `stored == signature`）；admin 明细表三处空态 info→caption。grep 确认 monitor_dashboard 仅被自动预警看板引用，无需参数开关；`render_monitor_detail_section`/`show_drilldown_modal` 不在页面渲染路径，未动。
- 改动 2：`alert_matrix.py` 空数据/空产品/空行 info→caption；整板加载失败 info→warning（错误必须可见）；`render_alert_matrix_section` 内部 `#### 🚦 预警矩阵` 标题移除（模块 subheader 承担，周次信息图例已有）。
- 改动 3：`alert_matrix_detail.py` 六处 info→caption（CPK/yield 已无预警、qtime 无明细、no_data 消息、非法状态说明、不支持详情 ×2）；warning/error 不动。
- 改动 4（模块化）：`自动预警看板.py` 模块一 subheader「🚦 预警矩阵」+ expander「产品 × 监控参数 · 上一周期预警状态」（默认展开；未加载仅按钮，info 删除）；模块二 subheader「⚠️ 超规片自动预警」+ expander「筛选控制台与预警结果」（控制台 + admin 修饰面板原位 + 查询门控及全部数据内容）。嵌套 expander 经 AppTest 探针确认 Streamlit 1.60 支持。
- 事故记录：页面重构脚本首次切分注释块时误丢模块二前奏（available_products/控制台/admin 面板三行段），随即从同 session 的 Read 记录原样恢复并 py_compile + 页面组合测试验证；用户在门控块内新增的 `data_forward_signature` 行全程保留。
- 测试：`test_monitor_query_gate.py` 改 6 项（`_assert_no_info`，门控行为不变）；`test_alert_matrix_ui.py` 改 3 项（标题断言移除+无 info、整板降级 warning、空产品 caption）；`test_auto_warning_page.py` 加结构探针（subheader/expander/info 记录器）+ `_assert_module_structure`。
- 回归：聚焦套件（monitor sections + 页面组合 + 页头）92 passed；全量 `pytest tests/unit tests/integration` 928 passed / 5 failed（66s），5 项均为基线预存在（hot_reload 页头专项资料两页、aoi_rs portal、yield_global_data_policy ×2、yield_dashboard_plotly_keys），ijp flake 本轮未出现，无新增失败。
- 环境噪音：组合跑批时 stderr 出现一次 "Windows fatal exception: code 0x80010108"（Excel COM RPC 断连，源自既有测试 `test_qtime_loader_fetched_once_for_all_products` 读真实资源工作簿，被单元格降级捕获），exit=0 全绿，与本轮改动无关。
- 未跑任何触及自动预警看板的 E2E（禁令）；`tests/e2e/alert_matrix_board.js` 等 E2E 断言未同步更新（已停跑，记录在案）。

## 2026-09-03 — Session 8（Phase 10：矩阵筛选条常驻修正，coder subagent）

- 需求方反馈：矩阵筛选条只在加载后渲染，期望与下方模块一致——常驻显示、未加载即可先选条件。
- TDD：先写失败测试（3 failed：页面未加载态筛选条断言、已加载态透传断言、section 外部选择模式），再实现转绿。
- 改动 1：`alert_matrix.py` `_render_matrix_filter_bar` 提升为公开 `render_alert_matrix_filter_bar`；`render_alert_matrix_section(payload, *, filter_selection=None)` 与 `render_alert_matrix_board(..., filter_selection=None)` 新增透传参数——传入时 section 跳过筛选条渲染（widget key 只一处），缺省保持原行为（fixture/独立场景兼容，既有切片测试零改动）。
- 改动 2：`自动预警看板.py` 模块一 Expander 内常驻渲染筛选条（`SessionManager.AVAILABLE_PRODUCTS` 为产品选项，与 payload 产品同源 7 个），位于加载/收起按钮上方；已加载分支把选择三元组透传给 board。用户 `data_forward_signature` 逻辑原样保留（3 处引用 grep 核实）。
- 测试：`test_auto_warning_page.py` 重构 `_stub_page_dependencies`（按钮/selectbox/multiselect 记录器 + `clicked_keys` 参数化点击模拟 + board 记录器），新增已加载态用例；`test_alert_matrix_ui.py` 新增外部选择用例；fixture `alert_matrix_app.py` 新增 `external_selection` 模式。
- 回归：聚焦套件 94 passed；全量 `pytest tests/unit tests/integration` 933 passed / 5 failed（48.32s），5 项均为基线预存在，无新增失败，ijp flake 本轮未出现。
- 未跑任何触及自动预警看板的 E2E（禁令）。

## 2026-09-03 — Session 9（Phase 11：筛选与操作按钮同行布局，coder subagent）

- 需求：操作按钮与筛选条件同行最右列（参照 Q-Time 页 st.columns + vertical_alignment="bottom" 布局）。
- TDD：先写失败测试（10 failed：gate 8 项因 fixture 引用新符号收集失败 + 页面 2 项行布局断言），实现后转绿。
- 改动 1：`alert_matrix.py::render_alert_matrix_filter_bar` 新增 `action_renderer` 参数（四列 `[1.0, 2.6, 1.6, 0.9]` + bottom 对齐，缺省三列旧布局）；页面新增 `_render_matrix_action_button`（按 loaded 状态渲染加载/收起按钮），按钮移入筛选行最右列。
- 改动 2：`monitor_dashboard.py::render_monitor_control_panel` 新增 `action_renderer`（四列 `[1.0, 2.6, 1.6, 0.8]`）；两个 multiselect 补显式 key（`monitor_products`/`monitor_factories`）；门控拆分——新增 `render_monitor_query_button()`（返回点击状态），`render_monitor_query_gate(filter_state, *, clicked=None)` 支持 clicked 覆盖（解决按钮在行内先渲染、签名在 widget 读取后计算的顺序问题），提交方式从 on_click 回调改为返回值式（语义等价，AppTest click 兼容）；`_submit_monitor_query` 删除。
- 改动 3：页面模块二用 `query_click_box` 捕获行内按钮点击状态传回门控；`data_forward_signature` 用户逻辑原样保留（3 处引用 grep 核实）。
- 测试：fixture `monitor_query_gate_app.py` 新增 `row_layout` 模式；`test_monitor_query_gate.py` +2（行内布局门控）；`test_auto_warning_page.py` 结构探针新增 columns/widget_events 记录器 + `_assert_row_layout`（两处 4 列 bottom 对齐 + 两模块连续 widget 序列断言）。
- 回归：聚焦套件 96 passed；全量 935 passed / 5 failed（50.47s），均为基线预存在，无新增失败，ijp flake 未出现。
- 未跑任何触及自动预警看板的 E2E（禁令）。
