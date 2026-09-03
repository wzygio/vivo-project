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

## Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| 基线测试与 coder 并发运行导致基线不准 | 1 | Phase 1+2 完成后重跑全量，逐项核实失败为既有问题 |
| E2E 首跑四态计数翻倍/为零 | 2 | help tooltip 使每个 st.button 渲染两个 button 节点；改用 `st-key-matrix_cell_*` 容器锚点定位与计数 |
| 真实 app 新进程直访页面报 `No module named 'yield_domain'` | 1 | 定位为页面缺 sys.path bootstrap（预存在惯例缺陷，矩阵首次触发）；补齐 bootstrap，非算法改动 |
