# Progress：Inline Sheet OOS 修饰刷新与决策持久化

## 2026-08-18 — Session 1

- 模块 1（需求制定）完成：
  - Issue：`.scratch/inline-sheet-oos-refresh/issues/01-sheet-oos-decoration-refresh-decision-persistence.md`
  - Category: enhancement；Status: ready-for-agent（Agent Brief 已填充，triage 记录已追加）。
- 模块 2（计划制定）进行中：
  - 已核实代码现状并记录于 findings.md（page_header / decorated_features / decorated_data /
    sheet_oos_decoration / excel_tools / ctq_service / 页面签名 / spc_dashboard 管理区）。
  - task_plan.md 已创建（Phase 0–8，含 TDD checklist）。
  - 待用户批准计划后进入模块 3。

## 2026-08-18 — Phase 0 基线

- worktree：`d:/wzy/Python/vivo-project-inline-sheet-oos-refresh`，分支 `feat/inline-sheet-oos-refresh`（自 master e1f0af4）。
- import 验证：worktree cwd + 主仓 `.venv` python，`src.inline_domain...` 解析到 worktree（pythonpath=["src","."]）。
- 基线 `pytest tests/unit -q`：**451 passed / 8 failed / 20 warnings（24s）**。既有失败基线：
  test_hot_reload::test_every_streamlit_page_uses_the_shared_page_header、
  test_aoi_rs_page::test_portal_navigation...、
  test_spc_dashboard::test_sheet_points_box_chart_draws_upper_lines_when_lower_specs_are_null、
  test_code_selector_filter ×2、test_yield_dashboard_plotly_keys、test_yield_global_data_policy ×2。
- Phase 1（页头契约）与 Phase 2（Excel 原子写）已启动 coder 子代理并行 TDD。

## 2026-08-18 — Phase 1 完成（coder 子代理 + 主线核验）

- `_refresh_data_callback`：全成功 → `invalidate_page_cache(product_code=scope)`（无作用域走 cached_funcs 清理）+ view_model memo 清理 + toast“✅ L1 快照与 L2 缓存已刷新。”；失败不推进 revision；无模块卸载/配置重读。按钮 help 文案同步更新。
- 测试：tests/unit/app/components/test_page_header_product_cache.py 新增 4 个用例。
- 证据：RED 2 failed/5 passed → GREEN 7 passed；主线复跑 7 passed 确认。
- 注意：全量 pytest 会改写 `resources/*.xlsx`（测试副作用，master 上不存在）；已 `git checkout -- resources/` 还原，最终提交前需再次检查。

## 2026-08-18 — Phase 2 完成（coder 子代理 + 主线核验）

- `excel_tools.py` 新增 `WorkbookWriteResult` + `replace_workbook_sheets(path, Mapping[sheet, df])`：
  进程内锁 → 同目录临时文件保存 → openpyxl 回读验证 → os.replace 原子替换；
  PermissionError/COM 失败/临时保存失败/验证失败均 written=False（error 含“请关闭 Excel 后重试”）；
  加密回退整体重写为明文并 logger.warning 明确记录。`replace_workbook_sheet` 委托保持旧兼容语义。
- 证据：RED ImportError → GREEN 16 passed（8 旧+8 新）；主线复跑 excel_tools 两文件 18 passed 确认。
- 必要测试调整：加密回退旧用例的 load_workbook patch 改为仅对原文件路径抛异常（新事务语义要求临时文件可回读）。
