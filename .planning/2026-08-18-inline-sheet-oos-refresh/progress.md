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

## 2026-08-18 — Phase 3 完成（coder 子代理，中断后 resume 完成 + 主线核验）

- core 层新增：`RefreshDecision`/`should_regenerate_detail`（missing→revision→决策→TTL4h→unchanged）、
  `get_decision_sheet_name`（<sheet>__flags）、`load_sheet_oos_decisions`（存在但读失败抛 ReadError）、
  `compute_decision_signature`（规范化+稳定排序 SHA-256，空表 "empty"）、`migrate_legacy_flags_if_needed`
  （幂等，keep=last，保留全部 flag）、`build_refresh_meta_row`/`load_refresh_meta`、
  `SheetOosDecorationWriteError`；`SheetOosDecorationResult` 增 decision_sheet/decision_df/refresh_reason。
- 持久化编排：迁移/读 __flags → merge → 判定 → `replace_workbook_sheets` 单事务；written=False 抛错。
- 兼容决策：未传 scope 时保持旧“总是持久化”语义（不维护 meta），aoi/decorated_data 零改动。
- 证据：RED ImportError ×2 → GREEN core/shared 46 passed；主线复跑 tests/unit/inline_domain 208 passed 确认。
- 注意：子代理曾中断（会话关闭），resume 后完成；resources/*.xlsx 测试污染已还原。

## 2026-08-18 — Phase 4 完成（coder 子代理 + 主线核验）

- 新增 `application/shared/decision_signature.py`：两阶段签名（file_stat 探针 + st.cache_data 缓存内容 hash，
  max_entries=64/ttl=4h，键全 str/int）；__flags 读失败上抛不降级；工作簿缺失返回 "empty"。
- 接线：`fetch_decorated_features`/`prepare_decorated_data` 增 product_revision/decision_signature（入缓存 key），
  spc/ctq/monitor service 与 SPC/CTQ/自动预警页面穿参（monitor 按 prod/scope 字典逐个取）；
  core 增 `_log_refresh_decision` 结构化日志（PRD §8 字段）；meta 只存共享产品 revision。
- 证据：RED（ImportError + 4×TypeError）→ GREEN inline_domain 220 passed；主线复跑
  tests/unit/inline_domain + tests/unit/app → 318 passed / 3 failed（均为基线失败）。
- 已知留项：monitor 下钻缓存（render_alarm_detail_tables 链路）未穿新参，门控正确但下钻缓存不随决策编辑即时失效（由 snapshot_signature 控制）——影响可接受，记录在案。

## 2026-08-18 — Phase 6 完成（coder 子代理 + 主线核验）

- `ctq_service.py:81` 补 `ttl=4*60*60`；核查发现 `monitor_service.py:297` 外层缓存同样无 ttl，一并补齐；
  spc 已有 4h ttl。aoi_tt/aoi_rs 外层缓存无 ttl 但不走共享管线，按 PRD 范围不改（记录在案）。
- 新增 `tests/unit/inline_domain/application/test_payload_cache_ttl.py`：参数化断言三处外层缓存
  `_info.ttl` 非 None 且 ≤14400s。
- 证据：RED 2 failed/1 passed → GREEN；inline_domain 223 passed；主线复跑 ttl 测试 3 passed 确认。

## 2026-08-18 — Phase 5 完成（coder 子代理 + 主线核验）

- 新增 `app/sections/spc/sheet_oos_admin.py` 纯逻辑层：build_decision_download_sheets（当前明细+决策台账，
  空表保留列头，不含 meta）、validate_decision_upload（键列/flag/重复键校验，空表合法）、
  parse_decision_upload（优先“决策台账”sheet，兼容旧单 sheet）、apply_decision_upload
  （replace_workbook_sheets 只写 __flags；签名一致 unchanged 不重写；失败 error 含“请关闭 Excel 后重试”；
  空表清空提示）。
- `render_sheet_oos_decoration_admin` 薄壳接入：成功 st.success + st.rerun（不再手动清缓存）；
  界面显示工作簿路径/产品 sheet/决策 sheet/refresh_reason。CTQ 复用同一渲染函数，自动覆盖。
- 证据：RED collection ERROR → GREEN 18 passed；主线复跑 18 passed 确认；sections 回归仅基线失败 ×1。
- 事件记录：子代理曾试探 git stash（pop 冲突中止，未产生新条目）；已核实工作区全部改动完好，
  遗留 stash@{0}（基于 13803fc）为会话前既有，未触碰。

## 2026-08-18 — Phase 7.2 全量回归

- worktree `pytest tests/unit -q`：**528 passed / 8 failed**（8 个失败与 master 基线完全一致：hot_reload、
  aoi_rs_page、spc 箱线图、code_selector_filter ×2、yield_plotly_keys、yield_global_data_policy ×2）。
- `pytest tests/integration -q`：初次 1 failed（test_spc_db：worktree 无 .env，DB 凭证缺失，环境问题）；
  `source ../vivo-project/.env` 后复跑 **9 passed**（含 test_spc_db）。非代码回归。
- 注意 .env 第 6 行 DB_CLIENT_ENCODING 有空格导致 source 告警，但关键 DB_* 变量加载成功。
