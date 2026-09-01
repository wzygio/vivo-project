# Progress：decoration-unify

## 2026-08-14 — 模块 1（需求制定）完成

- PRD：`.scratch/decoration-unify/PRD.md`（用户已批准方案 D1–D5）
- Issue：`.scratch/decoration-unify/issues/01-unify-decoration-into-shared.md`
  状态 `needs-triage` → `ready-for-agent`（Agent Brief 已填，triage 记录已追加）
- 前置工作（本日前已完成）：CPK 单轨改造 + 测试更新（409 passed/4 failed 基线）

## 2026-08-14 — 模块 2（计划制定）

- 计划目录 `.planning/2026-08-14-decoration-unify/` 建立（task_plan/findings/progress）
- D6 核实：aoi_tt/aoi_rs 明细均含 sheet_id/lot_id，键列定案（见 findings.md）
- 用户批准方式：对话中明确"跳过审核步骤，直接按照默认 plan 完成开发"

## 2026-08-14 — 模块 3 · Phase 0 安全网

- [x] 0.1 基线：`.venv/Scripts/python -m pytest tests/unit -q` → **409 passed / 5 failed**，
  失败均为既有基线且与 inline 域无关：
  `test_hot_reload.py::test_every_streamlit_page_uses_the_shared_page_header`、
  `test_code_selector_filter.py` ×2、`test_yield_global_data_policy.py` ×2。

## 2026-08-14 — 模块 3 · Phase 1 引擎归位（完成）

- [x] 1.1 `core/spc/spc_sheet_oos_decoration.py` → `core/shared/sheet_oos_decoration.py`；
  引用更新 12 处（src/app/tests）；引擎测试迁至 `tests/unit/inline_domain/core/shared/`。
  验证：inline_domain + app 239 passed（仅 hot_reload 基线失败）。
- [x] 1.2 引擎泛化：`_normalize_key_columns`/`load`/`merge`/`persist`/`_exclude_delete_flagged`
  增加 `key_columns` 参数（默认=OOS 常量，spc/ctq 零改动）。
  TDD：先写 `test_generic_key_columns_round_trip_for_non_spc_modules`（RED: TypeError）→
  实现后 GREEN。验证：tests/unit/inline_domain **150 passed**。

## 2026-08-14 — 模块 3 · Phase 2 应用层合并（完成）

- [x] 2.1 新增 `application/shared/decorated_data.py::prepare_decorated_data(scope=...)`；
  特征化测试 6 例（clip/clip_rules/flag=False/Delete/ctq 路由/未知 scope）先 RED（模块不存在）
  后 GREEN（6 passed）。
- [x] 2.2 `decorated_features.py` 单分支化，payload 移除 original_*，ctq 延迟导入消除；
  test_decorated_features / monitor mock 同步更新。
- [x] 2.3 删除 `spc_data_decoration.py`/`ctq_data_decoration.py`/`test_spc_data_decoration.py`；
  5 处测试 monkeypatch 改指 `decorated_data.ConfigLoader`；spc_service 空判断改修饰后 raw。
  验证：tests/unit/inline_domain 152 passed；全量 **412 passed / 5 failed = 基线**。

## 2026-08-14 — 模块 3 · Phase 3 aoi_rs 截断下移（完成）

- [x] 3.1/3.2 TDD：新测试 `test_service_returns_decorated_lot_and_sheet_points`
  （lot/sheet 点帧截断、spec 列不外泄、无规格 Code 保留真实值）先 RED 后 GREEN；
  旧契约测试 `test_service_keeps_raw_values_decoration_is_chart_level` 删除（D4 推翻旧设计）。
- service：`_build_chart_points` 内聚 build+attach+clip，payload/ViewModel 新增
  `lot_points_df`/`sheet_points_df`；section 删除 clip/build 调用只渲染
  （`attach_spec_values` 保留用于画规格线，属展示逻辑）；页面透传两帧（同 filter）。
- 接口变更连带更新：test_aoi_rs_page、test_aoi_rs_dashboard；夹具 `_pass_df` 补 lot_id。
  验证：inline_domain + app 242 passed（仅 hot_reload 基线失败）；
  section grep 无 clip/build_lot/build_sheet 调用。

## 2026-08-14 — 模块 3 · Phase 4/5 aoi 工作簿三态（完成）

- [x] 共享算法：`auto_decoration.py::apply_tri_state_decoration`（Delete 剔除 /
  False 释放 / True 截断），TDD 1 例先 RED 后 GREEN。
- [x] 4.x aoi_tt：`core/aoi_tt/aoi_tt_decoration.py`（键 prod_code+step_id+tt_name+sheet_id，
  工作簿 aoi_tt_sheet_oos_decoration.xlsx）；service 接入；测试 +3
  （默认兼容+落盘、flag=False 释放、Delete 剔除）；autouse fixture 重定向 project_root。
- [x] 5.x aoi_rs：`core/aoi_rs/aoi_rs_decoration.py`（键 +chart_kind/point_id 维度，
  工作簿 aoi_rs_sheet_oos_decoration.xlsx）；service `_build_chart_points` 改走修饰管线；
  测试 +2。缺陷修复：点帧无 prod_code 列导致键不匹配（detail 全 NaN），
  在归一化时按查询产品补齐。
- 验证：tests/unit/inline_domain 157 passed；全量 **417 passed / 5 failed = 基线**。

## 2026-08-14 — 模块 3 · Phase 6/7 文档与 E2E

- [x] 6.1 文档同步：`references/domain/Inline_domain/spec-infrastructure-architecture.md` §5、
  `inline_domain.md` 分层图更新至最终态（含"改工作簿→刷新缓存"操作契约）；
  generated 三份文档加状态更新并按最终态修订。
- [x] 6.2 全量回归：417 passed / 5 failed = 基线（见 Phase 5）。
- E2E 基础设施：8503 旧进程（PID 33172，改动前代码）重启为新代码；
  既有 `tests/e2e/aoi_tt_report.js` 断言文本过期（"By Lot（每个 Lot 的 TT 个数）"
  → 现标题 "By Lot（Lot 内平均每片 TT 个数）"），已修正。
- [x] 7.1（部分）aoi_tt E2E 通过：截图 output/screenshots/aoi_tt_e2e.png，
  console 仅有公司网络固有噪音（healthz/metrics，与上计划记录一致）；
  服务端证据：`resources/aoi_tt_sheet_oos_decoration.xlsx` 已生成（sheet=M626）。
- 新增 `tests/e2e/aoi_rs_report.js`、`tests/e2e/ctq_report.js`。
- [x] 7.2（部分）aoi_rs E2E 通过：渲染 3 图/Code，截图 output/test-results/aoi_rs_e2e.png；
  服务端证据：`resources/aoi_rs_sheet_oos_decoration.xlsx` 生成，含 2 行真实 OOS 点
  （M626/A8DMR/lot，prod_code 补齐正确）。
- aoi_rs Delete 验证：将 2 行 OOS 置 flag=Delete → admin 模式点「刷新缓存」→ 重新查询。
  首版断言（getByText lot_id）误报失败：lot_id 作为 x 轴刻度被同站点其他 Code 共享；
  改用 plotly 轨迹数据断言（gd.data 中 A8DMR 轨迹不含被删 lot）→ 实测 hits=[]，
  Delete 端到端生效。脚本修正后重跑通过（output/test-results/aoi_rs_delete_e2e.png）；
  验证后 flag 已恢复 True。
- [x] CTQ E2E 通过（tests/e2e/ctq_report.js，截图 output/test-results/ctq_e2e.png）。
- spc_cpk_cpm_decoration.js 首次失败排查：就绪指示硬编码旧预警指标 "ARRAY | 12450 | OVL1_Y"
  （预警周次随日期滚动，当前 W32 预警为 ARRAY|1J140|SE_L1T，CPK=1.155）。
  离线直连 DB 复核（build_spc_repository + get_spc_report_data，新代码路径）：
  CD1/1L650 期望值 1.663/1.554/1.385/1.365/1.441/1.381/1.389/1.396 与新单轨计算
  逐位吻合（CPK 单轨改动未破坏既有修饰数据语义）；仅将就绪指示改为
  「🚨 自动预警指标图像（N 个指标）」正则，不断言具体指标名。
- **E2E 运行环境修正**：发现 8503 原进程（含我首次重启）实际跑在系统 Python
  （streamlit 1.49，无 streamlit_echarts → monitor 页 ModuleNotFoundError）；
  改用 `.venv/Scripts/python.exe -m streamlit`（锁定 streamlit 1.60）重启。
  1.60 的 combobox aria-label 不再含 "Selected X" 前缀，单选取 input.value、
  多选取 stMultiSelect 容器文本；全部 8 个 e2e 脚本已改写为 1.60 兼容
  （另修正 waitForFunction 的 arg/options 形参错位），并在 .venv 环境下全量重跑。

## 2026-08-14 — 模块 3 · Phase 7 E2E 全部通过（锁定环境 .venv / streamlit 1.60）

| 脚本 | 结果 | 证据 |
|---|---|---|
| aoi_tt_report.js | 通过 | output/screenshots/aoi_tt_e2e.png (20:59) |
| aoi_rs_report.js | 通过 | output/test-results/aoi_rs_e2e.png (21:06) |
| aoi_rs_decoration_delete.js | 通过 | output/test-results/aoi_rs_delete_e2e.png (21:12)，轨迹数据断言 hits=[] |
| ctq_report.js | 通过 | output/test-results/ctq_e2e.png (21:17) |
| spc_cpk_cpm_decoration.js | 通过 | output/test-results/spc-cpk-decoration/e2e-pass.png (21:23)，8 行修饰值全匹配 |
| spc_cpk_alert.js | 通过 | 返回 result 无异常 (~21:28) |
| spc_filter_layout_mt_ch.js | 通过 | 返回 result 无异常 (~21:33) |
| spc_main_process_chamber.js | 通过 | output/screenshots/spc_main_process_chamber_e2e.png (21:39) |
| monitor_compliance_config.js | 通过 | output/test-results/monitor-compliance-e2e/downloaded-compliance-config.xlsx (20:14) |

- 服务端日志：仅公司网络固有噪音（healthz/host-config/metrics + websocket ConnectionReset），
  无应用 Traceback。
- aoi_rs Delete 验证后 flag 已恢复 True。
- 有意排除：无（首轮 1.49 环境下的通过记录被 1.60 全量重跑取代）。

## 2026-08-14 — 模块 4 项目沉淀（完成）

- [x] 8.1 ADR-0014 `docs/ADR/0014-inline-decoration-unify-shared-single-source.md` 建立；
  issue 状态 → complete（含完成记录）；checklist 全部以证据勾选。
