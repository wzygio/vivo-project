# Progress: IJP 溢流报表开发

## 2026-09-01

- [x] 样式截图与 13 组数据集 SQL 逐项分析完成。
- [x] 数据库只读探查完成：7 项数据源全部验证通过（`output/tmp/ijp_db_probe_result.md`）。
- [x] 数据源分析文档输出：`docs/dev_docs/dev_spec/qtime_domain/datasource-IJP溢流报表分析.md`。
- [x] Issue 创建并 triage 至 ready-for-agent：`.scratch/ijp-overflow-report/issues/01-create-ijp-overflow-report.md`。
- [x] 计划建立（Phase 2 完成，用户预先批准自动执行）。
- [x] Phase 3 TDD 切片：全部完成（RED→GREEN 逐切片推进）。
  - Tracer bullet：`tests/unit/qtime_domain/ijp/test_ijp_overflow_core.py` + `test_ijp_query.py` 先 RED（collection error），实现 `src/qtime_domain/core/ijp_overflow.py` 与 `src/qtime_domain/application/ijp/dtos.py` 后 GREEN（10 passed）。
  - Slice 1 仓储：`tests/integration/qtime_domain/test_ijp_repository_sql.py`（SQLite ATTACH 模拟 eda schema + 主 schema 的 dwr_mes_* 表）6 passed；`tests/unit/qtime_domain/ijp/test_ijp_repository.py`（绑定参数/方言分支/安全错误）5 passed。
  - Slice 2 服务：`tests/unit/qtime_domain/ijp/test_ijp_service.py` 3 passed（Fake port 正常/级联/错误传播）。
  - Slice 3 图表/表格：`tests/unit/app/charts/qtime_domain/test_ijp_chart.py` 4 passed；`tests/unit/app/sections/qtime_domain/test_ijp_dashboard.py` 7 passed（含 AppTest 门控/非法时间窗/空/错误/失效）。
  - Slice 4 页面：`tests/unit/app/pages/test_ijp_page.py` 静态薄入口测试 1 passed。
- [x] Phase 4 回归与 E2E。
  - 聚焦：`uv run pytest tests/unit/qtime_domain/ijp tests/unit/app/pages/test_ijp_page.py tests/unit/app/sections/qtime_domain/test_ijp_dashboard.py tests/unit/app/charts/qtime_domain/test_ijp_chart.py tests/integration/qtime_domain/test_ijp_repository_sql.py -q` → 36 passed。
  - 全量：`uv run pytest -q` → 806 passed / 8 failed；其中 7 项为既有失败基线（见下），1 项 `test_every_streamlit_page_uses_the_shared_page_header` 因页面清单 12→13 已更新，更新后仍因两个既有 `专项资料-*` 页面缺 `render_page_header` 而失败（HEAD 上即缺，属既有失败）。
  - 既有失败基线（与 IJP 无关，未修复）：`tests/test_spc_outlier_filter_issue.py` 3 项（加密 xlsx 环境相关，重跑时失败项会漂移）、`test_aoi_rs_page.py::test_portal_navigation_points_aoi_rs_to_the_streamlit_page`（config.js 内容断言）、`test_yield_dashboard_plotly_keys.py` 1 项、`test_yield_global_data_policy.py` 2 项（用户已修改的 config/global.yaml 与测试期望不一致）、`test_hot_reload.py::test_every_streamlit_page_uses_the_shared_page_header`（既有页面违规）。
  - E2E：`streamlit run tests/e2e/fixtures/ijp_app.py --server.port 8511` + `playwright-cli run-code --filename=tests/e2e/ijp_overflow_report.js` → `IJP E2E 通过：bar traces=4，viewport 无横向滚动`。覆盖：页面打开、查询门控提示、默认筛选查询、By天 堆叠图（4 条 bar trace）、明细表、筛选变更失效、空分支（CODE=C3BH2）、错误分支（M678 → 稳定错误文案）、1365×768 无页面级横向滚动。
  - E2E 截图证据：`output/test-results/ijp/ijp_report_main.png`（筛选区+图表）、`ijp_report_table.png`（明细表全列+原图链接+Total 行 3.667）、`ijp_report_empty.png`（空分支）、`ijp_report_error.png`（错误分支）。fixture 服务日志：`output/test-results/ijp/streamlit.log`。

### Phase 3/4 实现决策补充

- PANEL_LOCATION 用 `split_part` 语义的纯 Python 实现（`core/ijp_overflow.py`），仓储只查原始列；B0~B9 在 core 展开为 BOTTOM0~9（镜像 SERACH1 UNION ALL 第二支），CODE_RATIO 在展开前的原始行上按 GLASS_ID 内占比 ROUND 3 计算。
- 非 C3DM% 的 KONG* 映射统一取后缀 3 位（HL/HT/HR/HB + 数字）；原 SQL 中 KONGLEFT 分支误用 2 位后缀与 3 位边界比较（永不命中），按 brief 意图实现。
- `EVENT_TIME`（varchar）方言分支：PostgreSQL 用 `EVENT_TIME::TIMESTAMP` + `<> 'NaT'`，SQLite 契约测试走纯文本比较；按 `engine.dialect.name` 判定。
- By天 日期截断用 `SUBSTR(CAST(D.GLASS_START_TIME AS TEXT),1,10)`（PG/SQLite 双方言可执行），起始时间向前扩 7 天在 Python 侧计算后绑定；按天占比在仓储 pandas 侧聚合，By天 图不含边框筛选（与原 SERACH_BYDAY SQL 一致）。
- 明细 SQL 侧 `LIMIT {int(detail_limit)}`（内部常量 5000 强转 int，非用户输入）；UI 截断时 caption 提示。
- `tests/unit/app/components/test_hot_reload.py` 页面清单计数 12→13（新页面已用共享页头；该测试在 HEAD 上因两个 `专项资料-*` 页面缺页头本就失败，属既有基线）。

### 母流程复核（development-flow 验收，2026-09-01）

- 产物磁盘核验：后端/前端/测试/E2E 文件全部存在且落位正确。
- 复核聚焦测试命令：36 passed（与记录一致）。
- 复核全量回归：`uv run pytest -q` → 806 passed / 8 failed，逐项确认为既有基线（含 hot_reload 失败信息实证为两个既有 `专项资料-*` 页面，IJP 页面计数断言通过）。
- 复核 E2E：重新启动 fixture（8511）+ `playwright-cli run-code` 实跑通过（`IJP E2E 通过：bar traces=4，viewport 无横向滚动`），截图人工核看（筛选区/堆叠图/明细表/原图链接/Total 行均符合参考样式）；console 仅 Streamlit telemetry 网络阻断噪音。
- 分支策略偏差：按系统约束与用户既有未提交改动保护要求，未创建开发分支，直接在 master 工作区开发；无 git 写操作。
- Phase 5：ARCHITECTURE.md 已更新（qtime_domain 行含 IJP 子域），ADR-0020 已写（`docs/ADR/0020-ijp-overflow-report-subdomain.md`）。
