# ADR-0020：IJP 溢流监控报表子域与双方言 SQL 契约

- Status: Superseded by ADR-0021
- Date: 2026-09-01
- Scope: `src/qtime_domain/{core/ijp_overflow.py,application/ijp/,infrastructure/ijp_repository.py}`、
  `src/qtime_domain/composition.py`、`app/pages/IJP溢流监控报表.py`、
  `app/sections/qtime_domain/ijp_dashboard.py`、`app/charts/qtime_domain/ijp_chart.py`
- Trace: Issue `.scratch/ijp-overflow-report/issues/01-create-ijp-overflow-report.md`、
  Plan `.planning/2026-09-01-ijp-overflow-report/`、
  数据源分析 `docs/dev_docs/dev_spec/indicator_domain/datasource-IJP溢流报表分析.md`、
  E2E 证据 `output/test-results/ijp/`

## Context

内部已有一份 FineReport 实现的 IJP 溢流监控报表（13 组数据集 SQL，核心源为
`EDA.SPOT_EDA_OLED_VIEW_DFT_V` 关联腔室履历、产品规格、工单与 Cycle 视图），
需要复刻为仓库内 Python + Streamlit 报表。只读探查确认：全部数据源可访问；
`GLASS_START_TIME`/`CUT_START_TIME` 为 timestamp，而 FineReport SQL 中
`EVENT_TIME <> 'NaT'` 的防御只适用于 varchar 的
`EDA.DWD_GLASS_OLED_CYCLE_V3.EVENT_TIME`；`DWR_MES_PRODUCTSPEC` 基础表与
`DWR_MES_PRODUCTSPEC_V` 视图列集不同，明细 JOIN 与筛选项分别依赖两者。
原 SQL 的 PANEL_LOCATION 解析使用 PG 专属的 `split_part`，与仓库既有的
SQLite ATTACH 集成测试模式（ADR 见 Q-Time 集成契约测试）不兼容。

## Decision

1. IJP 溢流监控作为 `qtime_domain` 的子域落位（`application/ijp/`、
   `infrastructure/ijp_repository.py`、`core/ijp_overflow.py`），不新建顶层
   domain；组合根 `composition.py` 增加 `build_ijp_repository`，页面不直接
   访问数据库。
2. 派生字段在 core 纯 Python 计算：`map_panel_location`（C3DM% 与非 C3DM%
   两套后缀规则、KONG* 系列）、`map_bottom_breakout`（B0~B9 → BOTTOM0~9
   展开）、`extract_panel_id`（SUBSTR(57,14) 契约）、`build_image_url`。
   仓储 SQL 只使用 PG/SQLite 双方言可执行的构造（SUBSTR、CAST AS TEXT、
   `||`、窗口函数、expanding bindparam），保证 SQLite ATTACH 契约测试能
   真实执行。
3. timestamp 列不做 `<> 'NaT'` 判断；仅 varchar 的 `EVENT_TIME` 在筛选项
   查询中保留 `::TIMESTAMP` + `<> 'NaT'`（PG 方言分支，SQLite 契约测试走
   文本比较）。
4. 沿用 Q-Time 域的错误与降级契约：无快照降级，数据库失败包装为
   `IjpDataAccessError` 稳定中文文案（不含 SQL/凭据/traceback）；明细查询
   服务端 LIMIT 5000 并在 UI 提示截断。
5. By天 图口径：按天 × RS_CODE 占比堆叠（每天合计 100%），起始时间向前扩
   7 天（与原 `SERACH_BYDAY` 一致）；Target 值仅作图表参考线，无独立数据源。

## Alternatives considered

- 新建顶层 `ijp_domain`：拒绝。IJP 属 OLED 段质量监控族，任务书归入
  qtime_domain 规格目录，复用既有分层与组合根可避免顶层 domain 膨胀。
- 在 SQL 中原样保留 `split_part` 做 PANEL_LOCATION：拒绝。PG 专属函数使
  SQLite 契约测试无法执行，且派生逻辑放 core 可单测、可复用。
- 为明细/图表引入 Parquet 快照降级：拒绝。与 Q-Time 域现状一致，本期无该
  运维需求；数据库失败显示稳定错误即可。

## Consequences

- 正面：全部 SQL 走绑定参数且可由 SQLite 契约测试真实验证；PANEL_LOCATION
  等派生规则有独立单测；页面与既有报表保持一致的薄入口/section/chart 边界。
- 负面：PANEL_LOCATION 的 SQL 原版与 core 版存在两份语义描述，修改解析
  规则时需同步更新 core 与对应测试；明细 LIMIT 5000 之外的行需缩小筛选
  范围查看。
- 后续约束：新增 IJP 相关报表区块（如按边框/Total 占比图）应扩展
  `IjpDataPort`，不得在页面层新增直连数据库逻辑。

## Verification

- 聚焦测试：`uv run pytest tests/unit/qtime_domain/ijp tests/unit/app/pages/test_ijp_page.py tests/unit/app/sections/qtime_domain/test_ijp_dashboard.py tests/unit/app/charts/qtime_domain/test_ijp_chart.py tests/integration/qtime_domain/test_ijp_repository_sql.py -q` → 36 passed。
- 全量回归：`uv run pytest -q` → 806 passed / 8 项既有失败基线（与 IJP 无关，逐项核实）。
- E2E（隔离 fixture + playwright-cli）：门控、默认查询、堆叠图、明细表、筛选失效、空/错误分支、1365×768 viewport-fit 全部通过，截图见 `output/test-results/ijp/`。
