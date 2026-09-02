# ADR-0019：Q-Time 报表数据源与展示边界

- Status: Superseded by ADR-0021
- Date: 2026-09-01
- Scope: `src/qtime_domain/`、`app/{pages,sections,charts}/qtime_domain/`、`app/pages/Q_Time监控报表.py`
- Trace: Issue `.scratch/qtime-report/issues/01-create-qtime-report.md`、
  Plan `.planning/2026-09-01-qtime-report/`、
  Data source `docs/dev_docs/dev_spec/indicator_domain/Q-Time数据源分析.md`

## Context

FineReport Q-Time 报表使用 `eda.imp_qtime_tzbjx` 提供产品配置，并从
`mdw.qtime_tzbjx` 查询站点间过货明细。数据库元数据探查确认了两张目标表及
字段契约，但当前应用账号没有目标表的 `SELECT` 权限。可访问的
`mdw.dwr_qtime_info_v` 只覆盖 ARRAY、缺少数量/产品类型/规格等必需字段，且近期
查询触发资源治理超时，因此不能在生产查询失败时作为等价替代源。

## Decision

1. Q-Time 作为独立 `qtime_domain` 实现：应用层依赖数据端口，SQLAlchemy 仓储
   负责参数化查询与结果规范化，Streamlit 页面只组装服务和展示 section。
2. 生产明细只查询 `mdw.qtime_tzbjx`，产品选项查询
   `eda.imp_qtime_tzbjx`；厂别严格按 `f_step` 首字符映射 ARRAY/OLED/TP，时间窗
   使用 `[start, end)`，空产品集合表示不过滤产品。
3. 不对权限/连接错误静默切换到非等价视图。仓储把底层异常转换为不含 SQL、凭据
   或连接细节的稳定应用错误，页面显示可操作的安全提示。
4. 浏览器 E2E 通过隔离数据端口验证级联筛选、查询门控、柱线图、双语表格、空结果
   与错误状态；隔离 E2E 不作为生产数据库可读性的证据。生产部署仍须为应用账号
   授予两张目标表所需的只读权限。

## Alternatives considered

- **权限失败时回退 `mdw.dwr_qtime_info_v`**：拒绝。该视图只覆盖 ARRAY 且缺少
  完整业务字段，回退会把不完整数据伪装成三厂完整报表。
- **页面直接调用数据库工具**：拒绝。会把 SQL、错误处理和缓存边界耦合到 UI，
  难以在无生产权限时可靠验证业务流程。
- **让 E2E 依赖生产数据库**：拒绝。当前权限属于外部部署前置条件，依赖它会使 UI
  回归不可重复，也会把“界面通过”和“生产授权完成”错误地混为一谈。

## Consequences

- 正面：数据契约明确且可测试；用户输入不拼接进 SQL；UI 可在隔离环境完整回归；
  生产权限失败不会泄露敏感连接信息。
- 负面/约束：应用账号获得 `eda.imp_qtime_tzbjx` 和 `mdw.qtime_tzbjx` 的只读权限
  前，真实页面只能显示安全错误状态；目标表结构变化需同步更新仓储契约与集成测试。
- 后续约束：不得以字段不完整或厂别范围不同的数据源做静默降级；如未来采用新数据
  源，必须先证明其业务口径等价并更新本 ADR。

## Verification

- 聚焦单元/集成测试：22 passed，覆盖查询校验、厂别映射、参数绑定、SQL 契约、
  服务、图表、表格、页面入口和错误/空状态。
- 相关 app 回归：219 passed，2 个已确认既有基线排除；全量测试 759 passed，
  8 个失败均为未触碰模块的既有基线。
- 浏览器 E2E：隔离 Streamlit 页面覆盖 ARRAY/OLED 级联、查询、图表、表格、空结果、
  安全错误和重复 rerun；900 与 1365 宽度均无页面级横向溢出，控制台 0 errors。
- UI QA 证据：`output/test-results/qtime/`；关键测试：
  `tests/integration/qtime_domain/test_qtime_repository_sql.py`、
  `tests/unit/app/sections/qtime_domain/test_qtime_dashboard.py`、
  `tests/e2e/fixtures/qtime_app.py`。
