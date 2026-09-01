# Findings & Decisions: Q-Time 报表

## Requirements

- Python + Streamlit 复刻 FineReport Q-Time 页面。
- 新建独立 DDD domain 与 `app/pages` 页面。
- 分析图片和 SQL，输出数据源文档。
- 持续迭代直到 E2E 通过。

## Research Findings

- `QTIME_TZBJX` 在当前数据库 search path `mdw` 下解析为 `mdw.qtime_tzbjx`。
- 明细列完整匹配任务 SQL：`step_desc, lot_id, prod_qty, sub_prod_type, f_step, t_step, q_spec, wait_time, timekey, prodcode, update_time`。
- `eda.imp_qtime_tzbjx` 是五列配置表：`productspecname, f_step_id, t_step_id, q_spec, update_time`；FineReport 只用它获取产品列表。
- 当前应用账号对两张目标表都没有 SELECT 权限，但可通过系统目录确认 relation 和列类型。
- 可读 `mdw.dwr_qtime_info_v` 只从 ARRAY 历史源构建，缺少 `prod_qty/sub_prod_type/q_spec`，近期过滤查询仍超时；不可作为生产降级。
- 可读 `mdw.dwr_mes_ct_modeqtime` 仅覆盖少量产品且 M626 无规格，不能替代任务成品表。

## Visual/Browser Findings

- 参考使用界面顶部是一行灰底筛选：厂别、开始时间、结束时间、产品、站点 From-To、查询。
- 主体标题“北极星QTime监控”；青色 Lot 柱表示等待时长，红色水平线表示 QTime 规格。
- 下方九列双语表头，蓝色表头、交替浅蓝行；Lot 与图表 x 轴逐行对应。
- FineReport 通用分页/打印/导出/邮件工具栏不属于核心报表业务，本次排除。

## Technical Decisions

| Decision | Rationale |
|---|---|
| Repository + application service + pure presentation helpers | 符合 DDD 与页面不直连数据库约束，可隔离测试。 |
| SQLAlchemy bound params + expanding product list | 防 SQL 注入并保留产品空集合=全部语义。 |
| Plotly bar + line | 仓库已有 Plotly，能精确控制柱线、图例、单位和响应宽度。 |
| 浏览器 E2E 注入隔离数据 | 测试 UI 全链路且不依赖缺失数据库权限。 |

## Issues Encountered

| Issue | Resolution |
|---|---|
| 数据账号无目标表 SELECT | 不绕过权限；实现契约、测试隔离数据、UI 给明确错误，并在数据源文档记录。 |
| 替代 view 业务语义不完整 | 明确不采用，避免 ARRAY-only 数据冒充完整三厂报表。 |

## Resources

- `docs/dev_docs/dev_spec/qtime_domain/task-Q_Time报表开发.md`
- `ARCHITECTURE.md`
- `.scratch/qtime-report/issues/01-create-qtime-report.md`
- `src/shared_kernel/infrastructure/db_handler.py`
