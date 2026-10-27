# ADR-0036：报表与矩阵共用已发布结果的预警展示

- Status: Accepted（用户于2026-10-27明确要求实施，并澄清仅共享预警显示管路与内存）
- Date: 2026-10-27
- Scope: 监控报表的预警消费、自动预警矩阵的状态与详情、共同结果缓存；不合并各域原始分析流程

## Context

SPC报表从ViewModel内存明细构建单片预警，矩阵状态和详情分别读取工作簿，导致PPA预警排除只在报表生效。报表初始化、计算与图像失败也会使后续已保存预警无法显示。其他报表有相同的生产／展示耦合，Q-Time和AOI趋势尚无独立可读的发布结果。

## Decision

1. 原有各域生产流程继续负责分析和发布；展示一律消费本地发布结果，不在缓存miss或缺文件时回退运行分析。
2. 在`app/sections/shared/alert_results.py`装配跨报表共同展示服务。报表、矩阵状态和详情复用同一读取／筛选函数与原生结果缓存；域算法、Store协议和适配器仍归原域所有。
3. 共享范围仅是已发布结果读取、规范化、时间／产品／参数筛选、公开投影与状态归约。详细图像走原有独立管路，失败不能遮蔽已保存预警。
4. 保留不同报表的范围参数：Yield报表Lot近30天与矩阵上周、Q-Time报表选定厂别站点滚动窗口与矩阵全厂上周。相同参数保证同规则结果，不将不同范围硬改成同一数字。
5. Q-Time新增独立厂别结果Excel，AOI_RS／TT新增完整评估结果Excel；注册路径，不修改原始L1或人工决策。既有结果格式及初始化空模板约定保留。
6. 文件版本、规则配置、参考日、范围、有效性及产品指标revision参与共享缓存。Inline OOS发布门控另外跟踪有效结果内容，变化时写入正式结果。

## Alternatives

- 只给矩阵复制PPA过滤：变更小，但读取、策略注入和缓存失效继续分叉，无法保证以后规则一致。
- 让矩阵调用整个报表服务：可获得当前内存结果，但增加原始查询、重算和写入副作用，不符合本次“仅共享展示”的要求及ADR-0035。
- 用OOS或人工决策表重建趋势／Q-Time结果：这些文件缺少必要数据或含义不同，会引入新的分析引擎；采用正常生产流程发布真实投影。

## Consequences

同进程的报表和矩阵共享缓存计算与工作簿解析，并通过返回副本避免互相修改。独立进程继续仅共享文件。没有结果、覆盖不足或陈旧时不会自动补算，首次使用须由正常报告生产发布。旧成功结果是否仍可用按各域既有有效性处理，不把生产失败伪装成当前零预警。

图像与预警显示分开，跨域元数据格式没有强行统一。应用需要维护发布版本、消费规则和安全投影；这增加少量文件协议维护，但避免修改业务算法或引入数据库。

## Evidence

- [设计与验证契约](../../references/design/inline_domain/shared/architecture-published-alert-display.md)
- [矩阵消费架构](../../references/domain/inline_domain/monitor/architecture-alert-dashboard.md)
- [执行计划](../../.planning/published-alert-pipeline/task_plan.md)
- `tests/unit/app/sections/monitor/test_shared_alert_results.py`、`tests/unit/app/pages/test_inline_saved_alerts.py`、`tests/unit/inline_domain/application/monitor/test_trend_alert_results.py`与Q-Time独立发布、各域保存结果展示测试。
