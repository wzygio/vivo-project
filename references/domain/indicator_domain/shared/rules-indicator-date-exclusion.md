# Indicator 日期排除规则

核验日期：2026-10-13。适用范围：Q-Time 站间等待、蒸镀腔室停留、IJP 边框与大孔区报表。

## 配置与时间口径

`config/domain/indicator_domain.yaml` 的 `data_exclusion` 是唯一配置入口：

```yaml
data_exclusion:
  enabled: true
  start_date: "2026-10-01"
  end_date: "2026-10-06"
```

首尾日期均包含，覆盖 10 月 1 日零点至 10 月 7 日零点之前。10 月 7 日及之后仍可展示。
按全局日期前推之后的**报表显示日期**排除，不按 Excel/数据库原日期排除。
四天前推开启时，以上配置等价于剔除源时间 `[2026-09-27, 2026-10-03)` 的事实。

缺少配置或 `enabled: false` 时关闭。`end_date` 支持 ISO 日期或 `today`（服务器当天）；
配置错误、缺少启用策略的日期、起止倒置均拒绝，不能悄悄忽略。
此规则覆盖所有厂别、产品和线体，不依赖 Inline 的厂别白名单。

## 基础设施执行边界

共享实现位于 `src/indicator_domain/infrastructure/shared/date_exclusion.py`，不跨域调用 Inline 实现。

| 数据适配器 | 执行方式与时间列 |
|---|---|
| Q-Time 站间 | 仓储将源时间前推、应用当天截止，再过滤显示事件时间；直接查询、新鲜快照和失败降级共用输出边界。过滤后的事实才进入修饰、超规统计和告警。 |
| 蒸镀腔室 | `entry_time` 前推、当天截止后过滤，随后只对保留记录按未平移的源月份匹配产品；再进入单腔计算。 |
| IJP 边框 | 将排除显示区间反算为源区间，在 SQL 的 `D.GLASS_START_TIME` 上使用绑定条件，先过滤再 GROUP BY、比例分母计算和 LIMIT。日度与明细输出同时按显示 `day` / `print_time` 过滤。 |
| IJP 孔区 | 缺陷 CTE 按 `UPDATE_TIME` 的反算源区间过滤，随后关联、去重与计数；输出再按显示 `day` 过滤。履历回看窗口保留关联语义。 |
| IJP 批次选项 | 在现有有界查询内按周期表 `EVENT_TIME` 的反算排除区间过滤，产品范围不变。 |

日期 SQL 条件为半开源区间的补集：`event < excluded_start OR event >= excluded_end`。
日期使用绑定参数，SQL 时间表达式仅由适配器固定列名构造。
IJP 明细混合秒和小数秒时间字符串时先规范化再前推，防止有效时间变成空值。

## 保留与失效

- 仅过滤输出副本，不删除数据库事实、不裁剪原始 Q-Time Parquet、不改写 Excel 和人工决策。
- Q-Time、腔室缓存签名包含当前解析后的排除区间；关闭或修改区间时重新生成投影。
- 四类页面的提交结果签名包含排除区间，变化后要求重新查询，旧图表不继续渲染。
- 已移除腔室前端“10 月 1 日及以后全隐藏”的临时硬编码；过滤由基础设施配置统一控制。
- IJP 月/周固定日历分组仍沿用既有展示规则；仅统计未被排除的事实，空日不产生事实点。

## 验证入口

- `tests/unit/indicator_domain/infrastructure/shared/test_date_exclusion.py`：包含首尾整天、关闭、today、配置校验、输出副本与源窗口反算。
- `tests/integration/indicator_domain/test_indicator_date_exclusion.py`：Q-Time 直接/快照/降级、源文件保留、腔室、SQLite IJP 聚合与分母、边界恢复和策略签名。
- `tests/unit/app/sections/indicator_domain/`：提交结果失效、腔室保留 10 月 7 日及之后、空态与失败态。

2026-10-13 验证记录：

```powershell
.venv/Scripts/python.exe -m pytest tests/unit/indicator_domain tests/integration/indicator_domain tests/unit/app/sections/indicator_domain tests/unit/app/charts/indicator_domain tests/unit/app/pages/test_qtime_page.py tests/unit/app/pages/test_ijp_page.py tests/architecture --deselect tests/unit/app/sections/indicator_domain/qtime/test_qtime_dashboard.py::test_qtime_dashboard_gates_results_until_the_user_queries -q
# 219 passed, 1 deselected；此后补充源窗口启停和 SQL LIMIT 边界测试。
.venv/Scripts/python.exe -m pytest tests/unit/indicator_domain/infrastructure/shared/test_date_exclusion.py tests/integration/indicator_domain/test_indicator_date_exclusion.py -q
# 15 passed
```

排除的旧 UI 测试断言标题为“北极星QTime监控”，HEAD 页面已为“重点站点QTime监控”；
在单独进程加载 HEAD 的未修改 dashboard 源码后已复现同一失败。
使用 AppTest、临时 Excel/Parquet 和内存 SQLite 验证；未查询生产数据库，未启动临时服务。
本次修改范围的 `git diff --check`、新增 Python 语法和配置/文档路由检查通过。
