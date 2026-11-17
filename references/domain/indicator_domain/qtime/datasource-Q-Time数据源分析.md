# Q-Time 报表数据源分析

## 1. 结论

Q-Time 报表常规路径的生产查询契约由以下两张表提供；固定补充路径见第 7 节：

| 数据集 | 实际关系 | 职责 |
|---|---|---|
| `prodcode` | `eda.imp_qtime_tzbjx` | 产品/From-To/规格配置；FineReport 用 `productspecname` 生成产品选项。 |
| `Search`、`step_desc` | `mdw.qtime_tzbjx` | 已制备的 Lot 级 Q-Time 明细、规格、等待时长和筛选维度。 |

任务 SQL 中未限定 schema 的 `QTIME_TZBJX` 在当前数据库连接的 `search_path=mdw` 下解析为
`mdw.qtime_tzbjx`。应用实现显式写出 schema，避免环境 search path 变化导致查询漂移。

## 2. 参考页面所需数据

参考使用界面包含以下可观察区域：

1. 横向筛选条：厂别、开始时间、结束时间、产品、站点 From-To 和查询按钮。
2. Lot 级柱线图：青色柱为 `wait_time`，红色线为 `q_spec`，横轴为 `lot_id`。
3. 明细表：序号、QTime路径、Lot、产品数量、产品类型、From站点、To站点、Q-Time规格、等待时长。

FineReport 的分页、打印、导出和邮件属于平台工具栏，不是 Q-Time 业务数据契约。

## 3. 字段契约

### 3.1 `eda.imp_qtime_tzbjx`

数据库系统目录确认的字段如下：

| 字段 | 类型 | 用途 |
|---|---|---|
| `productspecname` | `varchar(40)` | 产品筛选选项。 |
| `f_step_id` | `varchar(40)` | 配置的 From 站点。 |
| `t_step_id` | `varchar(40)` | 配置的 To 站点。 |
| `q_spec` | `varchar(400)` | 配置源中的 Q-Time 规格文本。 |
| `update_time` | `timestamp` | 配置更新时间。 |

当前报表沿用 FineReport 的 `SELECT DISTINCT productspecname` 生成产品选项；图表与明细中的数值规格使用已制备明细表的 `q_spec`，避免在页面层重复解析配置文本。

### 3.2 `mdw.qtime_tzbjx`

| 字段 | 类型 | 页面/查询用途 |
|---|---|---|
| `step_desc` | `varchar(200)` | From-To 路径筛选、图例和“QTime监控”列。 |
| `lot_id` | `varchar(40)` | 图表横轴、LotID 明细列。 |
| `prod_qty` | `numeric` | 产品数量。 |
| `sub_prod_type` | `varchar(400)` | 产品类型。 |
| `f_step` | `varchar(40)` | FromOperation；也用于厂别映射。 |
| `t_step` | `varchar(40)` | ToOperation。 |
| `q_spec` | `numeric` | Q-Time 规格线及明细标准。 |
| `wait_time` | `numeric` | Lot 等待时长柱及明细值。 |
| `timekey` | `varchar(40)` | `YYYYMMDDHH24MISS` 字符串时间筛选和稳定排序。 |
| `prodcode` | `varchar(40)` | 产品多选过滤。 |
| `update_time` | `timestamp` | 明细制备更新时间，不直接展示。 |

页面派生字段 `shop` 不落库，规则与 FineReport 完全一致：

```text
f_step LIKE '1%' -> ARRAY
f_step LIKE '2%' -> OLED
otherwise        -> TP
```

## 4. 查询语义

- 站点选项优先复用已有快照；回退查询 `mdw.qtime_tzbjx` 时，先以绑定参数限定源 `timekey > '20260101'`，再按厂别筛选并对站点组合去重（2026-10-13 确认）。阈值按原始字符串比较，不应用显示日期前推；14 位的 `20260101000000` 大于该 8 位阈值，因此保留 2026-01-01 当天的有效时间记录。此条件仅用于站点选项的数据库回退查询。
- 时间窗口为半开区间：`timekey >= start_time AND timekey < end_time`。开始/结束时间转换为 14 位 `YYYYMMDDHH24MISS`。
- `step_desc` 与 `shop` 都是必选条件。
- 产品集合为空时不添加产品条件，含义是“全部产品”；非空时使用绑定参数的 `IN` 条件。
- 排序为 `step_desc, lot_id, timekey`，保证重复 Lot/多时点的显示顺序稳定。
- 所有用户筛选均使用 SQLAlchemy 绑定参数，不拼接到 SQL 文本。

## 5. 探查证据与权限边界

2026-09-01 使用项目 `DatabaseManager` 进行只读探查：

- `to_regclass('eda.imp_qtime_tzbjx')` 和 `to_regclass('qtime_tzbjx')` 均解析成功；后者落在 `mdw`。
- `pg_catalog.pg_attribute` 返回了上文记录的完整字段与类型。
- 当前应用账号对 `eda.imp_qtime_tzbjx`、`mdw.qtime_tzbjx` 的 `has_table_privilege(..., 'SELECT')` 均为 `false`，实际聚合查询得到 `permission denied`。

这是 2026-09-01 的历史权限记录。2026-11-17 的只读元数据核验已确认应用账号可读取这两张表及第 7 节补充来源，不能将当时的权限状态当作当前状态。

## 6. 未采用的替代数据源

只读探查还发现可读的 `mdw.dwr_qtime_info_v` 和 `mdw.dwr_mes_ct_modeqtime`，但不作为降级源：

- `dwr_qtime_info_v` 的 view definition 固定只读取 ARRAY 历史，缺少 `prod_qty`、`sub_prod_type` 和任务定义的制备规格；近期过滤查询也触发 20 秒 statement timeout/WLM 取消。
- `dwr_mes_ct_modeqtime` 仅覆盖少量产品，探查时没有参考产品 M626 的规格记录。

使用它们会把“三厂成品报表”静默改变成“ARRAY 局部事实”，属于业务语义错误。生产实现遇到权限或连接失败时应显示明确错误，不伪造或替换数据。

## 7. 固定补充路径 1A200 → 21100

用户确认本路径按两个站点的 TrackIn 进站时间相减；仅补充取数，标准明细输出后的监控、修饰、预警和图表均使用原实现。它不是其他路径失败时的替代源。

实现位于 [array_transfer_source.py](../../../../src/indicator_domain/infrastructure/qtime/array_transfer_source.py)，由生产组合根向站点仓储显式注入。直接构造旧仓储而未注入时不会访问补充来源；OLED、TP 保留原路径。

| 来源 | 已核验字段及用途 |
|---|---|
| `mdw.dwr_mes_lot_hist` | `lot_id`、`oper_code`、`event_time`、`event_timekey`、`prod_id`、`prod_ver`、`factory`、`prod_qty`；固定两站且 `event_name='TrackIn'`，进站时间使用半开源窗口 |
| `mdw.dwr_mes_productspec` | `productspecname`、`productspecversion`、`factoryname` 与起点的产品规格、版本、厂别关联，输出 `productcode`；只读本次规格集合并以创建时点限制主数据快照 |

- CPT 的实际去重基线为每批次、产品、起点时刻选择最早终点；允许不同起点关联同一终点，不增加返工分段。两端规格和厂别可以不同，产品归属取起点规格的唯一映射。
- `timekey` 为起点进站时间：真实样本存在多个起点共用终点，用终点作为键会与当前人工决策键冲突。源微秒保留到原仓储规范化，按秒级决策键仍有冲突时拒绝发布。
- 输出 `f_step='1A200'`、`t_step='21100'`、`step_desc='1A200->21100'`、`shop='ARRAY'`；时长单位小时，数量取起点真实 `prod_qty`，未知子类型为空。
- 规格尚未由用户提供，`q_spec` 为空；既有修饰函数及图表处理此输入，不新增路径专属预警分支。正规格的数值接入仍是用户后续提供的事项。
- 固定路径原始事实并入同一 ARRAY 快照。普通刷新也对该路径重读当前完整滚动源窗口，替换它的全部旧片段，以处理较早起点后来才取得终点的情形；成品源维持原增量策略。
- 新来源签名进入 ARRAY 快照策略，防止旧的 fresh 快照掩盖新路径；其他厂别不受该后缀影响。读取或映射失败沿用原异常/旧快照降级，不发布部分新鲜结果。
- 履历最多 100,000 行、主数据最多 10,000 行，以 `上限+1` 检测溢出；只读事务内设置 20 秒超时，不建临时表，超限拒绝而非静默截断。

2026-11-17：有界只读样本回放得到 589 条标准记录；经原监控时间政策后真实查询返回 523 条、覆盖 6 个启用产品，规格为空。采样结果不是对任意历史范围完整性的承诺。实现与验证记录见 [专项交付](../../../../docs/dev_docs/generated/indicator_domain/qtime/qtime-array-transfer-delivery.md)。
