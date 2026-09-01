# Q-Time 报表数据源分析

## 1. 结论

Q-Time 报表的生产查询契约由两张表提供：

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

因此当前开发环境可以验证 schema、参数化查询契约和隔离数据 E2E，但不能宣称已经完成真实 Q-Time 数据读取验证。部署前需要数据库管理员为应用账号授予这两张表的只读权限。

## 6. 未采用的替代数据源

只读探查还发现可读的 `mdw.dwr_qtime_info_v` 和 `mdw.dwr_mes_ct_modeqtime`，但不作为降级源：

- `dwr_qtime_info_v` 的 view definition 固定只读取 ARRAY 历史，缺少 `prod_qty`、`sub_prod_type` 和任务定义的制备规格；近期过滤查询也触发 20 秒 statement timeout/WLM 取消。
- `dwr_mes_ct_modeqtime` 仅覆盖少量产品，探查时没有参考产品 M626 的规格记录。

使用它们会把“三厂成品报表”静默改变成“ARRAY 局部事实”，属于业务语义错误。生产实现遇到权限或连接失败时应显示明确错误，不伪造或替换数据。
