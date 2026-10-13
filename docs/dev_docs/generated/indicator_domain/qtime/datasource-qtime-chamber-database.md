# Task2 蒸镀腔室 Q-Time 数据源分析

探查日期：2026-10-13。需求：[Task2](../../../dev_spec/indicator_domain/qtime/task-Q_Time单腔停留时间.md)。
来源：[O091 SQL](../../../../project_files/indicator_domain/qtime/O091_Search_Evaporator_Q_Time-SQL语句.md)、
[报表截图](../../../../project_files/indicator_domain/qtime/O091_Search_Evaporator_Q_Time.png)。

## 结论与访问权限

已通过项目 `DatabaseManager` 实际连接、系统目录探查和有界事实查询。源SQL中未限定schema的
`DWT_INOUT_SUB_UNIT_GLS` 对应 `mdw.dwt_inout_sub_unit_gls`；当前search_path为mdw。
两张生产必需表均有schema USAGE和表 SELECT权限，因此可以继续实现数据库版本。

| 关系 | SELECT / schema USAGE | 目录估计行数 | 用途 |
|---|---|---:|---|
| `mdw.dwt_inout_sub_unit_gls` | true / true | 0（明显不准确） | 进片、腔室IN/OUT事件、设备与产品规格 |
| `mdw.dwr_mes_productspec` | true / true | 3,961 | MES规格名 → 产品型号 |
| `eda.spc_tzbjx_oled` | true / true | 8,572,450 | 初版SPC产品映射来源；完整版不再依赖 |

目录估计0不代表空表：限定日期、设备、OLED、P/E且LIMIT 5的实际读取返回事实。
未执行无界COUNT、全量排序、EXPLAIN ANALYZE或全表取数。权限探查事务只读、10秒超时；
生产取数最多31个进片显示日，源扫描前后各补1日，60秒语句超时，结果超过500,000条拒绝发布。
保留事实、工作簿和历史快照，不执行数据库写入。

## 每项所需数据与字段

| 页面/报表数据 | 实际来源和类型 | 转换规则 |
|---|---|---|
| GlassID | 事件表 `glass_id varchar(25)` | 精确标识，禁止通过前缀猜型号 |
| LL1Time / 进片时间 | LL1、IN行 `oper_in_time timestamp` | 按设备/玻璃排序划分进片批次；每批取自己的LL1 |
| 线体 | `eqp_id varchar(25)` | `3CEE01/3CEE02`分别显示为已有`3CEE001/3CEE002` |
| 腔室名称 | `sub_unit_id varchar(25)`及下一相关事件的sub_unit_id | 固定11路径映射，见下表 |
| 排序 | `event_timekey varchar(25)` | 使用完整事件键；样本包含微秒，不能截成日期或秒 |
| OUT事件时间 | `oper_out_time timestamp` | OUT行使用OUT时间 |
| 下一事件时间 | IN用`oper_in_time`，其他行按源SQL用`oper_out_time` | LEAD后减当前事件时间，EPOCH单位秒 |
| 产品型号 | LL1 IN的`prod_id varchar(25)` → MES `productspecname varchar(40)` → `productcode varchar(40)` | 精确关联；未知/多型号隔离，最终严格限制enabled_products |
| OLED/P、E范围 | `factory`、`prod_type varchar(25)` | 沿用`factory LIKE 'OLED%'`、`prod_type IN ('P','E')` |
| 源分区日期 | `date_timekey varchar(25)` | 有界YYYYMMDD范围，不以日期代替事件顺序 |
| 规格/超限线 | 域配置 `qtime.chamber_target_seconds` | 秒；严格大于判超限，等于为正常；图轴共用此值 |
| 流片数量 | 有效配对的区间记录数 | 源摘要COUNT(1)是配对记录数，不能擅改为去重玻璃数 |
| Q-Time超时数量/占比 | 区间秒数 > 用户阈值；超时数/有效测量数 | 截图为源平台摘要；当前模块继续仅展示点位，不新增摘要表 |

截图中顶部`Time/时间`来自SEARCH-SUMMARY的`sub_unit_type`分组，底部横向腔室列来自
SEARCH-DETAIL的`sub_unit_type`，其值来自固定CASE，非另一个数据库字段。
源平台`${txtQTime}`为参数，不是数据表内规格；当前系统规格已有可配置的权威来源。

## 11条路径

| 区间 | 前腔模式 → 后腔模式 |
|---|---|
| PT→OC1 | BL0-PT% → BL1-C1% |
| OC1→OC2 | BL1-C1% → BL1-C2% |
| OC2→OC3 | BL1-C2% → BL3-C3% |
| OC3→OC4 | BL3-C3% → BL3-C4% |
| OC4→OC5 | BL3-C4% → BL5-C5% |
| OC5→OC6 | BL5-C5% → BL5-C6% |
| OC6→OC7 | BL5-C6% → BL7-C7% |
| OC7→OC8 | BL7-C7% → BL7-C8% |
| OC8→MC | BL7-C8% → BLA-MC% |
| MC→OC9 | BLA-MC% → BLB-CC% |
| OC9→OC10 | BLB-CC% → BLD-CD% |

统一前缀为`3CEE0%-`。Task1 E～O为上述11区间；保留D列整机LL1→UL1排除。
O091还列出OC10→OC11、OC11→LC，不属于现有模块的11区间，未扩大范围。
源SQL CASE虽列OC11→LC，输入筛选却未包含LC；不能假设这条源路径已有完整数据。

## 与源SQL和初版的关系

- Q-Time是前腔OUT到下一相关事件的差值，不是同一腔IN到OUT的工艺加工时长。
- 保留源SQL的相关腔筛选、下一事件配对、OUT条件和下一UNIT_ID非空条件；无下一腔不填零。
  事件精确重复先去重，避免LEAD读到重复的同一OUT。每条真实复过货独立保留。
- 原SQL仅按glass分区，LL_INFO仅按glass连接，存在跨线/复过货混配风险。
  完整版按设备、glass、最新LL1进片时间分区，LL1产品规格随同一次过货，不连接该glass全部历史LL1。
- 页面日期定义为进片显示日期，首尾日包含；不直接照搬源SQL“事件落在日期范围”的边界。
  前后1日扫描用于跨午夜配对，仍不保证跨越补齐窗口的长流程全部完成；缺少配对不补值。
- 前推先反算源窗口，输出再应用前推、服务器当天截止和Indicator日期排除，原始事实不改动。
- 现有SVG点位图、1000秒展示过滤、配置规格、线体/产品/腔室筛选继续生效。
  页面失败、空态和下载能力不扩展；不引入旧Excel的静默数据库失败回退。

## 产品归属的真实验证

同一显示日2026-09-05（当前前推4天，对应源进片日2026-09-01）：

1. 初步沿用SPC采样映射：21,230条区间；360个映射；启用型号3,600条；1,763片未匹配。
2. 进一步探查LL1的prod_id，实际规格名可匹配MES主数据；使用该来源后：
   **2,123片全部匹配、21,230条源区间、启用型号7,080条、未匹配0、冲突0**。
3. 启用型号为M626、Z517、Z553，样本线体为3CEE002，显示时间00:01:03～23:58:22。
   该日只有10条路径有事实；OC9→OC10缺少配对，不伪造第11条路径。
4. 样本结果无负数/缺失测量。未匹配0是本日证据，不意味着其它日期主数据永远齐全。

主数据同一规格名多个版本映射相同型号时去重；不同型号保留至Core识别为冲突并隔离。
未映射、冲突、排除和禁用型号不能被统计成“没有异常”的结论。

另对默认窗口2026-10-01～2026-10-13完成真实读取：输出策略后150,581条区间、
15,058片全部匹配，启用型号44,720条、未匹配0、冲突0，两条线体均有数据。
显示范围2026-10-07 00:00:18～2026-10-13 11:58:33，与当前10月1～6日排除和当日12时截止一致。
型号为M626、Z517、Z553、Z571、Z576；本次取数及Core生成约9.2秒，不含引擎首次连接。

## 实现与验证路线

- 数据库适配器：`src/indicator_domain/infrastructure/qtime/chamber_database.py`。
- 组合根：`build_chamber_qtime_service`；生产不读取Excel、不扫描SPC采样表。
- 应用契约增加可选日期，缓存包含日期、策略、启用范围、规格和来源身份；保留旧注入源兼容。
- SQL集成测试：`tests/integration/indicator_domain/test_chamber_database.py`。
- 浏览器fixture使用真实数据库适配器+隔离SQLite事实；live场景使用生产组合根。
- 最终验证结果见同目录 `qtime-chamber-database-delivery.md`；权威规则见
  [腔室数据库规则](../../../../../references/domain/indicator_domain/qtime/rules-chamber-database.md)。
