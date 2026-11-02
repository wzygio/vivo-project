# 入库不良率分析看板性能分析与优化建议

日期：2026-11-02。对象：[`入库不良率分析看板.py`](../../../../../app/pages/入库不良率分析看板.py) 及其 Yield 服务、领域计算、数据适配器和图像组件。

状态：诊断与实施建议，**未修改生产实现，未改变业务规则**。以工作区当前代码为准，包含本次任务开始前已有的未提交改动；不将 HEAD 当作当前运行版本。年度全产品半年／季度／月度看板不在本页调用链中，不能用其历史聚合替换本页 MWD。

## 1. 结论与优先顺序

优先减少同一输入的重复计算、重复 Excel 读取和首次展示前的强制工作量，再处理纯计算热点。页面已经有多层缓存、Code 筛选 fragment 和自动预警图像 memo；问题不宜归结为“缺少缓存”。

1. **P0：共享底层输入身份。** 趋势、Lot、Sheet 的指标 revision 被贯穿传入 Panel、修饰上下文、Code MWD 等共享计算，导致冷缓存下构建多套相同结果；Sheet 又以自己的签名获取 Lot，形成第二次 Lot 计算。
2. **P0：复用一次覆盖表读取。** `load_rate_overrides` 每次直接启动 Excel COM；当前冷链路会从两次 Lot 计算和一次 Sheet 计算进入读取。先共享经过校验的原生解析结果，再考虑普通 XLSX 直读。
3. **P1：按业务区分阶段显示、按需加载详情。** Sheet、Mapping 和自动命中 Code 的图像构建都位于宏观图之前；折叠状态不能省下预建图成本。保留预警先于详情独立发布的契约，让宏观图和结构化预警尽早可用。
4. **P1：减少大型缓存结果的反序列化和重复遍历。** `get_data_health` 只是取元数据，却通过返回完整 DataFrame 的缓存入口；按 Code 绘图又重复拼接同一份 Lot 明细。
5. **P2：数据库计划与计算内核专项优化。** Array 时间查询的底层扫描规模尚未测量；Mapping 的逐行坐标处理、Sheet 分配和 MWD 聚合需在消除重复后重新排序。不得直接重写随机分配和级联算法。

## 2. 当前调用链与成本放大方式

```text
页头、配置、三种指标签名、build_cache_context
  └─ generate_yield_alert_publications
       ├─ trend signature
       │    ├─ get_data_health → raw Panel
       │    ├─ get_mwd_trend_data → modified Panel → Code MWD → modifier context
       │    └─ 最新预警 + 全历史预警 → 趋势结果发布
       └─ lot signature
            ├─ get_data_health → raw Panel
            └─ get_lot_defect_rates
                 ├─ modified Panel、Array 时间、Code MWD、modifier context
                 ├─ 警戒线、覆盖表、Lot 分配／截断／覆盖
                 └─ Lot 结果发布
  └─ 读取已发布预警
  └─ sheet signature
       ├─ get_data_health → raw Panel
       └─ get_sheet_defect_rates
            ├─ modified Panel、Array 时间
            ├─ get_lot_defect_rates（sheet signature，无法命中上面的 lot entry）
            └─ 覆盖表、Lot → Sheet 分配、重聚合
  └─ get_mapping_data（trend signature）
  └─ 自动预警全部命中 Code 图像预建、memo、折叠容器
  └─ Group 月／周／日宏观图
  └─ 候选 Code 并集 → 手动 Code fragment
```

### 2.1 缓存存在，但共享层被拆成三套

页面第 52、57、61 行分别使用 `yield_trend_fluctuation`、`yield_lot_oos`、`yield_sheet_oos` 构造签名。即使三种 revision 都为 `0`，签名中仍包含不同指标名，三个字符串也不同。`yield_service.py` 的 `get_raw_panel_details`、`get_modified_panel_details`、`get_modifier_context`、`get_code_level_trend_data` 和 `_get_array_times` 均将该字符串作为缓存参数。

| 冷缓存下的工作 | 当前链路的预期次数 | 原因 |
|---|---:|---|
| 原始 Panel 读取／投影 | 3 | trend、lot、sheet 三套 key |
| 缺陷倍率处理 | 3 | 跟随三套 Panel key |
| 修饰表上下文／Code MWD | 各 3 | Lot 和 Sheet 的下游仍携带自己的 key |
| Lot 完整计算 | 2 | 一次供 Lot，一次由 Sheet 以另一 key 请求 |
| Array 时间读取 | 2 | Lot 与 Sheet 的 key 不同 |
| 覆盖表读取 | 3 | 两次 Lot 加一次 Sheet |

上述次数指单产品、同配置、同窗口、冷缓存的本页链路，不表示数据库必然执行三次 Panel SQL：第二、三套 raw cache miss 仍可能命中同一个磁盘快照。Array 时间没有同等持久快照保护，须单独测量。

关键源码：[`yield_service.py`](../../../../../src/yield_domain/application/yield_service.py) 的 `get_lot_defect_rates`（517）、`get_sheet_defect_rates`（589）及其第 622 行附近的嵌套 Lot 调用；[`indicator_cache.py`](../../../../../app/components/indicator_cache.py) 的签名构造。

### 2.2 Excel 与数据健康检查

- [`rate_override_repository.py`](../../../../../src/yield_domain/infrastructure/rate_override_repository.py) 的 `load_rate_overrides`（27）不区分普通／加密 XLSX，直接 `CreateObject("Excel.Application")`、Open、UsedRange、Close、Quit。警戒线读取已经有普通 XLSX 路径，不能把两者混为一谈。
- [`modifier_table_service.py`](../../../../../src/yield_domain/application/modifier_table_service.py) 的 `sync_modifier_table`（29）读取 Group、Code 两个 Sheet，计算当月及窗口内历史月参考值；写模式还可能同步工作簿和签名。重复调用不一定每次重写整本，但读取和计算依然发生。
- `get_data_health`（199）命中 `get_raw_panel_details` 时仍需取回完整 DataFrame，才提取 attrs；`st.cache_data` 的命中并不等于零复制。
- [`alert_result_workbooks.py`](../../../../../src/yield_domain/infrastructure/alert_result_workbooks.py) 的 `write_product`（113）已有 fingerprint 与记录比较；相同发布可跳过实际写入。不能将“每次调用生产用例”描述成“每次重写两个结果文件”。但是预警历史计算、策略读取、结果检查仍会运行。

### 2.3 首次可用时间与绘图

- 页面第 163、174 行在宏观图前加载全量 Sheet 与 Mapping，且基础校验把 Sheet 成功作为继续展示的条件。虽然预警结果先发布，宏观图仍要等待后续详情成功。
- [`yield_dashboard.py`](../../../../../app/sections/yield_domain/yield_dashboard.py) 的 `render_alert_code_expanders`（1265）先 `collect_memoized`，再进入外层 `expanded=False` 的 expander。首次和版本失效时，所有命中 Code 的趋势、Mapping、Lot 图仍会提前构建。相同会话和签名的后续运行已有 memo。
- `render_code_analysis_section` 已有 `@st.fragment` 和 `should_render` 查询门控；筛选区内的普通控件交互复用传入的数据。Group 宏观选择框在该 fragment 外，仍可能引起整页运行。
- `RenderGate.collect` 是 `[job() for job in jobs]`，集中计算用于减少界面抖动，并非并行执行器。
- `_prepare_lot_code_dataframe`（667）为每个 Code 重新 concat 全部 Group 的 Lot 数据，再筛当前 Code；手动多 Code 和自动区会放大这项成本。`prepare_union_data_for_filter` 已缓存，但将包含 full／display 周期的多个趋势表拼接后聚合；优化时应先对小型投影评估，不能贸然改候选排序。

## 3. 离线实测与适用边界

实测结果与复现说明见后续表格。所有数值仅代表本机单次离线样本，不是生产 SLA 或完整浏览器端加载时间。

## 4. 业务语义与文档一致性

优化必须保持下列约束；以当前代码、最近业务规则和明确决策共同解释，不能直接照搬较早概述。

| 范围 | 必须保持的行为 |
|---|---|
| 查询／时间 | 起点为当前日期回退三个月后所在月 1 日，可能覆盖四个自然月；源时间快照保持完整，返回上层才前推并应用今日中午截止，历史业务日全天保留 |
| 数据策略 | Work Order 在 SQL 边界过滤；非目标 Group 在返回上层时清空缺陷字段，保留投入事实，不能直接删除这些 Panel 以“减量” |
| 缺陷倍率 | 倍率放大生成带后缀的 Panel ID，影响后续 nunique；不能换成简单行权重或提前按原 Panel 去重 |
| Code MWD | 当月指定 → 最近更早月份指定 → 原始月度良损；月中锚点插值、稳定哈希扰动、月内整数总量、每日容量限制 |
| Group MWD | 日／周由最终 Code 日度汇总；月度仅被同月 Group 指定值覆盖，0 也有效；不能用月度覆写结果倒推日／周 |
| 完整周期 | 保留 `weekly_full` 等完整序列供 Lot 和历史发布；不能为了近三周期图表而缩减底层数据 |
| Lot | 当前日期所在月向前两个月月初起的入库窗口、190×30 的投入口径与 20% 过滤；所属 ISO 周 Code 良损、周总量整数分配、稳定随机扰动、警戒线截断、覆盖顺序 |
| Sheet | 使用完整 Sheet 名册分发 Lot Token，保留单 Sheet 容量、软上限、溢出再分配与覆盖后 Group 重聚合 |
| Mapping | 最新五个有效批次、Code 月度倍率边界 `[0.3,3.0]`、坐标规则、级联 0.95、固定抽样、热点脚本优先级与模式守恒边界 |
| 预警发布 | Lot／趋势独立成功或失败、先于 Sheet／Mapping、真实来源健康与 TTL、失败保留最后成功记录、结果文件缺失可重新生成、人工释放状态保留、原子替换和跨进程锁 |
| UI／缓存 | 启用产品范围；手动资源刷新策略；缓存只存原生载荷；普通模式的图表、hover、列菜单和导出不携带内部字段；每项指标／产品刷新隔离 |

发现两处需要在未来实现时核对的知识差异，本次没有改写其权威文件：

1. [`algorithm-mapping.md`](../../../../../references/domain/yield_domain/mapping/algorithm-mapping.md) 首节及末节仍称 Mapping 不读取 MWD 月度目标／不存在月度换算依赖；当前 `get_mapping_data` 已把 `get_modifier_context()["factors"]` 传入 `prepare_mapping_data`，且 [`algorithm-mwd-trend.md`](../../../../../references/domain/yield_domain/mwd_trend/algorithm-mwd-trend.md) 明确 Code Sheet 为 Mapping 提供倍率。准确理解是：**共享维护目标来源，随后独立级联，不要求最终数量相等**。
2. [`algorithm-sheet-lot.md`](../../../../../references/domain/yield_domain/sheet_lot/algorithm-sheet-lot.md) 的“无可用周度数据保留原始结果”应区分两种情况：完整周表缺失时，代码保留原结果；完整周表存在但某周未命中时，该周按 0，不回退原始 Lot 或日度。概览和 `simulation.py` 的当前实现已经区分。

概览仍有旧方法名、EMA 描述，以及“2 天回刷”字样；当前 MWD 是指定目标日度生成，仓储回刷读取全局配置，本工作区为 7 天。它们不能作为优化时恢复旧算法或缩短回刷的依据。

领域来源：[`MWD 规则`](../../../../../references/domain/yield_domain/mwd_trend/algorithm-mwd-trend.md)、[`指定良损设计`](../../../../../references/design/yield_domain/mwd_trend/algorithm-mwd-trend-specified-loss.md)、[`Sheet/Lot`](../../../../../references/domain/yield_domain/sheet_lot/algorithm-sheet-lot.md)、[`预警结果规则`](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)、[`ADR-0035`](../../../../ADR/0035-published-alert-results-for-matrix.md)、[`ADR-0002`](../../../../ADR/0002-performance-safe-smoke-and-cpm-aggregation.md)。

## 5. 分批实施建议

### P0-A：分离底层数据版本与展示指标版本

建议由 application/composition 管理共享计算输入身份，页面仍使用各指标 revision 管理结果与图像失效。不得直接把三种签名永久写死为相同常量。

- Panel key：产品、窗口、数据策略、真实源刷新 revision、日期前推与截止策略。
- modified Panel key：Panel 身份、缺陷倍率等实际处理参数。
- modifier／Code MWD key：上述输入、维护资源手动 revision、日期、`read_only` 及影响算法的配置。
- Lot key：Code MWD 身份、Array 时间版本、警戒线／覆盖资源 revision、Lot 策略。
- Sheet key：同版 Lot 结果身份、Sheet 策略、覆盖资源 revision。
- 页面指标 revision：保留各指标／产品的定向失效，但通过显式刷新编排决定是否也推进共享源身份。原有完整刷新成功后，所有受影响派生数据必须换版。

一次 application 计算过程中复用同版原生结果，Sheet 消费同次 Lot；跨请求仍走有界 `st.cache_data`。暂不将大型可变 DataFrame 改为全局 `cache_resource`，也不通过删除 key 字段破坏策略隔离。读模式与写模式的副作用差异必须保留；不能因详情预热了只读结果而跳过页面应执行的维护同步。

验收以调用次数和结果一致性为主：冷缓存的 Panel、修饰、Code MWD、Lot、Array 各只执行一次；切产品、切时间、策略变化和明确刷新应正确失效。对当前需独立指标 key 的测试应改为“展示隔离＋共享输入正确失效”的行为断言，而非简单删除已有断言。

### P0-B：覆盖表与修饰表读取复用

先在一次报告上下文内只读一次覆盖表，把经过校验的 DataFrame 同时交给 Lot 和 Sheet。进一步按**配置完整路径、Sheet、手动 revision** 缓存原生解析载荷；保留产品和资源隔离、空 Sheet 的合法语义、读取失败向上抛出的行为。

普通 XLSX 可参照 warning reader 增加直接解析，企业加密／权限保护文件仍保留 COM 回退。统一沿用现有百分比、空值、重复与缺 Sheet 规则，不能简单调用 `pd.read_excel` 后跳过这些业务解析。COM 生命周期必须由创建方负责；失败／中断不影响用户原有 Excel 会话。

修饰表的两个 Sheet 可考虑一次打开、一次解析；当月／历史月的重复 Panel 聚合可预先形成 `(month, group, code)` 投影，再保留“历史只补缺行、当前可更新、人工指定值不覆盖”的写规则。整本写回必须保留现有原子边界，不在页面直接持久化。

### P1-A：宏观结果先可用，详情明确触发

建议顺序：配置与健康检查 → 共享 MWD／Lot、独立预警发布 → 结构化预警和 Group 图 → 用户请求的 Sheet／Mapping／Code 图。

- 移除宏观显示对 Sheet 成功的依赖；Sheet／Mapping 失败仅影响对应详情区，继续使用安全业务提示。
- 自动区先展示命中 Code 列表，真正展开／选择时才构建对应 payload。仅设置 `expanded=False` 不足以实现懒加载。
- Sheet 可先保持一次整份结果按需加载，避免直接按 Code 重跑分配而改变 RNG 消费顺序；如果以后按 Lot 做增量计算，必须另行证明逐实体结果完全一致。
- Mapping 先延迟整份读取，之后再考虑已计算结果的轻量切片；先按筛选 Code 再执行级联可能改变批次集合和级联状态，不作为直接优化方案。
- Group 多选可考虑独立 fragment；Code 已有 fragment，应保留。正式引入动态 expander API 前需按项目安装版本核对公开 API，并为界面状态添加针对性验证。

### P1-B：轻量元数据与公共绘图索引

将来源健康随共享报告原生 payload 一次返回，或通过只返回小字典的缓存入口获取。小字典与数据必须绑定同一源版本／有效期，不能绕过刷新或把 stale 改成 fresh。

在一次图像准备中构建统一 Lot 明细与 `(group, code)` 索引；Mapping 坐标／矩阵也按输入版本、布局、脚本、批次、Code 共享计算。缓存结果仍为 DataFrame、数组和字典；图表对象在展示侧构建。对首次样式初始化、Python 绘图、JSON 序列化、浏览器渲染分开测量，避免只优化服务端却忽略图像数量和传输体积。

### P2-A：数据库先探查，再比较执行计划

本次没有执行生产 SQL，以下为代码风险点和后续验证任务，不是已测数据库根因：

- Panel 查询已经按产品、入库日期和工单约束，批次表已经直接过滤 `timekey` 分区，并按产品／Sheet 确定性选最新批次；本工作区还已移除最终跨表排序。不能把这些当作待新增功能。
- 仓储每段 `current_start + 30 days` 且首尾包含，通常相当于 31 个日历日；完整冷窗口分段串行查询。先记录各段行数、耗时、网络传输、Pandas 排序与快照落盘，不宜先并发查询放大数据库负载。
- Array 时间查询使用 `eda.spot_eda_array_hst_v`、`step_id='10000'` 与 `SUBSTR(sheet_id,1,9) IN (...)`，按 Sheet 求 `MIN(sheet_start_time)`，没有时间条件。Lot 集合虽有限，视图底层仍可能扫描大量历史；两个缓存身份还可能重复该查询。
- 先通过 pg_class／pg_stats／分区元数据和视图定义估算底层规模，必要时执行不带 ANALYZE 的 EXPLAIN；设置本次会话语句超时。不要用无界 COUNT、全表读取或 EXPLAIN ANALYZE 代替探查。
- 在得到计划后评估参数绑定、Lot 列／表达式索引、站点选择性、有限 Lot 分批、Sheet 键关联或独立的受控 Array 时间结果复用。不能随意加三个月下界：它可能丢失更早的首次 Array 投入，从而改变 MIN 语义。
- Panel、缺陷、产品、批次关联的索引／裁剪效果由实际执行计划确认；任何数据库索引或视图变更单独实施，本文不声称已经建立索引。

### P2-B：纯计算投影与诊断输出

先评估 Sheet／Lot 基础信息、去重投入、原始 Code 分子等纯聚合能否一次准备、多消费者复用。保持 nunique 键、排序、空值、分母和合成 ID，不能用普通 count 替代。

Mapping 的 `apply(axis=1)` 会逐条解析布局、构造映射字典、生成稳定种子和 RNG；可先缓存不可变布局解析和同一 `(panel_id,batch,layout)` 的重复结果，但须证明抽样输入顺序和每个坐标完全一致。**不静态重构 Mapping 级联、`_simulate_concentration` 或 `_distribute_sheet_from_lot`。** 若消除重复后这些内核仍主导耗时，应以专项动态证据及等价回放评估。

`simulation.py` 第 446 行附近仍在 Lot 模拟中收集全部调试明细、格式化百分比并写入固定 `output/logs/sheet_lot_processor-lot_rate.csv`；Sheet 中还有特定 Lot 的追踪日志。建议后续通过显式诊断开关控制，避免常规运行分配、格式化和重复覆盖文件；是否值得优先处理取决于细分计时，当前不据此宣称主要根因。

## 6. 验证、收益门槛与回退

| 验证层 | 具体场景与通过标准 |
|---|---|
| 调用放大 | 单产品首次分析统计底层 miss 次数；P0 后共享项各一次，覆盖表一次，Sheet 不重算 Lot |
| 数值等价 | 同版输入比较 Group／Code 的 full 与 display 周期、Lot／Sheet 明细、Mapping Panel／矩阵；整数与 ID 精确一致，浮点默认精确比较，任何容差需显式业务依据 |
| 随机顺序 | 固定种子、跨周跨月、低良损、零目标、多产品布局、热点配置；逐 Lot／Sheet 结果一致，不能只比总量 |
| 缓存与刷新 | 冷进程、同进程热命中、切产品、日期变化、今日截止、各指标单独刷新、原始源刷新、维护表手动 revision、只读／写模式、TTL 到期 |
| 故障 | 数据库失败的既有快照降级；维护表失败不缓存为合法空值；一类预警失败不阻止另一类；Sheet／Mapping 失败不阻止结构化预警发布 |
| 发布文件 | 同输入不改写；文件缺失能恢复；失败元数据可见；保留已有产品 Sheet、人工释放标记；并发锁和原子替换有效 |
| 普通界面 | 首屏宏观可用、无数据／失败提示、图像按需请求、Code fragment 不重新生产预警；列菜单、hover、下载不出现内部字段 |
| 数据库 | 低成本规模探查和计划留证后，受控窗口记录 SQL 时间、行数、分段、超时和 fallback；不以本次离线耗时宣称 SQL 已改善 |

推荐先以 P0 调用次数归一、全部结果等价作为硬门槛；耗时采用相同机器、相同冻结输入的多轮独立进程中位数和范围，至少分别测量冷 L2／热 L1、热 L2、实际受控数据库刷新、首次图像和后续交互。内存应采样峰值 RSS；仅记录结束 RSS 不能证明峰值下降。

本次单次试验不能设定可靠的秒级 SLA；P0 上线前再根据基线约定冷／热耗时目标。不要把可避免的重复次数直接乘成端到端加速倍数，也不要累加存在调用嵌套的 profiler 累计时间。

建议每批独立提交并保留现有入口适配：P0 计算共享、Excel 读取、界面懒加载分别可回退。资源工作簿、人工值、原始快照和数据库生命周期不得随优化删除或重置。回退时保留已发布成功数据与释放决策。

已有验证入口可复用：`tests/unit/yield_domain/application/test_yield_cache_correctness.py`、`test_alert_publication.py`、`tests/unit/app/pages/test_yield_dashboard_cache_wiring.py`、`test_yield_alert_publication.py`、`tests/unit/app/sections/yield_domain/test_code_analysis.py`、`test_yield_alert_code_expanders.py`，以及 `tests/unit/test_modifier_table.py`、`test_sheet_lot_weekly_baseline.py`、`test_sheet_lot_processor.py`、`test_mapping_monthly_factor.py`、`test_mapping_original_pipeline.py`、`test_mapping_layout.py`。本文仅列出后续实施的相关验证范围，不声称这些测试已全部运行。

本次只新增分析文档；权威业务规则、架构和知识路由均未改变，因此不改写 `references/index.md`、`CONTEXT.md` 或 `ARCHITECTURE.md`。生成物位于跨子模块的 `yield_domain/shared/`，符合现有目录职责。
