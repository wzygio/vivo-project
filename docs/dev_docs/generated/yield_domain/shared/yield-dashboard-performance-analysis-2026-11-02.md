# 入库不良率分析看板性能分析与优化建议

初次诊断：2026-11-02；实施更新：2026-11-03。对象：[`入库不良率分析看板.py`](../../../../../app/pages/入库不良率分析看板.py) 及其 Yield 服务、领域计算、数据适配器和图像组件。

状态：**前五项 P0/P1 已实施**，Array 时间查询和排序已暂停，相关代码保留。第 2～3 节保留 11 月 2 日的历史诊断与采样，不代表当前实现；第 5 节的 P0/P1 保留原方案，第 7 节记录落地结果及本次验证。最新权威数据流见 [Yield 报表数据流](../../../../../references/domain/yield_domain/shared/data-flow-yield-report.md)。年度全产品半年／季度／月度看板不在本页调用链中，不能用其历史聚合替换本页 MWD。

## 1. 结论与优先顺序

优先减少同一输入的重复计算、重复 Excel 读取和首次展示前的强制工作量，再处理纯计算热点。页面已经有多层缓存、Code 筛选 fragment 和自动预警图像 memo；问题不宜归结为“缺少缓存”。

1. **P0：共享底层输入身份。** 趋势、Lot、Sheet 的指标 revision 被贯穿传入 Panel、修饰上下文、Code MWD 等共享计算，导致冷缓存下构建多套相同结果；Sheet 又以自己的签名获取 Lot，形成第二次 Lot 计算。
2. **P0：复用一次覆盖表读取。** `load_rate_overrides` 每次直接启动 Excel COM；当前冷链路会从两次 Lot 计算和一次 Sheet 计算进入读取。先共享经过校验的原生解析结果，再考虑普通 XLSX 直读。
3. **P0：避免百万行重复格式化月份。** Code MWD 的 `_compute_code_raw_monthly_targets` 对整份明细执行 `dt.strftime("%Y-%m")`；微基准中这一操作中位数为 6.16 秒，唯一日期格式化后映射为 0.09 秒，输出键序列逐项一致。这只是键生成提速，不能直接换算为整页收益。
4. **P1：按业务区分阶段显示、按需加载详情。** Sheet、Mapping 和自动命中 Code 的图像构建都位于宏观图之前；折叠状态不能省下预建图成本。保留预警先于详情独立发布的契约，让宏观图和结构化预警尽早可用。
5. **P1：减少大型缓存结果的反序列化和重复遍历。** `get_data_health` 只是取元数据，却通过返回完整 DataFrame 的缓存入口；按 Code 绘图又重复拼接同一份 Lot 明细。
6. **P2：数据库计划与计算内核专项优化。** Array 时间查询的底层扫描规模尚未测量；Mapping 的逐行坐标处理、Sheet 分配和其他 MWD 聚合需在消除重复后重新排序。不得直接重写随机分配和级联算法。

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

### 3.1 样本与隔离方式

- 本机 Windows、项目 `.venv`，Streamlit **1.60.0**；产品 **M626**。没有启动 Streamlit／HTTP 测试服务，没有执行生产数据库查询，也没有改变正式服务。
- 冻结本地 Parquet 和相关 XLSX 输入后运行，Parquet 大小 11,243,734 字节，**1,258,622 行**；按现有时间前推、今日截止、窗口和目标 Group 策略投影后为 **1,238,235 行**、406 个 Lot、7,565 个 Sheet、60 个 Code。分析窗口为 **2026-08-01～2026-11-02**。
- 数据读取端口替换为离线适配器，阻断 `DatabaseManager` 构造及 `pd.read_sql`；Array 时间为空表、覆盖结果为 `(None,None)`。修饰表／警戒线／Mapping 配置来自冻结副本，修饰同步使用 `read_only=True`；因此没有验证真实 Array 时间、覆盖值、维护写回和企业文件 COM 的完整业务输出。
- 保留真正的服务缓存和领域计算。通过替换端口解析器隔离 I/O，没有用显式 `_data_port` 调用绕过生产缓存。每组进程先清理自己的内存缓存，再运行一次 cold 和一次 warm。
- “共享输入试验”只在诊断进程中统一 Panel、modified Panel、modifier、Code MWD、Array、Lot 六个入口的 `snapshot_signature`；产品、配置、窗口、维护签名和 `read_only` 不变。**它证明该样本存在重复开销，不是可直接上线的刷新设计**。

### 3.2 无 profiler 的冻结输入对照

表中阶段互不重叠；Lot、Sheet 阶段各自包含其嵌套依赖。合计包括趋势历史判定和候选集准备，不包括后面的独立图像样本。

| 服务链阶段 | 当前 cold（秒） | 共享输入 cold（秒） |
|---|---:|---:|
| 趋势健康读取 | 3.12 | 3.38 |
| Group MWD（包含首次 Code／修饰上下文） | 13.90 | 14.02 |
| 再次获取 Code MWD | <0.01 | <0.01 |
| Lot 健康读取 | 7.70 | 0.77 |
| Lot 结果 | 20.32 | 3.21 |
| Sheet 健康读取 | 3.55 | 0.60 |
| Sheet 结果（包含嵌套 Lot） | 22.26 | 3.96 |
| Mapping | 3.19 | 3.10 |
| 全历史趋势判定 | 0.30 | 0.31 |
| 候选 Code 并集 | 0.08 | 0.07 |
| **服务链合计** | **74.42** | **29.44** |
| 同进程 warm 合计 | 2.08 | 2.05 |

这组 cold 观察值减少 **60.45%**。输入文件 SHA-256、有效配置摘要和窗口一致；Group、Code、Lot、Sheet、Mapping 五类结果的冷／热、两模式摘要全部一致。摘要按列名、DataFrame 索引与逐行值哈希生成，未保存业务明细；它是本次样本一致性证据，不能替代后续多产品逐字段回归。

当前模式 warm 的三个健康读取合计约 **1.71 秒**，约占 **82%**。共享输入后 warm 总时间基本不变：共享解决冷计算放大，却没有消除为了 attrs 反序列化百万行 DataFrame 的成本。

实际内部函数计数与累计秒数如下；这些函数与上表存在嵌套，不与上表相加。

| 内部工作 | 当前次数／累计秒 | 共享次数／累计秒 |
|---|---:|---:|
| 离线 Panel 读取和投影 | 3／11.22 | 1／2.67 |
| 修饰上下文同步（只读） | 3／7.29 | 1／2.41 |
| Code MWD 核心 | 3／26.00 | 1／8.28 |
| Group MWD 核心 | 1／0.09 | 1／0.09 |
| Lot 核心 | 2／7.10 | 1／2.50 |
| Sheet 核心 | 1／3.38 | 1／3.27 |
| Mapping 核心 | 1／2.57 | 1／2.54 |
| Array 端口调用 | 2 | 1 |
| 覆盖表端口调用 | 3 | 2 |

覆盖端口已使用空替代，因此不能从该表推断真实 Excel 读取的毫秒成本。当前 M626 配置未触发缺陷倍率函数，不把三次 modified Panel 入口误报为三次实际倍率放大。

另外，为三个满足月均门槛的 Code 构建紧凑图像材料，当前／共享模式合计分别约 **1.18／1.20 秒**；没有选择 Sheet Lot，未包含实际 Sheet 详情图、`st.plotly_chart` 序列化和浏览器渲染。样本最新趋势命中 Code 数为 0，三个 Code 是额外选取的绘图样本，不能当作实际全部自动预警图像的数量或总耗时。

### 3.3 月份键专项微基准

[`data_preparation.py`](../../../../../src/yield_domain/core/mwd_trend/data_preparation.py) 的 `_compute_code_raw_monthly_targets` 第 74 行附近，对全量明细和缺陷子表分别执行日期转月份字符串；原始月率计算还会在三套 Code MWD 中重复。

对冻结快照的 **1,258,622 个日期**单独测量三轮：

| 月份键生成方式 | 三轮中位数（秒） | 一致性 |
|---|---:|---|
| 现有 `dt.strftime("%Y-%m")` | 6.164 | 参照 |
| `dt.to_period("M").astype(str)`，恢复原空值蒙版 | 0.601 | 与参照逐项一致 |
| 唯一日期格式化，建立映射后 `Series.map` | 0.088 | 与参照逐项一致 |

使用 `pd.testing.assert_series_equal` 检查整个序列，另检查空日期、月末／月初和跨年。微基准仅覆盖月份键，未改动生产函数，也未测全量业务函数替换后的总收益；时区、非法输入和完整 MWD 输出仍需实施时验证。

### 3.4 COM 探测、采样限制与证据

另一次冻结输入的独立 cProfile 运行显示以下累计时间；包含 profiler 本身开销，用于定位调用内部，不与上面的 wall-clock 计时混算：

| 函数 | 调用次数 | 累计秒 |
|---|---:|---:|
| `create_code_level_mwd_trend_data` | 1 | 8.55 |
| 其中 `prepare_code_raw_data` | 1 | 7.94 |
| 其中 `_compute_code_raw_monthly_targets` | 1 | 7.11 |
| `_distribute_sheet_from_lot` | 1 | 4.32 |
| `prepare_mapping_data` | 1 | 3.77 |
| 其中 `_modify_panel_position` | 29,008 | 2.13 |
| `_pickle.loads`（cold、warm 及依赖取回合计） | 23 | 5.58 |

这进一步支持优先处理月份键生成与共享结果取回成本；Sheet 内核也有成本，但其不受保护的外层聚合与缓存复用应先优化。表中的“其中”行已经包含在父项中。

早期允许覆盖表读取临时副本的探索中，曾观察到 Lot 阶段约 33 秒、Sheet 阶段约 102 秒；另一次调用在 `comtypes.client.CreateObject("Excel.Application")` 处抛出 `OSError`（WinError `-2146959355`），最终成为 `RateOverrideReadError`。这些运行没有形成稳定、完整的受控配对，**不据此计算 COM 总占比或生产耗时**。它们说明该外部读取路径应被单独计时和消除重复。现有读取器已执行其 Close／Quit finally；失败激活未能取得的 Office 对象无法按本次进程身份完整核实，未对其他 Excel 进程执行停止。

分析期间维护表的字节版本曾变化，因此最终对照使用单独冻结输入。各次运行的系统负载也未隔离；探索阶段 cold 出现约 58～74 秒波动，最终单对数值不代表可靠的长期改善比例。端到端尚未测量的成本包括：实际 Panel SQL／增量／冷 L1、Array SQL、覆盖读取、可写修饰同步、两类预警工作簿发布／读取、页头初始化、全部实际命中图像、浏览器传输与渲染。结束 RSS 只是进程结束前读数，不是峰值。

汇总证据：[`yield-dashboard-performance-evidence-2026-11-02.json`](yield-dashboard-performance-evidence-2026-11-02.json)。诊断脚本与完整阶段 JSON 位于 `output/tmp/yield_performance_20261102/`，属于可清理的本地证据，不是生产入口或长期依赖。复现命令：

```powershell
.venv/Scripts/python.exe output/tmp/yield_performance_20261102/probe.py
.venv/Scripts/python.exe output/tmp/yield_performance_20261102/probe.py --shared-inputs
.venv/Scripts/python.exe output/tmp/yield_performance_20261102/probe.py --shared-inputs --profile
.venv/Scripts/python.exe output/tmp/yield_performance_20261102/month_key_probe.py
.venv/Scripts/python.exe output/tmp/yield_performance_20261102/check_evidence.py
```

前两条用于无 profiler 对照；`--profile` 只定位内部热点，其耗时不可与无 profiler 的时间直接相减。脚本复用本地冻结副本；副本删除后重新运行会形成新基线。发布前仍需生产受控窗口基线，不能用本次探针替代完整页面验收。

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

### P0-C：月份键只生成一次，避免逐行格式化

在 `prepare_code_raw_data` 的日期规范化之后生成月份键并复用到月度分母、缺陷分子。优先试验“对唯一日期格式化后映射”或带原空值蒙版的 Period 键；不要在全量和缺陷子表各做一遍百万级 `strftime`。也可直接用月周期做内部 groupby，只有返回小型目标字典时才转字符串，但需校验键顺序和空月补齐行为。

此项只优化纯准备阶段，不进入稳定随机分配或 Mapping 级联。验收应先比较月份键和 `_compute_code_raw_monthly_targets` 的完整字典，再比较 Code／Group 所有周期结果；保留 `nunique(panel_id)`、零投入月份、跨月／跨年、NaT、合成 Panel ID，以及同一 Code 多条缺陷记录的语义。微基准已经证明键序列可等价生成，尚未证明生产函数替换后的端到端收益。

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

这一组解决的是：**从数据库把所需数据拿到本机，究竟慢在哪一步，能否在保持同样结果的前提下少找、少搬、少排一次数据。** 它不是一项已经确定的 SQL 改写，也不是要求同时做完下面所有动作。

可以把数据库理解成仓库。拿货慢，可能是目录不好找、一次搬得太多，也可能是搬到本机后又花时间整理。先分段计时，才知道应修哪一段。下面依次解释原建议的含义。

1. **先把取数时间拆开，找到真正慢的阶段。** 记录每个产品、每个时间分段的数据库执行、传回本机、本地排序去重和保存 Parquet 分别花多久、返回多少行。例如“数据库查得快，本地排序慢”和“数据库本身查得慢”需要完全不同的处理。当前 Panel 已按产品、日期、工单过滤，批次表也已有 `timekey` 分区限制与去重；这些已经存在，不是再做一次的优化目标。
2. **先查规模和预计路线，避免用大查询做诊断。** 读取数据库已有的行数／分区／索引统计，了解底层表有多大；再用不带 ANALYZE 的 EXPLAIN 看数据库预计如何找数据。这相当于先看仓库目录和取货路线，不先把所有货搬出来重新数。不能执行无界 COUNT、全表读取或带 ANALYZE 的大查询作为规模探查，受控查询也应有时间和返回量限制。
3. **只有路线确实有问题，才评估索引或关联方式。** 索引像按产品、日期做好的目录；关联方式像先拿到本次需要的 Sheet 清单，再找对应批次。要看实际计划，确认哪个步骤扫描太多、哪个关联产生重复行，再选择具体字段或改写方式。并非给所有字段加索引：索引占空间，也增加写入维护成本。数据库对象变更需要另行评估，不在本轮实施。
4. **按测量结果调整取数分段，而不是先增加并发。** 当前按大约一个月分块串行取数，代码首尾都包含时一块通常覆盖 31 个日历日。可以在探查规模后比较有限窗口：小块是否更稳，大块是否减少往返。很多小块会增加往返，多个大查询同时跑又可能压垮数据库，因此不能直接理解成“把四段改成四个线程”。不论怎样分块，都要保留完整窗口、超时和失败回退。
5. **Array 查询先搁置，恢复前再单独检查。** 原查询为有限 Lot 查所有历史中的最早 Array 投入时间，但按 Sheet 前缀匹配可能仍扫描大量底层历史。将来恢复时，应先看视图底层规模和执行计划，再考虑合适的键／索引、有限 Lot 分批或结果复用。不能简单加“只查近三个月”：真正最早投入可能早于三个月，查出来的日期就会错。**本轮已按要求关闭 Array 查询，所以上述优化现在不执行。**

最后用同一产品、同一有限时间窗口核对结果、行数和耗时；任何方案如果改变去重、批次选择、最早时间或失败时的快照回退，就不算等价性能优化。本轮没有查询生产数据库，没有创建索引或改写这些 SQL。P2-A 是下一轮有证据再选项的数据库专项工作。

### P2-B：纯计算投影与诊断输出

这一组解决的是：**数据已经在本机后，能否少做重复整理，少做无用输出。** 它与 P2-A 的数据库取数是两件事，也不等于“换掉良率算法”。

1. **复用“整理好的小表”，减少重复扫描大明细。** 比如 Lot／Sheet 的投入数、所属日期、各 Code 的不良 Panel 数，多个计算都要用。如果确认口径相同，可以从大明细整理一次，再给多个步骤使用。本轮 P0/P1 已完成其中一部分：共享 Panel／Lot 结果、批量月份参考值、共享绘图索引。P2-B 剩下的是检查其他底层基础聚合还有没有重复，不是把已经完成的工作再计一遍。
2. **减少 Mapping 每个 Panel 的重复解析。** 每条记录目前会读膜位、找行列编号、准备布局映射，并按既定种子生成坐标偏移。对于同一个布局，不必每次重建一模一样的行列查找表；同一个 Panel／批次／布局若重复出现，也可能复用纯坐标解析结果。本轮缓存的是已生成的展示矩阵；P2-B 说的是更靠前的逐行准备阶段，尚未改写它。
3. **让专门排查问题的日志按需产生。** Lot 模拟目前仍会收集调试明细、将比率转成百分比文字，再写 `output/logs/sheet_lot_processor-lot_rate.csv`，Sheet 中还有特定 Lot 的追踪日志。平时看图不一定需要这些文件。未来可以加显式诊断开关：需要排查时打开，平时省掉额外复制、文字格式化和磁盘写入。本轮保留这些诊断代码与既有文件，没有删除用户数据，也没有把它的耗时算作已获得的收益。
4. **重新测量，再决定是否值得继续。** 本轮重复计算和详情提前加载已经减少，剩余热点顺序可能改变。先测上述基础聚合、坐标解析和日志各占多少；占比很小的部分无需急着优化。如果最终主要耗时来自随机分配内核，另开专项动态分析，不能直接重写内核来追求快。

等价边界需要严格理解：同一 Panel 有两条缺陷记录，投入数仍可能只算一个 Panel，不能把去重数改成行数；增加数量时使用的合成 ID 也不能丢。随机种子相同但调用顺序变了，每张 Sheet 分到的不良数量仍可能变化。因此 **不静态重构 Mapping 级联、`_simulate_concentration` 或 `_distribute_sheet_from_lot`**；未来优化坐标准备也要逐条比对坐标、每个 Code／批次／Lot／Sheet 的结果，不能只看总体不良数相等。

P2-B 的预期结果是“相同算法、相同明细结果，更少重复准备和非必要输出”。它仍是后续建议，本轮没有做底层聚合重写、坐标随机算法替换或日志开关改造。

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

2026-11-02 诊断阶段已完成：当时的 current／shared／profile 三组诊断运行成功；输入／输出摘要与月份键检查通过，文内当时的 18 个本地链接可解析。该阶段没有生产实现变更。后续实施及本次回归以下节为准。

以上为 2026-11-02 诊断阶段的交付记录；2026-11-03 的实现、文档与验证以下节为准。生成物位于跨子模块的 `yield_domain/shared/`，符合现有目录职责。

## 7. 2026-11-03 实施结果

### 已完成的前五项

| 项目 | 本轮落地方式 | 核心验证 |
|---|---|---|
| P0-A 共享输入 | 独立产品输入 revision、实际来源健康代次、请求内 memo；规范化默认参数，Sheet 复用同一 Lot | 调用次数、读／写模式、产品与展示 revision 隔离、并发换代不污染缓存 |
| P0-B Excel 复用 | 覆盖表原生载荷缓存；普通 XLSX 直读，公式与企业保护保留安全 COM；修饰表一次打开和批量月度参考值 | 缺 Sheet、无效字段、普通／数组公式、安全打开／释放、历史与人工值保留、逐月等价 |
| P0-C 月份键 | 唯一日期格式化后映射，分母／分子共用月份键 | 跨年／空日期、重复缺陷、合成 ID、空月及索引场景 |
| P1-A 按需详情 | 宏观图独立 fragment；自动区逐 Code 展开、手动查询后请求；完整 Sheet／Mapping 延迟加载 | 初次进入零详情调用、原生 expander AppTest、关闭时零工作、局部失败和换代重跑 |
| P1-B 轻量缓存 | 健康小字典；一次 Lot 清洗索引；原生 Mapping 矩阵缓存；有界会话图像材料与实际数据摘要 | 健康命中不取整份 Panel、索引等价、布局／脚本／数据版本失效、旧图不复用 |

Array 查询开关位于 `config/domain/yield_domain.yaml:array_input_time.query_enabled`，当前 false。查询、自定义时间和排序分支均保留，运行时在访问端口前直接返回空时间表；排序入口关闭，Sheet 默认按 ID。重新启用时修改该开关并刷新页面配置，恢复前先完成 P2-A 中的 Array 专项检查。

最新数据流与算法已同步到 [报表数据流](../../../../../references/domain/yield_domain/shared/data-flow-yield-report.md)、[MWD](../../../../../references/domain/yield_domain/mwd_trend/algorithm-mwd-trend.md)、[Sheet/Lot](../../../../../references/domain/yield_domain/sheet_lot/algorithm-sheet-lot.md)、[Mapping](../../../../../references/domain/yield_domain/mapping/algorithm-mapping.md) 与 [预警规则](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)，并更新概览、知识路由及架构说明。此前的周度预警／上周 Lot 规则保留，月度数值图与月均筛选不删除。

### 验证方法与限制

验证使用临时工作簿、模拟 COM、Streamlit AppTest 和本地 Parquet。计算对照使用同一真实 Panel 快照、固定的原生维护目标夹具、空 Array 时间／覆盖／警戒线以及相同窗口，阻断生产 SQL 与 Office 创建；比较 Group、Code、Lot、Sheet 和 Mapping 的完整输出摘要以及冷／热调用次数。它验证计算准备与复用收益，**不包含真实企业 Excel、数据库、结果文件发布或浏览器端到端耗时**。

企业保护使早期 XLSX 冻结副本的字节发生变化，相关探索不作为严格配对证据；本轮最终计算对照不读取这些维护文件。普通／受保护工作簿、公式与写回语义由独立测试覆盖。受保护文件依然可能需要 Excel，本轮优化是减少重复打开，并非宣称企业保护不再需要 Office。

### 本次离线对照结果

证据：[2026-11-03 计算回放汇总](yield-dashboard-performance-evidence-2026-11-03.json)。窗口固定为 2026-08-01～2026-11-02，样本为 M626 本地 Parquet，实际投入处理 **1,258,622 行**。两模式使用同一快照字节、同一配置和固定维护输入，五类输出在冷／热两次中的列、类型、索引与逐行值摘要一致。旧实现从本轮修改前的版本提取到私有诊断进程，新实现使用当前工作区；不改正式服务或维护文件。

| 阶段 | 旧实现（秒） | 新实现（秒） |
|---|---:|---:|
| 冷缓存：宏观／预警数据计算 | 37.32 | 7.88 |
| 冷缓存：完整 Sheet／Mapping | 24.92 | 6.70 |
| 冷缓存：两阶段合计 | **62.23** | **14.58** |
| 热缓存：宏观／预警数据计算 | 1.80 | 0.61 |
| 热缓存：两阶段合计 | 1.87 | 0.67 |

完整冷链路在这一对观察值中减少约 **76.6%**。更直接的体验变化是旧页要等完整两阶段才显示宏观图，而新页在第一阶段完成后即显示；第二阶段在用户请求详情时才执行。此处均是 Python 计算准备时间，不能把 7.88 秒解释为实际网络浏览器首屏 SLA。

| 未命中的实际工作次数 | 旧实现 | 新实现 |
|---|---:|---:|
| Panel 读取／投影 | 3 | 1 |
| 修饰上下文准备（本次固定输入夹具） | 3 | 1 |
| Code MWD 核心 | 4 | 1 |
| Lot 核心 | 2 | 1 |
| 覆盖表端口 | 3 | 1 |
| Array 时间端口 | 2 | **0** |
| Sheet／Mapping 核心 | 各 1 | 各 1，且延迟到详情请求 |

旧版 Code 的第四次执行还来自位置／关键字及默认实参形式不同导致的缓存键差异；新版规范化这些形式，而非只把三种指标名称替换成常量。覆盖／Array 端口在此回放中是空输入，因此上表验证调用次数，不表示测量了真实数据库或 Office 的读取时间。修饰表批量聚合与真实 Excel 读取的改进由相应等价／I/O 测试验证，不计入这组固定维护夹具的耗时收益。

### 本次完成的检查

最终一次合并的定向验证 **439 项通过**（约 69 秒），覆盖 Yield 原有烟测、领域应用／基础设施、月份与修饰表、AppTest 原生展开状态、缓存与代次切换、矩阵文件消费者、Excel COM 模拟、跨进程发布及架构依赖。命令与完整目标列表保存在 `output/tmp/yield_optimization_20261103/verify.py`、`verification.json` 和 `verification.log`；计算回放脚本／原始结果在同目录，汇总证据已随本文保留。

本次未连接生产数据库、未运行正式服务的完整浏览器端到端测试，未以真实 Office 启动耗时计算提速比例；未创建临时 HTTP／Streamlit 测试服务。实际企业文件路径的运行开销还需在正式使用中观察。P2-A／P2-B 中尚未实施的数据库和内核工作不会被此次通过数或离线比例替代。
