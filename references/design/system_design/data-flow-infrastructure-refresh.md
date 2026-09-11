# Infrastructure 数据读取、快照与刷新机制

核验日期：2026-09-11。范围：当前工作区 `src/` 的生产事实读取、原始快照、
仓储输出及相关刷新入口；本文描述已实现行为，不代表服务器已经部署相应计划任务。
配置数值以运行环境文件为准。人工修饰、规格及历史汇总文件是另外的持久化对象，
不套用原始事实的增量规则。

## 1. 刷新由什么触发

系统没有一个自动刷新所有领域原始快照的统一后台循环。刷新发生在数据读取进入
仓储后，或调用专门的刷新入口时。

| 入口 | 当前行为 |
|---|---|
| 页面读取 | 应用缓存未命中后进入仓储；仓储检查快照版本、覆盖范围和 TTL，决定复用或查询数据源 |
| 页面刷新按钮 | 通过 application/composition 调用仓储；具体是增量还是完整请求窗口，见各模块说明 |
| Q-Time CLI | `tools/refresh_qtime_snapshots.py` 顺序刷新 ARRAY、OLED、TP；默认增量，`--full` 全窗口重取 |
| Q-Time 计划任务注册脚本 | `tools/register_qtime_snapshot_task.ps1` 默认每天 07:00 调用上述 CLI；可通过 `-At` 调整 |
| 预警矩阵预计算 | `tools/warm_alert_matrix.py` 计算矩阵并发布状态 JSON；`register_alert_matrix_task.ps1` 默认每天 07:30；调用现有读取链路，不强刷所有原始快照 |
| Inline 重建工具 | `tools/refresh_inline_snapshots.py` 是停写后的原始快照重建工具；默认只列计划，`--apply --writers-stopped` 才删除识别出的原始快照并重建，不是日常增量调度器 |
| Streamlit 启动脚本 | `run_hidden.vbs → start_streamlit.bat` 启动服务，本身没有调用原始快照刷新脚本 |

**TTL 是下次访问时的过期判断，不是定时器。** 没有页面访问、命令调用或已运行的
计划任务时，经过 12 小时并不会自动连接数据库。计划任务是否注册、是否启用及
是否执行成功，须在部署机器的任务计划程序核实，不能由仓库中存在脚本推断。

矩阵后台任务产出的是跨进程状态快照，不是 Streamlit 内存缓存，也不是所有领域
原始事实的完整副本。详情见[矩阵定时预计算](../../domain/Inline_domain/data-flow-alert-matrix-schedule.md)。

## 2. 三种时间与两类缓存

当前 [global.yaml](../../../config/global.yaml) 的相关配置为：

```yaml
application:
  cache_ttl_hours: 12
data_forward:
  enabled: true
  offset_days: 4
report_cutoff:
  latest_day_time: "12:00:00"
```

| 概念 | 定义 | 是否参与日期前推 |
|---|---|---|
| 源事件时间 | 数据库事实或原始 Parquet 保存的发生时间 | 原始存储不平移 |
| 显示时间 | 仓储交付报表使用的时间轴，通常为源时间加配置天数 | Yield、Inline、Q-Time、IJP 参与；Equipment、IQC 不参与 |
| 刷新时间 | 快照发布时刻、文件修改时间，用于 TTL | 不参与 |
| 原始事实快照 | 各模块源数据及覆盖元数据 | 保存源时间和完整查询范围，不按中午截断 |
| 派生缓存/状态快照 | 报表计算结果、应用缓存、矩阵状态 JSON | 截止策略与日期参与相关签名，避免复用旧时限的结果 |

共用逻辑见 [ConfigLoader](../../../src/shared_kernel/config.py)、
[DataForwardPolicy](../../../src/shared_kernel/data_forward.py)、
[ReportCutoffPolicy](../../../src/shared_kernel/report_cutoff.py)。

### 最新一天为什么不受前端筛选影响

最新报表日取服务器本地当天，截止点为该日配置时刻，包含 `12:00:00`，排除
`12:00:00.000001`。不使用前端选择的结束日，也不使用数据中最大的日期。
当前前推四天时，报表 9 月 11 日对应源日期 9 月 7 日，最新源日截止到 9 月 7 日中午。
如果用户查询截至报表 9 月 4 日，仍保留那个历史日期的完整一天。

仓储通常先平移到显示时间，再过滤输出。IJP 和 AOI_TT Particle Size 已在 SQL
中进行汇总，必须先限制查询结束时间，再执行聚合，否则下午记录可能混入上午计数。
原始快照不删除下午记录；跨日后，昨日成为历史，下午记录可以重新进入报表。
系统不会凭空补出尚未从源系统取得的数据。

完整时间链路及各字段口径见[报表日期前推数据链路](../../domain/shared_kernel/data-flow-report-date-forward.md)。

## 3. 各模块总览

下表“全窗口”指模块当前请求或滚动窗口，不指数据库全部历史。

| 模块 | 快照粒度 | 普通刷新 | 显式刷新 |
|---|---|---|---|
| Yield | 产品 + 静态数据策略签名 | 旧快照最新入库日期回读 2 天，补查至当前请求末日 | `force_refresh=True` 全请求窗口重取 |
| Inline 共享测量 | 每产品一份；SPC、CTQ、AOI_TT、Monitor 复用 | 覆盖截止日回读 2 天，替换区间 | 强刷跳过新鲜度检查，仍按增量 |
| AOI_RS | 每产品 RS 明细与过货分母两份 | 各自覆盖截止日回读 2 天，替换区间 | `refresh()` 先取得两份增量，再成组发布 |
| Q-Time | 每厂别一份，供多个产品/路径复用 | 源覆盖结束边界回读 2 天，替换区间 | 页面按钮全窗口；CLI 默认增量、`--full` 全窗口 |
| IJP | 仓储无上述原始 Parquet 快照 | 每次进入仓储，直接查询请求窗口 | 不存在原始快照尾部增量 |
| AOI_TT Particle Size | 独立数据库聚合查询 | 查询显示窗口反算后的源窗口 | 不复用共享测量的原始快照 |
| Equipment | 规格签名对应的真实/仿造测量快照 | TTL 过期后重查回溯窗口，真实快照压缩为最新状态 | 真实来源强刷仍是窗口重查 |
| IQC | 本地示例 JSON | 读取文件、解析日期 | 无数据库增量快照 |

## 4. Yield：增量、精确入库时刻和旧快照

实现：[PanelRepository](../../../src/yield_domain/infrastructure/repositories/yield_repository.py)、
[SQL loader](../../../src/yield_domain/infrastructure/data_loader.py)、
[YieldAnalysisService](../../../src/yield_domain/application/yield_service.py)。

### 4.1 时间窗口与增量合并

默认显示窗口从当前月第三个前序自然月的月初开始，到当前日期结束；仓储将请求
起止日期按前推天数反算为源日期。显式传入的窗口仍以请求为准。

快照位于 `data/yield_domain/yield/`，文件名含产品与静态数据策略签名。普通读取
要求快照在 TTL 内、有足够窗口覆盖且包含精确入库时间字段。旧版无覆盖元数据
快照保留兼容判定，但缺少精确时间字段时不能直接作为新鲜快照使用。

可增量时：

```text
查询起点 = max(本次源窗口起点, 旧快照最大 warehousing_time - 2 天)
查询终点 = 本次源窗口终点
```

底层分片完整取数后，将旧数据和新增数据合并；按 `panel_id + defect_desc`
去重保留后者，缺少缺陷描述列时按 `panel_id` 去重，并裁剪窗口起点以前的记录。
**这不是完整区间替换**：源系统删除某条旧记录后，仅靠这种追加去重不一定能删除
快照中的旧行。要重建当前窗口的完整事实，应使用 Yield 强刷。

没有快照、旧快照为空、缺少必要字段/覆盖信息、请求需要更早数据或强刷时，
查询整个请求窗口。成功的空窗口也会保存覆盖信息及新字段结构，后续可以命中 TTL。

### 4.2 `first_ship_date` 只有日期，如何在中午截断

不是用日期字段猜小时，而是新增一个精确事件字段：

| 数据库字段 | 原始快照字段 | 用途 |
|---|---|---|
| `first_ship_date` | `warehousing_time` | 继续决定入库业务日、历史查询、趋势归属及增量起点 |
| `first_ship_time` | `warehousing_event_time` | 精确入库时刻，仅用于最新业务日的截止判断 |

本次数据库只读核验确认 `first_ship_time` 存在，抽样格式为 20 位
`YYYYMMDDHHMMSSffffff`；实现按该格式解析为微秒时间戳。两字段的日期可能不同，
因此不能把 `first_ship_time` 直接替换为原来的归属日期。

两列按相同前推策略转换后，仓储执行：

1. 业务归属日在今天之前：按原业务日保留，不用精确时刻改变历史口径。
2. 业务归属日等于今天：只有精确入库时刻不晚于今天配置截止点的记录才保留。
3. 最新业务日精确时刻缺失或无法解析：不纳入该日截止结果；不把日期午夜当作真实入库时间。
4. 剔除内部精确时刻列后，再交付原有报表列与数据健康状态。

以服务器今天 9 月 11 日、前推四天为例：

| 源业务日期 | 源精确入库时间 | 最新日报表处理 |
|---|---|---|
| 9 月 7 日 | 9 月 7 日 11:59:59 | 显示归属 9 月 11 日，保留 |
| 9 月 7 日 | 9 月 7 日 12:00:00 | 保留 |
| 9 月 7 日 | 9 月 7 日 12:00:00.000001 | 排除 |
| 9 月 6 日 | 9 月 7 日 18:00:00 | 显示归属 9 月 10 日，沿用历史业务日口径 |

### 4.3 旧快照和失败行为

旧快照缺少 `warehousing_event_time` 时，下次读取会全请求窗口重取并发布新结构，
而不是只回读最近两天。发布保留源时间与下午数据，不要求预先删除生产快照。

任一数据库分片失败，不发布部分新窗口：有旧快照则返回旧数据并标记 `stale`，
无旧快照则为 `unavailable`。旧快照无法提供精确时刻时，其最新业务日记录会被
排除，历史业务日仍可保留。快照落盘失败会记录错误，内存中成功取得的新数据仍可
返回；这不等于磁盘快照已更新。当前 Yield 写入直接使用 `to_parquet`，不具备
Inline 的成组临时文件发布机制。

## 5. Inline 共享测量及 AOI_RS

实现：[共享测量仓储](../../../src/inline_domain/infrastructure/shared/measurement_snapshot_repository.py)、
[增量与发布公共逻辑](../../../src/inline_domain/infrastructure/shared/rolling_snapshot.py)、
[AOI_RS 仓储](../../../src/inline_domain/infrastructure/aoi_rs/snapshot_repository.py)。

原始快照从请求结束日第三个前序自然月的月初开始。此原始宽窗口独立于日期前推；
仓储读出后才平移、截断，再由相应报表链路筛选显示窗口。SPC、CTQ、AOI_TT 和
Monitor 共享每产品的测量源快照，不各自保存一套同源测量。

快照元数据含策略版本、`covered_from`、`covered_through`、`refreshed_at` 及
绑定 Parquet 的文件大小、修改时间；共享测量另核对 `.policy` 文件。

```text
普通读取可复用 = 版本/文件绑定有效
              且覆盖起点等于当前原始窗口起点
              且覆盖截止日不早于本次结束日
              且 0 <= 刷新年龄 < TTL

增量起点 = max(当前原始窗口起点, covered_through - 2 天)
```

无有效元数据、旧覆盖已完全落在窗口之外、缺少所需更早覆盖或旧截止晚于本次
结束日时，从当前原始窗口起点重建。损坏/版本不符不会直接充当有效覆盖。

刷新保留增量起点之前的旧行，用新查询结果**替换整个回读区间**，删除完全相同
的重复事实并裁剪过期月份。源系统在回读区间内删除或修正的记录能反映出来；
成功空查询也替换该区间并推进覆盖。不能用“返回最大事件日期”代替覆盖元数据，
因为某天没有记录也可以是已经成功查询过的日期。

共享测量和 AOI_RS 的强刷跳过 TTL，但仍使用上述增量起点。AOI_RS `refresh()`
先取得明细与分母两份数据，再成组发布；任一取数失败不发布另一份新结果。
发布使用临时文件、元数据提交标记及失败恢复，仓储同时使用线程锁与跨进程文件锁。

数据库失败时优先保留原有磁盘文件；可回退的旧快照仍需满足相应覆盖起点要求。
共享测量刷新结果用 `refreshed_from_db=False` 区分回退，不能把旧数据当作刷新成功；
缺少可用覆盖时抛出 `IncompleteMonitorSnapshotError`。AOI_RS 也按调用方要求检查
覆盖并回退或报错，其 `refresh()` 失败返回 `False`。它们并非全部使用 Yield/Q-Time
的同一套 `data_health` 契约。

## 6. Q-Time

实现：[QTimeRepository](../../../src/indicator_domain/infrastructure/qtime/repository.py)、
[QTimeSnapshotStore](../../../src/indicator_domain/infrastructure/qtime/snapshot_store.py)、
[刷新 CLI](../../../tools/refresh_qtime_snapshots.py)。

`data/indicator_domain/qtime/qtime_source_<shop>.parquet` 每厂别一份，原始查询不按
页面产品/路径拆文件。默认显示窗口为上一自然月月初到报表日次日零点，反算为
源窗口；区间是 `[start, end)`。

普通读取同时检查 TTL 与窗口覆盖。可增量时：

```text
查询起点 = max(本次源窗口起点, min(旧 source_end, 本次 source_end) - 2 天)
查询终点 = 本次 source_end（不含）
```

没有可用快照或旧覆盖起点晚于本次所需起点时全窗口重取。
`full_refresh=True` 同样全窗口。合并保留回读起点之前的旧事实，随后以新结果
替换整个尾部，再裁剪本次源窗口。

元数据嵌在 Parquet 中，含策略、厂别、源覆盖范围、刷新时间和行数，写入通过
临时文件及 `os.replace` 替换。当前代码策略为 `qtime-source-v4`；不同版本快照
被拒绝读取，即使文件存在也不能直接用于新鲜命中或旧数据回退。

源时间兼容 14 位秒与 20 位微秒时间键；报表输出仍为 14 位秒级字符串，内部
`_source_event_time` 保存微秒用于中午截止，不交付报表列。新结构正常刷新后保留
精确源时刻。不能把既有低精度数据解释为已经恢复了原本缺失的微秒。

数据库读取失败，有当前策略可读的旧快照时返回 `stale`，不推进刷新时间与覆盖；
没有时抛出数据访问错误。CLI 只有全部厂别实际从数据库刷新成功才返回成功，
旧数据回退不是成功刷新。当前锁按快照路径提供进程内线程互斥，不等同于 Inline
跨进程锁；临时文件替换本身也不保证多个独立刷新进程的完整事务顺序。

## 7. 无尾部增量的来源与辅助文件

| 来源 | 读取与截止行为 |
|---|---|
| IJP | 仓储直接构造 SQL，显示结束时刻先限制到今天中午，再反算源时间；明细、日汇总及 Glass 比例在相同时间限制内查询；历史截止日不受影响 |
| AOI_TT Particle Size | ARRAY/TP 的 `MIN(time)`、`COUNT(*)` 在 SQL 中完成；调用 loader 前反算受限源窗口，排他的结束边界使用截止点加 1 微秒，随后仍在输出边界过滤 |
| 主制程履历 | 按测量涉及的载体/站点查询履历，输出事件时间按同一前推及中午规则处理 |
| Monitor 报废数据 | 从 Excel 读取、按产品过滤，再转换日期和截断；没有按快照截止日回读两天的逻辑 |
| 规格、修饰、人工历史台账 | 依各自文件读取、签名、写入与业务维护规则处理；不作为可随意重建的原始事实快照，不按其文件更新时间执行中午事实截断 |
| IQC | 读取本地示例 JSON，inspection 按检验时间截断；lifetime 的批次只提供日期时，不能推断其日内时刻 |

代码入口：[IJP](../../../src/indicator_domain/infrastructure/ijp/repository.py)、
[Particle Size](../../../src/inline_domain/infrastructure/aoi_tt/particle_size_loader.py)、
[报废数据](../../../src/inline_domain/infrastructure/monitor/scrap_repository.py)、
[IQC](../../../src/iqc_domain/infrastructure/demo_repository.py)。

## 8. Equipment：当前状态快照

实现：[data_loader](../../../src/equipment_domain/infrastructure/data_loader.py)、
[仿造快照更新器](../../../src/equipment_domain/infrastructure/fake_data_updater.py)、
[配置](../../../config/domain/equipment_domain.yaml)。

真实测量按规格签名定位快照。TTL 内复用；过期或显式强刷时重查配置回溯窗口，
当前 `lookback_days=90`，不是旧覆盖结束点加尾部增量。按站点、腔室、参数保留
最新测量；查询空或失败路径不应被理解为已经发布了新快照，现有文件可能仍保留。
当前正常读取过期文件后查询得到空数据，并不自动把过期真实数据作为有效当前状态返回。

报表阶段再使用 `as_of` 做源时间新鲜度/未来时间校验，当前有效测量年龄配置为
3 天，并组合仿造来源。仿造快照有独立的按 TTL 周期推进、缺失创建及未来时间
修复机制；长时间未运行时按经过周期更新，不执行数据库七天明细补查。

真实/仿造来源都不参与四天前推，输出统一按真实今天中午截断。
**最新状态快照不是历史事件全集**：若某测量键仅保留的最新记录在中午以后，
截断会排除该条，不能据此还原该键中午前的旧测量。

## 9. 一周不访问与历史补录

以原始日期为例，Inline 快照已覆盖到 9 月 4 日，9 月 11 日再次读取：

```text
保留旧快照中 9 月 2 日之前的记录
从 9 月 2 日（9 月 4 日减 2 天）查询到 9 月 11 日
用查询结果替换该区间
更新完整覆盖元数据
最后按报表时间策略截断输出
```

9 月 5 日至 11 日均会补查，不会只查“当前最近三天”而漏掉四天。Q-Time 的
结束边界不包含次日零点：旧源覆盖到 `9 月 5 日 00:00` 时，下次从 `9 月 3 日
00:00` 回读至新的结束边界。Yield 使用旧记录的最大入库日期，若最大日期也是
9 月 4 日，则从 9 月 2 日补查；实际最大日期更早时可能查得更多。

这个结论依赖数据库查询成功、源事实仍存在且处于本次保留窗口。它不意味着系统
永久保存全部历史，也不保证发现已覆盖区间中很久以前的迟到补录：

- 上游修改发生在两天重叠区间之外，普通增量通常不会重新核验那段历史。
- Yield 的追加去重不保证同步源删除；Inline/Q-Time 在回读区间内以区间替换处理。
- 刷新失败的旧数据回退不是补查完成，不能推进覆盖日期或据此宣称当前数据完整。
- 手动“刷新”并非统一全量语义：Yield 和 Q-Time 页面可全窗口；Inline/AOI_RS 仍增量。

## 10. 验证与维护入口

有关最新日、历史日期、前推与原始快照保留的定点验证：

- [统一截止策略测试](../../../tests/unit/test_report_cutoff.py)
- [跨仓储截止与旧快照测试](../../../tests/unit/test_repository_report_cutoff.py)
- [Inline 覆盖与增量替换测试](../../../tests/unit/inline_domain/infrastructure/measurement/test_rolling_incremental.py)
- [AOI_RS 双快照测试](../../../tests/unit/inline_domain/infrastructure/aoi_rs/test_rs_rolling_incremental.py)
- [Q-Time 仓储测试](../../../tests/unit/indicator_domain/infrastructure/qtime/test_repository.py)

以上是可复现的代码验证入口，不替代部署机器上的任务执行状态或源数据完整性核查。
以后修改窗口、快照版本、合并方式、全量入口或时间策略时，同步更新本文相应模块，
并保持“计划执行时间”“缓存有效期”“数据事件截止时间”三者分别描述。
