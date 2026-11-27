# SPC 本地快照的用途与结构

核对日期：2026-11-20。本文以当前工作区的文件和代码接线为准，区分程序是否有读写路径，以及现有文件是否能被该路径接受。

`data/inline_domain/spc/` 有三类业务数据，另有过货快照的历史副本。SPC 点位明细存放在相邻的 `shared/` 目录。

| 路径 | 保存的粒度 | 当前使用情况 |
|---|---|---|
| `throughput_<产品>.parquet` | 产品、厂别、业务范围、日期、Sheet | SPC 报表仍在维护。更新时读取本文件以保留覆盖区间外的历史。当前历史预警看板没有接入这个读取器，未发现其他常规业务入口读取它来展示或计算结果。 |
| `capability_metadata/` | 产品参数目录、站点参数规格，按捕获代次存放 | 历史预警看板的 CPK／CPM 周度统计和历史汇总重建会读取 `current.json` 指向的代次。2026-11-21 清理了 Z622 的元数据，剩余 7 个产品通过本地签名校验。 |
| `sheet_membership/` | 产品、完整 ISO 周、厂别、站点、参数对应的 Sheet ID 集合 | SPC 报表和巡检发布，异常 API 读取，用于核对 CPK／CPM 记录并返回对应的完整 Sheet 名单。 |
| `legacy_spc__*.parquet`、`legacy_throughput_*.parquet` | 与过货快照相同 | 历史副本，未发现当前业务读写入口。现有 6 份文件分成 3 对，同一对的文件字节相同。 |

## 1. 按日过货快照

文件名为 `throughput_<产品>.parquet`。每行表示在某业务范围内，同一产品、同一厂别、同一天观测到的一个 Sheet 标识。同一 Sheet 同一天的多个站点、参数或点位在这里合并为一行，跨日仍分别记录。

| 字段 | 类型 | 含义 |
|---|---|---|
| `scope` | string | 业务范围，此目录下为 `spc`。 |
| `factory` | string | 厂别，例如 `ARRAY`。 |
| `prod_code` | string | 产品编码。 |
| `event_date` | timestamp[ns] | 将事件的报表显示时间归一到当日 00:00，粒度为天。 |
| `item_id` | string | 载体标识，在 SPC 中取自 `sheet_id`。 |

SPC 从修饰前的 Sheet 特征投影生成每日过货事实。存储层按本次覆盖窗口替换事实，保留窗口外历史，再按五列去重后原子更新文件。查询接口会应用 Inline 日期排除。该结构能支持按日统计、跨指标去重和预警分母计算，但没有 `step_id`、参数、点位或精确的 `sheet_start_time`。当前历史预警看板构造 `OosMonitorService` 时没有传入可选的 `throughput_reader`，所以该页面不会读取这些 Parquet。SPC 绘图和 CPK／CPM 周度总项目数也走各自的测量输入。见 [SPC 特征与过货发布](../../../../src/inline_domain/application/shared/decorated_features.py)、[过货存储](../../../../src/inline_domain/infrastructure/shared/throughput_history_store.py)和[历史预警看板接线](../../../../app/pages/历史预警看板.py)。

`legacy_*` 中，M626 的一对副本各有 4,081 行，M678 各有 3,668 行，Z517 各有 2,824 行。各对的 SHA-256 相同。常规存储路径由 `ThroughputHistoryStore.snapshot_path()` 生成，指向 `throughput_<产品>.parquet`。

## 2. 能力统计元数据快照

```text
capability_metadata/
  current.json
  <generation>/
    manifest.json
    <产品>_catalog.parquet
    <产品>_specs.parquet
```

| 文件 | 字段与粒度 |
|---|---|
| `<产品>_catalog.parquet` | 产品内的参数分类目录。`ref_param_name`、`data_type` 均为字符串，分别表示标准化的参考参数名和业务类型。产品由文件名及清单确定。 |
| `<产品>_specs.parquet` | 产品下的站点参数规格记录。字符串字段为 `prod_code`、`step_id`、`param_name`、`param_type`、`main_step_id`、`main_eqp_type`，数值字段为 `usl`、`lsl`、`ucl`、`lcl`、`target`。 |
| `current.json` | `generation` 选定当前代次，`manifest_sha256` 校验该代次清单。 |
| `<generation>/manifest.json` | `version`、`generation`、`captured_at`、`as_of`、`tables`、`products`、`read_only`、`temporal_policy`。产品下分别登记 `catalog` 和 `specs` 的 `filename`、`rows`、`sha256`。 |

由显式维护命令捕获元数据，写完一整代后切换 `current.json`。周度统计读取当前代次，按文件校验值、行数和字段结构验证后，与共享原始量测快照一起计算可评估的 CPK／CPM 项目数。旧代次即使仍在目录里，也不属于常规读取目标。本次发现 2 个代次，`current.json` 指向覆盖 8 个产品的代次，M626 实测有 136 条目录记录、188 条规格记录。这些数字属于此次核查的文件内容。日常 SPC 点位制备通过报表自己的元数据端口取参数目录及规格，Z622 的参数目录来自其监控组合工作簿。见 [元数据捕获与读取](../../../../src/inline_domain/infrastructure/monitor/capability_metadata_snapshot.py)、[能力汇总规则](../monitor/rules-cpk-summary-workbook.md)和[离线重建命令](../../../../tools/rebuild_capability_summaries.py)。

## 3. SPC 周度 Sheet 成员快照

文件名为 `sheet_membership/<产品>/<ISO年>-W<两位周号>.json`。一个文件对应一个产品的一个完整 ISO 周。它记录最终制备点位所对应的 Sheet 集合，供异常 API 将 CPK／CPM 项目的对象编号输出为完整的 Sheet ID 数组。

| 结构 | 字段与含义 |
|---|---|
| 文件顶层 | `schema_version`、`product`、`period_label`、`published_at`。`coverage_start` 和 `coverage_end_exclusive` 是周一至次周一的左闭右开覆盖范围。`policy_signature`、`source_generation`、`upstream_generation` 标识策略和数据代次。`groups` 是组对象数组。 |
| `groups[]` | 键为 `factory`、`step_id`、`parameter_name`。`sheet_ids` 是去重并按文本排序的字符串数组。`data_start`、`data_end` 为组内点位的最早及最晚显示时间。 |
| `groups[].anchors` | 可含 `cpk`、`cpm` 键，每个键对应锚点对象数组。每个对象包含数值 `value` 和时间 `period_start`、`period_end`，用于验证能力结果与成员集合是否匹配。 |

每组保存 Sheet 集合、组级时间边界和能力记录校验字段。读取时校验产品、完整周、策略及组内身份。API 还用能力记录的实际数据跨度和指标值核对锚点。缺失或校验失败时，该来源不可用。见 [成员发布与读取校验](../../../../src/inline_domain/application/spc/sheet_membership.py)、[API 读取接线](../../../../api/anomaly/composition.py)和[成员契约](../../../../docs/ADR/0042-api-sheet-membership-delivery-2026-11-04.md)。

跨周重测在点位制备阶段按 ISO 周保留每个 Sheet／点位的最新量测。成员快照保存已经制备好的周内名单，让异常 API 直接复用能力记录对应的数据范围。对制备完毕的点位分组和去重，成本取决于扫描的点位行数。请求时从量测重算，还要完成时移、周内去重、参数分类、排除规则和点位修饰，并核对历史能力记录。以本次 M626 的六周回放为例，离线从原始快照制备约 79 万个最终点位，耗时约 21 秒。

2026-11-21 核查发现，36 份文件仍使用周内去重改版前的签名，另外 12 份已使用新版策略，但签名中的规则工作簿大小来自 Excel 读取视图。实测同一工作簿在独立进程与 Excel 读取后显示的大小相差 512 字节，修改时间一致。策略签名因此改用规则文件路径和修改时间标识该工作簿的版本。见 [签名生成](../../../../src/inline_domain/composition.py)。

同日重建了 8 个产品在 `2026-W41` 至 `2026-W46` 的 48 份快照，共 3,357 个成员组。7 个产品复用已验证的共享量测和规格快照，Sheet 名单与重建前一致。Z622 使用当前监控组合，通过限定时间、批次及站点参数的只读 PDS 查询重新制备。28 条符合能力预警筛选条件的 CPK／CPM 记录均通过本地成员查询校验。Z622 有 2 条台账记录无法和重算结果配对，均未进入本次预警筛选结果，原台账值保留。验证覆盖本地读回和 API 集成测试。

同日扩展 Z622 的读取批次后，按要求清理 Z622 原始量测、过货、AOI RS、能力元数据和周度成员快照。普通报表按配置重新读取量测，离线能力元数据由 `tools/refresh_capability_metadata.py` 重新捕获。

## 4. 查询 Sheet 或点位时如何选文件

| 想查什么 | 应读哪里 |
|---|---|
| 某个 Sheet 在 SPC 中被记录在哪几天 | `throughput_<产品>.parquet`，按 `item_id` 和 `event_date` 筛选。 |
| 某产品的参数分类或某站点参数的规格 | `capability_metadata/current.json` 指向代次内的目录或规格文件。 |
| 某个完整周的 CPK／CPM 项目包含哪些 Sheet | `sheet_membership/<产品>/<周>.json`，按厂别、站点及参数定位组，再按调用侧契约校验覆盖和签名。 |
| 某厂别、站点、Sheet 的量测有多少条，且要按精确时间筛选 | `data/inline_domain/shared/inline_measurements_<产品>.parquet`，使用源时间 `start_time`，或按报表的日期前推政策转换后再比较显示时间。 |
| 某个精确区间内最终用于 SPC 绘图的修饰后点位 | [SPC 点位导出入口](../../../../tools/spc_point_export/SKILL.md)。其输入来自共享测量及统一 SPC 制备和修饰，最终按 `sheet_start_time` 筛选。 |

共享原始快照的粒度是量测记录。字段为 `factory`、`prod_code`、`start_time`、`sheet_id`、`lot_id`、`step_id`、`param_name`、`site_name`、`unit_id`、`param_value`。源时间经过显示时间转换后，在 SPC 制备阶段使用 `sheet_start_time`。继续处理才会得到报表最终点位。见 [共享测量快照边界](architecture-inline-infrastructure.md#21-共享测量快照保留起点)和 [SPC 点位数据流](../spc/data-flow-spc.md)。

以 M626／ARRAY／`L3MY6A02L01` 为例，2026-10-01 后在过货快照中有 5 行，分别对应 10 月 27 日、29 日、30 日、11 月 6 日、8 日。该文件无法增加 `step_id=14140` 条件。使用该站点筛选共享量测快照，得到 122 条量测记录，源时间为 11 月 2 日 20:10:27，按核查时配置前推 4 天后为 11 月 6 日 20:10:27。两份文件按不同粒度记录数据。
