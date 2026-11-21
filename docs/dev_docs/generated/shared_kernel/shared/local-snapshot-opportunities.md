# 本地快照适用性评估

评估日期为 2026-11-21。范围覆盖 `src/` 下六个顶层包，重点检查 Infrastructure 的读取、转换和持久化边界，并沿 Composition 和 Application 核对调用方。本文记录候选方案。证据来自当前代码、本地文件元数据和一次离线分段计时，实际调用频率及生产数据库耗时尚未测量。

| 优先级 | 可复用的数据 | 建议 |
|---|---|---|
| 首选 | Inline 按产品整理的参数目录、原始规格 | 扩大已有快照的读取范围。先对齐时效和覆盖，再试接一个普通产品。 |
| 有条件试点 | Inline 主工序 OUT 事件 | 唯一建议评估新增落盘的数据。先测跨请求重复读取量及耗时，再决定是否建设。 |

## 先复用已有的参数目录和规格

SPC 和 CTQ 的生产仓储共用量测准备链。准备点位时会取参数目录做分类、取规格匹配主工序，单独读取图表规格时还会再取规格并应用产品 YAML 覆盖。AOI TT 也通过元数据端口取得规格。普通产品的在线适配器从数据库连接产品目录、规范参数名、转换规格数值并补齐主工序字段。离线能力统计已有 `CapabilityMetadataSnapshot`，按产品保存上述基础字段，以清单、行数和 SHA-256 校验同一代数据。多个消费者可以共享这份小表，报表仍可分别应用自己的分类和覆盖规则。[仓储装配](../../../../../src/inline_domain/composition.py) · [在线元数据](../../../../../src/inline_domain/infrastructure/shared/measurement_metadata_loader.py) · [规格覆盖和点位准备](../../../../../src/inline_domain/infrastructure/shared/measurement_preparation.py) · [已有快照](../../../../../src/inline_domain/infrastructure/monitor/capability_metadata_snapshot.py)

**接入条件是报表允许使用该时点的规格。** 本次读取的 `current.json` 所指代次截至 11 月 17 日 14:46，覆盖 7 个产品。M626 有 136 行目录、188 行规格。当前启用 8 个产品，Z622 在该代次中缺席，且其参数目录由独立监控组合决定。现有捕获 SQL 带采集时点，普通在线读取的时间条件与它也有差异。应先为普通产品核对业务键、重复行、空值、规格值和路由字段，并明确规格变更或显式刷新多久可见。满足约定时直接读当前代次，缺产品、超期或校验失败时沿原有在线入口处理。Z622 需另验监控组合版本。优先测量节省的数据库往返和跨进程读取时间，小表本身的数值转换收益有限。[捕获语义和代次验证](../../../../../src/inline_domain/infrastructure/monitor/capability_metadata_snapshot.py) · [Z622 数据源](../../../../../src/inline_domain/infrastructure/spc/spc_repository.py)

## 唯一建议试点的新快照：主工序 OUT 事件

量测 Parquet 命中后，SPC／CTQ 的准备链仍可能为追溯设备查库。仓储用本次点位的 Sheet 和主工序站点限定范围，从查询起点向前回看一个月，按厂别和设备／腔室路线发起查询。ARRAY 腔室会合并两个来源，OLED 腔室会关联站点映射视图。相邻报表窗口或不同消费者可能反复读到同一批事件。真正的潜在收益来自省去重复读取、路由关联及事件时间解析。前文档中的 M626 离线 20 秒测量走的是无需设备历史的能力统计路径，不能用它估算此项收益。[历史查询](../../../../../src/inline_domain/infrastructure/shared/main_process_history_repository.py) · [追溯调用方](../../../../../src/inline_domain/infrastructure/shared/measurement_preparation.py) · [离线能力统计](../../../../../src/inline_domain/infrastructure/monitor/capability_snapshot_source.py)

- **保存什么。** 可以先选一种常用路线，按厂别、路线和源日期存事件，保留 Sheet、主工序站点、设备或腔室、源事件时间、来源和优先级。日期前推、当天截止以及“某个量测点应匹配哪台设备”在读取后执行。这样，同一批事件可供不同查询窗口重用。[匹配逻辑](../../../../../src/inline_domain/infrastructure/shared/main_process_trace.py)
- **最容易出错的地方是覆盖范围。** 当前 SQL 只查请求中的 Sheet 和站点。一个日期文件里有数据，仅证明已查范围内的事件。若按需填充，清单还需记录成功查询过的 Sheet／站点范围，包括已查无事件的组合。下一次请求只可复用已覆盖部分。若只能通过扩大到全厂全日扫描才能简化覆盖，优先保留现有定向查询。
- **何时重建。** 源端迟到或更正需要回补相应日期。路由 SQL、来源优先级或 OLED 站点映射变更，需要使受影响路线失效。无法识别映射版本时，先试点依赖较少的路线。
- **如何决定是否投入。** 在正常报表和巡检周期中按路线记录查询耗时、请求覆盖范围及其重叠比例。选一组真实重叠窗口，比较两次定向查询与“首次构建＋第二次读盘补缺”的总耗时，逐点核对匹配设备和事件时间。若主要耗时在后续逐点匹配，保存事件能节省的时间会较少。

## 几个容易高估收益的地方

| 已核查路径 | 当前判断 |
|---|---|
| SPC 周内去重及最终修饰点位 | M626 在显示日期 10 月 5 日至 11 月 15 日，基础筛选后、参数目录分类前有 799,162 行，周内去重后仍为 799,162 行。预加载后，仅构造 ISO 周键并执行现有去重函数，三次耗时 0.955、4.404、0.988 秒。这个样本另存一份去重明细几乎复制了整个输入。最终修饰点位还依赖规格、人工决定和时间策略。暂缓两种明细快照。[周内去重位置](../../../../../src/inline_domain/infrastructure/shared/measurement_preparation.py) · [去重实现](../../../../../src/inline_domain/infrastructure/shared/measurement_preprocessor.py) |
| CPK／CPM 周度能力项目数 | 两种指标共用点位制备和能力计算，现有内存缓存键含指标。若常见调用总是成对取得两者，先测同次计算返回两列的收益。落盘会引入规格、修饰策略、台账和原始数据的多重失效条件。[离线计算与缓存](../../../../../src/inline_domain/infrastructure/monitor/capability_snapshot_source.py) |
| Q-Time 产品及站点选项 | 仓储确实会对厂别快照读盘、规范字段、去重和排序。但本机 ARRAY、OLED、TP 三份文件分别仅 1,455、614、0 行，合计约 86 KiB。优先保留现有厂别快照，等选项读取成为实测瓶颈再考虑随同一文件写入索引。[选项读取](../../../../../src/indicator_domain/infrastructure/qtime/repository.py) · [快照存储](../../../../../src/indicator_domain/infrastructure/qtime/snapshot_store.py) |
| Yield 每张 Sheet 的最早阵列投入时间 | 站点 10000 的最早时间确实被 Sheet 和 Lot 分析复用，但当前配置关闭了查询，开启后也有按 Lot 集合的内存缓存。如果今后启用，需先证明有界源窗口覆盖每张 Sheet 的历史最早时间，再评估是否保存源时间并在读取后应用人工覆盖。[配置](../../../../../config/domain/yield_domain/sheet_lot.yaml) · [计算](../../../../../src/yield_domain/infrastructure/data_loader.py) · [调用与缓存](../../../../../src/yield_domain/application/yield_service.py) |
| 其他报表链 | Yield Panel、AOI RS、设备寿命已有事实快照。IJP 查询按粒度、筛选范围及明细限制取数。IQC 蒸镀和寿命走不同来源及投影，当前扫描未找到足以支持新增共享中间表的重复消费者。共享内核保留路径、锁和读取工具，各域负责各自的数据有效性。[Yield](../../../../../src/yield_domain/infrastructure/repositories/yield_repository.py) · [AOI RS](../../../../../src/inline_domain/infrastructure/aoi_rs/snapshot_repository.py) · [设备](../../../../../src/equipment_domain/infrastructure/data_loader.py) · [IJP](../../../../../src/indicator_domain/infrastructure/ijp/repository.py) · [IQC 蒸镀](../../../../../src/iqc_domain/infrastructure/eva_materials/evaporation_repository.py) · [IQC 寿命](../../../../../src/iqc_domain/infrastructure/lifetime/lifetime_repository.py) |

## 建议的新增规则

1. **先找到重复工作。** 写明哪些正常入口反复消费同一批事实，或同一入口在何种定期任务中跨进程重算。先查已有磁盘快照、内存缓存及发布物能否复用。
2. **选择最早且已经有收益的边界。** 优先考虑稳定的源事实、规范化维表或小型索引。把人工决定、当前时钟、查询窗口和频繁变化的公式放在下游。如果每次规则改动都要重建大部分历史，继续使用进程内缓存。
3. **比较同一项工作的全周期成本。** 分开测源查询、纯计算、首次写盘、命中读盘和回补。一次刷新周期内，多次命中累计省下的时间应能覆盖构建、校验和重建成本。此前的 20 秒覆盖六周点位制备，75 毫秒对应一次成员查询，两者的比值不能当作其他候选的预期加速倍数。
4. **让读取方能证明数据适用。** 记录粒度、源时间或显示时间、覆盖范围、上游代次和转换版本，区分成功的零行、未覆盖和读取失败。TTL 控制可接受的数据年龄，规则版本和上游变更决定是否仍可使用。为迟到、更正和人工刷新定义局部重建范围。
5. **先验证重建，再扩大使用。** 先对一个产品或路线比较业务键、重复项、空值和时间边界。验证冷启动、并发刷新、失败回退及新旧版本切换。整代通过校验后再原子发布，保留原入口的异常语义。生产 SQL 验证遵循项目的只读、分源时间过滤及有界查询约束。

## 复核路径

在仓库根目录运行以下只读命令可定位现有实现，并读取规模证据。文件随生产刷新，行数和计时对应本次检查时点。

```powershell
rg --files src -g '*.py'
rg -n 'read_parquet|to_parquet|read_weekly_totals|get_parameter_specs|get_main_process_history|keep_latest_measurements' src
.venv/Scripts/python.exe -c "from pathlib import Path; import pyarrow.parquet as pq; print([(p.name,pq.ParquetFile(p).metadata.num_rows,p.stat().st_size) for p in Path('data/indicator_domain/qtime').glob('*.parquet')])"
```

复现去重计时时，用当前日期前推策略把显示时间区间 `[2026-10-05, 2026-11-16)` 映射到 M626 原始 Parquet 的 `start_time`。按现有链应用排除站点、显示日期排除、无效值和参数名过滤，剔除空时间与空值。保留输入顺序，计时范围从构造 ISO 年周键开始，到 `keep_latest_measurements` 返回为止，连续执行三次。该测量不含读盘、参数分类、设备历史和点位修饰。

