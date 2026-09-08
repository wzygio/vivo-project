# 全指标预警看板：By Domain / By 指标实现说明

核对日期：2026-09-08。代码基线：`0aa08b5` 及核对时工作区。

本文记录**当前实际实现**，不是目标方案。范围是“自动预警看板”顶部的“全指标预警看板”矩阵及点击详情，不包括下方独立的 Inline 和 CPK 汇总看板。核对采用源码静态追踪，没有运行生产全量查询，也没有修改业务代码或工作簿。

## 1. 结论与分类标准

用户提出的三种分类是有效的，但要将“状态灯”和“点击详情”分开，并区分**接口复用**与**同一缓存条目复用**。

- **A：本地结果直读**。读取其他模块已产出的 Excel，做字段投影、时间筛选、存在性判断；不重新分析原始量测。Excel 在这里是“结果／修饰明细工作簿”，不是 `data/` 下的原始 Parquet 快照。
- **B：复用 domain application 管路**。调用报表已有 application 服务与缓存函数。仅当缓存函数及参与哈希的参数一致时，才真正共享同一条 L2 缓存；函数相同本身不足以保证命中。
- **C：独立业务计算管路**。矩阵另写一套原始数据分析与预警算法。当前八行没有另建这种完整引擎；矩阵确实有专用的组合、筛选、状态归约和外层缓存，但不能把这些轻量胶合逻辑等同于独立分析引擎。

| Domain | 子模块／指标 | 状态灯主要方式 | 点击红灯后的方式 | 与原报表缓存的关系 |
|---|---|---|---|---|
| inline_domain | AOI_RS 单片异常 | A：OOS Excel 产品 sheet | A：预警表；B：AOI_RS 报表数据与图像 | Excel 解析有缓存；图像服务复用不代表报表 L2 必然命中 |
| inline_domain | AOI_TT 单片异常 | A：OOS Excel 产品 sheet | A：预警表；B：AOI_TT 报表数据与图像 | 同上 |
| inline_domain | SPC 单片异常 | A：OOS Excel 产品 sheet | A：预警表；B：SPC 报表数据与图像 | 同上 |
| inline_domain | SPC 趋势波动（CPK） | B：SPC 周期能力 payload | B：SPC ViewModel 与预警图表 | 矩阵与 SPC 页基签名不同，不能认为直接共享整份 payload |
| inline_domain | CTQ 单片异常 | A：OOS Excel 产品 sheet | A：预警表；B：CTQ 报表数据与图像 | 同 AOI |
| yield_domain | Yield 单片异常（Lot 超规） | B：Lot 不良率＋预警线＋共用判据 | B：趋势、Lot、Sheet、Mapping 数据 | `read_only` 等参数不同，顶层缓存不保证与 Yield 页相同 |
| yield_domain | Yield 趋势波动 | B：Group/Code 趋势＋共用探测器 | B：同一 Yield 详情装配 | 同上 |
| indicator_domain | Q-Time 单片异常 | B：厂别级全站点、全产品监控缓存 | B：同一厂别监控入口，再按产品筛选 | 矩阵 `as_of=本周一`，报表 `as_of=当天`；非周一不能直接视为同一键 |

**特别说明：用户举例的 `aoi_tt_sheet_ooc_decoration.xlsx` 不是当前矩阵 AOI_TT 单片异常的输入。该行读取的是 `aoi_tt_sheet_oos_decoration.xlsx`。当前注册表没有单独的 OOC 行，也没有 IJP 行。**

## 2. 总体结构与状态生命周期

```text
自动预警看板.py：查询门控、产品／类型／厂别选择
  → alert_matrix.render_alert_matrix_board
    → get_cached_alert_matrix：矩阵级 st.cache_data
      → build_default_matrix_context：注入各 domain 数据入口（仅缓存 miss）
      → MATRIX_ROWS：8 个 evaluator，逐产品生成后台四态
    → compliance_config.xlsx：缓存外生成显示副本
    → 矩阵前端筛选与 no_data → 达标映射
    → 点击单元格
      → 非预警：仅状态说明
      → 预警：_cached_matrix_detail_bundle → domain 服务 → 明细／图像
```

后台四态为 `ok / alert / no_data / error`。前端将 `no_data` 显示为达标；`compliance_config.xlsx` 中产品×指标为 `True` 时，该显示副本也变为 `ok`，点击不进入详情 loader。原始矩阵缓存不被修改。加载失败若未被显式修饰则仍展示错误。

类型筛选切行、产品筛选切列；支持厂别的行以 `alert_factories` 与所选厂别交集调整灯色。Yield 两行不支持厂别细分，保持原状态。该前端过滤不等同于重新按厂别计算报表。

注意：厂别过滤发生在矩阵显示函数内，详情入口消费显示 payload，不接收 `filter_selection`。因此“厂别筛选后灯色”与“点击后详情按同一厂别过滤”不是同一个已实现保证；不要据前者推断后者。Excel 强制达标则直接作用于表格与详情共同消费的 payload，二者一致。

## 3. inline_domain

### 3.1 四个单片异常指标共用的 Excel 入口

实际调用链为：

```text
_sheet_oos_evaluator
  → build_oos_history_service
  → _build_oos_history_service
  → ExcelAlarmReader(ExcelAlarmStore(paths), "oos")
  → read_product(scope, product)
  → 产品 sheet → OOS 字段投影 → event_time / flag
  → build_sheet_oos_alerts → 是否存在上周未修饰预警
```

虽然函数名含 `history`，当前生产 composition 返回的是 **ExcelAlarmReader**，不是旧 `OosHistoryService + LiveHistoryStore` 计算组合。`ExcelAlarmReader.has_history()` 恒为 True，目的是让旧调用方始终走 Excel reader，即使缺文件也不会降级到原始数据分析。`OosHistoryService` 在这里仅提供静态字段投影能力。

`ExcelAlarmStore` 按配置解析各 scope 文件，`read_cached_alarm_workbook(path, mtime_ns, size)` 缓存整份工作簿，多个产品共享解析结果；标准读取失败时使用项目 Excel COM 读取能力。它不是四个 domain 报表的完整计算 payload 缓存。

判定时间窗统一为 `[上周一 00:00, 本周一 00:00)`，不含本周。存在 `flag=False` 的命中记录就为 `alert`；有可用明细但无命中为 `ok`。缺数据等情况按 reader／evaluator 契约返回 `no_data` 或抛异常后转 `error`。

### 3.2 AOI_RS → 单片异常

- 行键：`aoi_rs_sheet_oos`。
- 默认输入：`resources/inline_domain/aoi_rs/aoi_rs_sheet_oos_decoration.xlsx`，产品同名 sheet。
- 原始时间列：`sheet_start_time`；投影时间列：`event_time`。
- 指标字段：`rs_code`；项目标识：`point_id`；观测值：`value`；上限：`spec`。
- 状态灯：仅筛选已经判定为超规的 OOS 明细，不重新算 RS 点位或过货量。
- 点击详情：`_load_sheet_oos_alerts_display("aoi_rs", ...)` 从 Excel 构建预警表；`AoiRsReportService.get_aoi_rs_report_data()` 获取出图数据，按预警键筛选 `rs_details_df / pass_through_df / indicators_df / lot_points_df / sheet_points_df`，使用报表已有图表组件。
- 性能边界：**点亮矩阵是轻量 Excel 读取；点击图像可能访问仓储、原始快照并重新构建报表数据**，取决于 domain 缓存是否命中。

### 3.3 AOI_TT → 单片异常

- 行键：`aoi_tt_sheet_oos`。
- 默认输入：`resources/inline_domain/aoi_tt/aoi_tt_sheet_oos_decoration.xlsx`，不是 OOC 文件。
- 原始时间列：`start_time`；项目标识：`sheet_id`；指标：`tt_name`；观测值：`tt_qty`。
- 状态灯：Excel 已有 OOS 事实＋上周＋未修饰记录是否存在。
- 点击详情：Excel 构造预警表，再调用 `AoiTtReportService.get_aoi_tt_report_data()`，按命中指标筛选 TT 明细、指标帧，带规格表出图。
- 缓存关系：状态灯不会调用 AOI_TT 报表计算；详情复用报表接口，但矩阵签名与报表查询窗口等参数需逐项一致才能共享缓存条目。

### 3.4 SPC → 单片异常

- 行键：`spc_sheet_oos`。
- 默认输入：`resources/inline_domain/spc/spc_sheet_oos_decoration.xlsx`。
- 时间：`sheet_start_time`；项目：`sheet_id`；参数：`param_name`。
- OOS 投影：通常使用 `sheet_max` 为观测值；`oos_type=LSL` 时使用 `sheet_min`，并保留规格上下限。
- 状态灯：读取现成 OOS 判定，不在矩阵侧重新运行 SPC 单片判定。
- 点击详情：Excel 预警表＋`_load_spc_view()` → `SpcReportService.get_spc_report_data()`；复用报表的原始量测、单片特征、周期能力和图表能力。

### 3.5 SPC → 趋势波动（CPK）

- 行键：`spc_cpk_trend`。
- **不是单纯直读 CPK 修饰 Excel**。生产 `spc_cpk_loader()` 调用 `SpcReportService.fetch_spc_report_payload()`，取 `period_capability_df`。
- 查询对象：`SpcQueryConfig(prod_code, start_date, end_date, data_type_filter="SPC")`；时间区间由报表默认起点函数和 `MonitorAnalysisService.get_time_window()` 产生，不是仅查询一周明细。
- 判据：复用 SPC UI 的 `build_weekly_cpk_alerts()`，阈值 `CPK_ALERT_THRESHOLD`（1.33），筛选上一完整 ISO 周、低于阈值且未被 CPK 修饰排除的项目。输入是报表周期能力帧及其 `cpk_decorated` 语义，不应把它直接解释成对任意 Excel 的 `flag=False` 行计数。
- 点击详情：`_make_spc_cpk_loader()` 再经 `_load_spc_view()` 获取报表 ViewModel，应用同一个周度预警函数，筛选相关项目并绘图。
- 缓存关键差异：矩阵使用 `alert_matrix_board_v1` 作为基签名；当前 SPC 页面使用 `spc_capability_previous_week_anomalies_v2`。虽然调用同一个 cached application 函数，**这两者不是同一缓存键**。
- 进一步边界：状态灯 loader 显式传入 `capability_exempt_param_name_contains`；详情 `_load_spc_view()` 没有显式传入该参数。是否完全一致还取决于服务默认值与配置，不能仅根据注释认定复用同一条缓存。
- 与下方独立“CPK预警看板”不同：下方使用 `CpkWorkbookMonitorService` 和用户汇总表更新上一完整周／当月。顶部矩阵此行不是那套 Excel 汇总逻辑。

### 3.6 CTQ → 单片异常

- 行键：`ctq_sheet_oos`。
- 默认输入：`resources/inline_domain/ctq/ctq_sheet_oos_decoration.xlsx`。
- 时间／项目／参数与 SPC 同形：`sheet_start_time / sheet_id / param_name`。
- 状态灯：Excel OOS 明细＋上周未修饰筛选。
- 点击详情：Excel 预警表＋`CtqReportService.get_ctq_report_data()`，查询类型为 CTQ，按预警键筛选单片特征、原始量测等出图。

## 4. yield_domain

### 4.1 Yield → 单片异常（Lot 超规）

- 行键：`yield_lot_oos`。虽然页面称“单片异常”，实际统计对象为 **Lot**，不应改写成 Sheet 指标。
- 输入：`YieldAnalysisService.get_lot_defect_rates()` 返回的 Lot 数据，以及 `load_static_warning_lines()` 返回的预警线。
- 判据：复用 `app.components.alert_center.compute_lot_oos_records()`；矩阵只对该函数给出的超规记录按“入库时间”截取上一 ISO 周，再判断是否非空。阈值解释由共用函数负责，矩阵没有另写良率超规公式。
- 数据读取：通过 Yield application 获取，可能利用域内原始数据快照与中间缓存；不是直接把某个报警 Excel 当成最终判定源。
- `read_only=True`：避免矩阵消费时触发良损修饰表回写，不等于“完全不计算”或“不会查询数据库”。
- 点击详情：共用 `_make_yield_loader(mode="lot")`；除 Lot 数据外还获取 Group/Code 趋势、Sheet 数据与 Mapping，供预警详情图使用。因此点击红灯的成本可高于灯色计算。

### 4.2 Yield → 趋势波动

- 行键：`yield_trend_fluctuation`。
- 输入：`get_mwd_trend_data()` 和 `get_code_level_trend_data()`。
- 判据：`AlertService.get_dashboard_alert_records(group_data, code_data)` 的结构化探测记录非空即预警。
- 时间口径：**月／周环比的 period 制**，不再由矩阵裁成上一 ISO 周。不要用“上一周异常”概括这一行。
- 点击详情：`_make_yield_loader(mode="trend")` 复用同一个探测器和 Yield 图像组件；它与 Lot 详情共用数据装配，包含较多出图输入。

### 4.3 两行的缓存复用边界

矩阵采用与 Yield 看板相同的 `yield_dashboard_manual_refresh_v1` 基签名，并复用 `build_cache_context()` 的分析窗口、修饰签名等信息，这是明确的复用设计。

但矩阵显式传 `read_only=True`，Yield 主报表没有传该参数，服务默认是 `False`；这些顶层函数本身被 `st.cache_data` 装饰，`read_only` 参与键。另有配置对象可能经页面 Mapping 配置注入而不同。因此应描述为：

> application 管路和更底层数据可复用，但不能保证矩阵与主报表直接命中同一份顶层趋势／Lot 结果。矩阵状态与矩阵详情参数更接近，但完整命中仍需以实际键为准。

## 5. indicator_domain

### 5.1 Q-Time → 单片异常

- 行键：`qtime_sheet_oos`。
- 状态灯入口：`load_all_product_qtime_monitoring()` 对 ARRAY、OLED、TP 调用 `get_cached_shop_monitoring()`。
- 每个厂别：先取该厂别全部站点，`products=()` 表示全产品，进入 `get_cached_monitoring()`／`_cached_monitoring()`。结果按 `prodcode` 拆给各产品灯色；`AlertMatrixContext` 在一次构建中记忆全产品合并结果，避免每个产品再装配一遍。
- application 内部：`QTimeReportService.get_current_monitoring()` → `get_current_report()` → 数据端口；随后应用规格覆盖、修饰决策并构造 alerts。数据端口可复用已有原始快照，矩阵不直接读 Parquet，也不是只读 `qtime_oos_decoration.xlsx` 就得出所有灯色。
- 服务读取窗口：由 `as_of` 计算“上月 1 日至该日结束”；矩阵从 alerts 按 `prodcode` 和 `timekey` 再截取上一 ISO 周。
- 时间字段解析：当前矩阵截取 `timekey` 前 14 位，按 `%Y%m%d%H%M%S` 解析。
- 点击详情：复用同一 `load_all_product_qtime_monitoring()`，按产品筛选；alerts 再截取上周，details 保留服务当前读取窗口。因此详情里的全部记录／Lot 总数不应直接当成“上周报警数量”。

### 5.2 与 Q-Time 报表的缓存关系

共用入口和缓存函数是真的；“任何一天都共享同一条缓存”则不成立：

- 矩阵将参考日归一到本周一，然后传给 `as_of`。
- Q-Time dashboard 调用 `get_cached_shop_monitoring(as_of=None)`，包装器把 None 归一到当天。
- 例如周二 2026-09-08：矩阵键的 `as_of` 是 09-07，报表键是 09-08，二者不同。周一才有日期一致的可能。
- 缓存还包含厂别、全部站点描述、产品元组、决策工作簿 stat、计算版本和服务 `source_signature`。

矩阵灯色与其详情均使用同一参考周日期，因而比“矩阵 vs 今日报表”更容易命中相同厂别缓存。

## 6. 三层缓存及失效范围

| 层 | 所有者／函数 | 缓存内容 | 关键边界 |
|---|---|---|---|
| 矩阵组合缓存 | `_cached_alert_matrix_payload` | 产品×指标后台状态与元信息 | 产品元组、本周一、组合签名；TTL 取全局配置，最多 8 条 |
| 域内／输入缓存 | Excel 整簿解析、SPC/Yield/Q-Time application | 已解析表或报表计算结果 | 各自文件 stat、查询窗口、产品 revision、配置等参数 |
| 详情缓存 | `_cached_matrix_detail_bundle` | 点击项目的详情 bundle | detail_key、reference_date、signature；按需加载 |

当前矩阵组合签名明确采集：产品 revision、四个 Inline scope 的决策内容签名、Q-Time 决策工作簿 stat。**不要把它描述为对所有底层文件的完整指纹。**

例如，SPC CPK 配置／工作簿和 Yield 输入不是都作为独立分量直接进入最外层签名。Inline `get_scope_decision_signature()` 又走决策台账解析，不等同于对产品 sheet 的全部观测值做摘要。因此修改某个输入文件后，即使底层 reader 支持 stat 失效，也必须先发生外层矩阵缓存 miss 才会重新消费它。页面“刷新缓存”、TTL、跨周或已有签名变化是相关刷新路径；不能承诺任意 Excel 单元格变更都会立即自动重建所有灯色。

显示修饰 `compliance_config.xlsx` 则刻意在组合缓存**之后**读取，按文件 stat 缓存小表，更新显示不清理原始计算缓存。这是前端覆盖，不是第九个业务指标，也不是新计算链路。

## 7. 架构评价与建议（未在本任务修改）

1. **现状优点**：四个单片异常灯已是 Excel-only；复杂指标使用已有 domain application 与已有判据；详情按需加载，矩阵不自动渲染所有图像。
2. **需要避免的误解**：历史类名不是生产依赖；“同函数”不是“同缓存”；“read_only”不是“零计算”；顶部矩阵不是下方汇总看板；OOC 文件不是 OOS 行输入。
3. **缓存复用优化方向**：若要求“一次计算、两个页面共同使用”，应逐项统一有意共享的日期、签名、配置及读写模式，或把只读共同计算核下沉为统一缓存入口。不能简单删除 `read_only` 来换取缓存命中，以免恢复副作用。
4. **CPK 灯优化方向**：如后续确认现成 CPK Excel 足够表达该灯的周度判据，可另行批准将其替换为结果 reader；当前实现尚未这样改。
5. **输入一致性方向**：四个 Inline 灯使用投影 reader，详情预警表另走直接 Excel loader。虽然目标文件和周筛选相同，仍有两处解析／装配路径；未来可统一只读结果契约，减少字段兼容与状态／详情不一致风险。
6. **独立管路判断**：目前专用代码主要是跨域 orchestration、存在性判断、时间切片和显示适配，没有证据支持“矩阵另造了八套业务计算引擎”。真正应关注的是重复 cache miss 和不一致参数，而不是把所有矩阵专属代码都视为不良重复。

## 8. 源码索引与核对入口

以下链接均相对项目仓库，关键符号可直接搜索定位：

- [页面入口](../../../../app/pages/自动预警看板.py)：查询门控、模块组成。
- [矩阵 UI](../../../../app/sections/inline_domain/monitor/alert_matrix.py)：显示修饰、筛选、按钮。
- [行注册表与 evaluator](../../../../app/sections/inline_domain/monitor/alert_matrix_service.py)：`MATRIX_ROWS`、八行判定、组合签名。
- [矩阵缓存与生产装配](../../../../app/sections/inline_domain/monitor/alert_matrix_cache.py)：真实 loader、缓存参数。
- [矩阵详情](../../../../app/sections/inline_domain/monitor/alert_matrix_detail.py)：各域详情入口、出图输入和 lazy cache。
- [Inline composition](../../../../src/inline_domain/composition.py)：`_build_oos_history_service` 当前返回 `ExcelAlarmReader`。
- [Excel 结果 reader](../../../../src/inline_domain/application/monitor/excel_alarm_reader.py)、[Excel store](../../../../src/inline_domain/infrastructure/monitor/excel_alarm_store.py)：当前灯色的直接输入。
- [SPC 主报表](../../../../app/pages/SPC监控报表.py)、[SPC application](../../../../src/inline_domain/application/spc/spc_service.py)：签名、报表 payload 与 ViewModel。
- [Yield 主报表](../../../../app/pages/入库不良率分析看板.py)、[Yield application](../../../../src/yield_domain/application/yield_service.py)：`read_only` 与缓存窗口。
- [Q-Time dashboard](../../../../app/sections/indicator_domain/qtime/dashboard.py)、[共享监控缓存](../../../../src/indicator_domain/application/qtime/cached_monitoring.py)、[Q-Time service](../../../../src/indicator_domain/application/qtime/service.py)：`as_of` 与厂别级复用。
- [显示修饰管理器](../../../../app/manager/compliance_manager.py)：缓存外覆盖。

现有行为测试入口包括 `tests/unit/app/sections/monitor/test_alert_matrix_service.py`、`test_alert_matrix_ui.py` 与 `tests/unit/test_matrix_compliance.py`。本文中的“同键条件”来自调用参数核对，不是对线上缓存命中率的测量结果。
