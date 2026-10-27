# 全指标预警看板：By Domain / By 指标实现说明

更新日期：2026-10-27。顶部矩阵十三行的预警状态与明细统一调用 `app/sections/shared/alert_results.py::read_alert_result()`，与对应监控报表共用已发布Excel的读取、预警筛选和进程内结果缓存。共享范围仅为预警显示，原始处理、修饰、统计与结果生产仍由各报表负责；缺结果文件或缓存miss不触发分析补齐。图像详情独立、按需加载，图像失败不能遮蔽预警表。范围不包含下方 Inline／CPK／CPM 历史汇总看板。通用设计由 [已发布结果展示设计](../../../design/inline_domain/shared/architecture-published-alert-display.md) 维护。

## 共同预警展示入口

报表和矩阵传入相同的指标键、产品、参考日及范围参数时，复用同一个 `_cached_alert_result` 条目。共同服务返回状态、预警公开表、选中记录、厂别和发布元数据；矩阵仅将其归约为状态灯，详情复用同一公开表，不另外运行异常识别。原生表返回副本，不共享可变对象身份；独立定时进程仍只通过文件共享结果。

周期或范围不同必须显式表达：矩阵统一上周的指标保持上周，Yield报表Lot继续按最近过货日近30天，Q-Time报表继续选定厂别／站点滚动窗口。它们复用相同选择函数和缓存体系，不承诺不同范围的条数相同。其它域的周期、有效性和初始化空模板约定保持原样。

## 数据入口分类

- **A：本地结果直读**：读取其他模块已产出的 Excel，做字段投影、时间筛选与预警存在性判断，不重新分析原始量测。
- **B：application 管路复用**：调用报表已有服务和缓存；只有函数及参与哈希的参数一致，才共享同一缓存条目。
- **C：独立分析管路**：另建原始数据分析引擎。当前十三行没有这类引擎；轻量状态归约、组合和显示适配不是另一套业务算法。

| Domain | 指标键／显示名称 | 状态灯 | 红灯详情 |
|---|---|---|---|
| inline_domain | aoi_rs_sheet_oos／AOI_RS 单片异常 | A：AOI_RS OOS Excel | A：预警表；B：报表图像 |
| inline_domain | aoi_tt_sheet_oos／AOI_TT 单片异常 | A：AOI_TT OOS Excel | A：预警表；B：报表图像 |
| inline_domain | spc_sheet_oos／SPC 单片异常 | A：SPC OOS Excel | A：预警表；B：报表图像 |
| inline_domain | spc_cpk_trend／SPC 趋势波动（CPK） | A：CPK Excel | A：预警表；B：SPC 报表图像 |
| inline_domain | spc_cpm_trend／SPC 趋势波动（CPM） | A：CPM Excel | A：预警表；B：同一 SPC 报表图像缓存 |
| inline_domain | ctq_sheet_oos／CTQ 单片异常 | A：CTQ OOS Excel | A：预警表；B：报表图像 |
| yield_domain | yield_lot_oos／Yield 单片异常（Lot 超规） | A：已发布 Lot 超规工作簿 | B：Yield 详情 |
| yield_domain | yield_trend_fluctuation／Yield 趋势波动 | A：已发布趋势报警工作簿 | B：Yield 详情 |
| indicator_domain | qtime_sheet_oos／Q-Time 单片异常 | A：Q-Time各厂别已发布结果，按产品与上周筛选 | A：同一结果明细；B：点击加载原站点图像 |
| indicator_domain | ijp_aa_oos／IJP AA区超规 | A：IJP Excel 的 AA区 sheet | A：同一快照公开明细，无 application 图像加载 |
| indicator_domain | ijp_hole_oos／IJP 孔区超规 | A：IJP Excel 的 孔区 sheet | A：同一快照公开明细，无 application 图像加载 |
| equipment_domain | parts_oos／关键备件超规（全厂共用） | A：备件 Excel | A：同一快照公开明细 |
| iqc_domain | iqc_evaporation_ng／IQC 蒸镀材料NG | A：蒸镀材料 Excel | A：同一快照公开明细 |

Domain 仅用于文档分类和界面分组，**不参与刷新标识维度**。

## inline_domain

### AOI_RS、AOI_TT、SPC、CTQ：单片异常

读取 resources/inline_domain/<scope>/<scope>_sheet_oos_decoration.xlsx 的产品同名 sheet，实际路径服从全局资源注册。共同展示服务使用 ExcelAlarmReader.read_detail / ExcelAlarmStore，先按路径、mtime_ns、大小缓存整本工作簿，再读取保留原指标键的已验证明细。四个报表、矩阵状态和详情共用这条入口；不再分别读ViewModel明细或为详情另开读取器。缺明细不会回退运行原始分析。

| scope | 时间字段 | 项目／指标 |
|---|---|---|
| aoi_rs | sheet_start_time | point_id／rs_code |
| aoi_tt | start_time | sheet_id／tt_name |
| spc、ctq | sheet_start_time | sheet_id／param_name |

统一规范化后，以 [上周一 00:00, 本周一 00:00) 内 flag=False 的 OOS 记录判断是否预警。SPC额外使用 `spc.sheet_oos_alerts.excluded_param_name_contains`，参数名不区分大小写的普通文本匹配；当前排除PPA。报表、矩阵状态、详情与自动出图键均消费这份共同筛选，配置也参与缓存签名。参数预警排除不修改人工flag或点位，不等同于自动修饰豁免。aoi_tt_sheet_ooc_decoration.xlsx 等 **OOC** 文件不是这四行输入；IJP 使用下述独立快照行，不使用 Inline OOS/OOC 工作簿。

TT 结果读取遵守 [SPC 参数分类规则](../spc/data-flow-spc.md#23-匹配参数白名单与-spc-分类)：旧 AOI_TT Excel 中误收的 PPA 参数在 OOS/OOC 读取副本中排除，不修改工作簿或人工决策，不将 TT 数值拼装成 SPC 特征。SPC 预警仍读取自身结果文件。分类规则版本参与 Inline 单元格、跨进程矩阵快照及运行时计算的输入签名，避免复用修复前的结果。

红灯预警表仍读结果 Excel；需要图像时再调用对应报表服务获取量测／特征。因此状态灯 Excel-only 不代表红灯图像不访问原始快照。接口复用也不保证不同查询参数命中同一顶层缓存。

### SPC：趋势波动（CPK / CPM）

报表、矩阵状态与明细经共同预警服务调用 `build_capability_latest_reader` 装配的 CpkLatestExcelStore / CpmLatestExcelStore，读取配置指定的能力工作簿（默认 resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx）。CPK 读取产品同名 sheet，CPM 读取 `<产品>_cpm` sheet；两者共用工作簿解析缓存，独立判定各自指标。

共同服务使用已有 `latest_capability_alerts`：筛选上一完整 ISO 周、flag=False、对应 corrected 值 < 1.33，并遵守日期排除和可配置的最小 Sheet 数据跨度。状态不重新计算能力。详情展示厂别、站点、参数、超规周次和对应能力值；仅有预警时按厂别／站点／参数命中键筛选 SPC 三图。缺数据保留后台 no_data 语义，异常保留 error；某个指标读取或绘图失败不阻断其它预警。

图像加载调用 `SpcReportService.get_spc_report_data`，与 SPC 页面共用 `SPC_REPORT_CACHE_SIGNATURE`、`SPC_REPORT_CACHE_INDICATORS`、日期窗口、源输入签名、决策签名与 sigma 配置；相同输入命中同一 application payload 缓存，CPK / CPM 不分别计算。缓存缺失或真实输入变化时仍按已有服务构建。两种详情的图像 memo / chart key 分开，手动刷新可分别选中各指标／产品，不改变下方独立历史汇总算法。

### AOI 报表中的趋势预警（不新增矩阵行）

AOI_RS／AOI_TT报表的昨日密度预警也使用共同展示服务，分别以 `aoi_rs_trend`、`aoi_tt_trend` 读取独立发布的完整评估。算法、Total筛选和分母保持原域实现；不通过OOS文件重建趋势。这两个报表预警不属于上表十三行，当前矩阵不额外增加行。结果文件与失败隔离见 [共同展示设计](../../../design/inline_domain/shared/architecture-published-alert-display.md)。

## yield_domain

### Yield：单片异常（Lot 超规）

实际对象是 Lot。原 Yield 报表用既有判据生成结果，矩阵读取配置项 `lot_oos_results` 指定的工作簿，再截取上一完整 ISO 周；不再在状态读取路径调用完整 Lot 服务或查询源数据。

### Yield：趋势波动

原报表通过 `AlertService.get_dashboard_alert_records` 生成月／周、Group／Code 结构化结果，发布到 `trend_alert_results` 指定的工作簿。矩阵直读当前有效产品结果，仍为 period 制，不强行截成上一周事件。

两行分别保留本指标与产品的 revision，并跟踪结果文件版本、元数据有效性、策略与参考日。没有文件、过期、发布失败或覆盖不足时显示不可用，不回退分析；有效零报警与有效空来源分别保留 ok / no_data。红灯详情仍按需装配图像输入。

结果由原报表 application 独立发布，不受 Sheet/Mapping 或图像门控影响；文件解析缓存只负责加速读取。首次全产品补齐可用 `tools/publish_yield_alert_results.py` 调用同一生产用例，不新增调度机制。文件、元数据、生命周期和命令由 [Yield 结果规则](../../yield_domain/shared/rules-yield-alert-results.md) 唯一维护。详情的旧报表调用仍保持 `read_only=True`，不承诺图像读取零计算。

## indicator_domain：Q-Time 单片异常

正常厂别生产入口 `get_cached_shop_monitoring` 继续按原有服务获取全站点、全产品结果，成功后发布 `resources.indicator_domain.files.qtime_alerts`（默认 `resources/indicator_domain/qtime/qtime_alerts.xlsx`）。各厂别分别拥有告警sheet、最小Lot归属sheet和一行发布元数据；不改原始L1或人工决策表。

报表和矩阵都通过共同 `read_alert_result` 消费 `read_qtime_alert_result`。矩阵默认要求 ARRAY／OLED／TP 全部已有当日、TTL内、源健康有效且覆盖上周的发布，按产品筛选上一完整周；缺厂别或覆盖不足为error，不调用监控服务补齐。报表显式传选定厂别／站点及原滚动窗口，保留查询范围；相同范围命中相同展示缓存。

状态和预警表不创建数据库对象。Q-Time矩阵详情先展示共同保存记录及相应Lot计数；用户另点“加载预警站点图像”后才进入原有报表图像管路，失败仅提示图像不可用。定向刷新只推进对应指标／产品标识，不强刷所有厂别，也不发布缺失结果。

缺结果文件仍为不可用。用户明确要求预建时，可创建含三个厂别告警／Lot归属表头及空`__meta__`表头的初始化模板；全部业务表和元数据为空时返回`no_data`，展示“当前暂无预警记录”，不虚构成功、发布时间或当前来源健康。非空告警缺元数据、损坏模板、陈旧正式发布仍为error。开始真实发布后，矩阵仍要求所需三个厂别均有有效结果；由正常报表工作流发布，矩阵定时预计算不能代替生产。

## IJP、关键备件与 IQC 蒸镀材料：只读已发布快照

`app/sections/shared/published_alert_snapshots.py` 是报表和聚合页共同使用的文件验证适配器，原monitor路径保留兼容导入。共同 `read_alert_result` 装配它，并将结果选择缓存提供给报表、状态灯与详情。读取不导入其 application 计算/查询函数，不查询数据库、不发布或修改快照。默认路径均通过全局资源注册解析：

| 指标 | 配置键 | 默认位置 / sheet |
|---|---|---|
| IJP AA／孔区 | `resources.indicator_domain.files.ijp_alerts` | `resources/indicator_domain/ijp/ijp_alerts.xlsx` 的 `AA区` / `孔区` |
| 关键备件 | `resources.equipment_domain.files.parts_alerts` | `resources/equipment_domain/parts/parts_alerts.xlsx` 的 `超规备件` |
| IQC 蒸镀材料 | `resources.iqc_domain.files.evaporation_alerts` | `resources/iqc_domain/eva_materials/evaporation_alerts.xlsx` 的 `超规材料` |

- 每个读取版本以绝对路径、mtime_ns 和大小缓存整本 Excel，最多 16 项；IJP 两区及多个产品复用一次解析。该缓存只缓存文件解析，不是业务 application 缓存。
- 校验 `__meta__` 的成功状态、条数与公开 schema；发布时间必须为当前参考日，且距当前时刻不超过全局缓存 TTL，不接受未来时间。IJP 和 IQC 校验上一完整周范围，备件校验报告日；IQC 另校验该产品在发布范围内。
- 正式发布的有效空清单为 `ok`；文件/sheet/元数据缺失、读取失败、陈旧、周期不符或发布不完整均为 `error`。明确初始化的仅表头空表按下条规则识别为 `no_data`。不回退运行应用服务、不修补源文件，不将旧快照当作本期结果。
- 2026-10-27 用户要求预建空表，并确认“暂无数据”也显示绿色（无异常）。三本快照可仅有业务表头及空的 `__meta__` 表头，没有业务或发布记录；这些明确初始化的空表为 `no_data`，矩阵显示绿色，悬停和点击提示“当前暂无预警记录”。初始化空表不伪造成功状态、发布时间或周期，也不套用真实发布的有效期；随后产生正式发布元数据时恢复完整有效期检查。IJP 首次只发布其中一个区域时，另一个仍仅有表头的区域保持 `no_data`。所有已保存记录都必须有有效元数据，不能用删除元数据来掩盖非空预警；缺失文件、损坏及陈旧的正式发布仍为 `error`。
- IJP 按 `productcode` 精确匹配；IQC 按“产品型号”中以“、”分隔的产品代码精确匹配，支持一条检验关联多个产品，避免 `M67` 误匹配 `M678`。
- 备件快照没有产品归属；此行明确标为全厂共用，各产品列复用同一全厂状态和明细，不虚构产品映射。按真实“厂别”支持筛选，详情也按当前厂别切片。IJP/IQC 不将线体或 WMS 工厂编码擅自映射为矩阵 ARRAY/OLED/TP，行声明不支持厂别细分。
- 来源签名包含文件版本、参考日和即时有效性；即使文件未改写，过期也会使单元格及跨进程快照失效。点击详情再次检查文件有效性，详情只展示公开白名单；不构建旧报表图像 loader，不传递原始测量值、flag 或其他内部字段。
- 管理员定向刷新只清除对应文件版本的解析条目并推进所选单元格 revision，不触发报表计算。页面只在原有报表图像详情需要时才获取数据库对象和站点说明；新增四行详情不需要这些依赖。

结果发布规则由 [IJP 预警规则](../../indicator_domain/ijp/rules-ijp-alerts.md)、[关键备件数据流](../../equipment_domain/parts/data-flow-critical-parts.md#53-自动超规预警与-excel-快照)、[蒸镀材料规则](../../iqc_domain/eva_materials/rules-evaporation-product-scope.md#自动-ng-预警与本地快照) 分别拥有；矩阵只消费这些结果。

## 缓存与刷新机制

查询 → 读取各指标／产品的输入签名和 revision → 单元格缓存 → 组合后台状态 → compliance 显示副本 → 前端展示 → 红灯按需加载详情。

| 层 | 内容与键 | 失效范围 |
|---|---|---|
| 单元格 | _cached_alert_matrix_cell；指标、产品、周期、输入签名、revision；TTL 取全局配置，最多 1024 项 | 对应单元格 |
| application／输入 | 既有报表缓存、Excel 解析、Q-Time 厂别结果 | 真实输入与既有共享边界 |
| 共同预警展示 | `_cached_alert_result`；指标、产品、参考日、文件／策略／有效性签名、revision及范围，最多512项 | 相同参数的报表、状态灯和详情共同失效；不运行原始生产 |
| 详情 | _cached_matrix_detail_bundle；详情键、参考日、单元格签名及该单元格计算时间 | 对应详情，不依赖整板生成时间 |
| compliance | 小表文件签名，缓存外应用覆盖 | 仅显示副本 |

**没有整张矩阵计算缓存**。app/components/indicator_cache.py 管理指标／产品 revision，保存于 output/tmp/indicator_product_cache_revisions/，哈希文件名并原子替换。没有 domain 键，也不通过全局 st.cache_data.clear 或整函数 clear 刷新矩阵。

输入签名统一由共同展示服务提供：四个OOS工作簿及PPA／日期过滤、CPK／CPM能力工作簿与跨度策略、Yield结果与有效性、Q-Time已发布结果与源政策身份，以及IJP／备件／IQC文件有效性。共享工作簿修改可能使多个依赖单元格重新读取；手动推进某指标／产品revision则不会推进其他标识。

仅 ?admin=true 可见“指标缓存管理（管理员）”、定向刷新按钮、刷新标识和状态计算时间。聚合页关闭旧通用全量刷新入口。七个实际单产品 header 页面已迁移；多输出页显式声明其指标集合，刷新仅作用当前产品，图像 memo 同步跟踪新版本。见 [产品页面缓存审查](../../../../docs/dev_docs/generated/shared_kernel/shared/application-cache-design-audit-by-domain.md)。

## 显示与安全边界

后台保留 ok／alert／no_data／error。前端将 no_data 显示达标；compliance 对应单元格 True 时，显示和点击详情同为达标，不运行红灯 loader、不改原缓存。未修饰加载错误仍展示错误。

厂别筛选按 alert_factories 调整支持该功能的行，Yield 保持原状态。既有详情入口不承诺与厂别筛选完全同步。

本次不改变原始快照保留策略、不删除生产快照、不回写生产工作簿。隔离缓存解决局部刷新引起整板重算，不保证首次缺少 application 缓存时复杂指标也能零成本加载。

## 源码与验证

- app/sections/inline_domain/monitor/alert_matrix_cache.py：来源、单元格键、惰性装配。
- app/sections/shared/alert_results.py：报表／矩阵共同预警显示服务与缓存；同目录published_alert_snapshots.py负责IJP／备件／IQC的只读文件验证。
- 同目录 alert_matrix_service.py：十三行声明与 CPK / CPM 结果投影；alert_matrix.py／alert_matrix_detail.py：管理员、显示、详情；published_alert_snapshots.py：IJP/备件/IQC 本地快照直读及有效性检查。
- app/components/indicator_cache.py／page_header.py：共享标识与产品页刷新。
- tests/unit/app/sections/monitor/test_matrix_cell_isolation.py：跨指标／产品与 CPK 直读。
- tests/unit/app/sections/monitor/test_shared_alert_results.py：PPA过滤、文件／配置失效、同参数结果缓存复用、图像失败保留预警及内容变化在TTL内发布；tests/unit/app/pages/test_inline_saved_alerts.py验证报表初始化／生产失败仍显示保存结果。
- tests/unit/app/sections/monitor/test_published_alert_snapshots.py：三本 Excel、四个新行的精确产品匹配、全厂共用、公开详情、缺失/陈旧/损坏及签名失效；用禁止调用的替身验证 application 与 SQL 不会执行，文件内容保持不变。
- tests/unit/app/sections/monitor/test_cpm_matrix.py：CPM 周度判定、独立 sheet、三图及 SPC 页面 application 缓存复用。
- tests/e2e/fixtures/indicator_product_cache_app.py、tests/e2e/indicator_product_cache.js：真实矩阵／缓存组件的隔离浏览器验收，使用临时 Excel 与计数器，不运行生产数据库查询。

# 全指标预警矩阵定时预计算

## 运行方式

Windows 任务 `vivo-project Alert Matrix Warmup` 每天本地时间 09:02 运行
`tools/warm_alert_matrix.py`，使用项目 `.venv/Scripts/pythonw.exe`，工作目录为仓库根目录。
注册命令：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/register_alert_matrix_task.ps1
```

脚本默认拒绝覆盖同名任务；显式 `-Force` 可更新任务。任务使用当前登录用户的普通权限，
不存储密码、不显示控制台；支持锁屏运行、唤醒及错过时间后补跑。电脑需要开机，用户需已登录，
企业数据库网络和 Excel 环境需可用；关机或用户注销时无法按时执行。失败后间隔 15 分钟重试，最多两次；
同名任务不并行，单次最长两小时。任务触发按系统时区，中国标准时间下为北京时间 09:02，
安排在每日 09:00 服务重启之后；这是固定时间触发，不是服务健康检查成功后的联动任务。
预计算进程将状态发布到磁盘，服务重启清除内存缓存时不会删除该快照，页面校验有效后仍可复用。

手动运行：`.venv/Scripts/python.exe tools/warm_alert_matrix.py`。
`--check` 仅验证输出目录可以创建，不查询数据库。正常运行等价于矩阵查询，不强刷全部源快照，
不预建详情图像，不改变各指标的判定和只读约定。

## 跨进程复用

Streamlit 内存缓存不能由独立定时进程直接共享。因此任务调用既有全产品矩阵计算入口，
将原生单元格状态保存至 `output/cache/alert_matrix/daily.json`。跨域矩阵契约由
`app/sections/inline_domain/monitor/` 维护，文件只保存状态和签名，不保存 DataFrame、
数据库对象或原始驱动错误消息。写入先生成独立临时文件，再原子替换，失败不破坏旧快照。

页面发现当天、TTL 有效且格式正确的快照后，自动展开全指标矩阵，不必点击查询。
主动收起后同一天不会被自动重新展开；没有有效快照时保留原查询按钮。
普通矩阵查询逐单元格核对当前源文件/配置签名和产品指标 revision，只有匹配项才复用。
源数据变化或管理员定向刷新时，受影响单元格按原路径计算；其他产品和指标仍可复用。
快照同时受报告日期和 `application.cache_ttl_hours` 限制，不将昨日结果当作今日结果。
脚本发布的是未应用显示修饰的结果，页面仍照常应用显示配置。

## 状态和验证

每次任务生成 `output/logs/alert_matrix_warmup/<时间>-<唯一标识>.json`：

- `running`：已启动，尚未完成；强制终止或超时时可能停留在此状态，应同时查看任务计划程序结果。
- `success` / 退出码 0：矩阵计算完成，所有单元格均未出现 `error`；不表示所有指标达标或来源均 fresh。
- `partial` / 退出码 1：已发布含错误状态的矩阵，记录失败产品指标，不伪装成达标。
- `failed` / 退出码 2：整体失败，保留旧文件；页面仍按日期、TTL和签名判断其是否可用。

定点验证覆盖新进程复用、单元格失效、日期与 TTL、损坏文件降级、原子写失败保留旧文件、
任务退出码及页面自动展开/收起。此机制仅预计算矩阵状态，不能保证详情图像或良率完整页面无需后续加载。
