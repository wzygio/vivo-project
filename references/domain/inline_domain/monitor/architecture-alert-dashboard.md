# 全指标预警看板：By Domain / By 指标实现说明

更新日期：2026-10-20。顶部矩阵的 Inline 与 Yield 状态均读取已发布结果，整本 Excel 解析在内存中复用；Q-Time 保留共享监控管路，点击详情继续按需使用报表服务。范围不包含下方 Inline／CPK／CPM 历史汇总看板。

## 数据入口分类

- **A：本地结果直读**：读取其他模块已产出的 Excel，做字段投影、时间筛选与预警存在性判断，不重新分析原始量测。
- **B：application 管路复用**：调用报表已有服务和缓存；只有函数及参与哈希的参数一致，才共享同一缓存条目。
- **C：独立分析管路**：另建原始数据分析引擎。当前九行没有这类引擎；轻量状态归约、组合和显示适配不是另一套业务算法。

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
| indicator_domain | qtime_sheet_oos／Q-Time 单片异常 | B：共享厂别监控结果按产品筛选 | B：共享监控结果 |

Domain 仅用于文档分类和界面分组，**不参与刷新标识维度**。

## inline_domain

### AOI_RS、AOI_TT、SPC、CTQ：单片异常

读取 resources/inline_domain/<scope>/<scope>_sheet_oos_decoration.xlsx 的产品同名 sheet。矩阵生产上下文注入已有的 ExcelAlarmReader / ExcelAlarmStore，先按路径、mtime_ns、大小缓存整本工作簿，再从其中选择产品；同一文件版本不为每个产品重复打开 Excel。缺明细不会回退运行原始分析。

| scope | 时间字段 | 项目／指标 |
|---|---|---|
| aoi_rs | sheet_start_time | point_id／rs_code |
| aoi_tt | start_time | sheet_id／tt_name |
| spc、ctq | sheet_start_time | sheet_id／param_name |

统一投影后，以 [上周一 00:00, 本周一 00:00) 内 flag=False 的 OOS 记录判断是否预警。aoi_tt_sheet_ooc_decoration.xlsx 等 **OOC** 文件不是这四行输入；当前没有独立 OOC／IJP 行。

TT 结果读取遵守 [SPC 参数分类规则](../spc/data-flow-spc.md#23-匹配参数白名单与-spc-分类)：旧 AOI_TT Excel 中误收的 PPA 参数在 OOS/OOC 读取副本中排除，不修改工作簿或人工决策，不将 TT 数值拼装成 SPC 特征。SPC 预警仍读取自身结果文件。分类规则版本参与 Inline 单元格、跨进程矩阵快照及运行时计算的输入签名，避免复用修复前的结果。

红灯预警表仍读结果 Excel；需要图像时再调用对应报表服务获取量测／特征。因此状态灯 Excel-only 不代表红灯图像不访问原始快照。接口复用也不保证不同查询参数命中同一顶层缓存。

### SPC：趋势波动（CPK / CPM）

状态与预警明细复用 `build_capability_latest_reader` 装配的 CpkLatestExcelStore / CpmLatestExcelStore，读取配置指定的能力工作簿（默认 resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx）。CPK 读取产品同名 sheet，CPM 读取 `<产品>_cpm` sheet；两者共用工作簿解析缓存，独立判定各自指标。

统一使用 `build_latest_capability_alerts(metric=...)` 和已有 `latest_capability_alerts`：筛选上一完整 ISO 周、flag=False、对应 corrected 值 < 1.33，并遵守日期排除和可配置的最小 Sheet 数据跨度。状态不重新计算能力。详情展示厂别、站点、参数、超规周次和对应能力值；仅有预警时按厂别／站点／参数命中键筛选 SPC 三图。缺数据保留后台 no_data 语义，异常保留 error。

图像加载调用 `SpcReportService.get_spc_report_data`，与 SPC 页面共用 `SPC_REPORT_CACHE_SIGNATURE`、`SPC_REPORT_CACHE_INDICATORS`、日期窗口、源输入签名、决策签名与 sigma 配置；相同输入命中同一 application payload 缓存，CPK / CPM 不分别计算。缓存缺失或真实输入变化时仍按已有服务构建。两种详情的图像 memo / chart key 分开，手动刷新可分别选中各指标／产品，不改变下方独立历史汇总算法。

## yield_domain

### Yield：单片异常（Lot 超规）

实际对象是 Lot。原 Yield 报表用既有判据生成结果，矩阵读取配置项 `lot_oos_results` 指定的工作簿，再截取上一完整 ISO 周；不再在状态读取路径调用完整 Lot 服务或查询源数据。

### Yield：趋势波动

原报表通过 `AlertService.get_dashboard_alert_records` 生成月／周、Group／Code 结构化结果，发布到 `trend_alert_results` 指定的工作簿。矩阵直读当前有效产品结果，仍为 period 制，不强行截成上一周事件。

两行分别保留本指标与产品的 revision，并跟踪结果文件版本、元数据有效性、策略与参考日。没有文件、过期、发布失败或覆盖不足时显示不可用，不回退分析；有效零报警与有效空来源分别保留 ok / no_data。红灯详情仍按需装配图像输入。

结果由原报表 application 独立发布，不受 Sheet/Mapping 或图像门控影响；文件解析缓存只负责加速读取。首次全产品补齐可用 `tools/publish_yield_alert_results.py` 调用同一生产用例，不新增调度机制。文件、元数据、生命周期和命令由 [Yield 结果规则](../../yield_domain/shared/rules-yield-alert-results.md) 唯一维护。详情的旧报表调用仍保持 `read_only=True`，不承诺图像读取零计算。

## indicator_domain：Q-Time 单片异常

复用 get_cached_shop_monitoring，对 ARRAY／OLED／TP 获取全站点、全产品厂别监控结果；同一上下文内复用合并结果，再按产品与上一完整 ISO 周筛选。

保留原有厂别级 application 和原始 L1 共享策略，不建立产品级原始快照。矩阵与详情以实际参考日传递 as_of，与当天报表对齐；判据仍是上一完整周。Q-Time 单元格键包含参考日，并跟踪源 Parquet、决策表和配置签名。

定向刷新 Q-Time／M626 只推进该单元格标识，重新筛选共享结果，**不是强制重建所有厂别原始快照**。数据库来源更新仍用 Q-Time 原有工作流。共享底层文件真正改变后，多个依赖产品重新读取属于正常依赖失效。

## 缓存与刷新机制

查询 → 读取各指标／产品的输入签名和 revision → 单元格缓存 → 组合后台状态 → compliance 显示副本 → 前端展示 → 红灯按需加载详情。

| 层 | 内容与键 | 失效范围 |
|---|---|---|
| 单元格 | _cached_alert_matrix_cell；指标、产品、周期、输入签名、revision；TTL 取全局配置，最多 1024 项 | 对应单元格 |
| application／输入 | 既有报表缓存、Excel 解析、Q-Time 厂别结果 | 真实输入与既有共享边界 |
| 详情 | _cached_matrix_detail_bundle；详情键、参考日、单元格签名及该单元格计算时间 | 对应详情，不依赖整板生成时间 |
| compliance | 小表文件签名，缓存外应用覆盖 | 仅显示副本 |

**没有整张矩阵计算缓存**。app/components/indicator_cache.py 管理指标／产品 revision，保存于 output/tmp/indicator_product_cache_revisions/，哈希文件名并原子替换。没有 domain 键，也不通过全局 st.cache_data.clear 或整函数 clear 刷新矩阵。

输入签名：四个 OOS 工作簿、CPK / CPM 共用能力工作簿、Yield 两个结果工作簿及其读取有效性、Q-Time 来源／决策／配置。共享工作簿修改可能使多个依赖单元格重新读取；手动推进某指标／产品 revision 则不会推进其他标识。

仅 ?admin=true 可见“指标缓存管理（管理员）”、定向刷新按钮、刷新标识和状态计算时间。聚合页关闭旧通用全量刷新入口。七个实际单产品 header 页面已迁移；多输出页显式声明其指标集合，刷新仅作用当前产品，图像 memo 同步跟踪新版本。见 [产品页面缓存审查](../../../../docs/dev_docs/generated/shared_kernel/shared/application-cache-design-audit-by-domain.md)。

## 显示与安全边界

后台保留 ok／alert／no_data／error。前端将 no_data 显示达标；compliance 对应单元格 True 时，显示和点击详情同为达标，不运行红灯 loader、不改原缓存。未修饰加载错误仍展示错误。

厂别筛选按 alert_factories 调整支持该功能的行，Yield 保持原状态。既有详情入口不承诺与厂别筛选完全同步。

本次不改变原始快照保留策略、不删除生产快照、不回写生产工作簿。隔离缓存解决局部刷新引起整板重算，不保证首次缺少 application 缓存时复杂指标也能零成本加载。

## 源码与验证

- app/sections/inline_domain/monitor/alert_matrix_cache.py：来源、单元格键、惰性装配。
- 同目录 alert_matrix_service.py：九行判据与 CPK / CPM 结果投影；alert_matrix.py／alert_matrix_detail.py：管理员、显示、详情。
- app/components/indicator_cache.py／page_header.py：共享标识与产品页刷新。
- tests/unit/app/sections/monitor/test_matrix_cell_isolation.py：跨指标／产品与 CPK 直读。
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
