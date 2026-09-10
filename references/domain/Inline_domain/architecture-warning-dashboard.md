# 全指标预警看板：By Domain / By 指标实现说明

更新日期：2026-09-08。本次以指标 × 产品单元格缓存替代整张矩阵缓存，SPC CPK 改为结果 Excel 直读。范围仅为顶部全指标矩阵，不包含下方 Inline／CPK 历史汇总看板。

## 数据入口分类

- **A：本地结果直读**：读取其他模块已产出的 Excel，做字段投影、时间筛选与预警存在性判断，不重新分析原始量测。
- **B：application 管路复用**：调用报表已有服务和缓存；只有函数及参与哈希的参数一致，才共享同一缓存条目。
- **C：独立分析管路**：另建原始数据分析引擎。当前八行没有这类引擎；轻量状态归约、组合和显示适配不是另一套业务算法。

| Domain | 指标键／显示名称 | 状态灯 | 红灯详情 |
|---|---|---|---|
| inline_domain | aoi_rs_sheet_oos／AOI_RS 单片异常 | A：AOI_RS OOS Excel | A：预警表；B：报表图像 |
| inline_domain | aoi_tt_sheet_oos／AOI_TT 单片异常 | A：AOI_TT OOS Excel | A：预警表；B：报表图像 |
| inline_domain | spc_sheet_oos／SPC 单片异常 | A：SPC OOS Excel | A：预警表；B：报表图像 |
| inline_domain | spc_cpk_trend／SPC 趋势波动（CPK） | A：CPK Excel | A：同一 CPK Excel，无原始 SPC 查询 |
| inline_domain | ctq_sheet_oos／CTQ 单片异常 | A：CTQ OOS Excel | A：预警表；B：报表图像 |
| yield_domain | yield_lot_oos／Yield 单片异常（Lot 超规） | B：Lot 不良率＋共用判据 | B：Yield 详情 |
| yield_domain | yield_trend_fluctuation／Yield 趋势波动 | B：Group/Code 趋势＋共用探测器 | B：Yield 详情 |
| indicator_domain | qtime_sheet_oos／Q-Time 单片异常 | B：共享厂别监控结果按产品筛选 | B：共享监控结果 |

Domain 仅用于文档分类和界面分组，**不参与刷新标识维度**。

## inline_domain

### AOI_RS、AOI_TT、SPC、CTQ：单片异常

读取 resources/inline_domain/<scope>/<scope>_sheet_oos_decoration.xlsx 的产品同名 sheet。生产 composition 的 build_oos_history_service 返回 ExcelAlarmReader，不是旧原始快照重分析服务；缺明细不会回退运行原始分析。

| scope | 时间字段 | 项目／指标 |
|---|---|---|
| aoi_rs | sheet_start_time | point_id／rs_code |
| aoi_tt | start_time | sheet_id／tt_name |
| spc、ctq | sheet_start_time | sheet_id／param_name |

统一投影后，以 [上周一 00:00, 本周一 00:00) 内 flag=False 的 OOS 记录判断是否预警。aoi_tt_sheet_ooc_decoration.xlsx 等 **OOC** 文件不是这四行输入；当前没有独立 OOC／IJP 行。

红灯预警表仍读结果 Excel；需要图像时再调用对应报表服务获取量测／特征。因此状态灯 Excel-only 不代表红灯图像不访问原始快照。接口复用也不保证不同查询参数命中同一顶层缓存。

### SPC：趋势波动（CPK）

状态与默认详情均由 CpkLatestExcelStore 读取 resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx。读取产品 CPK sheet，不把 CPM sheet 当成 CPK 输入。

统一使用 build_latest_cpk_alerts：规范化结果字段，筛选上一完整 ISO 周、flag=False、cpk_corrected < 1.33。详情展示厂别、站点、参数、超规周次和 CPK 值，不再获取 SPC 周期能力 payload，不为默认详情运行原始 SPC 分析。缺数据保留后台 no_data 语义，异常保留 error。

这替代了旧设计“CPK 复用完整 SPC payload”的选择，不改变 SPC 主报表能力分析，也不修改下方独立 CPK 汇总看板的历史更新算法。

## yield_domain

### Yield：单片异常（Lot 超规）

实际对象是 Lot。复用 YieldAnalysisService.get_lot_defect_rates、load_static_warning_lines 和 compute_lot_oos_records，再将超规记录截取上一完整 ISO 周，没有另写良率判据。

### Yield：趋势波动

复用 get_mwd_trend_data、get_code_level_trend_data 和 AlertService.get_dashboard_alert_records。口径是月／周环比 period 制，不强行截成上一周事件。

两行分别传入本指标与产品的 revision。主报表的趋势／Lot／Sheet 分别生成签名，刷新 Lot 不会仅因同产品版本变化而使趋势缓存失效。红灯详情仍按需装配图像输入。

矩阵保持 read_only=True 避免修饰表回写；主报表默认模式可能不同。因此复用 application 管路，但不承诺所有顶层条目完全相同。read_only 不等于零计算或禁止读取数据库。

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

输入签名：四个 OOS 工作簿、CPK 工作簿、Yield 本产品上下文、Q-Time 来源／决策／配置。共享工作簿修改可能使该指标多个产品重新读取；手动推进某指标／产品 revision 则不会推进其他标识。

仅 ?admin=true 可见“指标缓存管理（管理员）”、定向刷新按钮、刷新标识和状态计算时间。聚合页关闭旧通用全量刷新入口。七个实际单产品 header 页面已迁移；多输出页显式声明其指标集合，刷新仅作用当前产品，图像 memo 同步跟踪新版本。见 [产品页面缓存审查](product-page-cache-audit.md)。

## 显示与安全边界

后台保留 ok／alert／no_data／error。前端将 no_data 显示达标；compliance 对应单元格 True 时，显示和点击详情同为达标，不运行红灯 loader、不改原缓存。未修饰加载错误仍展示错误。

厂别筛选按 alert_factories 调整支持该功能的行，Yield 保持原状态。既有详情入口不承诺与厂别筛选完全同步。

本次不改变原始快照保留策略、不删除生产快照、不回写生产工作簿。隔离缓存解决局部刷新引起整板重算，不保证首次缺少 application 缓存时复杂指标也能零成本加载。

## 源码与验证

- app/sections/inline_domain/monitor/alert_matrix_cache.py：来源、单元格键、惰性装配。
- 同目录 alert_matrix_service.py：八行判据与 CPK 结果投影；alert_matrix.py／alert_matrix_detail.py：管理员、显示、详情。
- app/components/indicator_cache.py／page_header.py：共享标识与产品页刷新。
- tests/unit/app/sections/monitor/test_matrix_cell_isolation.py：跨指标／产品与 CPK 直读。
- tests/e2e/fixtures/indicator_product_cache_app.py、tests/e2e/indicator_product_cache.js：真实矩阵／缓存组件的隔离浏览器验收，使用临时 Excel 与计数器，不运行生产数据库查询。
