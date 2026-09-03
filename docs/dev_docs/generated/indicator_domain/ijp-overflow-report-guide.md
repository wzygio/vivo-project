# IJP 溢流监控报表说明

> 适用页面：`app/pages/IJP溢流监控报表.py`  
> 文档依据：当前仓库源码、ADR-0020/0021/0022、IJP 数据源分析；核对日期：2026-09-03。

## 1. 报表用途

IJP 溢流监控报表用于观察 OLED 制程中 IJP 打印相关溢流缺陷的时间趋势、缺陷代码构成和具体发生位置，并下钻到 Glass、Panel、打印设备及原始缺陷图片。

它适合回答以下问题：

- 最近哪些日期的溢流缺陷代码构成发生了变化？
- 异常集中在哪个产品、工单类型、线体、设备、批次或 Cycle？
- 某个 Glass 上出现了哪些溢流相关 CODE，各自占该 Glass 受监控记录的多少？
- 缺陷位于 Panel 的哪一侧或哪个角，原始图片是什么？

该报表属于 `indicator_domain` 的 IJP 子模块。IJP 与 Q-Time 是指标监控领域下的同级能力，IJP 并不从属于 Q-Time。

## 2. IJP 溢流是什么

### 2.1 IJP

IJP 是 **Inkjet Printing（喷墨打印）**。在本项目的 OLED 制程语境中，它是 OLED 段的一道子工艺，用来制作 TFE（Thin-Film Encapsulation，薄膜封装）中的有机层。

公开研究也说明，OLED 的有机/无机复合 TFE 可以使用 IJP 打印丙烯酸酯类聚合物层，使其位于相邻无机阻隔层之间；这一结构用于阻隔水氧并缓解无机层裂纹传播。参见 [ACS：Organic/Inorganic Hybrid Thin-Film Encapsulation Using Inkjet Printing and PEALD](https://pubs.acs.org/doi/10.1021/acsami.1c12253)。

### 2.2 溢流

从喷墨成膜的一般机理看，“溢流”是液态墨水或树脂超出预定打印、围挡或成膜边界的现象。它可能与喷墨量、落点偏移、润湿和铺展、边界结构等因素有关。公开 OLED IJP 研究中，围挡结构用于限制液滴铺展和引导流动，双层围挡也被用于防止落墨溢出；参见 [Organic Electronics：Inkjet-printing line film with varied droplet-spacing](https://doi.org/10.1016/j.orgel.2017.08.012) 和 [SID：Effect of Double-Layered Bank Structure on Hole-Injection Properties in Inkjet-Printed OLED Devices](https://doi.org/10.1002/sdtp.14330)。

但必须区分工艺概念与本报表的统计口径：

- 本报表**不直接测量墨水体积、边界外扩距离或膜厚**。
- 本报表把 AOI/RS 缺陷数据中，命中指定 IJP 打印设备和指定 RS_CODE 的记录作为“溢流监控数据”。
- 因此它是基于检测与复判结果的质量监控报表，而不是 IJP 设备实时流量监控。

当前统计范围固定为：

- 3 条线体：`3CEE01`、`3CEE02`、`3CEE04`；
- 5 台 IJP 打印设备：`3CEE01-IK2-PR1`、`3CEE01-IK2-PR2`、`3CEE02-IK2-PR1`、`3CEE02-IK2-PR2`、`3CEE04-IKT-PRT`；
- 12 个溢流相关 RS_CODE：`C3DM0`～`C3DM5`、`C3RA1`～`C3RA3`、`C3ZC1`、`C3BH1`、`C3BH2`。

仓库目前没有提供这 12 个 CODE 各自的业务中文释义。因此，报表只能可靠地按 CODE 分类、统计和追溯，不能据此文档进一步断言每个 CODE 的物理缺陷类型；具体释义应以受控的 RS 缺陷代码字典为准。

## 3. 如何使用报表

### 3.1 推荐操作流程

1. 打开“**IJP溢流监控报表**”页面。
2. 设置开始日期和结束日期。页面默认选择昨天至今天；查询覆盖开始日 00:00:00 到结束日 23:59:59.999999。
3. 按需要选择产品型号、产品名称、工单类型、线体、设备、CODE、边框、批次和 Cycle；也可以输入一个或多个 Glass ID。
4. 如需在趋势图上显示参考线，填写 `Target值（%）`；不填写则不画参考线。
5. 点击“**查询**”。仅修改筛选条件不会自动查询；条件改变后，旧结果会失效，需要再次点击“查询”。
6. 先看 `OLED RS Overflow By天` 堆叠图判断 CODE 构成变化，再从明细表定位 Glass、Panel、设备和边框位置。
7. 点击明细中的“原图”链接查看对应缺陷图片；该地址是企业内网图片服务，需具备相应网络和访问条件。

### 3.2 筛选条件说明

| 筛选项 | 含义与用法 |
|---|---|
| 开始日期、结束日期 | 查询采用闭区间，覆盖开始日 00:00:00 至结束日 23:59:59.999999；结束日期早于开始日期时，页面会提示错误。 |
| Target值（%） | 只在日趋势图上绘制一条 0～100% 的水平虚线参考线，便于目视对照；不参与筛选，也不会自动产生告警。控件旁的帮助图标提供相同说明。 |
| 产品型号 | OLED 产品代码；也会约束产品名称、批次和 Cycle 的候选范围。 |
| 产品名称 | 产品规格名称。 |
| 工单类型 | OLED 工单的 `SUB_PROD_TYPE`。 |
| GlassID | 支持多个值，使用中文逗号、英文逗号或换行分隔；系统会去空格、去空值和去重。 |
| 线体 | IJP 设备号前 6 位，目前为 `3CEE01`、`3CEE02`、`3CEE04`。 |
| 设备 | 当前纳入监控的 5 台 IJP 打印设备。 |
| CODE | 当前纳入监控的 12 个 RS 缺陷代码；多选时查看所选代码的构成。 |
| 边框 | 标准位置包括 TOP、BOTTOM、LEFT、RIGHT 及四个角。底边记录在明细中还可能细分为 BOTTOM0～BOTTOM9。 |
| 批次 | Cycle 数据中的 `PICI`；候选项受时间和产品型号约束。 |
| Cycle | Cycle ID；候选项受时间、产品型号和批次约束。 |

除产品型号外，多选条件为空时表示“不按该维度过滤”，并不表示无数据。产品型号为空时仍会自动限定为 `config/global.yaml` 中 `product_registry.enabled_products` 的启用产品；下拉选项同样只展示“数据库存在且已启用”的产品。

## 4. 如何阅读结果

### 4.1 `OLED RS Overflow By天` 图

图表按“日期 × RS_CODE”显示 100% 堆叠柱：

- 每个颜色代表一个 RS_CODE；
- 每天各 CODE 的占比合计约为 100%；
- 占比计算为：`当日某 CODE 记录数 ÷ 当日全部受监控 CODE 记录数`；
- 它反映的是**缺陷代码构成**，不是良率、不良率、Glass 异常率或设备溢流率；
- 某个颜色占比上升，表示该 CODE 在当日受监控记录中的相对份额上升，不一定表示其绝对数量上升。

为保留趋势背景，图表的实际起点会比所选开始时间再向前扩展 7 天，结束时间不变。因此，图上可能出现早于明细查询开始时间的日期。

当前实现中，“边框”属于从图片名派生的明细条件，只应用于明细表；日趋势 SQL 不应用该条件。选择边框后，图表仍表示其他已选条件下的全部边框 CODE 构成，不能把它解读为该边框专属趋势。

### 4.2 明细表

| 列 | 含义 |
|---|---|
| Print Time | Glass 的打印/检测事实时间，经系统显示时间策略处理。 |
| ProductCode | 产品型号。 |
| Glass ID | 玻璃载体标识。 |
| Printer | 对应 IJP 打印设备。 |
| Panel ID | 从缺陷图片名称中提取的 Panel 标识。 |
| 原图 | 缺陷原始图片的内网链接。 |
| Panel Location | 从图片名称解析的边、角或底边细分位置。非 C3DM 类 CODE 还可能显示 `KONGLEFT`、`KONGTOP`、`KONGRIGHT`、`KONGBOTTOM`。 |
| CODE_RATIO | 同一 Glass 内：该 RS_CODE 记录数 ÷ 该 Glass 全部受监控 RS_CODE 记录数，保留 3 位小数。例如 `0.250` 表示 25%。 |

明细末尾的 `Total` 是当前展示行中 `CODE_RATIO` 数值的直接求和。由于同一 Glass/CODE 的比例可能在多条缺陷明细中重复，而且 BOTTOM 记录还可能展开出细分行，所以该 Total **不是总体溢流率，也不要求等于 1 或 100%**。判断代码构成优先使用上方日趋势图；判断单片 Glass 时，应结合该 Glass 的全部明细查看。

## 5. 时间口径

当前客户报表配置启用了“显示日期前推 4 天”：

- 页面输入和输出都是**显示时间**；
- 系统查询数据库时会自动换算为早 4 天的源时间窗口；
- 返回的 `Print Time` 和图表日期再前推 4 天展示；
- 原始数据库事实时间不会被修改。

例如页面显示 9 月 3 日的数据，对应的源系统事实日期通常是 8 月 30 日。与数据库原始记录核对时必须换算这 4 天；如果部署配置关闭了日期前推，则显示时间与源时间相同。

## 6. 使用建议

建议采用“趋势发现 → 维度收窄 → 图片确认”的方式排查：

1. 先用较宽条件查看各 CODE 的日构成是否突变；
2. 再按产品、线体、设备、批次或 Cycle 逐步缩小范围；
3. 用 CODE 和 Glass ID 定位到具体记录；
4. 结合 Panel Location 判断边缘聚集特征；
5. 打开原图做最终确认，并结合绝对缺陷数量、投入量及正式 RS CODE 字典判断严重程度。

不建议只根据堆叠图百分比或表格 Total 判断异常严重程度，因为当前页面没有展示每日投入量、正常品数量或总体良率。

## 7. 限制与异常提示

- 明细最多展示前 5000 行；达到上限时页面提示“已截断”，应缩小时间或其他筛选范围后重查。
- 当前没有本地快照降级。数据库不可用或权限异常时会提示：“IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。”
- 查询结果为空时会提示“当前筛选条件下暂无 IJP 溢流数据”，这只说明当前条件没有命中记录。
- 原图链接依赖内网图片服务；表格有记录但图片打不开时，应先检查网络、图片服务或访问权限。
- 报表只覆盖固定设备和固定 RS_CODE 白名单，不代表全部 IJP 设备、全部打印缺陷或全部溢流形态。
- Panel Location 依赖缺陷图片命名规则；图片名缺失或不符合约定时，位置可能为空。

## 8. 实现依据

- 页面入口：`app/pages/IJP溢流监控报表.py`
- 页面交互：`app/sections/indicator_domain/ijp/dashboard.py`
- 图表语义：`app/charts/indicator_domain/ijp/chart.py`
- 查询校验：`src/indicator_domain/application/ijp/dtos.py`
- 统计及数据访问：`src/indicator_domain/infrastructure/ijp/repository.py`
- CODE、设备和位置规则：`src/indicator_domain/core/ijp/overflow.py`
- 当前领域归属：`docs/ADR/0021-indicator-domain-peer-submodules.md`
- 源时间与显示时间：`docs/ADR/0022-source-and-display-time-boundary.md`
- 原始数据集分析：`docs/dev_docs/dev_spec/indicator_domain/datasource-IJP溢流报表分析.md`
