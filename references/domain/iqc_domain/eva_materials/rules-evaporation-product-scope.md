# IQC 蒸镀材料产品范围规则

确认日期：2026-10-13。需求：[Task2](../../../../docs/dev_docs/dev_spec/iqc_domain/eva_materials/task-蒸镀材料报表开发.md#task2报表优化)。决策：[ADR-0029](../../../../docs/ADR/0029-iqc-evaporation-source-contract.md)。

## 产品选择与物料范围

报告保留 V3／有机检验范围。产品选项按 `config/global.yaml` 的 `product_registry.enabled_products` 顺序提供，注释仅影响标签；查询使用原始产品代码。空选择表示全部启用型号。严格加载时，空配置列表表示没有可查询产品；缺失或格式错误不回退到 M678。

设所选有效产品集合为 P，物料号为 M，蒸镀批次为 B。物料 M 可展示的条件是：存在一个 B，使 M 与 B 关联，且 B 与 P 中至少一个产品关联。批次对应多个产品或物料均是合法关系；多个产品选择使用物料并集。

对关联使用 DISTINCT／集合成员资格，不将每个产品、批次组合展开成一条检验记录。检验事实原有重复、目录特性展开及缺测行保留，不进行无依据去重。

2026-10-13 后续展示优化：表格“产品型号”使用当前所选范围内的关联产品代码，与筛选框对应。用户进一步要求删除“蒸镀批次”字段，公开表格、缓存和 CSV 均不包含该字段；蒸镀周期仍用于内部产品关联。WMS `EATTRIBUTE10` 常为“通用”，仍按源属性读取，但不再送入公开表格、缓存或 CSV，也不用于产品筛选。

同一物料存在多个所选产品时，以“、”合并去重值，按启用配置顺序排列；单选仅展示该产品。该字段表示物料在所选范围内的产品集合，不表示某个来料批号的实际消耗归属。仍保持一条检验事实一行，不按产品／周期展开。

“检验日期范围”初始默认值为上个月 1 日至今天，按日历月份计算，支持跨年和闰年；用户手动选择的日期仍遵循既有查询门控。

## 关联来源与批次键

1. 坩埚履历 `mdw.dwr_mes_ct_cruciblelothistory` 的 `PLANORGANICSPEC` 为物料号，`CYCLETIME` 为蒸镀批次，当前源格式为 `YYYYMMDD`。
2. 按原坩埚报表限定 `EVENTNAME='KitBatchCrucible'`、`MACHINENAME!='3CEE03'`、批次非空，并要求同一 `CRUCIBLELOTNAME` 存在 `mdw.dwr_mes_consumablehistory` 的上机履历。
3. 沿用 Mapping 的批次提取规则：从 `mdw.dwr_mes_productrequest.DESCRIPTION` 开头读取 `YYYY/M/D` 或 `YYYY/MM/DD` 日期，转换为 `YYYYMMDD` 与坩埚周期匹配。无日期前缀的工单不能匹配；无法解析的有效前缀导致关联读取失败，不伪造日期。
4. **用户确认采用工单主数据范围，包含尚未入库工单。** 工单 `PRODUCTSPECNAME` 关联 `mdw.dwr_mes_productspec.PRODUCTSPECNAME`，取 `PRODUCTCODE`；`SUBPRODUCTIONTYPE` 受全局 `data_source.work_order_types` 控制。不要求 CT Panel 或入库 Panel 存在，也不复制 Mapping 的入库时间窗口。
5. 原回收表、规格分类表、供应商批号表用于原坩埚报表其它字段，不影响物料／周期成员关系；筛选查询不读取回收重量。

检验日期仅限定 WMS 检验事实，不限制关联历史的蒸镀批次日期。检验显示日期前推及当日截止也不改写批次键。

## 分层、缓存与失败

- composition 注入严格启用产品集合、工单类型及既有共享数据库、时间策略。application 接受产品选择并取启用范围交集；无有效范围不读取数据库。
- IQC infrastructure 在同一只读事务内读取检验事实，再限定当前检验物料查询成员关系，并按物料集合过滤；不导入 Yield 私有仓储。查询日期、物料、产品、工单参数均绑定。
- infrastructure 将所选关联聚合后映射到检验事实；core 保持测点 CASE、IQC／COA 任一 NG 规则和缺失值，删除批次后的公开投影共 27 列。除公开产品字段外，其它关联元数据不进入公开表格、缓存或 CSV。
- 缓存身份包含日期、前推、截止、有效产品、启用配置、工单类型、测点判定版本及产品范围版本 `request-master-cycle-v3`，隔离此前的“通用”及 28 列缓存。配置撤销选项会清理选择；有效范围或策略改变后隐藏旧结果，重新查询。
- 关联权限、解析或数据库读取失败使整次读取失败；不返回未筛选全集、示例数据或成功空集。配置／读取失败只显示业务提示，日志仅记录安全错误码与异常类型。

## 证据边界

完整原坩埚 SQL 与独立工单主数据关联用于实库物料集合对账；原 WMS SQL 的字段多重集及原 CASE 用于检查检验事实、规格、测点和结果。关系型夹具覆盖同周期多物料、多产品、重复履历和未入库工单；AppTest／浏览器覆盖产品状态、配置、空集、失败、CSV 及视口。

具体执行结果与命令见 [Task2 来源与验收](../../../../docs/dev_docs/dev_spec/iqc_domain/eva_materials/datasource-蒸镀材料产品关联分析.md)。源码范围：`src/iqc_domain/{application,core,infrastructure}/eva_materials/`，页面协调：`app/sections/iqc_domain/eva_materials/`。
