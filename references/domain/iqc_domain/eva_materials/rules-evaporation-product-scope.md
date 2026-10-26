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

## 自动 NG 预警与本地快照

2026-10-26 新增：当前公开报表的“IQC结果”或“COA结果”为 `NG` 时自动预警，并保存超规材料 Excel 快照。预警日期默认上一自然周（周一至周日），产品范围为全部严格启用产品，与 SPC/IJP 的预警周期一致。页面原有日期、产品筛选和“查询”门控继续只控制普通检验明细。

- 页面打开或重跑时自动获取上一周数据，无需点击查询。全部产品的成员关系、工单范围、源日期前推和截止规则沿用既有报表服务与 adapter。无启用产品或筛选配置失败时不取数、不发布新快照。
- 使用报表已经完成的测点 CASE 判定结果，不重新使用数据库原始结果列。任一结果为 `NG` 就选择该事实；两项都为 `NG` 只列一条。不同检验事实即使外观重复也保留，不对物料号或特性项目擅自去重。缺测仍遵循既有 ELSE OK 规则。
- Core `build_evaporation_alerts()` 只返回 `REPORT_COLUMNS` 的 27 列公开数据。预警表、缓存、原生下载和快照不包含蒸镀批次、WMS“通用”产品属性、工单关联字段、内部主键、flag 或调试信息。
- 自动预警使用现有 `fetch_report_payload()` 原生 DataFrame 缓存，键继续包含窗口、前推、截止、有效产品、启用配置、工单及判定版本；与相同窗口/产品的手动查询共享缓存。页头“刷新缓存”同时使自动预警取数失效，下一次页面执行重新计算、发布；注入的测试服务绕过生产缓存。
- 有 NG 时“蒸镀材料超规预警中心”自动展开，展示完整 NG 清单；有检验数据但无 NG、没有检验数据、源读取或快照更新失败分别显示独立业务状态。预警更新失败不会关闭普通明细查询，也不把旧快照作为当前成功结果展示。技术日志只记录安全错误码和异常类型。
- Application `publish_evaporation_alerts()` 拥有 `EvaporationAlertStore` 端口；composition 注入 Infrastructure 的 `EvaporationAlertWorkbook`，页面不直接读写 Excel。
- 路径唯一配置来源为 `config/global.yaml` → `resources.iqc_domain.files.evaporation_alerts`，默认 `resources/iqc_domain/eva_materials/evaporation_alerts.xlsx`。第一次成功计算并发布时自动创建，无需预置业务记录。
- `超规材料` sheet 保存最近一次成功计算的完整超规清单；`__meta__` 保存窗口起止、产品范围、发布时间、条数和成功状态。成功的空结果清除旧清单，保留明确空结果元数据；源读取/计算失败不发布，Excel 写入失败保留原文件。
- 工作簿使用 OS 文件锁串行化读改写，内存生成并核验后通过同目录临时文件原子替换。保留其他 sheet，失败清理本次临时文件。该文件是最新结果快照，不是追加型历史台账；发布时间反映本次发布，源数据仍遵循报表缓存和源时间语义。

验证：`tests/unit/test_iqc_evaporation_alerts.py` 覆盖 OR 条件、双 NG 单行、重复事实保留、公开投影和跨年周；`tests/integration/test_iqc_evaporation_alert_workbook.py` 验证快照、清空、其他 sheet 保留、写失败与配置完整路径；`tests/e2e/test_iqc_evaporation_alerts_apptest.py` 使用真实 section、服务和 Core，注入合成源与临时工作簿，验证自动执行、全部产品、日期/产品筛选独立、正常/空/失败状态以及普通模式字段边界。现有 SQL、产品成员关系、缓存与查询门控测试继续适用。
