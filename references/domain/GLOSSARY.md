# Domain Glossary

本词汇表记录 OLED 显示屏制造领域在本工作区中的稳定术语、别名和标识规则。代码、Issue、测试和文档应优先使用这里定义的名称；未经证据确认的项目特例不要写成通用定义。

## 生产载体与标识

- **Lot（批次）**：生产批次单元。一个 Lot 通常包含 30 个 Sheet；Lot ID 通常为 9 位。
- **Sheet（大板）**：前段工艺中的整张显示大板。Sheet ID 通常为 11 位，其中前 9 位为 Lot ID。
- **Glass（玻璃基板）**：Sheet 在蒸镀工艺段一切为二后形成的载体。Glass ID 通常为 12 位，其中前 11 位为 Sheet ID。
- **Panel（屏体）**：在屏体段将大板切割为终端产品尺寸后形成的单个屏体。Panel ID 通常为 15 位，其中前 11 位为 Sheet ID，后 4 位表示坐标。
- **裁切数**：一个 Sheet 可切割出的 Panel 数量，等于 X 向裁切数与 Y 向裁切数之积。Sheet 尺寸相近时，裁切数越小通常表示 Panel 尺寸越大。最常见的裁切数为190，即10行19列
  - X、Y 向裁切数字段可能命名为 `PRODUCTCOUNTTOXAXIS`、`PRODUCTCOUNTTOYAXIS`。
  - Glass 裁切数通常为 Sheet 裁切数的一半，相关字段可能命名为 `SUBPRODUCTUNITQUANTITY1`。
- **Mapping（拼板图）**：按照 Panel 位置坐标在数据维度上拼接形成的大板视图，用于调查前段工艺问题。拼接范围和粒度由具体分析需求决定。
- **膜位**：Panel 在 Mapping 中对应的位置。为Panel_ID的后四位，分别对应行号和列号。以一个裁切数为190的大板来说，它的行序号为“1A-1E,2A-2E”（共十行），列序号为“A0-H0,J0-N0,P0-U0”（共十九列，字母跳过 I 和 O）。
  * 示例：因此假如一个panel_id为“L3MO6605N161DJ0”，“1DJO”就是它的膜位，代表其行序号为“1D”，列序号为“J0”

### Mapping图绘制方法
对于实际生产的产品，其裁切数并不固定位190。因此Mapping图的完整绘制步骤如下：
1. 获取裁切数，了解正确的行数和列数
  - 数据表名称：`mdw.spot_dwr_mes_productspec_v`
  - Panel → 产品型号的关联路径：`chip_id → eda.cell2_chip_t.product_id（latest_flag='T'）→ productspecname → productcode`
  - 关联细节（已验证）：AVI 是 CELL 段站点，`cell2_chip_t.chip_start_time` 是段内更早站点的过货时间（段内过货一般一天），关联时前推 3 天兜底（实测残留未映射为个位数）；`productspecname → productcode` 一一对应（每 spec 仅一行），无需聚合去重；chip 侧同一 `chip_id` 存在多行 `latest_flag='T'`，需按 `chip_id` 聚合取一条。
  - 查询语句（仅取 ARRAY 厂别的规格行——该行才代表真实裁切布局，筛选后任意一条皆可）：
```
select
	productcode,
	productcounttoyaxis,
	productcounttoxaxis
from mdw.spot_dwr_mes_productspec_v
WHERE productcode = 'C451'
and factoryname = 'ARRAY'
```
  - 字段方向已验证：`productcounttoyaxis` = 行数、`productcounttoxaxis` = 列数（标准 190 裁切为 10 行 × 19 列）。
2. 根据行数和列数编写对应的行序号和列序号（**字母跳过 I 和 O**，避免与数字 1/0 混淆，经真实数据验证）：
  - 行序号：第二位为从“A”到“行数的1/2对应的字母”（跳过 I/O），然后在前面分别加上1和2。示例：如果行数为8，则第二位为“从A到D（8的1/2是4，第四个字母是D）”，在前面分别加上1和2之后，最终的行序号为“1A-1D”、“2A-2D”
  - 列序号：第一位为从“A”到“列数对应的字母”（跳过 I/O），第二位固定为0。示例：如果列数为12，则字母序列为“A-H、J-M”（跳过 I），最终的列序号为“A0-M0”；标准 19 列为“A0-H0、J0-N0、P0-U0”
  - 超出 24 个可用字母的布局（如 32×33）暂无法按该规则生成序号，应作为数据质量状态单独处理
3. 根据行序号和列序号绘制最终的Mapping图

## 缺陷与检测

- **异常／缺陷／不良**：在制造质量语境中，指产品检测发现的质量问题。使用时应结合上下文区分单个缺陷与汇总异常事件。
- **Defect Group（缺陷组）**：一组相关的 Defect Code。
- **Defect Code（缺陷代码）**：标识具体缺陷类型的代码。
- **CT**：屏体厂的一组核心检测站点的代称，包括AVI、MVI、APP屏体缺陷通常在这组站点检测并记录。
- **AVI（Automatic Visual Inspection）**：屏体厂的核心检测站点之一，拍出缺陷位置并进行初步缺陷分类（判定Defect Code）。
- **MVI（Manual Visual Inspection）**：屏体厂的核心检测站点之一，根据AVI的检测结果进行人工复盘（确定最终的Defect Code）。
- **APP（Appearance）**：屏体厂的核心检测站点之一，对panel进行特定的外观检并判定外观类型的Defect Code。

## 制造工艺

- **Q-Time（过货时间）**：同一生产载体从 From 站点离开到进入 To 站点之间的等待时长。`wait_time > q_spec` 表示该环节可能发生滞留，可能增加 OLED 材料暴露时间并影响质量。Q-Time 报表采用 `[start_time, end_time)` 时间窗口，并按 Lot 展示等待时长与规格；人工修饰中 `flag=True` 表示修饰到规格内，`False` 表示保留真实超规并预警，`Delete` 表示删除记录。
- **四大工艺**：`ARRAY`、`OLED`、`TP`、`CELL`。
  - **ARRAY（阵列）**：制作 TFT 背板。
  - **OLED（蒸镀／EVA，Evaporation）**：制作有机发光层。
  - **TP（Touch Panel）**：制作触控层。
  - **CELL（屏体封装）**：完成屏体封装及 CT 检测。
- **厂别**：每道主要工艺对应的工厂标识。在部分数据和业务语境中，“工艺”与“厂别”会被关联使用，但分析时应保留原字段含义。

## OLED 膜厚趋势

- **母机台**：膜厚测量单元所属的设备级父单元。当前由 `unit_id` 的首段归一化得到，并与源数据 `equipment_id` 交叉校验。
- **源时间**：制造事实由数据库或原始快照记录的真实发生时间。日期前推不改写源时间或原始 Parquet。
- **显示时间**：报表面向用户使用的时间轴。启用 `data_forward` 时，显示时间等于源时间加配置天数；关闭时两者相同。
- **日期前推**：只在仓储输出边界将源时间映射到显示时间的报表策略。直接数据库查询会先把显示窗口反向换算为源时间窗口，缓存签名必须包含策略状态与偏移天数。
- **工艺腔体代码**：从 EVA/TFE 膜厚源字段解析出的 OC、LC、MC、CVD、IJP、TP_NG 等业务分图代码；它与物理 `sub_equip_id` 分别保存，不互相替代。
- **经过腔室**：同一玻璃在母机台下的腔室历史 `sub_equip_id`，表示经过/暴露关系。一个膜厚测点可以进入多个经过腔室泳道，不表示测点只能归属于一个腔室。
- **精确工序关联**：玻璃、母机台和 `step_id` 均匹配的经过腔室关系，代码值为 `exact_step`。
- **母机台回退关联**：当前路由不存在精确工序历史时，使用同玻璃、同母机台历史建立的经过腔室关系，代码值为 `mother_machine_fallback`；该策略必须在诊断信息中可见。

### EVA 膜层腔室映射

- **EVA 膜层腔室映射**：`chamber.xlsx` Sheet2 前两列定义的 `process_chamber_code` 到物理 `sub_equip_id` 的静态对应关系。用于 EVA 图像分组的自动默认腔室选择，不改变过货历史关联或手动候选腔室。
- **未映射膜层**：当前 EVA 图像分组的膜层未出现在该映射中。界面不自动选择任何腔室并显示提示；操作员仍可从完整候选列表手动选择。

## Inline检测

- **监控指标**：由“站点 + 参数”共同确定的监控对象。单独的站点或参数通常不足以唯一标识一个指标。
- **SPC（Statistical Process Control，统计过程控制）**：针对监控指标，在一个 Sheet 的多个点位测量，并以汇总值（通常为均值）作为该 Sheet 的测量值；再依据指标上下限通过自动预警报表进行监控。
- **AOI（Automatic Optical Inspection）**：Array/OLED前段的检测站点，主要用于检测图形、残留、颗粒
- **RS（Review Station）**：对AOI拍出的缺陷点位进行复判，判定具体的Code，其与AOI的关系类似于MVI相较于AVI
  * 注意，这里的Code也是一种缺陷代码，但与Defect Code并不一致，一般用长度为五位的代码指代
- **LOI（Light On Inspection）**：OLED段检测站点，点亮后的显示缺陷
