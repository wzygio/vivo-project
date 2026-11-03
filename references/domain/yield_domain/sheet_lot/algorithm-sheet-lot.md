# Sheet/Lot 不良率处理算法

对应程序：`src/yield_domain/core/sheet_lot/sheet_lot_processor.py`

## 一、Lot 级：`calculate_lot_defect_rates`

1. 按 Lot 统计唯一 Panel 数，取 Lot 入库日期，并合并该 Lot 的 Array 投入时间。
2. 只保留最近两个月窗口内的 Lot，再按 `total_panels / (190 × 30) >= 20%` 过滤。
3. 对有效 Lot 统计 Code 级不良 Panel 数，并汇总得到 Group 级原始结果。
4. 若 `sheet_hotspot_config.enable=True` 且存在周度 MWD 数据：
   - 按 Lot 入库日期匹配 ISO 周；
   - 读取该周、该 Code 的 MWD 不良率；
   - 按 Lot 投入数和固定随机扰动，将周度不良总量分配到各 Lot；
   - 完整周度表缺失时保留原始结果；完整周度表存在但该 Lot 所属周没有命中时按 0，不回退原始 Lot 或日度良损。
5. 若开启缺陷截断，根据 Warning Line 对超过上限的 Code 数量进行软截断；未超过上限的不良数量不变。
6. 读取 Excel 覆盖配置，计算 Lot 级覆盖率：
   - 以所属周 MWD 不良率为基础；
   - 加上同 Lot Sheet 覆盖率之和除以 `30 + Sheet 数量` 的结果。
7. 将覆盖率换算为不良 Panel 数，重新按 Code 汇总到 Group，生成表格和图表结果。

## 二、Sheet 级：`calculate_sheet_defect_rates`

1. 按 Sheet 统计唯一 Panel 数，并合并 Lot、入库日期和 Array 投入时间。
2. 按 Sheet 过货率过滤有效 Sheet。
3. 对有效 Sheet 统计 Code 级原始不良率。
4. 若开启 `sheet_hotspot_config`，读取 Lot 级结果，把每个 Lot/Code 的不良数量按 Sheet 投入数分配；每张 Sheet 有软上限，超出的数量重新分配。
5. 读取 Excel 中的 Sheet 级覆盖率，覆盖已有 Code；合法 Sheet 缺少该 Code 时补充对应记录，非法 Sheet 不补数据。
6. 根据最终 Code 结果重新聚合 Group，生成表格和图表结果。

## 三、共同特点

- Group 结果始终由最终 Code 结果重新汇总，避免 Group 与 Code 数量不一致。
- 模拟、截断和覆盖都作用于 Code 级明细。
- 找不到有效基础数据时返回空结果或 `None`，不继续生成虚构的实体数据。

## 四、报表读取与按需执行（2026-11-03）

Lot、Sheet 通过同一共享输入代次、参数规范化和有界缓存复用 Lot 结果；同次操作的中间 DataFrame 只取回一次。Sheet 仍在完整名册和全部 Code 上执行原分发，不能先按用户选中的 Code／Lot 缩小输入而改变随机数顺序。用户请求详情时才加载整份 Sheet，失败不阻断宏观趋势或已发布预警。

覆盖表解析在 Lot／Sheet 间共享。普通 XLSX 直读；公式（含数组公式）与企业保护文件保留只读 Excel COM 计算／读取，失败不能返回合法空覆盖。缺产品 Sheet 仍表示没有覆盖，覆盖公式、百分比、缺值、合法 Sheet 补行与 Group 重聚合语义保持。

`config/domain/yield_domain.yaml:array_input_time.query_enabled` 当前为 false：Array 时间端口不会被调用，Lot／Sheet 的 `array_input_time` 保持空值。按阵列时间排序的选项关闭，Sheet 默认按 ID 排序；原查询、自定义时间和排序代码保留供显式重新启用。详见 [报表数据流](../shared/data-flow-yield-report.md)。
