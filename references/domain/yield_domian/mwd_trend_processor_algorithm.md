# MWD 月周日趋势处理算法

主流程入口：`src/yield_domain/core/mwd_trend/mwd_trend_processor.py`

完整的数据流、公式、跨月数字示例和边界见
[`docs/dev_docs/generated/yield_domian/mwd-processor-opt-algorithm.md`](../../../docs/dev_docs/generated/yield_domian/mwd-processor-opt-algorithm.md)。
本文只记录稳定的领域规则与代码路由。

## 一、事实源与业务优先级

- `resources/入库良率修饰表.xlsx` 是 MWD 月度目标的业务控制入口；
- `<产品>_Code级` Sheet 驱动 Code 趋势，并提供 Mapping 月度倍率；
- `<产品>_Group级` Sheet 独立驱动 Group 趋势，人工指定优先级最高；
- Group 不再由其下 Code 日度汇总反推，不要求 `Group = ΣCode`；
- Code 与 Group 各自的最终日度整数是本级周度、月度结果的唯一事实源。

旧的 Code baseline、EMA、TrendRegulator、月度对账和月/周/日人工覆盖链路已停止
消费；`resources/趋势图人工修正.xlsx` 保留，但不再参与 MWD 计算。

## 二、修饰表解析

`modifier_table.py` 负责读取、校验、回写和目标解析。目标良损按以下顺序确定：

```text
当月指定良损
  → 最近一个更早月份的指定良损
  → 当月原始良损
  → 不生成目标，保留原始日度不良数
```

良损必须满足 `[0, 1]`。当月原始良损由与趋势相同的 Panel 明细计算。缩放倍数为：

```text
round(回退后的目标良损 / 当月原始良损, 3)
```

原始良损为零或缺失时，Mapping 倍率回退为 `1.0`。工作簿读取复用共享的
`read_workbook_sheet`；企业加密文件才回退 Excel COM。写回失败时不推进签名，
后续同步会继续重试。

## 三、Code 与 Group 日度生成

Code 使用 `daily_generator.generate_daily_counts`；Group 使用
`daily_generator.generate_group_daily_counts`。两者采用相同算法：

1. 把每个月目标良损锚定在当月 15 日；
2. 相邻锚点间线性插值得到逐日基线 `b_d`；
3. 叠加由“产品、缺陷、日期”稳定哈希产生的确定性扰动 `n_d`；
4. 计算日度权重 `w_d = b_d × n_d × P_d`；
5. 对每个自然月计算目标整数 `T_m = round(r_m × ΣP_d)`；
6. 在单日投入容量内，按权重把 `T_m` 分配为日度整数；
7. 从最终日度直接聚合周度和月度。

整数分配函数 `allocate_integer_counts` 位于 `daily_generator.py`。它保证：

```text
0 <= 当日不良数 <= 当日投入数
月内日度不良数合计 = 容量允许范围内的月度目标整数
```

## 四、跨月平滑的准确边界

“平滑”指月中锚点之间的归一化前基线率连续，不是在月初把上月目标直接切换成
本月目标。扰动加入后，系统再按自然月独立归一化并执行整数分配。

由于相邻月份可能使用不同的归一化系数，最终日度整数在月界仍可能跳变。业务已接受
这一边界，并将“月度目标精确、单日不超过投入”置于“最终日度月界连续”之前。

## 五、Mapping 一致性边界

Mapping 与 Code MWD 共享 Code Sheet 的指定来源和月度调节方向，但 Mapping 随后
仍执行自己的最新批次选择、整数抽样/复制和批次级联衰减。因此不要求 Mapping 最终
计数与 MWD 月度整数严格相等。

Mapping 月度倍率的轻量防御规则为：非有限值、负数或超过 10 倍时记录错误并按
`1.0` 处理。业务正常倍率通常小于 1。

## 六、代码路由

- `modifier_table.py`：修饰表读取、校验、回写、回退目标和 Mapping 倍率；
- `daily_generator.py`：插值、稳定扰动、逐月归一化和整数分配；
- `mwd_trend_processor.py`：Code/Group 编排及周月聚合；
- `aggregation.py`：从最终日度聚合周度、月度；
- `formatting.py`：生成前端使用的数据结构；
- `mapping/mapping_processor.py`：应用 Code 月度倍率后继续既有级联逻辑；
- `application/yield_service.py`：同步修饰表、构建两级目标并接入缓存。
