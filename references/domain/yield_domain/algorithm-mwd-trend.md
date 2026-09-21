# MWD 月周日趋势处理算法

主流程入口：`src/yield_domain/core/mwd_trend/mwd_trend_processor.py`

完整的数据流、公式、跨月数字示例和边界见
[`docs/dev_docs/generated/yield_domian/mwd-processor-opt-algorithm.md`](../../../docs/dev_docs/generated/yield_domian/mwd-processor-opt-algorithm.md)。
本文只记录稳定的领域规则与代码路由。

## 一、事实源与业务优先级

- `resources/yield_domain/入库良率修饰表.xlsx` 是 MWD 月度目标的业务控制入口；
- `<产品>_Code级` Sheet 驱动 Code 趋势，并提供 Mapping 月度倍率；
- Group 日度严格由其下 Code 最终日度汇总，满足 `Group = ΣCode`；
- `<产品>_Group级` Sheet 只覆写 Group 月度结果，不反向生成 Group 日度；
- Code 日度全部由完整的月度目标生成；Panel 明细只提供每日总投入容量和
  `(Defect Group, Code)` 清单，并提供按月汇总的原始 Code 良损，不提供原始日度
  不良数；
- 月度目标依次使用当月指定、最近更早月份指定、原始月度良损；不使用 0 或原始
  日度不良数回退；
- Code 最终日度是 Code 周/月与 Group 日/周的事实源；已覆写的 Group 月度允许与
  Group 日度月合计不同。

旧的 Code baseline、EMA、TrendRegulator、月度对账和月/周/日人工覆盖链路已停止
消费；`resources/yield_domain/趋势图人工修正.xlsx` 保留，但不再参与 MWD 计算。

## 二、修饰表解析

`modifier_table.py` 负责读取、校验、回写和目标解析。目标良损按以下顺序确定：

```text
当月指定良损
  → 最近一个更早月份的指定良损
  → 当月原始良损
  → 若修饰表缺少该月行，则从 Panel 明细按月汇总原始良损
```

良损必须满足 `[0, 1]`。当月原始良损由与趋势相同的 Panel 明细计算。

修饰表同步以分析窗口明细和该产品既有台账的不良类型并集补齐月份：当月有投入、
某不良未发生时，仍追加该月行并把“当月良损”写为 `0`；已有当月行也应更新为
`0`。新行的“指定良损”保持空白，已有人工值保持不变。历史月份只补缺行，
不重算已有历史参考值。整月没有投入数据时不凭空补零，也不覆盖已有参考值。
零值参考行不改变上述目标回退优先级，历史指定值仍可决定当月趋势。

缩放倍数为：

```text
round(clip(回退后的目标良损 / 当月原始良损, 0.3, 3.0), 3)
```

倍率计算结果（含零）截断到 `[0.3, 3.0]`，工作簿“缩放倍数”列与 Mapping
使用相同结果；仅倍率发生变化也触发同步回写。该边界不改变趋势的指定良损目标。
原始良损为零或缺失时，Mapping 倍率回退为 `1.0`。工作簿读取复用共享的
`read_workbook_sheet`；确切的 Sheet 缺失返回空表，其他格式 `ValueError` 回退 Excel
COM，COM 仍失败时向上抛出，避免误覆盖人工值。写回失败时不推进签名，后续同步会
继续重试。

## 三、Code 日度生成与 Group 汇总

Code 使用 `daily_generator.generate_daily_counts`，执行以下算法：

1. 从 Panel 明细按日计算总投入 `P_d`，并提取 `(Defect Group, Code)` 唯一清单；
2. 按自然月、Code 汇总原始月度良损，不保留原始日度不良数；
3. 建立“日期 × Code”容量网格；
4. 按当月指定、最近更早月份指定、原始月度良损形成完整月度目标；
5. 把每个月目标良损锚定在当月 15 日；
6. 相邻锚点间线性插值得到逐日基线 `b_d`；
7. 叠加由“产品、缺陷、日期”稳定哈希产生的确定性扰动 `n_d`；
8. 计算日度权重 `w_d = b_d × n_d × P_d`；
9. 对每个自然月计算目标整数 `T_m = round(r_m × ΣP_d)`；
10. 在单日投入容量内，按权重把 `T_m` 分配为日度整数；
11. 从 Code 最终日度聚合 Code 周度和月度。

整数分配函数 `allocate_integer_counts` 位于 `daily_generator.py`。它保证：

```text
0 <= 当日不良数 <= 当日投入数
月内日度不良数合计 = 容量允许范围内的月度目标整数
```

Group 不执行上述日度生成算法。`mwd_trend_processor.py` 将 Code 最终日度按
`(日期, defect_group)` 求和得到 Group 日度；日期和每日投入直接复用 Code
`daily_full`，不再从 Panel 准备原始 Group 日度宽表。随后聚合 Group 周度和月度基础
值；Group Sheet 只覆写最终 Group 月度结果。

## 四、跨月平滑的准确边界

“平滑”指月中锚点之间的归一化前基线率连续，不是在月初把上月目标直接切换成
本月目标。扰动加入后，系统再按自然月独立归一化并执行整数分配。

由于相邻月份可能使用不同的归一化系数，最终日度整数在月界仍可能跳变。业务已接受
这一边界，并将“月度目标精确、单日不超过投入”置于“最终日度月界连续”之前。

## 五、Mapping 一致性边界

Mapping 与 Code MWD 共享 Code Sheet 的指定来源，但倍率受 `[0.3, 3.0]` 限制，随后
仍执行自己的最新批次选择、整数抽样/复制和批次级联衰减。因此不要求 Mapping 最终
计数与 MWD 月度整数严格相等。

Mapping 入口再次应用同一倍率边界；非有限值或无法解析的倍率记录错误并按
`1.0` 处理。边界限制的是月度倍率，并不保证整数取整、级联衰减、热点处理后的
最终格子计数仍在原值的 0.3～3 倍内。

看板的手动 Code 筛选与自动预警缺陷图像共用月均不良率门槛：取当前展示窗口
（近三个月）最终月度不良率的算术平均，要求 `月均 >= 门槛`。配置项为
`config/domain/yield_domain.yaml` 的 `dashboard.code_monthly_rate_threshold`，
默认 `0.0001`（0.01%）。自动区还必须命中趋势或 Lot 预警。没有月度数据的 Code
不满足门槛，即使存在 Mapping 也不展示；默认阈值下，三个月全为零也不展示。

## 六、代码路由

- `modifier_table.py`：修饰表读取、校验、回写、回退目标和 Mapping 倍率；
- `daily_generator.py`：Code 插值、稳定扰动、逐月归一化和整数分配；
- `mwd_trend_processor.py`：Code 编排、Code 到 Group 日度汇总及 Group 月度覆写；
- `aggregation.py`：从最终日度聚合周度、月度；
- `formatting.py`：生成前端使用的数据结构；
- `mapping/mapping_processor.py`：应用 Code 月度倍率后继续既有级联逻辑；
- `application/yield_service.py`：通过 `get_modifier_context` 共享修饰表同步、两级目标和
  Mapping 倍率缓存。
