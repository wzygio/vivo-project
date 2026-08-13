# 不良 Panel 数对齐实施方案（Task2 简化版）

## 目标

修正 Code 级 MWD 中 EMA 拖尾导致月度不良 Panel 数被显著放大的问题，同时满足：

- `defect_multipliers` 继续直接作用于初始 `panel_details_df`，且只执行一次；
- MWD 的 `defect_panel_count` 始终是非负整数；
- 自动校准先执行，人工月/周/日修正后执行；
- Mapping 保持现有批次筛选、坐标修饰和 Rate-Based 级联衰减逻辑不变。

不再为 MWD 和 Mapping 建立强制数量级约束，也不在两条流水线之间增加月度基准依赖，因为月度与批次无法形成严格对应关系。

## MWD 数据流

```text
modified panel_details_df（已应用 defect_multipliers）
  -> 生成原始 Code×月份整数不良 Panel 目标
  -> EMA + deterministic noise
  -> TrendRegulator
  -> 按 Code×月份执行整数总量校准
  -> Monthly 人工修正
  -> Weekly 人工修正
  -> Daily 人工修正
  -> 从最终 Daily 重算 Weekly / Monthly
  -> 格式化不良率
```

## 整数月度校准

对每个 Code 和自然月：

- `T`：修饰后原始 Panel 明细聚合得到的月度整数不良 Panel 数；
- `A_d`：EMA、噪声和 TrendRegulator 后的日度整数不良 Panel 数；
- `S = sum(A_d)`。

当 `T>0` 且 `S>0`：

```text
q_d = A_d * T / S
```

使用最大余数法把 `q_d` 转回日度整数，并保证日度整数之和等于 `T`。

边界行为：

- `T=0`：当月日度数量全部为 0；
- `T>0,S=0`：按日度 `total_panels` 权重分配；
- 无投入日期不分配；
- 单日数量不得超过当日 `total_panels`；
- 低不良 Code 允许出现 0/1 跳变，不生成浮点 Panel。

校准函数中的浮点值只用于临时配额计算，不写入业务结果。

## 人工修正规则

人工修正发生在自动校准之后，执行顺序为：

```text
Monthly -> Weekly -> Daily
```

冲突优先级为：

```text
Daily > Weekly > Monthly > 自动校准
```

- 月/周人工不良率先换算为该周期的整数目标，再按当前日度形状使用最大余数法分配；
- 日人工不良率直接换算成当日整数 Panel 数；
- 人工修正之后不再自动校准；
- 最终 Weekly 和 Monthly 只从最终 Daily 聚合，避免三个周期口径倒挂。

## Mapping 决策

`mapping_processor.py` 本次不修改算法：

- 保留最新批次筛选；
- 保留确定性坐标修饰；
- 保留 `FIRST_REDUCTION_FACTOR`；
- 保留 `SECOND_REDUCTION_FACTOR = 0.95`；
- 保留 `target_count` 和固定种子 `sample()` 抽样。

仅通过回归测试和最终算法文档确认其现有行为，不引入 MWD 月度数据依赖。

## 验收标准

1. 自动校准后，每个 Code×月份的日度整数总和等于修饰后原始数据的月度整数总数。
2. `T=0`、`S=0`、无投入日期和单月单个不良 Panel 均有测试。
3. 所有 MWD `defect_panel_count` 均为非负整数。
4. 人工覆盖在自动校准后执行，并遵循 Daily > Weekly > Monthly。
5. 最终 Weekly/Monthly 可从最终 Daily 精确重算。
6. Mapping 现有级联衰减回归测试通过。
7. Yield 领域烟测、编译检查和相关单元测试通过。

## 交付物

- MWD 实现与自动化测试；
- Mapping 现有行为回归验证；
- `references/design_references/feat_design/` 下的 MWD 与 Mapping 函数级算法说明。
