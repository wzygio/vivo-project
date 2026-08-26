# MWD 月周日趋势处理算法

主流程入口：`src/yield_domain/core/mwd_trend/mwd_trend_processor.py`

本文按实际调用顺序说明 Code、Group 两级趋势的计算过程。每个步骤都标注了对应模块和主要函数，便于从业务逻辑直接定位程序。

## 一、Code 级趋势

入口函数：`MWDTrendProcessor.create_code_level_mwd_trend_data`

### 1. 准备原始日度数据

对应程序：`data_preparation.py` 的 `prepare_code_raw_data`

- 将 `warehousing_time` 转换为日期；
- 按日期统计去重后的每日投入 Panel 数 `total_panels`；
- 按日期、Defect Group、Defect Code 统计去重后的日度不良 Panel 数 `defect_panel_count`；
- 形成后续计算使用的 Code 级日度长表，并确定分析截止日期。

### 2. 补齐日历和 Code 组合

对应程序：`data_preparation.py` 的 `pad_daily_data_to_end`

- 从数据最早日期补齐到分析截止日期；
- 生成“日期 × Group × Code”的完整组合；
- 缺失日期的投入数和不良数补为 0，保证 EMA 和后续周期聚合使用连续日历。

### 3. 生成自动日度趋势

对应程序：`ema.py` 的 `calculate_code_ema_noise`，以及 `code_baseline.py` 中的 baseline 读取与刷新函数

- 根据 Code baseline 和历史数据计算自适应 EMA；
- 将 EMA 不良率乘以缩放系数和每日投入数，得到自动日度不良数；
- 按配置加入确定性波动，使相同输入可重复得到相同结果。

### 4. 执行日度趋势上限调节

对应程序：`trend_regulator.py` 的 `TrendRegulator.regulate_code_daily_base`

- 将每个 Code 的 warning line 上限映射到日度数据；
- 计算当前日度不良率 `defect_panel_count / total_panels`；
- 对超过上限的日度点进行确定性压制，并将不良数限制为非负整数；
- 未提供 `warning_lines` 时直接返回原数据，不执行压制。

该步骤只直接处理 Code 自动日度数据，不直接处理周度或月度结果。

### 5. 月度总数对账与日度整数重新分配

对应程序：`allocation.py` 的 `reconcile_code_daily_counts` 和 `allocate_integer_counts`

该步骤发生在趋势调节之后、人工修正之前。它的目的不是重新计算月度目标，而是让自动日度形状与原始数据中的月度整数总量对齐。

#### 5.1 确定每组对账目标

`reconcile_code_daily_counts` 分别处理自动日度结果和原始日度数据：

1. 将日期转换为月份；
2. 按 `(defect_group, defect_desc, month)` 对原始日度不良数求和；
3. 将这个整数合计作为该 Group、Code、月份的 `target_total`；
4. 对自动日度结果按相同维度逐组重新分配。

因此，对账粒度是“单个 Group + 单个 Code + 单个月份”，不同 Code 或不同月份之间不会相互借用不良数。

#### 5.2 准备权重、容量和有效目标

对每个待分配月份：

- `weights`：优先使用趋势调节后的每日 `defect_panel_count`，用于保留自动趋势的日度形状；如果整个月的自动不良数都为 0，则改用每日 `total_panels` 作为权重；
- `capacities`：每日 `total_panels`，表示单日最多能够分配的不良 Panel 数；容量会先向下取整，并将空值、无穷值和负数归零；
- `effective_target`：将原始月度目标限制在 `0` 到当月所有日容量之和之间。

其约束可以概括为：

```text
0 <= 每日分配不良数 <= 当日 total_panels
所有日度分配数之和 = effective_target
effective_target = min(max(原始月度目标, 0), 当月日容量总和)
```

正常情况下，原始月度目标不超过容量总和，因此最终日度整数之和等于原始月度不良总数；如果目标异常地超过容量，只能分配到容量上限。

#### 5.3 按权重迭代分配

`allocate_integer_counts` 使用以下过程分配月度目标：

1. 按当前有效权重计算各日应分得的精确份额；
2. 如果某日份额达到或超过其剩余容量，先把该日填到容量上限；
3. 从剩余目标中扣除已经分配的数量，并在其他仍有容量的日期之间继续按权重分配；
4. 如果剩余日期的权重总和为 0，则改用各日剩余容量作为权重；
5. 重复以上过程，直到目标分配完毕或所有日期都达到容量上限。

该迭代过程可以处理高权重日期提前饱和的情况，避免简单比例分配后出现单日不良数超过投入数。

#### 5.4 转换为整数并补齐余数

比例分配首先得到浮点份额，随后：

1. 对每个日期的份额向下取整；
2. 计算“有效目标减去已取整合计”得到的整数余数；
3. 按各日期小数部分从大到小排序；
4. 在尚未达到容量上限的日期上依次补 1，直到余数补完。

最终结果同时满足整数、非负、单日不超过投入数，以及月度合计对齐有效目标四个条件。

需要注意：月度对账会在趋势调节器之后重新分配日度不良数。趋势调节器提供的日度形状仍作为分配权重，但其单日 warning line 上限不是本步骤的容量约束；本步骤的硬容量是每日 `total_panels`。

### 6. 执行周、日、月人工修正

对应程序：

- `pipeline.py` 的 `run_manual_period_pipeline`：控制统一执行顺序；
- `manual_overrides.py`：应用周期修正，并在需要时重建日度数据；
- `allocation.py` 的 `allocate_integer_counts`：将修正后的周期整数总数安全分配到日度。

处理顺序如下：

1. 使用 `aggregation.py` 的 `safe_trend_aggregator` 从自动日度聚合周度；
2. 使用 `manual_overrides.py` 的 `apply_code_period_overrides` 应用周度修正；
3. 使用 `rebuild_code_daily_from_weekly` 将被修正的周度目标重新分配到日度；
4. 使用 `apply_code_daily_overrides` 直接覆盖指定日期；
5. 如果存在日度修正，重新聚合周度；
6. 从最终日度聚合月度；
7. 最后使用 `apply_code_period_overrides` 应用月度修正，且不回写日度。

### 7. 聚合并格式化输出

对应程序：

- `aggregation.py` 的 `safe_trend_aggregator`：从日度汇总周度和月度；
- `formatting.py` 的 `format_code_results`：生成前端使用的结果结构。

最终输出包括月度、周度、完整日度和近期日度结果。

## 二、Group 级趋势

入口函数：`MWDTrendProcessor.create_mwd_trend_data`

Group 级不会重新计算一套 Group EMA，而是以已经完成 Code 级处理的日度结果为来源。

### 1. 准备 Group 日度骨架

对应程序：

- `data_preparation.py` 的 `prepare_group_raw_data`：按日期统计每日投入数，并识别目标 Defect Group；
- `data_preparation.py` 的 `pad_daily_data_to_end`：补齐 Group 级连续日历。

### 2. 从 Code 日度结果汇总 Group 日度结果

对应程序：`mwd_trend_processor.py` 的 `_build_group_daily_from_code_data`

- 读取 Code 级输出中的 `daily_full`；
- 按日期和 Defect Group 汇总 `defect_panel_count`；
- 将汇总值写入 Group 日度骨架。

因此，Group 自动日度结果会继承 Code 级 EMA、趋势调节和月度总数对账后的结果，但 Group 自身不直接调用 `ema.py`、`trend_regulator.py` 或 `reconcile_code_daily_counts`。

### 3. 执行 Group 周、日、月人工修正

对应程序：

- `pipeline.py` 的 `run_manual_period_pipeline`：控制统一执行顺序；
- `manual_overrides.py` 的 `apply_group_period_overrides`、`rebuild_group_daily_from_weekly` 和 `apply_group_daily_overrides`：执行 Group 周期修正；
- `allocation.py` 的 `allocate_integer_counts`：把修正后的周度整数目标重新分配到各日，并限制在每日投入容量内。

具体顺序与 Code 级一致：周度修正先重建日度，日度修正后重新聚合，月度修正只影响最终月度表。

### 4. 聚合并格式化输出

对应程序：

- `aggregation.py` 的 `safe_trend_aggregator`：从 Group 日度汇总周度和月度；
- `formatting.py` 的 `format_group_results`：生成前端使用的月度、周度、完整日度和近期日度结果。

模块级 `create_mwd_trend_data` 是兼容入口：先调用 Code 级入口，再把 Code 结果传给 Group 级入口。

## 三、人工修正的统一优先级

统一由 `pipeline.py` 的 `run_manual_period_pipeline` 执行：

```text
自动日度
  -> 周度聚合与周度修正
  -> 根据周度结果重建日度
  -> 应用日度修正
  -> 重新聚合周度、月度
  -> 应用月度修正
```

三种修正的作用范围为：

- 周度修正：修改周度目标，并回写重建对应日度，因此会影响随后生成的月度结果；
- 日度修正：直接覆盖指定日期，并重新生成周度和月度结果；
- 月度修正：只修改最终月度表，不回写日度或周度。

修正率会按周期投入数换算为整数不良数，并限制在投入数范围内。周期到日度的整数分配由 `allocation.py` 的 `allocate_integer_counts` 完成。

## 四、主流程关系概览

```text
Code 原始 Panel 明细
  -> data_preparation.py：统计并补齐 Code 日度数据
  -> ema.py / code_baseline.py：生成自动日度趋势
  -> trend_regulator.py：执行 Code 日度上限调节
  -> allocation.py：按原始月度整数总数对账并重新分配日度
  -> pipeline.py / manual_overrides.py：执行周、日、月人工修正
  -> aggregation.py / formatting.py：生成 Code 月、周、日输出
  -> mwd_trend_processor.py：按 Group 汇总 Code daily_full
  -> pipeline.py / manual_overrides.py：执行 Group 人工修正
  -> aggregation.py / formatting.py：生成 Group 月、周、日输出
```

`mwd_trend_processor.py` 主要负责组织以上步骤、传递数据和保留兼容调用入口；具体计算逻辑由各专用模块承担。
