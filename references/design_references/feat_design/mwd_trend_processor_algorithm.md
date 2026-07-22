# MWD 月周日趋势处理算法

## 1. 适用范围与数据口径

本文描述 `src/yield_domain/core/mwd_trend/mwd_trend_processor.py` 的当前实现。

Code 级流水线接收 Panel 明细，核心字段为：

- `warehousing_time`：入库日期，入口按 `%Y%m%d` 解析；
- `panel_id`：Panel 唯一标识；
- `defect_group`：不良组；
- `defect_desc`：Defect Code。

应用层传入的是经过 `defect_multipliers` 修饰后的 Panel 明细。MWD Core 不再次应用倍率，因此月度校准目标天然来自“修饰后的原始明细”。

Code 级最终业务字段 `defect_panel_count` 始终为非负整数。EMA 中间值可以是浮点不良率，但不会作为最终 Panel 数输出。

## 2. 总体数据流

```text
修饰后的 panel_details_df
  -> _prepare_code_raw_data
  -> _pad_daily_data_to_today
  -> _calc_code_ema_noise
  -> TrendRegulator.regulate_code_daily_base
  -> reconcile_code_daily_counts
  -> apply_code_manual_overrides_to_daily
       Monthly -> Weekly -> Daily
  -> _safe_trend_aggregator（日 -> 周/月）
  -> _format_code_results
```

Mapping 不参与这条流水线，MWD 也不读取批次 Mapping 的数量或级联结果。

## 3. 公共入口

### `MWDTrendProcessor.create_code_level_mwd_trend_data`

Code 级主入口，执行顺序如下：

1. `_prepare_code_raw_data` 生成原始日粒度长表；
2. `_execute_unified_pipeline` 执行日历补齐、EMA、噪声、调节器和自动聚合；本入口向旧月/周覆盖函数传入空字典，确保自动阶段不提前执行人工修正；
3. `reconcile_code_daily_counts` 将自动日结果校准到原始 `Code × 月份` 整数总量；
4. `apply_code_manual_overrides_to_daily` 按月、周、日顺序修改已校准的日数据；
5. 从最终日数据重新聚合周/月；
6. `_format_code_results` 计算不良率并生成展示字段。

任何未捕获异常会记录日志并返回 `None`。

### `MWDTrendProcessor.create_mwd_trend_data`

Group 级主入口。它不重新计算 Group EMA，而是把 Code 级 `daily_full` 按 `defect_group` 汇总后注入 Group 日度骨架，再复用统一流水线进行周/月聚合和 Group 人工覆盖。

当前 Group 级行为保持原样：月/周覆盖在统一流水线中执行，日覆盖在流水线返回后执行。Task2 的整数月度校准和“最终日反算周/月”只作用于 Code 级。

### 模块级 `create_mwd_trend_data`

兼容历史调用方式的入口。它先调用 Code 级入口，再把 Code 结果传给 Group 级入口。`resource_dir` 参数仅为旧签名兼容保留，当前算法不使用它。

## 4. 整数分配与月度校准

### `_allocate_integer_counts`

通用的“带单行容量上限的整数总量分配器”。输入为权重 `weights`、各日最大容量 `capacities` 和目标总数 `target_total`。

处理步骤：

1. 将容量的 `NaN/±inf` 置零，向下取整并截断为非负整数；
2. 有效目标为 `min(max(target_total, 0), sum(capacities))`；
3. 清洗权重；若剩余有效权重总和为零，则回退到剩余容量作为权重；
4. 按权重计算浮点配额；某行配额达到容量时先把该行填满，再把剩余目标重新分配给其他行；
5. 对最终浮点配额向下取整；
6. 按小数余数从大到小补发剩余的 1 个 Panel，稳定排序用于解决同余数并列；
7. 返回整数数组。

该函数保证：

```text
0 <= allocated[d] <= capacity[d]
sum(allocated) = min(target_total, sum(capacities))
```

### `MWDTrendProcessor.reconcile_code_daily_counts`

把 EMA/噪声/调节器后的日度形状校准到修饰后原始数据的月度整数总量。

分组键为：

```text
defect_group + defect_desc + 自然月
```

对每组定义：

- `T`：原始日表在该月的不良 Panel 数之和；
- `A_d`：自动流水线得到的第 `d` 天不良 Panel 数；
- `C_d`：第 `d` 天投入数 `total_panels`；
- `S = ΣA_d`。

分配规则：

- `T = 0`：所有日期写 0；
- `T > 0 且 S > 0`：以 `A_d` 为权重调用整数分配器；
- `T > 0 且 S = 0`：以 `C_d` 为回退权重调用整数分配器；
- `C_d = 0`：该日容量为 0，不会分到不良 Panel；
- 原始目标超过可用总投入时，以总投入为物理上限。

因此，正常数据条件下有：

```text
Σ calibrated_daily_count(Code, month)
  = Σ modified_raw_count(Code, month)
```

校准只修改 `defect_panel_count`，保留日期、投入数、Group 和 Code。

## 5. 人工修正

### `MWDTrendProcessor.apply_code_manual_overrides_to_daily`

所有 Code 人工修正都作用于校准后的日表，优先级为：

```text
Daily > Weekly > Monthly > 自动校准
```

月和周覆盖：

1. 月键兼容 `YYYY-M`、`YYYY-MM` 和尾随“月”；周键兼容 `YYYY-Wn` 与 `YYYY-Wnn`；
2. 配置不良率乘以该周期 `total_panels` 总和并四舍五入，得到周期整数目标；
3. 以当前日度不良数作为形状权重，通过 `_allocate_integer_counts` 分配；
4. 当前形状全零时自动回退到投入权重；
5. 非数字、`NaN`、无穷大或无法匹配的配置被忽略，负数按 0 处理。

日覆盖：

1. 解析配置日期；
2. `round(rate × 当日投入)` 得到整数数量；
3. 数量截断到 `[0, 当日投入]`；
4. 同一 Code/日期直接覆盖月、周阶段产生的日值。

人工修正后不再执行月度自动校准。Code 级周/月结果只从最终日结果重算，避免周期之间口径倒挂。

### 旧覆盖函数

- `_apply_manual_overrides`：Group 月/周宽表覆盖；直接用周期投入乘配置率；
- `_apply_daily_manual_overrides`：Group 日覆盖；
- `_apply_code_manual_overrides`：旧 Code 月/周长表覆盖，统一流水线仍保留该扩展点，但 Code 主入口在自动阶段传入空配置；
- `_apply_code_daily_manual_overrides`：旧 Code 日覆盖，当前 Code 主入口已由统一的后校准覆盖函数替代。

## 6. 自动趋势流水线

### `_execute_unified_pipeline`

Group 与 Code 共用的内部骨架：

1. `_pad_daily_data_to_today` 补齐日期；
2. 调用注入的 EMA/基础日数据函数；
3. 调用调节器约束日数据；
4. 从日数据聚合月和周；
5. 应用周覆盖；若有覆盖，则只重塑命中的 Group/Code，并物理替换对应日数据；
6. 从最终日数据重新聚合月，再应用月覆盖；
7. 返回月、周、日三张表。

Code Task2 入口只借用其中的自动日结果，之后执行新的整数校准和人工修正。

### `_pad_daily_data_to_today`

- Group 宽表：按日期索引补齐到目标结束日，缺失填 0；
- Code 长表：构造“完整日期 × 所有 Code”笛卡尔积，合并真实数量；每日投入只保留一份全局值。

### `_calc_code_ema_noise`

逐 Code、逐自然月计算：

1. `NoDefect` 原样保留；
2. 对每个 Code 从月初建立完整日历；
3. 加载产品 Code baseline，按 Code 和月份解析初始锚点；
4. `_calculate_adaptive_shadow_ema` 计算平滑不良率；
5. 乘 `scaling_factor`，再乘当日投入并四舍五入成整数；
6. `_inject_deterministic_noise_code_level` 注入可复现的正弦噪声并再次取整。

### `_calculate_adaptive_shadow_ema`

使用分子动量 `t_n`、分母动量 `t_d` 和 `alpha = 2/(span+1)`：

- 优先使用外部 baseline 作为月初锚点；
- 无外部锚点时可选用全局均值或首日率初始化；
- 当日无投入时沿用上一个平滑率；
- 对绝对突变、相对暴增、相对暴跌进行异常判断；异常日以历史基础率替代真实率更新动量；
- 正常日以真实分子/分母更新动量。

### `_inject_deterministic_noise_code_level`

使用时间戳与稳定 Code 字符和生成正弦相位，按 `volatility` 调整非负整数不良数。相同输入和参数得到相同结果。

### `_safe_trend_aggregator`

- 只保留锚点日前约三个月窗口；
- `total_panels` 先按日期去重后聚合，防止 Code 长表重复累加分母；
- Group 使用宽表按月/周求和；
- Code 使用 `时间周期 + defect_group + defect_desc` 聚合不良数，再合并全局投入。

## 7. 数据准备、重塑与格式化

### `_prepare_code_raw_data`

每日投入为当日 `panel_id.nunique()`；每日 Code 不良数为 `日期 + Group + Code` 下的 `panel_id.nunique()`。无不良日期补为 `NoDefect/0`。

### `_prepare_group_raw_data`

生成日期索引宽表：`total_panels` 是每日唯一 Panel 数，每个 Group 列是该组每日唯一不良 Panel 数。

### `_generate_code_daily_from_weekly_baseline`

旧周覆盖重塑器。建立“日期 × Code”网格，以投入和确定性正弦波生成日权重，并用最大余数法保证每个 `周 × Code` 的日整数和等于周目标。

### `_generate_daily_from_weekly_baseline`

Group 版周覆盖重塑器，按每周投入与确定性波动权重分配 Group 周目标，同样使用最大余数法守恒。

### `_format_code_results`

生成 `monthly`、`weekly`、`daily_full` 和最近七日 `daily`：

- `defect_rate = defect_panel_count / total_panels`，零投入时为 0；
- 过滤 `NoDefect`；
- 月标签为 `YYYY-MM月`，周标签为 ISO `YYYY-Wnn`；
- 依据真实日期排序、截取周期，避免跨年字符串排序错误。

### `_format_group_results`

把 Group 宽表 melt 成展示长表，输出同样的四种周期结果，并为每个 Group 计算 `defect_rate`。

