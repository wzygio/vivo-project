# MWD 月周日趋势处理算法

对应程序：`src/yield_domain/core/mwd_trend/mwd_trend_processor.py`

## 一、Code 级趋势

`create_code_level_mwd_trend_data` 的处理顺序：

1. 按日期、Group、Code 统计原始不良 Panel 数，并统计每日投入 Panel 数。
2. 补齐日期和 Code 的日历数据。
3. 根据 Code baseline 计算 EMA 平滑不良率，乘缩放系数和每日投入数，生成自动日度不良数。
4. 经过趋势调节器后，将日度结果校准到原始数据的月度整数总量。
5. 执行人工修正：
   - 周度修正：先修改周度目标，再按日度投入和原日度形状重建日度；
   - 日度修正：直接覆盖指定日期；
   - 月度修正：只修改最终月度结果，不回写日度。
6. 根据最终日度重新聚合周度和月度，并整理为展示结果。

## 二、Group 级趋势

`create_mwd_trend_data` 不重新计算 Group EMA，处理步骤为：

1. 先完成 Code 级趋势计算。
2. 将 Code 日度结果按 Group 汇总，形成 Group 日度数据。
3. 复用与 Code 相同的人工修正顺序：周度修正重建日度，日度修正后重新聚合，月度修正只影响月度。
4. 输出 Group 的月度、周度和日度结果。

模块级 `create_mwd_trend_data` 只是兼容入口：先调用 Code 级，再调用 Group 级。

## 三、人工修正的关键规则

统一由 `run_manual_period_pipeline` 执行：

```text
自动日度
  -> 周度聚合与周度修正
  -> 根据周度结果重建日度
  -> 应用日度修正
  -> 重新聚合周度、月度
  -> 应用月度修正
```

修正率会按周期投入数换算为整数不良数，并限制在投入数范围内。日度整数分配使用当前不良数作为主要形状；形状全为零时使用投入数作为权重。

## 四、主要辅助步骤

- `data_preparation.py`：准备 Code/Group 原始数据并补齐日期。
- `ema.py`：按 Code 计算自适应 EMA 和自动日度不良数。
- `trend_regulator.py`：对自动日度趋势进行调节。
- `allocation.py`：在容量限制下分配整数不良数，并完成月度总量校准。
- `manual_overrides.py`：处理月度、周度、日度修正以及周度重建日度。
- `aggregation.py`：从日度聚合周度、月度。
- `formatting.py`：生成前端使用的月度、周度、完整日度和近期日度结果。

`mwd_trend_processor.py` 现在主要负责组织以上步骤和兼容旧调用入口。
