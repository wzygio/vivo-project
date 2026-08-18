# MWD 月周日趋势处理算法

对应程序：`src/yield_domain/core/mwd_trend/mwd_trend_processor.py`

> 2026-08-18 起，数据修饰由"入库良率修饰表"（`resources/入库良率修饰表.xlsx`）
> 的人工指定良损驱动。旧的 "Codebaseline→EMA→趋势调节→月度校准→人工覆盖
> （趋势图人工修正.xlsx）" 链路已整体删除。

## 一、修饰表（modifier_table.py）

工作簿按 `<产品型号>_Group级` / `<产品型号>_Code级` 划分 Sheet，列固定为
`不良类型 | 周期类型 | 时间标签 | 当月良损 | 指定良损 | 缩放倍数`。

1. **读取**：openpyxl 优先，企业加密文件回退 Excel COM；文件/Sheet/列缺失按
   空表语义（全部回落原始数据）。
2. **当月良损回写**：每次计算（页面 cache miss 或 CLI）只更新当月行的
   `当月良损`（缺失行追加），口径为与趋势图一致的 panel 明细
   （不良 Panel 去重数 / 当月投入 Panel 去重数）。
3. **目标良损回退链**：当月 `指定良损` → 最近一个有 `指定良损` 的上个月 →
   当月 `当月良损` → 0（无行且从未指定）。
4. **缩放倍数**：`round(回退后指定良损 / 当月良损, 2)`；当月良损为 0/缺失记 1.0。
   `指定良损` 签名（blake2b，存于 `<表名>.sig.json`，按 `<产品>:<级别>` 分键）
   变化或当月良损内容变化时才写回工作簿；写回失败仅记日志。
5. CLI：`tools/update_yield_modifier_table.py --product <code> [--month YYYY-MM]`。

## 二、Code 级趋势

`create_code_level_mwd_trend_data` 的处理顺序：

1. 按日期、Group、Code 统计原始不良 Panel 数，并统计每日投入 Panel 数（同前）。
2. 补齐日期和 Code 的日历数据（同前）。
3. `daily_generator.generate_daily_counts` 按 `modifier_targets`
   （{defect_desc: {月份: 目标良损}}）生成日度不良数：
   - 月度目标量 `round(目标良损 × 当月投入总数)`；
   - 跨月平滑基线：各月目标良损锚定月中（15 日）线性插值，两端平延，无阶梯；
   - 确定性扰动：`1 + volatility × (2u − 1)`，`u = blake2b(产品|缺陷|日期)/2^64`，
     白噪声无周期，同输入多次运行结果完全一致；
   - 月内按权重 `base × noise × 当日投入` 做整数分配（复用
     `allocation.allocate_integer_counts`，单日上限 = 当日投入），
     月度合计精确等于目标量。
   - 未指定的缺陷保持原始日度不良数。
4. 周度/月度由最终日度经 `safe_trend_aggregator` 直接聚合（无任何人工覆盖）。
5. 经 `format_code_results` 输出（`weekly_full` 全窗口、`weekly` 近 3 周）。

## 三、Group 级趋势

`create_mwd_trend_data` 不独立生成：由 Code 日度结果按 Group 汇总形成 Group
日度数据，再由日度直接聚合周/月并输出。Group 级 Sheet 照常回写当月良损与
缩放倍数，仅作展示参考，不驱动生成。

## 四、Mapping 月度缩放

`mapping/mapping_processor.py` 步骤 2.5（位置修饰之后、级联衰减之前）：
对每个 `(batch_no, defect_desc)` 按"批次所属月份的缩放倍数"（Code 级 Sheet）
确定性缩放不良行数（下调 `random_state=42` 抽样；上调复制 + `_SIM_M` 后缀）。
Mapping 不良数最终影响因子 = defect_modifier（YAML 全局）× 缩放倍数（月度）×
级联衰减（红线，逻辑未改动）。

## 五、主要辅助步骤

- `data_preparation.py`：准备 Code/Group 原始数据并补齐日期。
- `modifier_table.py`：修饰表读写、当月良损、回退解析、缩放倍数、签名。
- `daily_generator.py`：指定良损 → 日度生成。
- `allocation.py`：权重整数分配（容量受限）。
- `aggregation.py`：从日度聚合周度、月度。
- `formatting.py`：生成前端使用的月度、周度、完整日度和近期日度结果。

已删除：`code_baseline.py`、`ema.py`、`trend_regulator.py`、
`manual_overrides.py`、`pipeline.py` 及 `趋势图人工修正.xlsx` 的消费代码
（文件保留作参考）。
