# SPC 能力指标计算规则：Cpk 与 Cpm

本文记录当前代码实际执行的统计口径、计算步骤和边界处理。核对日期：2026-10-08；OOS/OOC 与能力计算关系补充核验于 2026-10-10，见第 1.3 节。

当前 SPC 报表的核心口径是：**μ 取周期内 Sheet 明细的 `sheet_mean` 等权平均；σ 取同周期全部有效点位 `param_value` 的样本标准差（`ddof=1`）；Cpk 取最近规格距离除以 3σ；Cpm 取 `Cp / (1 + |Ca|)`。**

本文负责能力算法；测量清洗、Sheet 修饰和报表编排的完整链路见 [SPC 数据流](data-flow-spc.md)。以下公式描述计算器产出的能力值，报表后续还可能应用能力台账替换，见第 8 节。

## 1. 计算范围与输入

### 1.1 当前报表计算上一完整 ISO 周

`SpcReportService.fetch_spc_report_payload()` 调用 `build_period_capability_report(..., previous_week_only=True)`，以查询截止日 `end_date` 所在周为参考，计算：

```text
[上周一 00:00:00，本周一 00:00:00)
```

包含上周一，不包含本周一；不是向前滚动 7 天，也不是最近有数据的一周。上周没有有效输入时返回空能力表，不回退到更早周。

例如查询截止日为 2026-10-08，能力窗口为 `[2026-09-28 00:00:00, 2026-10-05 00:00:00)`，周期标签为 `2026-W40`。

分组键为：

```text
prod_code + factory + step_id + param_name
+ period_type + period_label + period_sort
```

不同产品、厂别、站点、参数和周期分别计算。设备、腔室、Lot、点位名称不参与周期能力分组。

通用计算器保留 `previous_week_only=False` 的月周日模式：按月、ISO 周、日聚合统计，但只有月、周计算 Cpk/Cpm，日行的两项能力固定为 `NaN`。当前 SPC 服务使用上一完整周模式，不能将通用函数的默认行为当成当前报表的计算范围。

### 1.2 两张输入表的含义

| 输入 | 关键字段 | 用途 |
|---|---|---|
| Sheet 明细 `sheet_features` | `sheet_id`、`sheet_start_time`、`sheet_mean`、`usl`、`lsl` | 计算 μ、Sheet 均值备用标准差、Sheet 数、周期规格 |
| 点位明细 `raw_measurements` | `sheet_id`、`sheet_start_time`、`param_value` | 当前配置下计算 σ 和点位数 |

当前服务传入共享特征管线输出的**修饰后 Sheet 特征及修饰后点位**。虽然参数名叫 `raw_measurements`，这里并非直接使用未经处理的数据库原始值；前面的源校正、基础设施日期排除、清洗、最新点位去重、异常点过滤、Sheet OOS 修饰已经影响输入。日期排除先于去重、Sheet 判定和修饰，排除点不会参与 μ、σ 或能力计算，见 [Inline 日期排除规则](../shared/rules-inline-date-exclusion.md)。完整规则由 [SPC 数据流](data-flow-spc.md) 和 [Sheet OOS 规则](../shared/rules-sheet-oos-decoration.md) 维护。

能力豁免在计算前同时作用于两张表。配置 `spc.spc_cpk.exempt_param_name_contains` 按参数名包含普通文本、不区分大小写匹配；当前配置包含 `PPA`，命中参数不计算 Cpk/Cpm，但其点位和 Sheet 明细仍可供其他报表输出使用。

### 1.3 OOS/OOC 与 CPK/CPM 的先后关系

**当前 SPC 主链路先应用 OOS 点位修饰，再计算 CPK/CPM；能力不达标不要求先发生 OOS 或 OOC。** 单片异常识别与周期能力判定使用不同条件，不能相互代替。

| 处理 | 输入与判定 | 对能力计算数据的影响 |
|---|---|---|
| Sheet OOS 清单 | 修饰前 Sheet 的 `sheet_max > usl` 或 `sheet_min < lsl`。 | 生成异常明细；清单筛选本身不删除计算点位。 |
| Sheet OOC 清单 | 修饰前 Sheet 的 `sheet_mean > ucl` 或 `sheet_mean < lcl`，且该 Sheet 没有点位越过 USL/LSL。 | 生成异常明细；不改写或剔除测量点位。 |
| SPC 点位修饰 | 按产品、站点、参数、Sheet 匹配 OOS 决策，先按原 USL/LSL 执行传统修饰，再在专用窗口内按中央规格区间进一步修饰。 | 启用修饰且规格有效时串行改写点位的 `param_value`，再用最终点位重算 Sheet 均值、最大值、最小值。 |
| CPK/CPM 计算 | 修饰后的 Sheet 特征和点位，经能力窗口、参数豁免及有效输入检查。 | 从这些输入计算 μ、σ 和能力值，不再按 OOS/OOC 状态筛除点位。 |

`build_sheet_ooc_detail()` 中排除 OOS Sheet 是**报警分类优先级**：同一 Sheet 指标已按点位极值触发 OOS，就不重复登记为 OOC。函数操作输入副本并返回清单，既不修改 Sheet 极值或规格限，也不将 OOS Sheet 从能力样本中移除。这里的“修饰前”仍已完成源校正、基础设施日期排除、清洗和异常点过滤。

实际改值由 `core/spc/spc_point_decoration.py` 的 `apply_spc_point_decoration()` 执行：先传统 OOS 修饰，再执行专用中央区间修饰；`flag=True`、无决策或空值默认启用，两个阶段均尊重 `flag=False`；旧 `Delete` 在 SPC 中兼容为 False，保留点位。显示日期 2026-10-05 至当天按配置比例派生专用目标，第一阶段输出仍在目标区间外的点进一步移到区间以内，原规格内的点也可能命中。窗口外只执行传统阶段；每阶段目标区间内输入不变，不删点，不改写原规格限，缺少有限有效双边范围时保留原值。当前比例、完整动作、margin 及缓存规则见 [SPC 专用点位修饰](rules-spc-point-decoration.md)。

`prepare_decorated_data()` 使用修饰后的点位重新生成 `sheet_mean`、`sheet_max`、`sheet_min`；`SpcReportService` 将修饰后的两张表传入 `build_period_capability_report()`。当前 `point_value` 配置下，μ 来自重算后的 Sheet 均值，σ 来自修饰后的点位。改值因此可以同时影响 μ 和 σ，但不保证 CPK/CPM 达标。切换 `sheet_mean` 口径时，σ 也使用修饰后 Sheet 均值。

**仅触发 OOC、没有 OOS 的数据，不按 UCL/LCL 修饰或剔除，但可以命中日期窗口内的中央规格区间修饰。** OOC 工作簿的决策不进入上述点位链路；OOC 明细来自修饰前特征，不用修饰后特征重新分类。UCL/LCL 不参与 CPK/CPM 公式。全部点位位于原规格内，仍可能因波动较大或均值偏向边界而使能力值低于 1.33。

例如同一指标的 USL=10、LSL=2、UCL=8、LCL=4，两片均属于上一完整周且通过其他筛选；以下先展示传统模式（新规则关闭或窗口外）：

| Sheet | 修饰前点位 | 修饰前清单分类 | 默认 OOS 修饰后 |
|---|---|---|---|
| A | 8、12 | OOS；即使均值也越过 UCL，不重复列入 OOC。 | 保留两个点，将 12 改为规格内值（约 8.8～9.6，具体值由稳定哈希确定）。 |
| B | 8、9 | OOC；均值 8.5>UCL，点位均未超规格。 | 保留原来的 8、9。 |

上侧替换范围按 `10 - (5%～15%) × 8` 计算，即约 8.8～9.6。两片仍共有 4 个点参与点位 σ 统计，μ 使用重算后的两片均值等权平均；不是删除 A 的超规点后只统计 3 个点。后续独立能力值替换只覆盖 CPK 或 CPM 数值，不反向修改点位、Sheet 特征或 OOS/OOC 分类，见第 8 节。

若处于新规则窗口内且允许修饰，目标为 `[4, 8]`：A 的 12 与 B 的 9 都会移到约 7.4～7.8，两片的 8 不变，仍有4个点。OOS/OOC 分类仍取修饰前数据；能力使用这批新修饰点位及重算后的 Sheet 特征。

需要区分上游的物理过滤：清洗、最新点位去重、异常点规则和日期排除等确实可能减少计算输入，但这不是 OOS/OOC 清单筛选或 SPC OOS 点位修饰的删除行为，处理阶段见 [SPC 数据流](data-flow-spc.md)。

## 2. 每个指标的数据来源

| 符号 / 字段 | 当前含义 | 计算或取值方法 |
|---|---|---|
| xᵢⱼ / `param_value` | 第 i 个 Sheet 的第 j 个有效点位值 | 取共享管线处理后的点位明细 |
| nᵢ | 第 i 个 Sheet 参与均值计算的有效点数 | 对该 Sheet 有效点位计数 |
| x̄ᵢ / `sheet_mean` | 单个 Sheet 的均值 | 该 Sheet 所有有效点位值的算术平均 |
| m | 周期内参与 μ 计算的 Sheet 特征行数 | 排除 `sheet_mean`、`usl`、`lsl` 缺失行后计数；公式使用的行数，不是输出字段 |
| μ / `mean_value` | 周期均值 | 对有效 Sheet 明细的 `sheet_mean` 等权求平均 |
| N / `point_count` | 周期有效点位数 | 点位组内 `param_value.count()` |
| x̄points | 周期全部有效点位的均值 | 点位标准差内部使用的中心值；不作为周期 `mean_value` 输出 |
| σ / `std_value` | 当前配置下的周期样本标准差 | 全部有效点位 `param_value.std(ddof=1)` |
| `sample_count` | 周期不同 Sheet 数 | 有效 Sheet 明细的 `sheet_id.nunique()`；不作为 σ 的分母 |
| USL / `usl` | 规格上限 | 有效 Sheet 组内 `usl` 的首个值 |
| LSL / `lsl` | 规格下限 | 有效 Sheet 组内 `lsl` 的首个值 |
| W | 规格跨度 | `USL - LSL` |
| M | 规格中点 | `(USL + LSL) / 2` |
| h | 半规格宽度 | `(USL - LSL) / 2` |
| Cp | 宽度能力基准 | `W / (6σ)`；Cpm 计算中的中间量，不单独输出 |
| Ca | 均值相对规格中点的偏移比例 | 数学上可定义有符号值 `(μ-M)/h`；当前代码直接计算其绝对值 `abs(μ-M)/h` |
| Cpu、Cpl | 上、下侧能力中间量 | 分别为 `(USL-μ)/(3σ)`、`(μ-LSL)/(3σ)`，不单独输出 |
| `target` | 规格表的工程目标值 | 组内首个非缺失值；周期输出中若仍缺失，补为 M；不参与当前 Cpk/Cpm |
| `ucl`、`lcl` | 控制上、下限 | 组内各自首个非缺失值；缺列补 `NaN`，不参与当前 Cpk/Cpm |

本文 σ 表示代码使用的**样本标准差**，是数据统计量，并非已知的总体标准差。

## 3. μ：从点位到 Sheet，再从 Sheet 到周期

### 3.1 先生成单个 Sheet 的 `sheet_mean`

`preprocess_sheet_features()` 默认按以下键识别同一个 Sheet 指标：

```text
factory + prod_code + sheet_id + step_id + param_name
```

计算前按上述键再加 `site_name` 识别重复点位，按 `sheet_start_time` 升序排列，每个点位保留最后一条，即最新记录。上游制备也有点位去重；这里说明特征计算器自身的去重步骤。

随后对该 Sheet 所有保留的有效 `param_value` 求平均：

$$
\bar{x}_i = \frac{1}{n_i}\sum_{j=1}^{n_i}x_{ij}
$$

对应代码聚合为 `sheet_mean=('param_value', 'mean')`。缺失值不计入均值；没有有效值的 Sheet 均值为缺失。Sheet 特征的 `sheet_start_time` 取这些保留点位时间的最小值，用来决定该 Sheet 所属周期。

若输入含 `data_type`，共享管线先按其拆开生成 Sheet 特征。当前 SPC 服务已限定为 SPC 范围。

### 3.2 周期 μ 是 Sheet 明细均值的平均

周期计算先排除以下任一字段缺失的 Sheet 特征行：

```python
valid_df = df.dropna(subset=['sheet_mean', 'usl', 'lsl'])
```

再在周期组内计算：

$$
\mu = \frac{1}{m}\sum_{i=1}^{m}\bar{x}_i
$$

对应 `float(values.mean())`，结果写入 `mean_value`。

每条 Sheet 特征行权重相同，**不按 Sheet 点位数加权**，也不使用中位数。正常的一 Sheet 指标一行输入下，m 就是参与计算的 Sheet 数。

周期函数本身不会再按 `sheet_id` 去重。如果调用者传入重复 Sheet 特征行，这些行会重复参与 μ，而 `sample_count` 仍按不同 `sheet_id` 计数，所以 m 不一定等于 `sample_count`。

## 4. σ：当前使用全部点位的样本标准差

### 4.1 当前配置与选择顺序

[inline_domain.yaml](../../../../config/domain/inline_domain.yaml) 当前设置：

```yaml
spc:
  spc_cpk:
    period_sigma_source: "point_value"
```

服务调用时优先使用传入的非空 `period_sigma_source`，否则读取该配置。归一化函数接受 `point_value`、`sheet_mean`，忽略大小写和两端空白；其他值回退到 `sheet_mean`。配置缺失或读取失败也默认 `sheet_mean`；通用计算函数不指定 `sigma_source` 时默认同样是 `sheet_mean`。

因此“当前使用点位标准差”来自当前配置，函数签名的默认值仍是 Sheet 均值标准差。`period_box_source` 是分布图样本来源，不决定能力 σ。

### 4.2 点位 σ 的具体步骤

`_build_period_measurement_stats()` 执行：

1. 检查点位表是否有 `prod_code`、`factory`、`sheet_id`、`step_id`、`param_name`、`sheet_start_time`、`param_value`。输入为空或缺任一必要列时，返回空统计映射。
2. 用 `pd.to_numeric(..., errors='coerce')` 转换 `param_value`；不能转换的值变成 `NaN`，并移除 `param_value` 缺失行。
3. 用 `pd.to_datetime(..., errors='coerce')` 转换时间，移除时间缺失行，再按周期窗口筛选。
4. 按 `prod_code + factory + step_id + param_name + period_type + period_label` 汇总全部点位。不会先求各 Sheet 标准差，也不会先给各点位名称分别求标准差。
5. 对该组的点位序列调用 `float(values.std(ddof=1))`，写入 `std_value`；点位数写入 `point_count`。

设该组点位值为 x₁、x₂、…、xN：

$$
\bar{x}_{\mathrm{points}} = \frac{1}{N}\sum_{k=1}^{N}x_k
$$

$$
\sigma_{\mathrm{points}} =
\sqrt{\frac{\sum_{k=1}^{N}(x_k-\bar{x}_{\mathrm{points}})^2}{N-1}}
$$

这里分母是 **N−1**，不是 N，也不是 Sheet 数减一。平方偏差以 **全部点位均值 x̄points** 为中心，不以能力公式中的 μ 为中心。

该 σ 包含同一 Sheet 内点位差异及不同 Sheet 间差异；不使用极差/d₂、移动极差、各 Sheet 标准差的平均或组内合并标准差。实现保留 `Series.std(ddof=1)` 的浮点行为，未改用 pandas 原生 `groupby.std()`，避免近乎常量的数据因不同浮点计算顺序变成零标准差。

### 4.3 μ 与 σ 的样本选择分别执行

μ 从有效 Sheet 特征行生成，σ 从点位表独立生成，再按产品、厂别、站点、参数和周期匹配。点位统计不按照有效 Sheet 的 `sheet_id` 再做交集，也不检查点位对应的 USL/LSL 是否有效。

Sheet 按聚合后的最早点位时间归入周期，点位按自身时间归入周期。因此如果同一 Sheet 的不同点位跨越周界，两者的时间归属可能不同。复算时应分别遵循两张输入表的筛选步骤，不能假设 σ 使用的点位一定恰好是 μ 中所有 Sheet 的全部点位。

### 4.4 备用口径与回退

`sheet_mean` 模式使用同一组有效 Sheet 明细的均值序列计算：

$$
\sigma_{\mathrm{sheet}} =
\sqrt{\frac{\sum_{i=1}^{m}(\bar{x}_i-\mu)^2}{m-1}}
$$

对应 `sheet_mean.std(ddof=1)`，均值 μ 保持原算法不变。

| 情况 | 实际 σ | 输出 `sigma_source` / `point_count` |
|---|---|---|
| 选择 `point_value`，找到该周期点位统计 | 采用点位统计的 `std_value` | `point_value` / 实际有效点数 |
| 选择 `point_value`，找不到该周期点位统计 | 回退 Sheet 均值标准差 | `sheet_mean` / `NaN` |
| 选择 `sheet_mean` | Sheet 均值标准差 | `sheet_mean` / `NaN`，即使另有点位输入也不统计 |

**有点位统计但只有 1 个点**时，`std(ddof=1)` 为 `NaN`；代码仍采用该结果，不回退 Sheet 均值标准差，Cpk/Cpm 均为 `NaN`。回退条件是不存在点位统计项，不是点位标准差为缺失或零。

只有 1 条有效 Sheet 特征时，Sheet 均值标准差也为 `NaN`；但如果选择点位口径且该 Sheet 有至少 2 个有效点，仍可得到点位 σ 和能力值。当前代码没有额外的最少 Sheet 数或最少样本数门槛。

## 5. USL、LSL、规格中点与目标值

规格最初由 `load_parameter_specs()` 从 `mdw.dwd_imp_dv_param_spec` 读取；USL、LSL、UCL、LCL、`target` 转为数值，不能转换的内容视为缺失。产品 `spc_spec_override` 可覆盖已匹配的规格字段，具体取数和覆盖机制见 [SPC 数据流](data-flow-spc.md)。

Sheet 特征按 `prod_code + step_id + param_name` 左连接规格。周期聚合在有效 Sheet 行中分别取 `usl`、`lsl` 的 `first`，不会对规格求平均，不按设备/腔室或历史规格版本拆组，也不会逐条验证组内规格是否一致。

派生量为：

$$
W = USL-LSL,\qquad M=\frac{USL+LSL}{2},\qquad h=\frac{W}{2}
$$

`target` 取周期组内首个非缺失值；若仍缺失，周期输出字段补为 M。**这个补值不参与能力公式**：`calculate_cpk()`、`calculate_cpm()` 都只接收 μ、σ、USL、LSL 四个参数。

M 由上下规格现算，不能用数据库 `target` 替代。当前 Cpm 是规格中点偏移算法；不执行 Taguchi 公式 `W / (6√(σ² + (μ-target)²))`。图表 Target 线的取值规则由 [SPC 数据流](data-flow-spc.md) 维护，不能用周期表补值推断绘图行为。

## 6. Cpk 与 Cpm 的逐步计算

### 6.1 共同准入条件

`has_valid_capability_inputs()` 要求：μ、σ、USL、LSL 均非缺失，σ ≥ 0，LSL ≠ 0，USL > LSL。任何条件不满足，两项能力均返回 `NaN`。

LSL=0 是项目表示上限型规格时的既有排除规则，不是数学公式要求下限必须非零。当前函数不在单边规格下改算 Cpu 或 Cpl。UCL、LCL、`target` 不参与准入。

实现使用 `pd.isna()` 和上述大小比较，未另行执行 `isfinite()` 检查；以下常规公式及大小关系以有限数值输入为前提。

### 6.2 Cpk：最近规格边界的余量

当 σ > 0：

$$
C_{pu}=\frac{USL-\mu}{3\sigma},\qquad
C_{pl}=\frac{\mu-LSL}{3\sigma}
$$

$$
C_{pk}=\min(C_{pu},C_{pl})
=\frac{\min(USL-\mu,\mu-LSL)}{3\sigma}
$$

代码先计算 `nearest_distance = min(usl-mean_value, mean_value-lsl)`，再除以 `3*std_value`，结果写入 `cpk`。不对距离取绝对值：μ 严格位于规格内时 Cpk 为正，位于边界时为 0，越过任一边界时为负。

### 6.3 Cpm：Cp 按规格中点偏移折减

当 σ > 0，代码依次计算：

$$
C_p=\frac{W}{6\sigma}
$$

$$
|Ca|=\frac{|\mu-M|}{h}
$$

$$
C_{pm}=\frac{C_p}{1+|Ca|}
=\frac{USL-LSL}{6\sigma\left(1+\frac{|\mu-(USL+LSL)/2|}{(USL-LSL)/2}\right)}
$$

代码中的局部变量 `ca` 已是绝对值，不保留偏高/偏低方向。代入的是比例小数，例如偏移占半规格宽度的 25%，使用 `0.25`。

结果写入 `cpm`。μ=M 时 Cpm=Cp；偏离中点时按 `1+|Ca|` 折减。在有效双边规格、有限输入且 σ>0 时，Cpm 始终为正，即使 μ 已超出规格也不变为负数。

同一 μ、σ 和规格下，Cpk 可等价写成 `Cp*(1-|Ca|)`；由此当前 Cpm ≥ Cpk，均值居中时二者相等。这是当前两项公式的性质，不表示可以把 Cpm 与其他软件的 Taguchi Cpm 直接比较。

### 6.4 零标准差与缺失处理

先检查共同准入条件，再执行零标准差分支：

| 输入条件 | Cpk | Cpm |
|---|---|---|
| μ、σ、USL、LSL 任一缺失 | `NaN` | `NaN` |
| σ<0，或 USL≤LSL，或 LSL=0 | `NaN` | `NaN` |
| σ=0，LSL<μ<USL | `+inf` | `+inf` |
| σ=0，μ 等于任一规格边界 | `0` | `+inf` |
| σ=0，μ 在规格外 | `-inf` | `+inf` |

Cpm 的零标准差分支直接返回 `+inf`，不再计算偏移折减。无穷值是代码对退化数据的处理结果，不代表实际过程能力无限好。计算器没有给 μ、σ、Cp、Ca 或最终能力执行四舍五入；返回 Python 浮点值，显示格式由后续展示层决定。

## 7. 从点位开始的完整复算示例

以下是构造数据，假设同产品、同厂别、同站点、同参数、同一能力周，输入已完成清洗和修饰，规格为 USL=16、LSL=4。

| Sheet | 有效点位值 | 点数 | `sheet_mean` |
|---|---|---:|---:|
| A | 8、10 | 2 | `(8+10)/2 = 9` |
| B | 12、14、16 | 3 | `(12+14+16)/3 = 14` |

逐步计算：

1. `sample_count=2`，m=2；μ=`(9+14)/2=11.5`。
2. `point_count=N=5`；全部点位均值 x̄points=`(8+10+12+14+16)/5=12`。它与 μ=11.5 不同，因为两片点数不同。
3. 点位平方偏差和=`(8−12)²+(10−12)²+(12−12)²+(14−12)²+(16−12)²=40`。
4. σ=`√(40/(5−1))=√10≈3.16227766`，`sigma_source=point_value`。
5. W=`16−4=12`，M=`(16+4)/2=10`，h=`12/2=6`。
6. Cp=`12/(6√10)≈0.63245553`。
7. |Ca|=`|11.5−10|/6=0.25`。
8. Cpk=`min(16−11.5, 11.5−4)/(3√10)=4.5/(3√10)≈0.47434165`。
9. Cpm=`0.63245553/(1+0.25)≈0.50596443`。

若同样的 Sheet 明细改用 `sheet_mean` 模式，则 σ=`√(((9−11.5)²+(14−11.5)²)/(2−1))=√12.5≈3.53553391`，μ 仍为 11.5，Cpk/Cpm 随新的 σ 重新计算。示例中的小数仅为阅读展示，计算器不按这些展示位数截断中间量。

## 8. 公式结果与报表最终值

`build_period_capability_report()` 输出 `mean_value`、`std_value`、`sigma_source`、`sample_count`、`point_count`、规格与 `cpk`、`cpm` 等字段。`period_start`、`period_end` 取有效 Sheet 组内最早/最晚时间，是实际样本时间，不是窗口的日历起止边界。

服务随后依次调用 Cpk 和 Cpm 的 `prepare_capability_decoration()`。`apply_capability_decoration()` 默认保留计算值；命中能力台账键、启用替换且有可用替换值时，覆盖对应的 `cpk` 或 `cpm`。这个步骤不重算 μ 或 σ，也不改变上述公式。

因此按 Sheet 和点位复算得到的是计算器结果；核对服务最终能力值时，还需考虑后续能力替换。台账匹配与状态规则由 [SPC 数据流](data-flow-spc.md) 维护。

明细工作簿新增、数值刷新、历史保留和不达标风险台账筛选的完整规则见 [SPC 能力明细工作簿规则](rules-spc-cpk&cpm-decoration.md)。明细中的 `flag=False` 表示不启用能力值替换，不等同于指标小于 `1.33`。

周度能力预警另要求有效 Sheet 的首末检出时间跨度至少 48 小时；少于 48 小时或无法确认跨度时不新增预警明细，也不在 SPC 页面或监控读取中预警。该门槛不参与能力公式或数值准入，短跨度仍正常计算并保留 CPK/CPM。

## 9. 实现依据

| 责任 | 源码与符号 |
|---|---|
| 能力准入、Cpk/Cpm 公式、周期 μ/σ 和回退 | [spc_calculator.py](../../../../src/inline_domain/core/spc/spc_calculator.py)：`has_valid_capability_inputs()`、`calculate_cpk()`、`calculate_cpm()`、`_period_frame()`、`_build_period_measurement_stats()`、`normalize_period_sigma_source()`、`build_period_capability_report()` |
| 点位去重、单片均值、Sheet 时间与规格关联 | [monitor_calculator.py](../../../../src/inline_domain/core/monitor/monitor_calculator.py)：`preprocess_sheet_features()` |
| 修饰后 Sheet 与点位输入的组装 | [decorated_data.py](../../../../src/inline_domain/application/shared/decorated_data.py)：`_preprocess_sheet_features_by_type()`、`prepare_decorated_data()` |
| OOS/OOC 清单分类与 SPC 点位改值 | [sheet_oos_decoration.py](../../../../src/inline_domain/core/shared/sheet_oos_decoration.py)：`build_sheet_oos_detail()`；[spc_point_decoration.py](../../../../src/inline_domain/core/spc/spc_point_decoration.py)：`normalize_spc_decisions()`、`apply_spc_point_decoration()`；[sheet_ooc_decoration.py](../../../../src/inline_domain/core/shared/sheet_ooc_decoration.py)：`build_sheet_ooc_detail()` |
| 上一完整周、能力豁免、配置选择和后处理编排 | [spc_service.py](../../../../src/inline_domain/application/spc/spc_service.py)：`exclude_cpm_cpk_parameters()`、`SpcReportService.fetch_spc_report_payload()` |
| 规格读取与覆盖 | [measurement_metadata_loader.py](../../../../src/inline_domain/infrastructure/shared/measurement_metadata_loader.py)：`load_parameter_specs()`；[measurement_preparation.py](../../../../src/inline_domain/infrastructure/shared/measurement_preparation.py)：`get_spec_limits()` |
| 当前标准差口径 | [inline_domain.yaml](../../../../config/domain/inline_domain.yaml)：`spc.spc_cpk.period_sigma_source`；[config.py](../../../../src/shared_kernel/config.py)：`ConfigLoader.get_spc_period_sigma_source()` |
| 能力值替换 | [cpk_decoration.py](../../../../src/inline_domain/core/spc/cpk_decoration.py)：`apply_capability_decoration()` |
| 现有算法验证 | [test_spc_calculator.py](../../../../tests/unit/inline_domain/core/spc/test_spc_calculator.py) |
| OOC 分类与 SPC 修饰验证 | [test_sheet_ooc_decoration.py](../../../../tests/unit/inline_domain/core/shared/test_sheet_ooc_decoration.py)、[test_decorated_data.py](../../../../tests/unit/inline_domain/application/shared/test_decorated_data.py) |
