# Mapping 数据处理与修饰算法

## 1. 边界与总体流程

Mapping 与 MWD 是两条独立流水线。Mapping 只使用 Panel 明细中的批次、Code 和坐标，不读取 MWD 月度目标，也不向 MWD 回写数量。

```text
panel_details_df
  -> BatchStatistics.get_batch_input_counts
  -> prepare_mapping_data
       有效批次筛选
       最新五批排序
       Panel 坐标确定性偏移
       By Code 的 Rate-Based 级联衰减
       固定种子抽样
  -> Panel ID 解析为矩阵坐标
  -> 按 Batch × Code 聚合 heatmap_matrix
  -> apply_hotspot_modification_to_matrix
       original / random / additive / multiplicative
```

Task2 不修改 Mapping 级联逻辑；本文记录当前实际行为。

## 2. 批次级准备

### `prepare_mapping_data`

输入：Panel 明细、首批缩放因子 `scaling_factor`、最小批次投入阈值 `min_panel_threshold`。

输出：经过坐标偏移和级联抽样后的不良 Panel 记录，并附加 `batch_total_input`。

处理步骤如下。

#### 2.1 批次投入与阈值

调用 `BatchStatistics.get_batch_input_counts`，按批次计算唯一 `panel_id` 数作为投入分母。只保留投入数达到阈值的批次。

#### 2.2 日期解析与最新五批

内部 `_clean_batch_date` 从批次文本提取 `YY/MM/DD` 或 `YYYY/MM/DD`。可解析批次按日期倒序选最新五批，再重新按 Old → New 排序供级联处理。

全部解析失败时，回退为批次字符串倒序选五个，再按字符串升序处理。

#### 2.3 只保留不良记录

只保留目标批次且 `defect_desc.notna()` 的记录。良品参与批次投入分母，但不进入 Mapping 点位和不良数分子。

#### 2.4 确定性坐标偏移

逐批次调用 `get_deterministically_modified_panel_id(panel_id, batch_no)`。可解析坐标在行、列方向分别偏移 `[-2, 2]`，并裁剪到 10 × 19 的有效范围；无法解析或偏移为零时保留原 Panel ID。

#### 2.5 Rate-Based 级联衰减

常量：

```text
FIRST_REDUCTION_FACTOR = scaling_factor
SECOND_REDUCTION_FACTOR = 0.95
SEED = 42
```

级联状态 `max_allowed_rates` 以 `defect_desc` 为键，因此不同 Code 独立衰减。

对每个批次、每个 Code：

- `batch_total`：该批次唯一 Panel 投入数；
- `current_count = len(df_code_group)`：当前位置偏移完成、批次/Code 筛选后的候选不良 Panel 记录数；
- `current_rate = current_count / batch_total`。

最老批次：

```text
target_rate = current_rate × FIRST_REDUCTION_FACTOR
ceiling[code] = target_rate
```

后续批次：

```text
new_ceiling = previous_ceiling × 0.95
target_rate = min(current_rate, new_ceiling)
ceiling[code] = new_ceiling
```

注意，状态中保存的是纯理论 `new_ceiling`，不是 `target_rate`。因此即使某批真实不良率很低，下一批的天花板仍沿理论曲线继续衰减。

目标不良 Panel 数为：

```text
target_count = int(target_rate × batch_total)
target_count = clamp(target_count, 1, current_count)  # current_count > 0
```

这里 `target_count` 是级联良损率换算后的目标不良 Panel 数；`current_count` 是进入本批次级联前的候选不良 Panel 记录数。在当前一条记录代表一个 Panel/Code 的数据约定下，抽样记录数就是抽样 Panel 数。

当 `target_count < current_count` 时，使用 `DataFrame.sample(n=target_count, random_state=42)` 保留固定数量的记录；否则全部保留。固定种子保证同一输入的抽样结果可复现。

#### 2.6 元数据

最终为每条记录映射批次投入数到 `batch_total_input`，供前端计算/展示批次不良率。

### `BatchStatistics.get_batch_input_counts`

按 `batch_no` 对 `panel_id` 去重计数，返回每批投入数。它是 Mapping 级联的分母来源。

### `BatchStatistics.calculate_batch_defect_stats`

按 `batch_no + defect_desc` 对 `panel_id` 去重计数，合并批次投入并计算：

```text
defect_rate = defect_count / total_input
```

该函数提供通用批次统计，不直接改变 `prepare_mapping_data` 的级联状态。

## 3. Panel 坐标处理

### `get_deterministically_modified_panel_id`

解析原 Panel 坐标后，以 `panel_id + batch_no` 生成随机种子，分别产生行列偏移。行裁剪到 `[0, 9]`，列裁剪到 `[0, 18]`，再重建 Panel ID。

### `parse_panel_id_to_coords`

从 Panel ID 的固定片段读取膜位：

- 行码 `1A..1E, 2A..2E` 映射为 0..9；
- 列码 `A0..S0` 映射为 0..18；
- 格式无效时返回 `None`。

### `reconstruct_panel_id`

保留 Panel ID 前 11 个字符，用新行列坐标重建尾部膜位编码。

## 4. Mapping 矩阵修饰入口

### `apply_hotspot_modification_to_matrix`

输入是已按 Panel 坐标聚合的二维矩阵，以及产品、批次、Code、批次位置和修饰脚本列表。

1. `_mapping_script_matches` 筛选启用且产品、Code、批次/批次位置均匹配的脚本；
2. 无匹配脚本时原样返回；
3. 以第一个匹配脚本决定模式，只叠加相同模式的后续热点规则；
4. 执行模式运算；
5. 普通加值/倍率模式最终转成整数并截断到非负；随机模式自身直接输出整数守恒矩阵；
6. 异常时记录日志并返回原矩阵。

矩阵修饰发生在 `prepare_mapping_data` 级联抽样之后，只改变热图格点数量或分布，不反向改变级联状态。

## 5. 脚本匹配函数

### `_mapping_script_matches`

要求 `enable=True`，并依次检查：

- `target_product`；
- `target_code`；
- `target_batch`/`target_batches`；
- `target_batch_index`。

显式批次条件和批次位置条件同时存在时，两者必须都匹配。

### `_matches_target`

支持单值、列表和 `ALL`。目标为空表示不限制。

### `_normalize_batch_text`

移除“批次”“蒸镀批”等文本，识别两位或四位年份，将日期统一为 `YYYYMMDD`；非日期文本去除常见分隔符。

### `_matches_batch_target`

先尝试原文相等，再比较规范化批次；允许规范化目标包含于实际批次文本，以兼容批次后缀。

### `_matches_batch_index`

支持：

- 非负索引：从最老批次起的 0-based 位置；
- 负索引：`-1` 为最新批次；
- 索引列表；
- `oldest`、`latest`、`middle`、`all`。

## 6. 修饰模式

### original / raw / none

直接返回原矩阵。

### multiplicative

第一个同模式脚本的 `normal_multiplier` 作为所有普通格点的倍率。依脚本顺序构造热点蒙版，尚未被更早脚本占用的热点使用该脚本的 `hotspot_multiplier`。同一格点命中多个规则时，先匹配脚本优先。

### additive

与倍率模式的优先级相同。普通格点使用第一个脚本的 `normal_multiplier_in_add_mode` 作为加值；新命中的热点使用对应脚本的 `hotspot_adder`。历史拼写 `addtive` 会规范化为 `additive`。

### 热点蒙版

支持三类规则：

- `row`：整行；
- `col`：整列；
- `position`：单个行列坐标。

脚本配置字典使用 10 行 × 21 列名称映射，但只对实际矩阵中存在的行列生效；当前 Panel 坐标矩阵通常为 10 × 19。

## 7. 随机分布模式

### `_apply_random_mapping_distribution`

首先计算原矩阵总不良数：

```text
total_defects = sum(all cells)
```

零总数直接返回非负整数矩阵。随机种子由产品、批次、Code 和配置 seed 经 SHA-256 稳定生成，因此相同输入可复现。

#### even / balanced

使用整除把总数平均放到所有格点：

```text
base, remainder = divmod(total_defects, cell_count)
```

每格先放 `base`，再随机选择 `remainder` 个不同格点各加 1。总数严格守恒。

#### poisson（默认）

名称为 poisson，但实际最终抽样使用多项分布：

- `variation = 0`：所有格点概率相等；
- `variation > 0`：先用 Gamma 分布生成正权重，再归一化成格点概率；
- 用 `rng.multinomial(total_defects, probabilities)` 一次分配所有不良数。

Gamma 参数为：

```text
shape = 1 / variation²
scale = variation²
```

`variation` 越大，格点概率差异通常越大。多项分布天然保证输出全为非负整数，且矩阵总数与输入完全相同。

### `_stable_mapping_seed`

连接所有种子组成部分，计算 SHA-256，取前 16 个十六进制字符并映射到 NumPy RNG 范围。它避免 Python 进程级哈希随机化影响随机矩阵复现。

### `_normalize_mapping_mode` / `_to_number`

前者统一模式名称并兼容 `addtive`；后者把配置值转为浮点，失败时使用默认值。

## 8. 守恒与非守恒边界

- `prepare_mapping_data` 的级联抽样会减少不良 Panel 记录数，这是设计行为；
- random 矩阵修饰严格保持进入修饰器时的矩阵总数；
- additive 和 multiplicative 会改变矩阵总数；
- original 不改变矩阵；
- Mapping 数量不要求与 MWD 月度数量相等，也不建立月度到批次的换算依赖。

