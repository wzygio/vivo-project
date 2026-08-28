# MWD 当前算法耗时评估与简化建议

> 评估对象：
> [`mwd_trend_processor.py`](../../../../src/yield_domain/core/mwd_trend/mwd_trend_processor.py)
> 及其直接调用链。算法语义参见
> [`mwd-processor-opt-algorithm.md`](mwd-processor-opt-algorithm.md)。
>
> 本文最初用于性能诊断；其中“删除 Code 原始日度不良数计算”已按新业务契约实施。
> 原始基准数据继续保留，用于说明优化来源；实施后的生产耗时仍需重新测量。

## 一、结论

当前运行时间变长的主要原因不是 `allocate_integer_counts` 的容量约束和整数分配，
而是**同一份 Panel 明细被重复扫描和重复解析**。

冷缓存打开看板时，Group MWD、Code MWD、Mapping 会分别调用
`YieldAnalysisService._build_modifier_context`。每次调用又分别计算 Group 和 Code 的
当月良损。因此一轮页面加载最多发生：

```text
3 次 _build_modifier_context
  × 2 个层级（Group、Code）
  = 6 次 Panel 全表复制、日期解析、月份字符串格式化和分组去重
```

在本次 18.4 万行合成样本中，这 6 次扫描实测约 **8.26 s**；同一输入的 Code 与
Group 核心趋势流水线阶段中位数合计约 **0.95 s**。所以第一优先级应是合并修饰
上下文和当月良损扫描，而不是删除整数分配。

算法可以简化，但应分两层处理：

1. **优先简化调用和数据准备**：修饰上下文每次页面加载只构建一次；日期只解析
   一次；Group/Code 当月良损在同一份当月切片上一起计算。
2. **随后简化日度生成实现**：保留现有业务约束，只减少逐 Code 的 DataFrame
   复制、字符串月份生成和多次局部赋值。

仅删除容量分配循环，性能收益很小，却会削弱“月合计准确、单日不超过投入”的安全
边界，不建议作为性能优化重点。

## 二、测量口径

### 2.1 环境与样本

- Python 3.11.9、pandas 2.3.1、NumPy 2.3.2；
- CPU：Intel64 Family 6 Model 165；
- 合成数据：92 天、每天 2,000 个 Panel，共 184,000 行；
- 80 个 Code、8 个 Group；Code 补齐后为 7,360 行日度长表；
- 目标覆盖 3 个自然月；
- 各阶段执行 3～5 次，表格使用中位数；
- 不包含数据库查询、Streamlit 绘图和网络时间。

合成样本用于比较各阶段的相对成本，不等同于生产机器的绝对响应时间。实际耗时会
随 Panel 行数、Code 数、日期跨度、缓存命中情况以及 Excel 文件类型变化。

### 2.2 核心流水线结果

| 层级 | 阶段 | 中位耗时 | 同层级占比 |
|---|---|---:|---:|
| Code | 原始日度准备（优化前基准） | 210.19 ms | 31.0% |
| Code | 日期 × Code 补齐 | 8.37 ms | 1.2% |
| Code | 指定良损日度生成 | 357.91 ms | 52.8% |
| Code | 月聚合 | 15.14 ms | 2.2% |
| Code | 周聚合 | 12.05 ms | 1.8% |
| Code | 五份结果格式化 | 74.02 ms | 10.9% |
| **Code** | **阶段合计** | **677.68 ms** | **100%** |
| Group | 原始日度准备 | 197.38 ms | 71.7% |
| Group | 日期补齐 | 0.51 ms | 0.2% |
| Group | 指定良损日度生成 | 38.15 ms | 13.9% |
| Group | 月聚合 | 5.04 ms | 1.8% |
| Group | 周聚合 | 4.48 ms | 1.6% |
| Group | 四份结果格式化 | 29.73 ms | 10.8% |
| **Group** | **阶段合计** | **275.30 ms** | **100%** |

这张表只覆盖 `MWDTrendProcessor` 内部。看板冷缓存路径中，修饰上下文的重复扫描
比上述核心流水线更慢。表中 Code“原始日度准备”的 210.19 ms 是删除原始 Code
日度不良聚合之前的结果，不代表当前实现；当前实现只计算每日总投入和 Group/Code
唯一清单，需在同一生产快照上重新基准测试。

## 三、主要耗时与根因

### 3.0 MWD 处理流程 × 分层 × 根因矩阵

下表按 MWD 实际处理顺序展开。单元格中的 `3.1-A`、`3.1-B` 等编号表示后文分析的
具体根因，而不是新的业务步骤。

| MWD 处理流程 | application | core | infrastructure |
|---|---|---|---|
| 1. 确定分析窗口并取得 Panel 明细 | `YieldAnalysisService` 确定起止时间，并通过 `get_modified_panel_details` 取得同一份 Panel 数据。Group、Code、Mapping 三个缓存入口会分别请求这份数据。 | 本步骤尚未计算良损。 | Repository/数据库/Parquet 快照负责提供 Panel 明细；缓存命中情况决定是否发生外部读取。 |
| 2. 构建修饰上下文 | **3.1-A（P0）**：`get_mwd_trend_data`、`get_code_level_trend_data`、`get_mapping_data` 在各自 cache miss 时分别调用 `_build_modifier_context`，使同一上下文最多构建 3 次。 | `_build_modifier_context` 进入 `modifier_table.sync_modifier_table`，随后解析 `targets`、`group_targets`、`factors` 和 `signature`。 | 每次构建都可能重复读取 Group/Code Sheet；企业工作簿的 COM 路径会放大重复 I/O。 |
| 3. 计算当前月原始良损并同步修饰表 | Application 只负责触发和传递 Panel、月份、文件路径，**并不在本层计算 Group/Code 良损**。 | **3.1-B（P0）**：`sync_modifier_table` 分别以 `level="group"`、`level="code"` 调用 `compute_current_month_loss`。两次都复制全表、解析日期、生成月份字符串、过滤当前月并计算总投入，只在最后的分组字段不同。输出是当前月每个 Group/Code 的汇总良损 `Series`。 | `read_workbook_sheet`/写回边界负责持久化；当前把部分文件格式 `ValueError` 误判为 Sheet 缺失，既有正确性风险，也使真实 COM 耗时未被本次基准覆盖。 |
| 4. 解析 Code/Group 月度目标与 Mapping 倍率 | `_build_modifier_context` 将 Core 结果组装成三个入口使用的上下文。 | `resolve_monthly_targets` 产生 Code/Group 月度目标；`compute_scale_factors` 产生 Mapping 月度倍率。 | 修饰工作簿是人工指定值的存储边界，不负责趋势计算。 |
| 5. 准备 Code 日度容量与原始月度良损 | Application 将同一 Panel 明细、Code 修饰目标和截止日期交给 `create_code_level_mwd_trend_data`。 | **已优化**：`prepare_code_raw_data` 计算每日总投入、提取 `(Group, Code)` 唯一清单，并按“自然月 + Code”计算原始月度良损；不再执行“日期 + Group + Code”的原始不良 Panel 去重。日度容量网格的 `defect_panel_count` 仅为 0 占位。 | 无新增外部 I/O；成本主要是 pandas 内存计算。 |
| 6. 解析有效月度目标并生成 Code 最终日度，再聚合周/月 | Application 接收并缓存 Code MWD 结果。 | **新契约**：`generate_daily_counts` 按“当月指定 → 最近更早月份指定 → 原始月度良损”得到每个 Code/月的有效目标，再按月度基线、确定性扰动和每日总投入生成全部日度整数；不回落原始日度数。逐 Code 小 DataFrame 操作仍是 **3.2（P1）**。 | 无。 |
| 7. 生成 Group 日/周/月 | `get_mwd_trend_data` 先取得已生成的 Code 结果，再调用 Group Core。 | **3.3-Group（P1）**：`prepare_group_raw_data` 仍从 Panel 重新计算每日总投入和原始 Group 日度不良宽表；但 `_build_group_daily_from_code_data` 只保留其中的 `total_panels` 骨架及 Group 名单，原始 Group 日度不良列随后被 Code 最终日度聚合结果替换。因此这部分 Group 不良计算是可删除的冗余。Group Sheet 只在 `_apply_group_monthly_overrides` 覆写最终月度，不反向生成日度。 | 无。 |
| 8. 格式化月/周/日结果 | Application 将格式化结果提供给看板。 | **3.4（P2）**：完整表与近期窗口分别执行 copy、日期格式化、良损计算和排序，存在重复工作。 | 无。 |
| 9. Mapping 消费月度倍率 | `get_mapping_data` 再次构建/消费修饰上下文并调用 Mapping 流水线。 | MWD Core 不参与 Mapping 后续抽样、复制、位置修饰和级联衰减；这里只共享 Code Sheet 推导出的月度倍率。 | Mapping 数据读取及结果输出属于其自身边界。 |

#### 3.0.1 直接回答：3.1 和 3.3 算的是不是同一件事

**不是“3.1 在 application 算、3.3 在 core 算”的同一项计算。** 3.1 的重复由
Application 的三个入口触发和放大，但 Group/Code 原始良损的实际计算也位于 Core
的 `modifier_table.py`；Infrastructure 只承担 Panel 和 Excel 的读写边界。

两者的关系应拆成“相同事实基础”和“不同输出目的”来看：

| 维度 | 3.1 当前月原始良损 | 当前 Code 日度容量准备 |
|---|---|---|
| 输入事实 | Panel 明细、`panel_id`、日期、Group、Code | 同左，但不再按日统计 Code 不良 Panel 数 |
| 可共享工作 | DataFrame 复制、日期标准化、时间过滤、总投入去重 | 同左；Group/Code 唯一清单可从同一标准化事实派生 |
| 时间粒度 | 只取当前月 | 完整 MWD 分析窗口，并保留每天 |
| 输出粒度 | 每个 Group/Code 一个当前月汇总良损 Rate `Series` | 每日总投入容量网格 + `(Group, Code)` 清单 + 每个 Code/月的原始月度良损；不包含原始日度不良事实 |
| 使用目的 | 回写修饰表、解析指定值回退和 Mapping 倍率 | 补齐有效月度目标并生成全部 Code 日度整数，再聚合为周/月 |
| 能否互相替代 | 不能。它提供月度目标来源，不提供日容量 | 不能。它提供日容量和 Code 清单，不提供目标良损 |

所以，两者仍然重复处理同一批 Panel 事实中的日期和总投入，但不再重复计算 Code
不良数：3.1 保留当前月 Group/Code 汇总原始良损，用于形成月度目标和 Mapping 倍率；
日度准备只保留容量与 Code 清单。公共标准化 Panel 事实仍可进一步共享。

另外，当前 Group 路径已经不同于 Code 路径：Group 日度来自 Code 最终日度聚合，
因此 `prepare_group_raw_data` 中“按日期 + Group 计算原始不良数宽表”的部分并非必要；
该路径只需要每日总投入骨架和 Group 名单。

### 3.1 P0：修饰上下文重复构建，且每次构建扫描 Panel 两遍

这里的“修饰上下文”不是月/周/天趋势数据，而是 MWD 和 Mapping 开始计算前需要的
一组**控制参数**。`_build_modifier_context` 最终返回：

| 字段 | 含义 | 使用方 |
|---|---|---|
| `targets` | Code 在各月份的目标良损 | Code MWD |
| `group_targets` | Group 在各月份的目标良损 | Group MWD |
| `factors` | Code 在各月份的 Mapping 缩放倍数 | Mapping |
| `signature` | Code/Group 人工指定内容的签名 | 缓存与变更识别 |

为了构造这些参数，程序不仅要读取修饰表，还会用 Panel 明细重新计算当前月的“原始
良损”，把它同步到修饰表，再根据“指定良损”解析月度目标和 Mapping 倍率。因此它
处于**应用服务和修饰表同步层**，发生在 `MWDTrendProcessor` 真正生成日度趋势之前。

#### 3.1.1 为什么一次上下文会扫描两遍

调用链如下：

- `yield_service.py::get_mwd_trend_data`；
- `yield_service.py::get_code_level_trend_data`；
- `yield_service.py::get_mapping_data`；
- 三者都会进入 `yield_service.py::_build_modifier_context`；
- `_build_modifier_context` 调用 `modifier_table.py::sync_modifier_table`；
- `sync_modifier_table` 再按 `("group", "code")` 两次调用
  `compute_current_month_loss`。

这里的“两遍”对应两套业务口径：

```text
第 1 遍：level="group"
  当前月 Panel 明细
    → 按 defect_group 分组
    → 计算每个 Group 的原始良损

第 2 遍：level="code"
  当前月 Panel 明细
    → 按 defect_desc 分组
    → 计算每个 Code 的原始良损
```

两遍都需要相同的公共步骤：复制全表、解析 `warehousing_time`、筛选当前月、计算当前
月总投入 Panel 数。区别只在最后按 `defect_group` 还是 `defect_desc` 分组。当前实现
把公共步骤也做了两次。

`compute_current_month_loss` 每一遍都会：

1. `panel_details_df.copy()`，复制整份明细；
2. 对整列 `warehousing_time` 执行 `pd.to_datetime`；
3. 对整列执行 `.dt.strftime("%Y-%m")`，生成月份字符串；
4. 只保留当前月；
5. 重新计算当月 `panel_id.nunique()` 作为总投入；
6. 分别按 Group 或 Code 计算不良 Panel 去重数和原始良损。

所以“扫描两遍”并不是 Group/Code 目标算法必须要求读取两次，而是当前函数以 `level`
为参数、一次只返回一个层级，造成公共准备工作无法复用。完全可以先得到一份当月
切片和一个当月总投入，再从这份切片分别计算两级结果。

#### 3.1.2 为什么一轮页面加载会放大为六遍

页面分别请求 Group MWD、Code MWD 和 Mapping。三个公开方法虽然各自有 Streamlit
缓存，但在它们同时 cache miss 时，各自在自己的函数体内调用一次
`_build_modifier_context`，没有共享该次调用的返回值：

```text
Group MWD cache miss   → 构建上下文 → Group 扫描 + Code 扫描
Code MWD cache miss    → 构建上下文 → Group 扫描 + Code 扫描
Mapping cache miss     → 构建上下文 → Group 扫描 + Code 扫描
                                      -----------------------
                                      共 6 次全表处理
```

这里的重复有两个乘数：

- **层级内重复**：一份上下文为 Group、Code 各扫描一次，即 `× 2`；
- **使用方重复**：Group MWD、Code MWD、Mapping 各建一份上下文，即 `× 3`。

因此 3.1 的优化也分两步：先把一份上下文内的两次当月计算合并，再让三个使用方共享
同一份上下文。只做其中一步，仍会保留另一维度的重复。

18.4 万行样本的实测结果：

| 操作 | 中位/合计耗时 |
|---|---:|
| 单次 Group 当月良损 | 1,368.61 ms |
| 单次 Code 当月良损 | 1,362.70 ms |
| 三份上下文、共六次扫描 | 8,256.65 ms |

`.dt.strftime("%Y-%m")` 是明显热点。诊断用的等价方式改为“日期解析一次 + 月份类型
比较 + 同一当月切片同时聚合 Group/Code”后，中位耗时为 **61.52 ms**。这不是最终
实现基准，但证明主要成本来自重复解析和字符串化，而不是业务分组本身。

本次算法修正进一步放大了该问题：

- 修正前，Group 入口调用已缓存的 Code 结果；随后页面直接请求 Code 时通常命中
  同一个缓存。加上 Mapping，冷路径通常构建两份修饰上下文；
- 修正后，Group 改为读取 Group Sheet 并独立生成，因此 Group、Code、Mapping
  各自构建一份上下文，冷路径变为三份；
- 即修正增加了一次完整上下文构建，而 Code 日度算法主体没有显著变复杂。

### 3.2 P1：Code 日度生成按 Code 重复执行 pandas 小表操作

`daily_generator.py::generate_daily_counts` 对每个 `(defect_group, defect_desc)` 调用
一次 `_generate_defect_daily`，每个 Code 内再按月份分组并用 `result.loc` 写回。

扩展测试显示耗时近似随 Code 数线性增长：

| Code 数 | 日度行数 | 全部有目标 | 全部无目标（旧回退路径） |
|---:|---:|---:|---:|
| 20 | 1,840 | 90.89 ms | 2.26 ms |
| 80 | 7,360 | 360.66 ms | 9.56 ms |
| 200 | 18,400 | 889.80 ms | 17.34 ms |

“全部无目标”列是优化前诊断数据。新契约下该输入会在目标覆盖校验阶段失败，不再作为
可运行的快速路径。

80 个 Code 的 `cProfile` 结果中：

- `_generate_defect_daily` 累计约 0.655 s；
- 7,360 次 BLAKE2b 哈希累计约 0.055 s；
- 240 次整数分配累计约 0.040 s；
- 其余大部分时间落在逐 Code 的 DataFrame copy、日期转换、`strftime`、groupby、
  `loc` 赋值和索引对齐。

因此，“哈希太重”与“容量循环太重”都不是主结论，主要问题是大量小 DataFrame
操作的固定开销。

### 3.3 P1：Code 原始日度不良已删除；Group 冗余准备仍待处理

3.3 已经进入 `MWDTrendProcessor` 核心计算。此时 3.1 产生的 `targets` 和
`group_targets` 已经准备好，程序现在要把 Panel 事实明细转换为月/周/天趋势真正使用
的**日度基础数据**。

`create_mwd_trend_data` 与 `create_code_level_mwd_trend_data` 仍接收同一份 Panel 明细，
但 Code 路径已不再聚合原始日度不良数：

```text
Code MWD
  Panel 明细
    → prepare_code_raw_data
    ├─ 按日计算 total_panels
    ├─ 提取 Group + Code 唯一清单
    └─ 按自然月 + Code 计算原始月度良损
    → 日期 × Code 容量网格（defect_panel_count 初始为 0）
    → 当月指定 / 最近指定 / 原始月度良损
    → Code 最终日度生成
    → 周/月聚合

Group MWD
  同一份 Panel 明细
    → prepare_group_raw_data
    → 每日总投入 + 每个 Group 的原始日度不良数（宽表）
    → 只保留每日总投入骨架和 Group 名单
  Code daily_full
    → 按日期 + Group 汇总 Code 最终日度不良数
    → Group 日度/周度
    → Group Sheet 只覆写月度结果
```

当前状态需要区分已完成与尚未完成的优化：

- **Code 已完成**：删除了按“日期 + Group + Code”执行 `panel_id.nunique()` 的原始
  日度不良聚合。Panel 明细提供每日总投入、实际 Group/Code 组合，以及按“自然月 +
  Code”聚合的原始月度良损。修饰目标未覆盖时回退原始月度良损，因此不需要原始
  日度数承担回退职责。
- Group 的最终日度不再由原始 Group 日度不良数生成，而是由 Code `daily_full` 按
  Group 汇总。`_build_group_daily_from_code_data` 从 `padded_daily` 只复制
  `total_panels`，随后用 Code 汇总结果填充各 Group 列。
- Group Sheet 的人工指定值只通过 `_apply_group_monthly_overrides` 覆写月度结果，不会
  反向改变 Group 日度或周度。

因此，剩余可消除的不只是两条路径前面的公共日期准备，还包括
`prepare_group_raw_data` 对“日期 + Group 原始不良数宽表”的计算：这些 Group 不良列
随后被丢弃。Group 路径实际只需要：

1. 与 Code 路径共享的标准化日期和每日 `total_panels`；
2. 用于补齐输出列的 Group 名单；
3. 已经生成的 Code `daily_full`。

优化前两条准备链合计约 **407.57 ms**。其中 Code 原始日度不良聚合已删除；当前
Code 路径直接生成最终日度长表，再从最终 Code 日度派生 Group 日度。具体节省需用
相同样本重新测量，不能直接把旧 Code 阶段的 210.19 ms 全部视为收益，因为日期解析、
每日总投入和 Code 清单提取仍然保留。

Group Sheet 仍然具有最高的月度覆写优先级，但“月度覆写”不等于“独立生成日度”。
这正是原始 Group 日度不良数可以删除，而 Group 月度人工指定仍需保留的原因。

#### 3.3.1 3.1 与 3.3 的区别

两项都出现了“同一份 Panel 明细被重复复制、解析和聚合”，但它们处于不同层级，
计算目的也不同：

| 对比维度 | 3.1 修饰上下文重复构建 | 3.3 Code 容量准备 / Group 冗余准备 |
|---|---|---|
| 所在层级 | Application 触发与放大；实际良损计算在 Core `modifier_table.py` | MWD Core 趋势计算 |
| 发生时间 | 生成趋势之前 | 已拿到目标良损之后 |
| 主要目的 | 计算当前月汇总原始良损，解析目标和 Mapping 倍率 | 构造完整窗口的每日容量与 Code 清单，再生成最终 Code、Group 趋势 |
| 时间范围 | 只需要当前月 | MWD 完整分析窗口，通常约三个月 |
| Group/Code 扫描原因 | 同一函数一次只算一个层级，公共当前月准备未复用 | Code 仍需日容量、清单和月度原始良损；Group 原始日度不良已无用途，但实现仍在计算 |
| 单轮重复数量 | 最多 `3 个使用方 × 2 个层级 = 6` 遍 | Group、Code 两条核心入口各 1 遍，共 2 遍 |
| 直接输出 | 当前月每个 Group/Code 的汇总良损 Rate；随后形成 `targets`、`group_targets`、`factors`、`signature` | Code 日度容量网格和原始月度良损；Group 准备函数还产出随后被替换的原始 Group 日度 Count 宽表 |
| 是否包含 Excel | 是，负责读取和按需同步修饰表 | 否，只做内存中的趋势数据准备 |
| 本次样本耗时 | 六遍约 8.26 s | 优化前两条准备链合计约 0.41 s；Code 优化后待复测 |
| 可合并程度 | 很高：当前月切片和两级汇总可一次计算，结果可供三个使用方共享 | Code 原始日度聚合已删除；公共日容量仍可共享，Group 原始日度聚合仍可删除 |

可以把两者简化理解为：

```text
3.1 回答“当前月各 Group/Code 的汇总原始良损是多少，修饰目标和倍率是什么？”
3.3 回答“整个趋势窗口中，每天投入多少、有哪些 Group/Code，并应生成多少最终不良？”
```

3.1 是**参数准备重复**；3.3 当前剩余问题是**日容量重复准备和无效 Group 聚合**。
Code 日度不再使用 Panel 原始不良 Count，因此它与 3.1 不再重复计算同一份 Code
不良事实。两者仍共享日期标准化、总投入和 Code 清单等基础事实，可以继续复用。

### 3.4 P2：结果格式化重复处理同一张完整表

`format_code_results` 分别生成 `weekly`/`weekly_full` 和 `daily`/`daily_full`。
当前会针对同一个输入重复 copy、日期格式化、计算良损、排序和 Categorical 构造，
只是最后保留的时间窗口不同。Code 格式化约 74.02 ms，Group 约 29.73 ms。

### 3.5 P3：整数分配不是主要热点

隔离执行 240 次、每次 31 行的 `allocate_integer_counts`，合计约 **14.78 ms**；
`cProfile` 口径约 40 ms，差异来自分析器开销。它远小于修饰上下文重复扫描和逐 Code
pandas 操作。

现有分配器还保证目标在月容量内、单日不超过投入、月整数合计准确，并处理零权重和
日期饱和。即使原始权重不超过投入，月内归一化也可能使某日理想份额超过当日容量，
因此不能据此直接删除容量处理。

## 四、Excel 路径的额外发现

当前工作区的 `resources/yield_domain/入库良率修饰表.xlsx` 不是 ZIP/OpenXML 文件。
对它执行 `pd.read_excel` 得到：

```text
ValueError: Excel file format cannot be determined, you must specify an engine manually.
```

但 `excel_tools.read_workbook_sheet` 把所有 `ValueError` 都当成“Sheet 不存在”，直接
返回空表，只有其他异常才回退 Excel COM。因此本次环境中读取两个产品的修饰表都在
约 5 ms 内返回空结果，**没有真正测到企业加密工作簿的 COM 时间**。

这带来两个风险：

1. 当前资源状态下 `modifier_targets` 可能为空，测试没有覆盖真实人工指定已生效的
   完整页面路径；
2. 对能够进入 COM 的其他文件状态，三次上下文构建可能重复打开 Group/Code Sheet，
   外部 I/O 会进一步放大冷启动时间。

该问题首先是读取正确性问题。建议区分“指定 Sheet 缺失”与“文件格式无法识别”的
ValueError，并在修复后用真实资源重新测量冷启动。本次诊断只读探测了业务工作簿，
没有调用同步写回。

## 五、简化方案与优先级

### 5.1 P0：一轮加载只构建一次修饰上下文

把修饰上下文提升为页面加载或应用服务层的共享结果，由 Group MWD、Code MWD 和
Mapping 共同消费；缓存键继续包含产品、Panel 快照签名和修饰表签名。

同时让当月良损计算一次返回 Group/Code 两级结果：

```text
Panel 明细
  → 日期解析一次
  → 当月过滤一次
  → 当月总投入去重一次
  ├─ Group 不良 Panel 去重
  └─ Code 不良 Panel 去重
```

这不改变月度目标解析或 Mapping 倍率语义，是风险最低、收益最高的简化。按本次
样本，当月良损计算有从约 8.26 s 降到百毫秒量级的空间；最终收益需在真实资源和
完整页面上复测。

### 5.2 P0：用日期范围过滤替代整列月份字符串生成

优先使用半开区间：

```text
month_start <= warehousing_time < next_month_start
```

它避免创建整列字符串和 Period 对象，语义也最直接。日期列若已由上游标准化，下游
不应再次全列解析。

### 5.3 P1：共享 MWD 基础日汇总

本项已完成第一部分：Code 路径现在只准备标准化日期、日总投入和 Group/Code 清单，
直接生成最终日度长表；原始 Code 日度不良聚合和回退语义已经删除。

尚未完成的部分是让 Group 路径复用同一份基础结果，并删除其随后被丢弃的原始 Group
日度不良宽表。更彻底的入口可以一次返回两级结果：

```text
create_all_mwd_trend_data(panel_df, modifier_context)
  ├─ shared daily base
  ├─ Code generated daily → Code weekly/monthly
  └─ Code final daily → Group daily/weekly → Group monthly override
```

业务上的 Code 月度生成目标和 Group 月度覆写目标仍分别保留，共同基础事实只计算一次。

### 5.4 P1：把 Code 日度生成改成“数组计算、一次写回”

保持算法公式不变，内部可以：

1. 在整张 padded 表上只生成一次月份键和日期表示；
2. 只为存在目标的 Code 建立索引；
3. 用 NumPy 数组计算基线、噪声、容量和月度分配；
4. 写入一个结果数组，循环结束后一次赋回 DataFrame；
5. 不再逐 Code copy、重复 `to_datetime`/`strftime` 和多次 `loc`。

BLAKE2b 只占日度生成的一小部分，而且保证跨进程确定性。除非有等价输出测试，不建议
为了几十毫秒更换哈希算法。

### 5.5 P2：完整格式化一次，再切近期窗口

weekly 和 daily 分别先生成完整结果，再截取 `weekly` 最近 3 周和 `daily` 最近 7 天，
减少重复 copy、良损计算和排序。需要保持完整日期与短日期的 `time_period` 契约。

### 5.6 P3：保留容量分配器，最多增加快速路径

若仍希望降低其认知复杂度，可先做普通最大余数分配，只有检测到份额超过容量时才
进入现有饱和重分配。但它不是当前热点，应排在最后，并用以下不变量测试保护：

- 每日整数且非负；
- 每日不超过投入；
- 月合计等于 `min(target_total, 月容量)`；
- 相同输入结果稳定；
- 零权重、零投入、高目标和部分日期饱和均有覆盖。

## 六、建议实施顺序与验收

| 顺序 | 改动 | 预期价值 | 业务风险 |
|---:|---|---|---|
| 1 | 修饰上下文共享；Group/Code 当月良损一次计算 | 极高 | 低 |
| 2 | 月份范围过滤；复用已解析日期 | 极高 | 低 |
| 3 | 修复 Excel ValueError 分类并复测真实 COM | 正确性优先 | 中 |
| 4 | ~~删除 Code 原始日度不良聚合，改用原始月度良损回退~~（已完成）；继续共享 Group/Code 基础日汇总 | 中高 | 中 |
| 5 | Code 日度数组化、一次写回 | 中 | 中 |
| 6 | 完整结果格式化一次再切片 | 低到中 | 低 |
| 7 | 分配器快速路径 | 低 | 中 |

实施后应在同一份生产快照上分别记录：

- Group、Code、Mapping 全部 cache miss 的整页墙钟时间；
- `_build_modifier_context` 调用次数和 Panel 日期解析次数；
- 修饰表标准读取/COM 打开次数及耗时；
- Code 数、补齐后日度行数和各阶段耗时；
- 第二次加载的全缓存命中时间。

功能回归至少验证：月目标合计、单日容量、指定目标缺失时回退原始月度良损、输入原始
日度数不影响生成结果、Group Sheet 月度优先、Mapping 月度倍率、跨进程确定性以及
修饰表人工值不被错误覆盖。

## 七、最终判断

当前算法**可以明显简化和加速**，但简化重点应放在数据流和 pandas 执行方式，而不是
删除业务约束：

- 主要性能问题：修饰上下文重复构建、月份字符串化、Group 路径重复准备；
- 次要性能问题：逐 Code 小 DataFrame 操作和重复格式化；
- 非主要问题：BLAKE2b 与 `allocate_integer_counts`；
- 最高优先级正确性风险：企业加密工作簿的 `ValueError` 被误判为 Sheet 缺失。

Code 原始日度不良聚合已经删除，正式契约为“指定值回退 + 原始月度良损 → 完整月度
目标 → 日度整数 → 周/月聚合”。建议继续完成 5.1～5.3 的剩余部分，并在真实工作簿
上复测。
