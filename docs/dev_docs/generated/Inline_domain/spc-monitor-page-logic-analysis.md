# Inline Domain 特殊处理逻辑：SPC 监控报表

> 状态日期：2026-08-19
>
> 页面入口：`app/pages/SPC监控报表.py`
>
> 前端实现：`app/sections/inline_domain/spc/spc_dashboard.py`
>
> 后端范围：`src/inline_domain/`、`src/shared_kernel/config.py`
>
> 配置入口：`config/inline_config.yaml`

本文按“前端样式”和“后端处理”整理 Inline Domain 中与 SPC 展示相关的特殊逻辑。它描述当前源码行为，不再按历史修改过程组织。

“增加单位”目前尚未实现，本文只记录其状态，不分析或设计实现方案。

## 1. 结构总览

```text
SPC 监控报表及 Inline 特殊处理
├─ 前端样式（app/）
│  ├─ 折线图 / 箱线图：根据参数名与配置决定
│  ├─ 仅绘制上限：LSL 为空或等于 0
│  ├─ By 过货时间：折线图 X 轴使用实际过货时间
│  └─ 增加单位：尚未实现，本次不处理
│
└─ 后端处理（src/）
   ├─ 异常值剔除：按 spc_outlier_filters.xlsx 的数值边界删除测量点
   ├─ 参数豁免：共享算法，AOI TT/RS 分别做字段和业务键适配
   ├─ CPK 计算：均值固定取 Sheet Mean，sigma 来源可配置
   └─ param_clip_rules：已删除，不再参与运行
```

职责边界如下：

| 特殊逻辑 | 当前所有者 | 是否直接影响 SPC 页面 |
|---|---|---|
| 折线图 / 箱线图选择 | 前端 dashboard | 是 |
| 仅绘制上限 | 前端 dashboard | 是 |
| By 过货时间使用时间轴 | 前端 dashboard | 是 |
| 增加单位 | 未实现 | 否 |
| 异常值剔除 | Inline measurement infrastructure | 是，发生在页面取数之前 |
| 参数豁免 | Inline shared Core + AOI TT/RS 适配器 | 否；属于同一 Inline Domain 的 AOI 特殊处理 |
| CPK 计算口径 | SPC Core/Application | 是 |
| `param_clip_rules` | 已删除 | 否 |

## 2. 页面入口与数据流

`app/pages/SPC监控报表.py` 只负责页面装配：

1. 构造 `SpcQueryConfig` 和 SPC repository；
2. 调用 `SpcReportService.get_spc_report_data()` 获取测量点、Sheet 特征、周期能力及指标数据；
3. 将 `period_sigma_source` 传给后端能力计算；
4. 将 `period_box_source` 和数据帧传给 dashboard；
5. 由 dashboard 完成筛选、图表样式选择和规格线绘制。

```text
app/pages/SPC监控报表.py
  ├─ ConfigLoader.get_spc_period_sigma_source()
  │    └─ SpcReportService
  │         └─ measurement preparation → SPC calculator
  │
  └─ ConfigLoader.get_spc_period_box_source()
       └─ spc_dashboard.py
            ├─ 月/周/天分布图
            └─ Sheet 点位图
```

图表类型不再由 `SpcReportService` 决定，也不会通过后端 DataFrame 的 `chart_type` 列传给页面。

## 3. 前端样式

### 3.1 折线图还是箱线图

配置位于 `config/inline_config.yaml`：

```yaml
spc:
  chart:
    line_param_name_contains:
      - "UNI"
```

调用关系：

```text
inline_config.yaml
  └─ ConfigLoader.get_spc_line_chart_param_name_contains()
       └─ spc_dashboard.py::_resolve_chart_type(param_name, tokens)
            ├─ 参数名包含任一 token → line
            └─ 未命中或配置为空     → box
```

匹配规则为：去除配置值首尾空白、忽略空值、不区分大小写、按普通子串匹配而非正则表达式。程序内没有 `UNI` 硬编码；配置无效或为空时安全回退为箱线图。

该选择只作用于两张 Sheet 点位图：

- By 主站点设备/腔室；
- By 过货时间。

月、周、天分布图始终使用箱线图。`spc.spc_cpk.period_box_source` 只决定这些箱线图使用 Sheet Mean 还是原始点位值，不决定图表类型。

主要实现：

- `spc_dashboard.py:563`：`_resolve_chart_type()`；
- `spc_dashboard.py:844`：`_create_sheet_points_box_chart()`；
- `spc_dashboard.py:1088`：`render_spc_indicator_sections()` 读取配置并按指标决策；
- `src/shared_kernel/config.py:180`：`get_spc_line_chart_param_name_contains()`。

### 3.2 仅绘制上限，不绘制下限

该逻辑完全属于前端规格线展示，入口为 `spc_dashboard.py:655` 的 `_apply_measurement_spec_lines()`。

```text
先绘制 USL
  └─ LSL 为空或 LSL == 0
       ├─ 是：再绘制 UCL，然后返回
       └─ 否：继续绘制 LSL、UCL、LCL、Target、CL
```

因此，页面把 `LSL` 为空或等于 `0` 解释为“仅上限规格”。Sheet 点位图和月/周/天分布图共用该入口。

该规则只是显示策略，不会改写后端规格值。需要特别注意：CPK 后端仍会把数值 `LSL=0` 当作真实下限参与双边公式，见 4.3 节。

### 3.3 By 过货时间使用时间轴

`spc_dashboard.py:865-967` 对“按过货时间排序”的折线图使用实际 `sheet_start_time` 作为 X 轴：

- 先按 `sheet_start_time`、`sheet_id` 排序；
- X 值为 `sheet_start_time`，Y 值为 `param_value`；
- X 轴类型显式设为 Plotly `date`；
- 刻度格式为月日和时分；
- Sheet ID 通过 `customdata` 保留在悬浮提示中；
- 缺少有效过货时间的点不进入该折线。

只有“By 过货时间 + 折线图”使用连续时间轴。若当前参数配置为箱线图，仍按 Sheet 分组展示箱体，不改变箱线图原有分类轴语义。

### 3.4 增加单位

状态：尚未实现，本次不展开。

## 4. 后端处理

### 4.1 异常值剔除

实现入口为：

`src/inline_domain/infrastructure/measurement/measurement_preparation.py:235`

规则文件为：

`resources/inline_domain/spc_outlier_filters.xlsx`

它位于共享 measurement preparation 中，在参数排除、去重和数据类型归类之后执行，在日期及页面查询维度过滤之前完成。

处理流程：

```text
spc_outlier_filters.xlsx
  ├─ 主路径：Excel COM 解密读取
  │    └─ 输出 CSV 到 output/decrypted_files/spc_outlier_filters.csv
  └─ 降级路径：读取已有且表头有效的 CSV
       └─ 按产品 + step_id + param_name 匹配规则
            ├─ param_value <= lower_col → 删除
            └─ param_value >= upper_col → 删除
```

关键语义：

- `prod_col` 可使用 `ALL` 匹配全部产品；
- `step_id` 会兼容 Excel/CSV 数字产生的 `.0` 后缀；
- `param_name` 不区分大小写，但要求完整相等；
- 上下边界均为包含边界，即等于边界也会被剔除；
- 没有可用规则、规则结构无效或过滤执行异常时，保留原始数据并记录日志，属于 fail-open 策略；
- 剔除是物理删除测量行，不是图表隐藏，也不是规格值修饰。

### 4.2 参数豁免

配置位于 Inline 顶层，而不是 `spc` 节点：

```yaml
auto_decoration:
  exempt_param_name_contains:
    - "PPA"
```

当前调用关系：

```text
inline_config.yaml
  └─ ConfigLoader.get_auto_decoration_param_exemptions()
       ├─ AoiTtReportService
       │    └─ aoi_tt_decoration.py
       │         └─ apply_tri_state_decoration(parameter_col="tt_name")
       └─ AoiRsReportService
            └─ aoi_rs_decoration.py
                 └─ apply_tri_state_decoration(parameter_col="rs_code")
```

共享算法只实现于 `src/inline_domain/core/shared/auto_decoration.py`：

- `_parameter_exemption_mask()` 对指定参数列做不区分大小写的普通子串匹配；
- `apply_tri_state_decoration()` 统一处理 Delete、释放、豁免和自动截断；
- 处理优先级为：`Delete > 参数豁免 / flag=False > flag=True 自动截断`；
- 豁免表示保留真实测量值，不会阻止管理员通过 `Delete` 删除记录。

AOI TT/RS 文件不是重复实现豁免算法，而是必要的领域适配器：

| 适配器 | 参数列 | 数值/规格适配 | 业务键特点 |
|---|---|---|---|
| `core/aoi_tt/aoi_tt_decoration.py` | `tt_name` | `tt_qty` / USL | `(prod_code, step_id, tt_name, sheet_id)` |
| `core/aoi_rs/aoi_rs_decoration.py` | `rs_code` | By Lot、By Sheet 归一化为 `value/spec` | 键中包含 `chart_kind`、`point_id`，隔离 Lot/Sheet 决策 |

评估结论：当前策略正确。共享 Core 保证算法和优先级只有一个来源，TT/RS 适配器负责各自字段与业务键，Application 读取配置后显式注入 Core，依赖方向合理。

边界风险：同一 token 当前会同时作用于 `tt_name` 和 `rs_code` 两个命名空间。如果未来 TT 与 RS 对同一文本需要不同语义，应把配置拆分为 `aoi_tt`、`aoi_rs` 子列表，但不应在两个适配器中重新硬编码匹配逻辑。

这项逻辑属于 Inline Domain 的 AOI 特殊处理，不直接进入 `SPC监控报表.py` 的运行调用链。

### 4.3 CPK 计算方式

配置位于 `config/inline_config.yaml`：

```yaml
spc:
  spc_cpk:
    period_sigma_source: "point_value"
    period_box_source: "point_value"
```

两个配置项含义不同：

| 配置项 | 作用 | 是否参与 CPK |
|---|---|---|
| `period_sigma_source` | 决定周期能力计算的标准差来源 | 是 |
| `period_box_source` | 决定月/周/天箱线图的样本来源 | 否 |

`src/inline_domain/core/spc/spc_calculator.py:350` 的 `build_period_capability_report()` 始终以周期内 Sheet Mean 的均值作为 `mean_value`。只有 sigma 来源可切换：

- `sheet_mean`：周期内 Sheet Mean 的样本标准差，`ddof=1`；
- `point_value`：周期内全部有效点位 `param_value` 的样本标准差，`ddof=1`；若对应周期没有可用点位统计，则回退到 Sheet Mean sigma。

公式位于 `spc_calculator.py:57`：

```text
CPK = min(USL - mean_value, mean_value - LSL) / (3 × std_value)
```

边界处理：

- 任一输入缺失时返回 `NaN`；
- `USL <= LSL` 或标准差小于 0 时返回 `NaN`；
- 标准差为 0 时，根据最近规格距离返回正无穷、0 或负无穷；
- 当前为双边公式，`LSL=0` 仍会参与计算，不沿用前端“仅显示上限”的解释。

能力计算完成后，`src/inline_domain/core/spc/cpk_decoration.py` 仍允许通过 CPK 修饰工作簿显式覆盖最终 CPK；这是后置人工修饰，不改变基础计算公式。

### 4.4 `param_clip_rules` 已删除

`param_clip_rules` 规格调节链路已全部删除，当前运行时不存在以下行为：

- 从 `inline_config.yaml` 读取参数专用裁剪规则；
- 在 `ConfigLoader` 中加载该规则；
- 在 Sheet OOS 修饰前偏移或扩展 USL/LSL；
- 通过 `clip_rules` 参数把规则传入共享修饰函数。

当前 Sheet OOS 修饰只使用正式规格：`flag=True` 按正式规格自动截断，`flag=False` 保留真实值，`Delete` 删除对应测量行。

## 5. 配置归属

`config/inline_config.yaml` 当前相关配置可归纳为：

```yaml
spc:
  spc_cpk:
    period_sigma_source: "point_value" # 后端 CPK sigma 口径
    period_box_source: "point_value"   # 前端周期箱线图样本口径
  chart:
    line_param_name_contains:           # 前端 Sheet 点位图样式
      - "UNI"

auto_decoration:
  exempt_param_name_contains:           # Inline AOI TT/RS 后端共享豁免
    - "PPA"
```

配置虽然集中在一个文件中，但所有权仍按消费者划分：图表配置由前端消费，CPK sigma 配置由 SPC 后端消费，参数豁免配置由 AOI TT/RS Application 读取后注入共享 Core。

## 6. 关键源码索引

| 逻辑 | 主要文件 |
|---|---|
| 页面装配 | `app/pages/SPC监控报表.py` |
| 图表类型、规格线、时间轴 | `app/sections/inline_domain/spc/spc_dashboard.py` |
| 共用折线 trace | `app/components/distribution_charts.py` |
| Inline 配置读取 | `src/shared_kernel/config.py` |
| 异常值剔除 | `src/inline_domain/infrastructure/measurement/measurement_preparation.py` |
| 参数豁免共享算法 | `src/inline_domain/core/shared/auto_decoration.py` |
| AOI TT 参数豁免适配 | `src/inline_domain/core/aoi_tt/aoi_tt_decoration.py` |
| AOI RS 参数豁免适配 | `src/inline_domain/core/aoi_rs/aoi_rs_decoration.py` |
| CPK/CPM 计算 | `src/inline_domain/core/spc/spc_calculator.py` |
| CPK 人工修饰 | `src/inline_domain/core/spc/cpk_decoration.py` |

## 7. 最终结论

- 前端只管理展示语义：图表类型、规格线显示和时间轴。
- 后端只管理数据语义：异常值剔除、参数豁免、能力计算和人工修饰。
- 参数豁免的共享算法与 TT/RS 领域适配分层合理，不属于三份重复实现。
- `period_box_source` 是展示数据口径，`period_sigma_source` 才是 CPK sigma 口径，两者不可混用。
- 前端“LSL 为 0 时仅显示上限”与后端“LSL 为 0 时仍参与双边 CPK”是当前明确存在的口径差异。
- `param_clip_rules` 已退出系统；“增加单位”尚未实现。
