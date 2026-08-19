# SPC 监控报表页面关键逻辑分析与修正结果

> 分析对象：`app/pages/SPC监控报表.py`、`app/sections/inline_domain/spc/spc_dashboard.py`、`src/inline_domain/`、`src/shared_kernel/config.py`、`config/inline_config.yaml`
> 修正日期：2026-08-19
> 本文描述本次修正后的源码行为。

## 1. 修正结论

| 事项 | 修正结果 | 当前所有者 |
|---|---|---|
| 折线图/箱线图选择 | 已从 `spc_service.py` 移除；由 dashboard 根据 `inline_config.yaml` 决定 | 前端 `spc_dashboard.py` |
| 图表类型配置 | 新增 `spc.chart.line_param_name_contains`，当前配置为 `UNI` | `config/inline_config.yaml` |
| `param_clip_rules` | 配置、加载器、调用参数、核心偏移函数及专项测试均已删除 | 不再存在 |
| 参数豁免 | 保留共享算法及 AOI TT/RS 适配；配置从 `spc` 子节点提升为 Inline 顶层配置 | 共享 Core + 领域适配器 |
| 仅上限绘制 | 逻辑不变，仍由前端根据 LSL 是否为空或为 0 判断 | 前端 `spc_dashboard.py` |
| CPK 计算 | 本次未改变 | SPC Core/Application |

页面入口只负责装配：`app/pages/SPC监控报表.py:94-101` 获取报表数据，`:142-149`、`:175-180` 把数据交给 dashboard。图表样式、规格线绘制均由重构后的 `app/sections/inline_domain/spc/spc_dashboard.py` 管理。

## 2. 修正后的调用关系

```text
config/inline_config.yaml
  ├─ spc.chart.line_param_name_contains
  │    └─ ConfigLoader.get_spc_line_chart_param_name_contains()
  │         └─ spc_dashboard.py::_resolve_chart_type()
  │              └─ Sheet 点位折线图 / 箱线图
  │
  ├─ spc.spc_cpk
  │    ├─ period_sigma_source → CPK/CPM sigma 口径
  │    └─ period_box_source   → 月周天箱线图样本口径
  │
  └─ auto_decoration.exempt_param_name_contains
       └─ ConfigLoader.get_auto_decoration_param_exemptions()
            ├─ AOI_TT application → aoi_tt_decoration
            └─ AOI_RS application → aoi_rs_decoration
                    └─ shared/auto_decoration.py（唯一豁免算法）
```

`SpcReportService` 现在只返回测量、Sheet 特征、能力指数和指标元数据，不再向任何 DataFrame 添加 `chart_type`。

## 3. 折线图还是箱线图

### 3.1 配置

配置位于 `config/inline_config.yaml`：

```yaml
spc:
  chart:
    line_param_name_contains:
      - "UNI"
```

`src/shared_kernel/config.py:181-198` 的 `get_spc_line_chart_param_name_contains()` 读取并归一化列表：去除首尾空白，忽略空字符串和 `null`；配置缺失、类型错误或读取失败时返回空列表。

配置为空意味着所有 Sheet 点位图安全回退为箱线图。程序中不再保留 `UNI` 硬编码。

### 3.2 前端决策

`app/sections/inline_domain/spc/spc_dashboard.py:563-574` 的 `_resolve_chart_type()` 接收当前 `param_name` 和配置列表：

```text
参数名包含任一配置值（不区分大小写、普通文本匹配） → line
否则                                             → box
```

`render_spc_indicator_sections()` 在 `spc_dashboard.py:1100` 每次渲染只读取一次配置，并在遍历指标时按当前 `param_name` 决策。配置签名也被加入自动预警图表的 memo key（`:1133-1144`），因此配置变化后不会继续复用旧样式图表。

### 3.3 后端已移除的职责

`src/inline_domain/application/spc/spc_service.py` 已删除：

- `INDICATOR_CHART_TYPE_*` 常量；
- `assign_indicator_chart_type()`；
- 对原始点位、Sheet 特征、周期能力和指标列表写入 `chart_type` 的四处调用。

这使 Application 返回纯业务数据，图像样式统一留在前端。

### 3.4 哪些图会切换

图表类型配置只作用于两张 Sheet 点位分布图：

- By 主站点设备/腔室；
- By 过货时间。

`spc_dashboard.py:844-958` 根据前端算出的 `chart_type` 创建点位折线或 Sheet 箱线。

月/周/天分布图仍始终是箱线图。`_create_period_overview_chart()` 在 `spc_dashboard.py:772-815` 无条件使用 `create_box_distribution_trace()`；`period_box_source` 只决定箱体取 Sheet Mean 还是点位值，不决定图形种类。

## 4. 仅绘制上限、不绘制下限

该逻辑仍完全属于前端。`spc_dashboard.py:655-678` 的 `_apply_measurement_spec_lines()`：

1. 先尝试绘制 USL；
2. 将 LSL 转为数值；
3. LSL 为空或等于 `0.0` 时，只再绘制 UCL 并立即返回；
4. 只有 LSL 非空且非零时，才继续绘制 LSL、UCL、LCL、Target 和 CL。

因此，`LSL=0/空` 是当前页面的“仅上限规格”展示标记。Sheet 点位图和月/周/天箱线图共用该规格线入口。

## 5. `param_clip_rules` 已删除

本次删除了整条规格偏移链路：

- `config/inline_config.yaml` 中的 `spc.sheet_oos_decoration.param_clip_rules`；
- `ConfigLoader.get_spc_sheet_oos_clip_rules()`；
- `application/shared/decorated_data.py` 读取和传递 `clip_rules` 的逻辑；
- `core/shared/sheet_oos_decoration.py::_apply_clip_rules()`；
- `apply_sheet_oos_decoration()`、`prepare_sheet_oos_decoration()` 的 `clip_rules` 参数；
- 配置读取、应用管线和扩展截断边界的专项测试。

修饰引擎现在只使用数据源提供的正式 USL/LSL。`flag=True` 仍会把越规值截回正式规格区间内，`False` 保留真实值，`Delete` 删除对应点位，但不存在参数专用规格偏移。

## 6. 参数豁免策略分析

### 6.1 修正后的配置作用域

配置已从错误的 SPC 子作用域：

```yaml
spc:
  auto_decoration:
```

提升为 Inline 共享作用域：

```yaml
auto_decoration:
  exempt_param_name_contains:
    - "PPA"
```

读取位置是 `src/shared_kernel/config.py:200-220`。这与实际消费者一致：该策略目前服务于 AOI TT 与 AOI RS，并不是 SPC 专属配置。

### 6.2 是否存在三份重复逻辑

不存在。三个文件的职责不同：

| 文件 | 职责 |
|---|---|
| `core/shared/auto_decoration.py` | 唯一的参数名匹配、豁免掩码、自动截断和三态优先级算法 |
| `core/aoi_tt/aoi_tt_decoration.py` | 把 TT 的 `tt_name`、`tt_qty`、USL 和 TT 工作簿键映射给共享算法 |
| `core/aoi_rs/aoi_rs_decoration.py` | 分别把 RS By Lot/By Sheet 的 `rs_code`、值、规格和 RS 工作簿键映射给共享算法 |

`aoi_tt_decoration.py`、`aoi_rs_decoration.py` 没有再次实现 `str.contains()` 豁免算法；它们只是通过 `parameter_col` 和 `exempt_param_name_contains` 参数调用 `apply_tri_state_decoration()`。

### 6.3 共享算法的实际语义

`src/inline_domain/core/shared/auto_decoration.py:37-58` 的 `_parameter_exemption_mask()`：

- 对调用方指定的参数列匹配；TT 使用 `tt_name`，RS 使用 `rs_code`；
- 配置值去空白；
- 不区分大小写；
- 按普通子串匹配，不使用正则；
- 任一 token 命中即豁免自动截断。

`apply_tri_state_decoration()`（`auto_decoration.py:207-268`）的优先级为：

```text
Delete > 参数豁免 / flag=False > flag=True 自动截断
```

也就是说，豁免只代表“保留真实测量值”，不能阻止管理员用 `Delete` 删除记录。

### 6.4 AOI TT 适配

`src/inline_domain/core/aoi_tt/aoi_tt_decoration.py:83-143`：

- 规格按 `(step_id, tt_name)` 匹配 USL；
- 工作簿键为 `(prod_code, step_id, tt_name, sheet_id)`；
- `parameter_col="tt_name"`；
- 豁免参数保留真实 `tt_qty`；非豁免且 `flag=True` 的越规值执行单边上限截断。

应用服务在 `src/inline_domain/application/aoi_tt/aoi_tt_service.py:90-98` 读取一次共享配置并显式注入。这是正确的依赖方向：Core 不主动访问配置文件。

### 6.5 AOI RS 适配

`src/inline_domain/core/aoi_rs/aoi_rs_decoration.py:110-142,145-205`：

- By Lot 与 By Sheet 使用不同规格和数值列；
- 统一归一化为 `value/spec` 后调用共享三态算法；
- `parameter_col="rs_code"`；
- 工作簿键额外包含 `chart_kind` 与 `point_id`，避免 Lot/Sheet 决策互相污染。

应用服务在 `src/inline_domain/application/aoi_rs/aoi_rs_service.py:77-84` 注入共享豁免配置。

### 6.6 正确性评估

当前结构正确，理由如下：

1. 豁免算法只有一个来源，避免 TT/RS 漂移；
2. TT/RS 保留领域适配器是必要的，因为参数列、值列、规格键和工作簿键不同；
3. 配置由 Application 读取后注入 Core，Core 不依赖 `ConfigLoader`；
4. `Delete` 优先级在共享函数中统一保证；
5. 配置已提升到与消费者一致的 Inline 共享作用域。

需要保留的业务边界是：当前 token 会同时应用于 `tt_name` 和 `rs_code` 两个不同命名空间。如果未来同一文本在 TT 与 RS 中需要不同语义，应把配置扩展为按模块分组，例如 `aoi_tt`、`aoi_rs` 各自列表；不应在两个领域文件中重新硬编码。目前配置只有 `PPA`，且需求表达为 Inline 共享“参数豁免”，继续统一应用是合理的。

## 7. CPK 计算方式（未改变）

`config/inline_config.yaml` 当前配置：

```yaml
spc:
  spc_cpk:
    period_sigma_source: "point_value"
    period_box_source: "point_value"
```

- `period_sigma_source` 决定 CPK/CPM 的标准差来源；
- `period_box_source` 只决定月/周/天箱线图样本来源，不参与 CPK。

`src/inline_domain/core/spc/spc_calculator.py:350-462` 始终以周期内 Sheet Mean 的均值作为 `mean_value`：

- `sheet_mean`：sigma 为周期内 Sheet Mean 的样本标准差，`ddof=1`；
- `point_value`：sigma 为周期内全部有效点位值的样本标准差，`ddof=1`；点位统计缺失时按周期回退到 Sheet Mean sigma。

CPK 公式位于 `spc_calculator.py:57-74`：

```text
CPK = min(USL - mean_value, mean_value - LSL) / (3 × std_value)
```

仍需注意：页面把 `LSL=0` 当作仅上限展示标记，但 `calculate_cpk()` 仍将 0 当作真实下限参与双边公式；本次没有修改这一既有口径。

计算完成后，`core/spc/cpk_decoration.py:234-256` 仍允许管理员通过 CPK 修饰工作簿显式覆盖最终 CPK。

## 8. 验证结果

聚焦回归覆盖：

- 新图表配置读取和空值归一化；
- dashboard 按前端配置选择折线；
- SPC service payload 不再包含 `chart_type`；
- Sheet OOS 修饰在删除规格偏移后仍保持 True/False/Delete 语义；
- 共享参数豁免及 Delete 优先级；
- AOI TT/RS Core 与 Application 集成。

执行结果：`105 passed`。另有 6 条既有 Pandas `FutureWarning`，均来自 AOI RS 数值类型转换/赋值，不是本次变更引入的测试失败。
