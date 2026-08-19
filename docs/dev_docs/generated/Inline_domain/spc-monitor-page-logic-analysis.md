# SPC 监控报表页面关键逻辑分析

> 分析对象：`app/pages/SPC监控报表.py`、`src/inline_domain/`、`src/shared_kernel/config.py`、`config/inline_config.yaml`  
> 核验日期：2026-08-19  
> 本文描述当前源码实际行为，不代表期望行为或改造方案。

## 1. 结论摘要

| 关注项 | 决策/实现位置 | 当前实际规则 |
|---|---|---|
| 折线图还是箱线图 | `src/inline_domain/application/spc/spc_service.py:44-60`；消费位置为 `app/sections/spc/spc_dashboard.py:562-570,841-940` | 参数名包含 `UNI`（不区分大小写、普通字符串匹配）时，两个 Sheet 点位分布图画折线；其余参数画箱线。月/周/天分布图不读取该规则，始终画箱线图。 |
| 仅绘制上限 | `app/sections/spc/spc_dashboard.py:652-675` | 始终先尝试画 USL；当 LSL 为空或等于 `0` 时，把规格视为仅上限展示，只再画 UCL，然后提前返回，不画 LSL、LCL、Target、CL。 |
| `param_clip_rules` 规格调节 | 配置读取：`src/shared_kernel/config.py:181-213`；调用：`src/inline_domain/application/shared/decorated_data.py:95-108`；执行：`src/inline_domain/core/shared/sheet_oos_decoration.py:135-163,316-361` | 规则只调节 Sheet OOS 人工修饰阶段使用的临时截断上下限，不修改官方规格，不改变最初的 OOS 识别，也不直接改变图中规格线。当前 YAML 只有注释示例，实际规则列表为空。 |
| `spc_cpk` CPK 方式 | 配置读取：`src/shared_kernel/config.py:153-178`；服务编排：`src/inline_domain/application/spc/spc_service.py:213-240`；计算：`src/inline_domain/core/spc/spc_calculator.py:57-74,298-347,350-462` | `period_sigma_source` 决定 CPK 的标准差来自 Sheet Mean 还是点位值；均值始终是周期内 Sheet Mean 的均值。`period_box_source` 只决定月/周/天箱线图的数据源，不参与 CPK。当前两项均为 `point_value`。 |

页面文件本身主要负责装配和调用：它在 `app/pages/SPC监控报表.py:94-101` 调用应用服务，在 `:142-149` 和 `:175-180` 把结果及箱线图数据源配置交给 dashboard。上述业务判断没有直接写在页面文件中。

## 2. 总体调用链

```text
app/pages/SPC监控报表.py
  ├─ ConfigLoader.get_spc_period_sigma_source()
  ├─ SpcReportService.get_spc_report_data()
  │    └─ fetch_spc_report_payload()
  │         ├─ fetch_decorated_features(scope="spc")
  │         │    └─ prepare_decorated_data()
  │         │         ├─ 读取 param_clip_rules
  │         │         ├─ Sheet OOS 三态修饰/截断
  │         │         └─ 用修饰后的点位重算 Sheet 特征
  │         ├─ assign_indicator_chart_type()
  │         ├─ build_period_capability_report()
  │         │    └─ calculate_cpk()
  │         └─ prepare_cpk_decoration()（可选人工覆盖计算结果）
  └─ app/sections/spc/spc_dashboard.py
       ├─ 月/周/天箱线图
       ├─ Sheet 点位折线图/箱线图
       └─ 规格线绘制
```

CPK 和图表消费的都是 Sheet OOS 修饰后的点位与重新计算的 Sheet 特征。服务在 `spc_service.py:226-234` 明确只把修饰后的数据传入周期能力计算。

## 3. 折线图与箱线图的选择

### 3.1 后端规则

规则定义在 `src/inline_domain/application/spc/spc_service.py:44-60` 的 `assign_indicator_chart_type()`：

```text
param_name 包含 "UNI"（case=False, regex=False） → chart_type = "line"
其他参数，包括空值                           → chart_type = "box"
```

服务分别在 `spc_service.py:208-209,241,250` 给原始点位、Sheet 特征、周期能力结果和指标列表附加 `chart_type`。所以图表类型是应用服务拥有的业务标记，不由 Streamlit 页面根据参数名再次推断。

### 3.2 前端消费

`app/sections/spc/spc_dashboard.py:562-570` 的 `_resolve_chart_type()` 从后端 DataFrame 读取首个有效 `chart_type`；只有值为 `line` 时返回折线，缺列、空表或其他值均安全回退为 `box`。

`_create_sheet_points_box_chart()` 在 `spc_dashboard.py:841-940` 消费这个结果：

- 按过货时间排序：`line` 创建一条 `lines+markers` 点位趋势；否则按 Sheet 创建箱线；
- 按主站点设备/腔室排序：`line` 按腔室创建多条点位趋势；否则按 Sheet 创建箱线并以腔室着色。

### 3.3 重要例外：月/周/天图始终是箱线图

`spc_dashboard.py:769-812` 的 `_create_period_overview_chart()` 无条件调用 `create_box_distribution_trace()`，不接收 `chart_type`。因此即使参数名包含 `UNI`：

- “月周天分布”仍是箱线图；
- “Sheet点位分布 By主站点设备/腔室”和“By过货时间”才切换为折线图。

这一边界由 `tests/unit/app/sections/spc/test_spc_dashboard.py:516-524,642-669,1153-1192` 覆盖；后端 `UNI` 分类由 `tests/unit/inline_domain/application/spc/test_spc_service.py:136-148` 覆盖。

## 4. 仅绘制上限、不绘制下限

统一规格线入口是 `app/sections/spc/spc_dashboard.py:652-675` 的 `_apply_measurement_spec_lines()`。执行顺序为：

1. 从当前指标数据中取第一条至少包含一个数值规格/控制限的记录；
2. 先尝试绘制 USL；
3. 把 LSL 转为数值；
4. 若 LSL 为 `NaN/None` 或严格等于 `0.0`，只再尝试绘制 UCL 并立即返回；
5. 只有 LSL 非空且非零时，才继续绘制 LSL、UCL、LCL、Target 和 CL。

所以“只画上限”不是 YAML 开关，也不在 `spc_service.py` 中，而是 dashboard 层根据规格数据的 LSL 值判断。`spc_dashboard.py:939-943` 和 `:811-815` 表明 Sheet 点位图与月/周/天图共用该入口。

相关测试位于 `tests/unit/app/sections/spc/test_spc_dashboard.py:1060-1088`，明确断言 `lsl=0` 时只存在 `USL`、`UCL` 两个标注；紧随其后的测试覆盖 LSL 为空的情况。

补充边界：`src/inline_domain/core/spc/spc_calculator.py:39-42` 同样将 `LSL=0` 注释为源系统的“仅上限规格”标记，但该判断目前只用于令 CPM 返回 `NaN`，并没有用于切换 CPK 公式，详见第 6.4 节。

## 5. `param_clip_rules` 规格调节

### 5.1 配置结构与当前状态

配置路径是：

```yaml
spc:
  sheet_oos_decoration:
    param_clip_rules:
      # - param_name_contains: "PPA"
      #   lower_offset: -0.5
      #   upper_offset: 0.5
```

当前 `config/inline_config.yaml:13-16` 的示例全部被注释，因此 `_load_yaml()` 得到的 `param_clip_rules` 为 `null`，`ConfigLoader.get_spc_sheet_oos_clip_rules()` 最终返回空列表，运行时没有参数专用规格偏移。

### 5.2 配置读取和校验

`src/shared_kernel/config.py:181-213` 负责读取并归一化规则：

- `param_clip_rules` 必须是列表；
- 每项必须是字典；
- `param_name_contains` 去空格后不得为空；
- `lower_offset`、`upper_offset` 必须可以转成浮点数，缺省为 `0.0`；
- 非法项被跳过；读取异常记录日志并回退为空列表。

### 5.3 规则如何进入数据管线

`src/inline_domain/application/shared/decorated_data.py:95-108` 在 Sheet OOS 修饰前读取规则并传给 `prepare_sheet_oos_decoration()`。处理完成后，再使用修饰后的点位和原始官方规格重新计算 Sheet 特征。因此该规则会间接影响后续图表点位、Sheet 统计和 CPK 输入，但不会回写数据库或 Parquet 快照。

### 5.4 匹配和偏移算法

`src/inline_domain/core/shared/sheet_oos_decoration.py:135-163` 的 `_apply_clip_rules()`：

```text
effective_lsl = official_lsl + lower_offset
effective_usl = official_usl + upper_offset
```

匹配规则如下：

- 对 `param_name` 做不区分大小写的普通子串匹配，不使用正则；
- 按配置顺序执行；
- `matched` 掩码保证同一行只使用第一个命中的规则；
- 在复制出的临时规格表上偏移，不修改上游官方 `lsl/usl`。

### 5.5 生效范围

规则不是全局“规格覆盖”，只在 `sheet_oos_decoration.py:316-361` 的人工 Sheet OOS 修饰阶段生效：

1. 系统先用官方规格生成 OOS 明细；
2. `Delete` 的 Sheet 被删除；
3. 仅对 `flag=True` 的活动修饰行建立临时有效规格；
4. 点位超过临时 USL/LSL 时，才按临时规格跨度截回线内；
5. `flag=False` 保留真实点位，规则对其不起作用。

实际截断位置由 `_clip_inside_spec()`（`sheet_oos_decoration.py:109-132`）确定：落在有效规格跨度向内 5%～15% 的确定性位置。若任一上下限缺失或 `usl <= lsl`，不执行截断。

因此，正的 `upper_offset` 会放宽修饰时的上截断边界，负的 `lower_offset` 会放宽下截断边界；它们既不改变最初“是否 OOS”的判断，也不改变 dashboard 绘制的官方 USL/LSL。偏移后的点位仍可能位于官方规格之外，这是当前设计允许的结果。

## 6. `spc_cpk` 与 CPK 计算方式

### 6.1 两个配置项的职责

当前配置为：

```yaml
spc:
  spc_cpk:
    period_sigma_source: "point_value"
    period_box_source: "point_value"
```

| 配置项 | 允许值 | 用途 |
|---|---|---|
| `period_sigma_source` | `sheet_mean`、`point_value` | 决定周期 CPK/CPM 的标准差 `std_value` 来源。 |
| `period_box_source` | `sheet_mean`、`point_value` | 决定月/周/天箱线图每个箱体使用 Sheet Mean 还是所有点位值；不参与能力指数计算。 |

`ConfigLoader.get_spc_period_sigma_source()` 位于 `src/shared_kernel/config.py:153-165`。它读取字符串并转小写，但不在配置层校验枚举；后续 `normalize_period_sigma_source()`（`spc_calculator.py:342-347`）把未知值回退为 `sheet_mean`。

`ConfigLoader.get_spc_period_box_source()` 位于 `config.py:168-178`，在配置层直接把未知值回退为 `point_value`。页面在 `SPC监控报表.py:147,179` 把它传给图表构造函数，dashboard 在 `spc_dashboard.py:781-789` 决定箱线样本来源。

### 6.2 周期聚合口径

`SpcReportService` 在 `spc_service.py:213-234`：

- 先排除参数名包含 `PPA` 的记录，使其不参与 CPM/CPK，但仍可进入图表；
- 根据最新有效 Sheet 和查询结束日确定能力窗口结束日；
- 把修饰后的 Sheet 特征传入 `build_period_capability_report()`；
- 只有 `period_sigma_source=point_value` 时才额外传入修饰后的原始点位。

`build_period_capability_report()` 按产品、厂别、站点、参数以及月/周/天周期分组。无论 sigma 选择为何，以下口径固定：

```text
mean_value  = 周期内各 Sheet Mean 的算术平均值
sample_count = 周期内唯一 Sheet 数
USL/LSL/控制限/Target = 分组中的第一条值
```

源码位置为 `src/inline_domain/core/spc/spc_calculator.py:350-414`。

### 6.3 标准差来源

当 `period_sigma_source=sheet_mean`：

```text
std_value = 周期内 Sheet Mean 的样本标准差，ddof=1
```

当 `period_sigma_source=point_value`：

```text
std_value = 周期内所有有效 param_value 的样本标准差，ddof=1
point_count = 有效点位数
```

点位统计实现在 `spc_calculator.py:298-339`。若某个周期找不到对应的点位统计，`spc_calculator.py:416-442` 会对该周期回退到 Sheet Mean 标准差，并把输出列 `sigma_source` 标记为实际使用的来源。

当前 YAML 为 `point_value`，所以正常情况下：均值使用 Sheet Mean 的均值，sigma 使用所有点位的样本标准差。这是一个“均值与 sigma 使用不同粒度”的混合口径。

### 6.4 CPK 公式和边界

`src/inline_domain/core/spc/spc_calculator.py:57-74` 的公式是：

```text
Cpu = (USL - mean_value) / (3 × std_value)
Cpl = (mean_value - LSL) / (3 × std_value)
CPK = min(Cpu, Cpl)
```

等价源码写法为：

```text
CPK = min(USL - mean_value, mean_value - LSL) / (3 × std_value)
```

边界行为：

- 任一输入为 `NaN`：返回 `NaN`；而聚合前又要求 `sheet_mean/usl/lsl` 均非空，因此 LSL 真正缺失的记录不会进入能力结果；
- `USL <= LSL` 或标准差小于 0：返回 `NaN`；
- 标准差为 0：均值在线内返回 `+inf`，恰在线上返回 `0`，越规返回 `-inf`；
- 均值越过任一规格线时，最近规格距离为负，CPK 为负数。

需要特别注意：虽然 dashboard 和 CPM 把 `LSL=0` 识别为“仅上限规格”，`calculate_cpk()` 当前没有单边分支。它仍把 `0` 当作真实下限参与 `min(Cpu, Cpl)`。因此当前仅上限参数的 CPK 不是单独的 `Cpu=(USL-mean)/(3σ)`，而仍是上述双边最近距离公式。这是当前实现的口径差异，不是本文推测。

### 6.5 CPK 计算后的人工覆盖

服务在计算完周期 CPK 后调用 `prepare_cpk_decoration()`（`spc_service.py:235-240`）。`src/inline_domain/core/spc/cpk_decoration.py:204-250` 会读取 `resources/spc_cpk_decoration.xlsx`：

- 默认保留计算得到的 CPK；
- 只有管理员把对应周期记录的 `flag` 显式设为 True，且 `cpk_corrected` 是有效数值时，才用人工值覆盖 `cpk`；
- 覆盖发生在公式计算之后，与 `inline_config.yaml` 的 `spc_cpk` 取样口径是两层独立机制。

页面的 CPK 指标、表格和预警消费的是这一步之后的最终 `cpk`。

## 7. 配置修改的实际影响矩阵

| 修改 | Sheet 点位图类型 | 月周天箱线样本 | 图中规格线 | OOS 初判 | True 修饰后的点位 | CPK 均值 | CPK sigma | 最终 CPK |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 参数名包含/不包含 `UNI` | 是 | 否 | 否 | 否 | 否 | 否 | 否 | 否 |
| LSL 改为 `0`/空 | 否 | 否 | 是，仅画上限 | 是 | 是 | 是 | 是 | 是，且 `0` 仍参与当前 CPK 双边公式 |
| 修改 `param_clip_rules` | 否 | 间接 | 否 | 否 | 是，仅 `flag=True` | 间接 | 间接 | 间接 |
| 修改 `period_sigma_source` | 否 | 否 | 否 | 否 | 否 | 否 | 是 | 是 |
| 修改 `period_box_source` | 否 | 是 | 否 | 否 | 否 | 否 | 否 | 否 |
| CPK 修饰表 `flag=True` | 否 | 否 | 否 | 否 | 否 | 否 | 否 | 是，公式计算后覆盖 |

## 8. 关键测试索引

- 图表类型标记：`tests/unit/inline_domain/application/spc/test_spc_service.py:136-148`
- 月周天始终箱线、Sheet 图使用后端类型：`tests/unit/app/sections/spc/test_spc_dashboard.py:516-524,642-669,1153-1192`
- 仅上限画线：`tests/unit/app/sections/spc/test_spc_dashboard.py:1060-1126`
- `param_clip_rules` 读取：`tests/unit/test_spc_config.py:6-31`
- 参数专用截断边界：`tests/unit/inline_domain/core/shared/test_sheet_oos_decoration.py:127-145`
- CPK 最近规格距离公式：`tests/unit/inline_domain/core/spc/test_spc_calculator.py:44-49`
- `LSL=0` 的 CPM 单边标记：`tests/unit/inline_domain/core/spc/test_spc_calculator.py:38-41`
