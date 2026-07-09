# SPC 箱线图绘制逻辑全流程

生成日期：2026-07-07

本文档描述 `CPM/CPK监控报表` 当前 SPC 箱线图的数据读取、字段使用、特征加工、能力值计算与前端绘制流程。对应页面入口为 `app/pages/CPM监控报表.py`，核心展示逻辑在 `app/sections/spc_cpm_dashboard.py`。

## 1. 页面入口

页面文件：`app/pages/CPM监控报表.py`

页面启动后执行以下流程：

1. 读取当前产品型号：
   - `SessionManager.get_active_config()`
   - `active_config.data_source.product_code`
2. 构造 `SpcQueryConfig`：
   - `prod_code`: 当前产品型号
   - `start_date`: `get_default_cpm_start_date(default_end_dt.date())`
   - `end_date`: `SpcAnalysisService.get_time_window()` 返回的结束日期
   - `data_type_filter`: 初始写为 `"SPC"`
3. 通过页面 Header 注册：
   - `刷新数据`: 调用 `SpcAnalysisService.safe_refresh_snapshots(...)`，刷新底层 Parquet 快照。
   - `刷新缓存`: 由 Header 清理 Streamlit cache。
4. 调用 `CpmReportService.get_cpm_report_data(...)` 加载并计算报表数据。
5. 从返回的 `CpmReportViewModel` 中取出：
   - `period_capability_df`
   - `sheet_features_df`
   - `raw_measurements_df`
   - `indicators_df`
6. 渲染筛选器，点击“查询”后，对三张 DataFrame 统一执行前端筛选。
7. 调用 `render_cpm_indicator_sections(...)` 分指标渲染箱线图。

## 2. 服务层缓存与数据模型

服务文件：`src/spc_domain/application/cpm_service.py`

入口函数：

```python
CpmReportService.get_cpm_report_data(
    _db_manager,
    query_config_json,
    snapshot_signature,
)
```

该函数使用：

```python
@st.cache_data(show_spinner=False, max_entries=1)
```

因此同一 Streamlit 进程内会缓存服务层结果。页面 Header 的“刷新缓存”会清理该缓存。

服务层强制覆盖查询类型：

```python
query_config.data_type_filter = "SPC"
```

也就是说 CPM/CPK 报表只取白名单分类后的 SPC 参数，不取 CTQ/AOI/报废。

返回模型：

| 字段 | 来源 | 用途 |
|---|---|---|
| `period_capability_df` | `build_period_capability_report(...)` | 月/周/日 CPM/CPK 折线 |
| `sheet_features_df` | `preprocess_sheet_features(...)` | 月/周/日 Sheet Mean 箱线图、规格线、指标列表 |
| `raw_measurements_df` | `SpcRepository.get_spc_measurements(...)` | Sheet 点位箱线图 |
| `indicators_df` | `sheet_features_df[["prod_code", "factory", "step_id", "param_name"]]` 去重 | 页面筛选器联动 |

## 3. 底层数据读取

读取仓储：`src/spc_domain/infrastructure/repositories/spc_repository.py`

DAO：`src/spc_domain/infrastructure/data_loader.py`

### 3.1 量测明细表

读取函数：

```python
load_spc_measurements(db_manager, start_str, end_str, prod_code)
```

底层物理表：

| 厂别 | 物理表 | 原始 ID 字段 | 原始时间字段 | 统一后的 ID 字段 | 统一后的时间字段 |
|---|---|---|---|---|---|
| ARRAY | `eda.spc_tzbjx_array` | `sheet_id` | `sheet_start_time` | `sheet_id` | `sheet_start_time` |
| OLED | `eda.spc_tzbjx_oled` | `glass_id` | `glass_start_time` | `sheet_id` | `sheet_start_time` |
| TP | `eda.spc_tzbjx_tsp` | `glass_id` | `glass_start_time` | `sheet_id` | `sheet_start_time` |

同时 JOIN：

```sql
JOIN DWR_MES_PRODUCTSPEC P ON T.product_spec = P.PRODUCTSPECNAME
```

用于将底层产品规格名转换为产品代码：

```sql
P.PRODUCTCODE AS prod_code
```

量测 SQL 返回字段：

| 输出字段 | 来源 | 后续用途 |
|---|---|---|
| `factory` | SQL 中按分表写死：`ARRAY`/`OLED`/`TP` | 筛选、分组、指标标题 |
| `prod_code` | `DWR_MES_PRODUCTSPEC.PRODUCTCODE` | 产品过滤、规格绑定、分组 |
| `sheet_start_time` | ARRAY: `sheet_start_time`; OLED/TP: `glass_start_time` | 时间过滤、去重排序、周期归属、横轴排序 |
| `sheet_id` | ARRAY: `sheet_id`; OLED/TP: `glass_id` | Sheet 级聚合、点位图按 Sheet 分组 |
| `step_id` | `T.step_id` | 站点筛选、规格绑定、指标分组 |
| `param_name` | `T.param_name` | 参数筛选、规格绑定、指标分组 |
| `site_name` | `T.site_name` | 点位去重、腔室/点位箱线图默认分组 |
| `param_value` | `T.param_value` | 原始点位箱线图 y 值、Sheet 特征计算 |

DAO 层会做：

1. SQL 窗口函数去重：
   - 分组键：`P.PRODUCTCODE, T.id_col, T.step_id, T.param_name, T.site_name`
   - 排序：`T.time_col DESC`
   - 保留 `rn = 1`
2. `param_value` 转数值：
   - `pd.to_numeric(..., errors="coerce")`
3. 删除 `param_value` 为空的记录：
   - `dropna(subset=["param_value"])`

### 3.2 规格线表

读取函数：

```python
load_spc_spec_limits(db_manager, prod_code)
```

底层表：

```sql
dwd_imp_dv_param_spec
```

SQL 返回字段：

| 输出字段 | 来源 | 后续用途 |
|---|---|---|
| `prod_code` | `dwd_imp_dv_param_spec.prod_code` | 规格绑定 join key |
| `step_id` | `dwd_imp_dv_param_spec.step_id` | 规格绑定 join key |
| `param_name` | `dwd_imp_dv_param_spec.param_name` | 规格绑定 join key |
| `usl` | `dwd_imp_dv_param_spec.usl` | 规格红线、CPM/CPK 计算 |
| `lsl` | `dwd_imp_dv_param_spec.lsl` | 规格红线、CPM/CPK 计算 |
| `ucl` | `dwd_imp_dv_param_spec.ucl` | 管控绿线 |
| `lcl` | `dwd_imp_dv_param_spec.lcl` | 管控绿线 |

规格字段会执行数值转换：

```python
for col in ["usl", "lsl", "ucl", "lcl"]:
    spec_df[col] = pd.to_numeric(spec_df[col], errors="coerce")
```

仓储层随后会读取产品 YAML 中的 `spc_spec_override`，可覆盖：

```python
["ucl", "lcl", "usl", "lsl", "target"]
```

因此 `target` 不是 DAO 原始 SQL 固定返回字段，但可能通过 YAML 覆盖注入。若没有 `target`，能力计算中会使用 `(usl + lsl) / 2` 作为目标值。

### 3.3 参数白名单表

读取函数：

```python
load_param_whitelist(db_manager, prod_code)
```

底层表：

```sql
eda.IMP_SPC_TZBJX
DWR_MES_PRODUCTSPEC
```

SQL 返回字段：

| 输出字段 | 来源 | 后续用途 |
|---|---|---|
| `ref_param_name` | `IMP_SPC_TZBJX.parmtername` | 与 `param_name_upper` 合并，过滤有效参数 |
| `data_type` | `IMP_SPC_TZBJX.data_type` | 经 `classify_param_type` 归类后筛选 SPC |

仓储层处理：

1. `ref_param_name` 转大写并去空格。
2. `data_type` 经 `classify_param_type(...)` 映射为标准类型。
3. 因 CPM 页面服务层强制 `data_type_filter = "SPC"`，仓储层只保留 `data_type == "SPC"` 的白名单参数。
4. 与量测明细合并：

```python
df_filtered["param_name_upper"] = df_filtered["param_name"].str.upper()
df_filtered.merge(
    raw_whitelist,
    left_on="param_name_upper",
    right_on="ref_param_name",
    how="inner",
)
```

合并完成后，量测明细会新增标准化后的 `data_type` 字段。

### 3.4 Parquet 快照与仓储层过滤

仓储快照路径：

```text
data/{prod_code}/spc_snapshot_{prod_code}.parquet
```

快照 TTL：

```python
SNAPSHOT_TTL_HOURS = 8
```

仓储层读取逻辑：

1. 若快照存在、未超 TTL、且最大 `sheet_start_time >= req_end_dt`，命中快照。
2. 否则从数据库全量拉取。
3. 数据库拉取失败时，若本地旧快照存在，则容灾回退。
4. 写入快照前会按 3 个月滚动窗口保留数据。

注意：仓储层内部会按 `req_end_dt - relativedelta(months=3)` 构造 3 个月滚动数据范围；服务层随后还会按页面 `query_config.start_date/end_date` 再过滤一次。

仓储层最终还会执行：

1. 物理异常点过滤：`_apply_outlier_filters(...)`
   - 规则来源：`resources/spc_outlier_filters.xlsx`
   - CSV 降级路径：`resources/xlsx_to_csv/spc_outlier_filters.csv`
   - 规则字段包括 `prod_col`, `step_col`, `param_col`, `lower_col`, `upper_col`
   - 对 `param_value` 做上下边界剔除
2. 可选维度过滤：
   - `factory`
   - `step_id`
   - `param_name`

## 4. Sheet 级特征加工

函数：`src/spc_domain/core/spc_calculator.py::preprocess_sheet_features`

输入：

```python
measure_df = raw_measurements_df
spec_df = spec_df
```

默认去重键：

```python
filter_keys = [
    "factory",
    "prod_code",
    "sheet_id",
    "step_id",
    "param_name",
    "site_name",
]
```

默认 Sheet 聚合键：

```python
group_keys = [
    "factory",
    "prod_code",
    "sheet_id",
    "step_id",
    "param_name",
]
```

处理步骤：

1. 按 `sheet_start_time` 升序排序。
2. 按 `filter_keys` 去重，保留每个点位最新记录。
3. 按 `group_keys` 聚合为 Sheet 级特征：

| 输出字段 | 计算方式 | 用途 |
|---|---|---|
| `sheet_mean` | 同一 Sheet/站点/参数下 `param_value` 均值 | 月/周/日箱线图 y 值、CPM/CPK 计算 |
| `sheet_max` | `param_value` 最大值 | 兼容 SPC 规则链路 |
| `sheet_min` | `param_value` 最小值 | 兼容 SPC 规则链路 |
| `sheet_start_time` | 原始点位最早时间 | 周期归属、图表窗口 |
| `data_type` | 若原始数据存在则取 first | 后续筛选/追踪 |

4. 将 Sheet 特征与规格线表左连接：

```python
join_keys = ["prod_code", "step_id", "param_name"]
```

连接后 `sheet_features_df` 主要包含：

| 字段 | 用途 |
|---|---|
| `factory` | 指标分组、筛选 |
| `prod_code` | 指标列表、规格绑定结果 |
| `sheet_id` | Sheet 数统计、CPM/CPK sample_count |
| `step_id` | 指标分组、筛选 |
| `param_name` | 指标分组、筛选 |
| `sheet_mean` | 图一箱线图 y 值、能力值计算 |
| `sheet_max` | Sheet 极值 |
| `sheet_min` | Sheet 极值 |
| `sheet_start_time` | 周期归属 |
| `data_type` | 参数类型 |
| `usl`, `lsl`, `ucl`, `lcl` | 图表规格线与能力值计算 |
| `target` | 可选，来自 YAML 规格覆盖 |

## 5. CPM/CPK 周期能力数据

函数：`src/spc_domain/core/cpm_calculator.py::build_period_capability_report`

服务层先确定能力计算结束日：

```python
resolve_period_capability_end_date(sheet_features_df, query_config.end_date)
```

规则：

1. 如果 `sheet_features_df["sheet_start_time"]` 有最新可用日期，则取：
   - `min(最新 Sheet 日期, query_end_date)`
2. 否则取 `query_end_date`

这样做是为了避免查询截止日之后没有数据时，周/日能力窗口落在空日期上。

### 5.1 固定周期轴

函数：

```python
build_period_axis(end_date)
```

固定生成：

| 类型 | 数量 | 标签格式 |
|---|---:|---|
| 月 | 2 | `YYYY-MM` |
| 周 | 3 | `YYYY-Www` |
| 日 | 7 | `YYYY-MM-DD` |

排序值：

| 类型 | `period_sort` 范围 |
|---|---|
| 月 | `101`, `102` |
| 周 | `201`, `202`, `203` |
| 日 | `301` 到 `307` |

### 5.2 Sheet 特征展开为月/周/日周期行

函数：

```python
_period_frame(sheet_features, end_date)
```

同一条 Sheet 特征会按时间分别映射到：

1. 月周期：
   - `period_type = "month"`
   - `period_label = sheet_start_time.strftime("%Y-%m")`
2. 周周期：
   - `period_type = "week"`
   - `period_label = ISO year + ISO week`
3. 日周期：
   - `period_type = "day"`
   - `period_label = sheet_start_time.strftime("%Y-%m-%d")`

只保留落在固定周期轴内的记录。

### 5.3 能力值聚合

分组键：

```python
[
    "prod_code",
    "factory",
    "step_id",
    "param_name",
    "period_type",
    "period_label",
    "period_sort",
]
```

每组计算：

| 输出字段 | 计算方式 |
|---|---|
| `sample_count` | `sheet_id.nunique()` |
| `mean_value` | `sheet_mean.mean()` |
| `std_value` | `sheet_mean.std(ddof=1)` |
| `usl` | 本组第一个非空 `usl` |
| `lsl` | 本组第一个非空 `lsl` |
| `ucl` | 本组第一个非空 `ucl`，否则 NaN |
| `lcl` | 本组第一个非空 `lcl`，否则 NaN |
| `target` | 本组第一个非空 `target`，否则 `(usl + lsl) / 2` |
| `cpm` | `calculate_cpm(mean_value, std_value, usl, lsl, target)` |
| `cpk` | `calculate_cpk(mean_value, std_value, usl, lsl)` |

CPM 公式：

```text
CPM = (USL - LSL) / (6 * sqrt(std_value^2 + (mean_value - target)^2))
```

CPK 公式：

```text
CPK = min(USL - mean_value, mean_value - LSL) / (3 * std_value)
```

注意：

- `std_value` 使用样本标准差 `ddof=1`。
- 如果某个周期只有 1 个 Sheet，`std_value` 为 NaN，因此 CPM/CPK 也会是 NaN。
- 前端折线会过滤 NaN 能力值，所以这种周期不会显示折线点。

## 6. 前端筛选逻辑

文件：`app/sections/spc_cpm_dashboard.py`

筛选器函数：

```python
render_cpm_filters(indicator_df)
```

控件：

| 控件 | 字段来源 | 行为 |
|---|---|---|
| 指标 | 固定 `["CPM", "CPK"]` | 决定右轴折线指标 |
| 厂别 | `indicator_df["factory"]` | 单选 |
| 站点 | 根据厂别筛选 `indicator_df["step_id"]` | 多选 |
| 参数名称 | 根据厂别+站点筛选 `indicator_df["param_name"]` | 多选，站点变化后自动全选 |
| 查询 | 当前厂别、站点、参数完整时启用 | 点击后才渲染图表 |

查询后统一过滤三张表：

```python
filter_cpm_report(...)
```

过滤字段：

| DataFrame | 使用字段 |
|---|---|
| `period_capability_df` | `factory`, `step_id`, `param_name` |
| `sheet_features_df` | `factory`, `step_id`, `param_name` |
| `raw_measurements_df` | `factory`, `step_id`, `param_name` |

## 7. 指标分组与页面布局

函数：

```python
render_cpm_indicator_sections(...)
```

分组维度：

```python
sheet_features_df.groupby(["factory", "step_id", "param_name"])
```

每个指标显示一个 `st.expander`，标题格式：

```text
{factory} | {step_id} | {param_name}
```

顶部指标：

| 指标 | 计算方式 |
|---|---|
| `Sheet数` | `indicator_features_df["sheet_id"].nunique()` |
| `点位数` | `len(indicator_raw_df)` |
| `周期数` | `len(indicator_capability_df)` |
| `中位CPM/CPK` | `indicator_capability_df[metric_key].median()` |

布局：

1. 右侧先显示 `Sheet排序` 下拉框。
2. 下一行左右并排：
   - 左侧：月/周/日 Sheet Mean 箱线图 + CPM/CPK 折线图。
   - 右侧：Sheet 点位箱线图。

## 8. 图一：月/周/日 Sheet Mean 箱线图 + CPM/CPK 折线

函数：

```python
_create_period_overview_chart(
    sheet_features_df,
    period_capability_df,
    metric_key,
    metric_label,
    title,
)
```

### 8.1 使用的输入字段

来自 `sheet_features_df`：

| 字段 | 用途 |
|---|---|
| `sheet_start_time` | 生成月/周/日周期标签 |
| `sheet_mean` | 箱线图 y 值 |
| `usl`, `lsl` | 红色规格线；也用于 y 轴范围 |
| `ucl`, `lcl` | 绿色管控线 |
| `target` | 橙色 Target 线，可选 |

来自 `period_capability_df`：

| 字段 | 用途 |
|---|---|
| `period_type` | 区分月/周/日 |
| `period_label` | 横轴周期标签 |
| `period_sort` | 横轴排序 |
| `period_end` | 推断图表结束日期 |
| `sample_count` | hover 信息 |
| `mean_value` | hover 信息 |
| `std_value` | hover 信息 |
| `cpm` 或 `cpk` | 右轴折线 y 值 |

### 8.2 横轴周期生成

图表结束日期优先级：

1. `period_capability_df["period_end"].max()`
2. `sheet_features_df["sheet_start_time"].max()`
3. `date.today()`

然后调用：

```python
build_period_axis(axis_end_date)
```

并生成显示标签：

```text
月 | 2026-05
周 | 2026-W24
日 | 2026-06-25
```

这保证即使某些周期没有数据，横轴仍保留固定位置。

### 8.3 箱线图数据

函数：

```python
_sheet_period_points(sheet_features_df, period_axis_df)
```

处理：

1. 删除 `sheet_start_time` 或 `sheet_mean` 为空的行。
2. 同一条 Sheet 级特征复制成月、周、日三份周期记录。
3. 与固定周期轴 inner merge，仅保留当前窗口内周期。

绘制：

```python
go.Box(
    x=[display_label] * len(y_values),
    y=y_values,  # sheet_mean
    boxpoints=False,
)
```

颜色：

| 类型 | 线色 | 填充色 |
|---|---|---|
| 月 | `#2563eb` | `rgba(37, 99, 235, 0.18)` |
| 周 | `#16a34a` | `rgba(22, 163, 74, 0.18)` |
| 日 | `#f59e0b` | `rgba(245, 158, 11, 0.18)` |

### 8.4 CPM/CPK 折线

函数：

```python
_capability_axis_frame(period_capability_df, period_axis_df, metric_key)
```

该函数把 `period_capability_df` 合并到固定周期轴上，缺失周期保留 NaN。

绘制逻辑：

```python
for period_type in ["month", "week", "day"]:
    type_capability = capability_df[capability_df["period_type"] == period_type]
    type_capability = type_capability.dropna(subset=[metric_key])
    go.Scatter(...)
```

因此月、周、日是三条独立折线：

| period_type | trace 名称示例 |
|---|---|
| `month` | `月CPM` 或 `月CPK` |
| `week` | `周CPM` 或 `周CPK` |
| `day` | `日CPM` 或 `日CPK` |

右轴：

```python
yaxis="y2"
```

右轴布局：

```python
yaxis2 = {
    "title": metric_label,
    "overlaying": "y",
    "side": "right",
    "showgrid": False,
    "zeroline": False,
    "rangemode": "tozero",
}
```

### 8.5 规格线

函数：

```python
_apply_measurement_spec_lines(fig, spec_source)
```

取 `sheet_features_df.dropna(subset=["usl", "lsl"]).head(1)` 作为规格线来源。

绘制线：

| 线 | 字段 | 颜色 |
|---|---|---|
| USL | `usl` | 红色 `#dc2626` |
| LSL | `lsl` | 红色 `#dc2626` |
| UCL | `ucl` | 绿色 `#16a34a` |
| LCL | `lcl` | 绿色 `#16a34a` |
| Target | `target` 或 `(usl + lsl) / 2` | 橙色 `#f97316` |
| CL | `(ucl + lcl) / 2`；若缺失则回退 Target | 绿色 `#16a34a` |

当选择 CPK 时，额外在右轴上绘制：

```text
CPK = 1.33
```

## 9. 图二：Sheet 点位箱线图

函数：

```python
_create_sheet_points_box_chart(
    raw_measurements_df,
    sort_mode,
    title,
    spec_df=indicator_features_df,
)
```

### 9.1 使用的输入字段

来自 `raw_measurements_df`：

| 字段 | 用途 |
|---|---|
| `param_value` | 箱线图 y 值 |
| `sheet_start_time` | “按过货时间排序”时排序 |
| `sheet_id` | “按过货时间排序”时分组 |
| `site_name` | 当前数据实际用于“按腔室排序”的默认分组 |
| `factory`, `step_id`, `param_name` | 进入函数前已用于过滤指标 |

可选腔室字段优先级：

```python
["chamber", "chamber_id", "sub_equip_id", "eqp_id", "main_eqp_type", "site_name"]
```

当前底层 SQL 固定返回的是 `site_name`，因此如果没有额外字段，右侧图的“按腔室排序”实际按 `site_name` 分组。

来自 `spec_df` / `indicator_features_df`：

| 字段 | 用途 |
|---|---|
| `usl`, `lsl`, `ucl`, `lcl`, `target` | 规格线与 y 轴范围 |

### 9.2 数据清洗

```python
df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
df["sheet_start_time"] = pd.to_datetime(df.get("sheet_start_time"), errors="coerce")
df = df.dropna(subset=["param_value"])
```

### 9.3 两种排序/分组模式

#### 模式 A：按过货时间排序

触发条件：

```python
sort_mode == "按过货时间排序"
```

处理：

1. 按 `sheet_start_time`, `sheet_id` 排序。
2. 以 `sheet_id` 为横轴分组。
3. 每个 Sheet 的所有点位 `param_value` 形成一个箱线。

绘制：

```python
go.Box(
    y=y_values,
    name=sheet_id,
    boxpoints=False,
    marker_color="#1d4ed8",
    showlegend=False,
)
```

#### 模式 B：按腔室排序

默认模式。

处理：

1. 按优先级寻找腔室字段。
2. 若无其他腔室字段，则使用 `site_name`。
3. 每个腔室/点位标签下所有 `param_value` 形成一个箱线。

绘制：

```python
go.Box(
    y=y_values,
    name=chamber,
    boxpoints=False,
    marker_color=SHEET_BOX_PALETTE[idx % len(SHEET_BOX_PALETTE)],
    showlegend=True,
)
```

### 9.4 规格线

右侧 Sheet 点位图同样调用：

```python
_apply_measurement_spec_lines(fig, spec_df)
```

使用同一套 USL/LSL/UCL/LCL/Target/CL 规则。

如果 `usl > lsl`，会将 y 轴范围设为：

```python
[lsl, usl]
```

## 10. 两张箱线图的关键区别

| 项目 | 图一：月/周/日 Sheet Mean 箱线图 | 图二：Sheet 点位箱线图 |
|---|---|---|
| 数据粒度 | Sheet 级 | 原始点位级 |
| DataFrame | `sheet_features_df` | `raw_measurements_df` |
| y 值 | `sheet_mean` | `param_value` |
| x 轴 | 固定周期轴：2月 + 3周 + 7天 | Sheet ID 或腔室/点位 |
| 分组 | 月/周/日 | `sheet_id` 或 `site_name` 等腔室字段 |
| 是否叠加能力折线 | 是，右轴 CPM/CPK | 否 |
| 规格线来源 | `sheet_features_df` 第一条规格 | `indicator_features_df` 第一条规格 |

## 11. 字段血缘总览

```text
eda.spc_tzbjx_array / oled / tsp
    factory
    product_spec -> DWR_MES_PRODUCTSPEC.PRODUCTCODE -> prod_code
    sheet_id/glass_id -> sheet_id
    sheet_start_time/glass_start_time -> sheet_start_time
    step_id
    param_name
    site_name
    param_value
        |
        v
load_spc_measurements
        |
        v
SpcRepository.get_spc_measurements
    + 参数白名单 data_type
    + 物理异常值过滤
        |
        +------------------------------+
        |                              |
        v                              v
raw_measurements_df               preprocess_sheet_features
用于图二 Sheet 点位箱线图       聚合为 sheet_features_df
                                   sheet_mean/sheet_max/sheet_min
                                   + usl/lsl/ucl/lcl/target
                                        |
                                        +-----------------------------+
                                        |                             |
                                        v                             v
                               图一 Sheet Mean 箱线图       build_period_capability_report
                                                             月/周/日 CPM/CPK 折线
```

## 12. 当前实现的注意事项

1. CPM/CPK 报表服务层强制只取 `SPC` 类型参数。
2. 底层量测数据最多会通过仓储层维护 3 个月 Parquet 快照，但页面服务层还会按照 `query_config.start_date/end_date` 再做一次内存过滤。
3. 图一的横轴固定保留 2 个月、3 周、7 天的位置，缺数据的位置不会伪造点。
4. 月/周/日 CPM/CPK 折线是三条独立 trace，不会跨类型连线。
5. 若某周期只有 1 个 Sheet，样本标准差为空，因此 CPM/CPK 不显示该周期点。
6. 图二的“按腔室排序”在当前 SQL 字段下默认使用 `site_name`，除非上游额外提供 `chamber`、`chamber_id`、`sub_equip_id`、`eqp_id` 或 `main_eqp_type`。
7. `target` 不是规格 DAO 固定字段；没有 YAML 覆盖时，能力计算与 Target 线会回退到 `(usl + lsl) / 2`。

