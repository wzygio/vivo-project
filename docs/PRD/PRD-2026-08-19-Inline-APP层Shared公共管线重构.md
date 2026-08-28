# PRD：Inline APP 层 Shared 公共管线重构

- 日期：2026-08-19
- 状态：待评审（未实施）
- 适用范围：`app/sections/inline_domain/` 下 `spc`、`ctq`、`aoi_rs`、`aoi_tt` 四个报表 section
- 关联代码：`app/sections/inline_domain/*/`、`app/components/distribution_charts.py`、`app/manager/render_gate.py`、`app/utils/step_labels.py`、`src/shared_kernel/config.py`、`src/inline_domain/core/shared/`（后端先例）
- 需求来源：`docs/dev_docs/dev_spec/Inline_domain/refactor-app_shared.md`

## 1. CAPABILITY

报表前端需要一条**统一作用于 inline 各页面的公共管线（public pipeline）**：筛选级联、绘图类型决策、规格线绘制、月周天/过货时间坐标轴等跨页面一致的前端处理逻辑，集中在 `app/sections/inline_domain/shared/` 单一模块维护；`spc`、`ctq`、`aoi_rs`、`aoi_tt` 四个 section 只保留各自业务差异（指标语义、规格口径、文案、session key 前缀），不再各自复制或跨模块私有导入公共逻辑。

交付后：

1. 修改任一公共绘图/筛选行为只需改 shared 一处，四个页面同时生效；
2. `ctq` 不再从 `spc` 导入私有函数（`_create_period_overview_chart` 等），改为经由 shared 公共 API；
3. 修复 CTQ 图表类型解析的实际缺陷（见 2.2 问题 3）；
4. 四个页面现有单元测试全部保持通过，shared 模块获得直接单元测试覆盖。

## 2. 背景与现状

### 2.1 当前结构

```text
app/sections/inline_domain/
├── spc/spc_dashboard.py        1215 行：筛选 + CPK 预警 + 修饰后台 + 全部绘图逻辑
├── ctq/ctq_dashboard.py         283 行：筛选 + 复用 spc 私有函数绘图
├── aoi_rs/aoi_rs_dashboard.py   502 行：筛选 + 三张图
├── aoi_tt/aoi_tt_dashboard.py   499 行：筛选 + 三张图（与 aoi_rs 大面积逐字重复）
└── monitor/monitor_dashboard.py（本次不动）

src/inline_domain/ 后端先例：
├── application/shared/         decorated_data.py、decorated_features.py
└── core/shared/                sheet_oos_decoration.py、auto_decoration.py
```

后端已存在 `shared/` 先例，前端建立同层 shared 与项目既有约定一致。

### 2.2 当前问题

1. **跨模块私有导入**：`ctq_dashboard.py:13-18` 从 `spc_dashboard` 导入 `_create_period_overview_chart`、`_create_sheet_points_box_charts`、`_resolve_chart_type`、`render_sheet_oos_decoration_admin` 四个私有/公共混合符号，形成隐性耦合；spc 内部任何改名都会静默破坏 ctq。
2. **筛选逻辑四份复制**：`_unique_sorted`、`_normalise_selection`、`_filter_signature`、`get_available_factories`、`get_steps_for_factory`、级联筛选面板（`render_*_filters`，含厂别切换清空、站点变更自动全选参数、查询签名门控）在 spc/ctq 之间、aoi_rs/aoi_tt 之间逐字重复；`filter_*_report` 同样逐字重复。
3. **实际缺陷——CTQ 图表类型解析签名错配**：`ctq_dashboard.py:270` 调用 `_resolve_chart_type(indicator_features_df, indicator_raw_df)`，而 spc 中定义为 `_resolve_chart_type(param_name, line_param_name_contains)`。对 DataFrame 迭代得到的是列名，列名必然出现在 `str(df)` 表头中，因此**CTQ 当前无论配置如何一律返回 line**。`tests/unit/app/sections/ctq/test_ctq_dashboard.py:114` 断言 `== "line"` 是依赖该意外行为通过的（测试参数 `SE_L1T_UNI` 含配置 token `UNI`，修正后结果仍为 line，测试可保持不变）。
4. **AOI 双模块逐字重复**：常量（`CODE_PALETTE`、`PERIOD_BAR_COLORS`、`PERIOD_TYPE_NAMES`、`_PERIOD_SEPARATORS`、厂别选项）、`_add_spec_trace`、月周天趋势图（x 轴分组 + 零宽分隔符 + 过货量柱 + 折线，约 90 行逐字相同）、By Lot/By Sheet 点线图（x 按 `first_start_time` 排序、按 code 分线 + 规格线）均重复。
5. **规格线/坐标轴规则散在 spc 单文件深处**：折线/箱线决策（配置驱动）、LSL 为空或等于 0 仅绘上限、By 过货时间横轴替换为时间（`type="date"`）、规格值格式化、y 轴范围推导、月周天 display label，全部是页面级公共规则但只能被 spc/ctq 私下共享。

### 2.3 需求审查结论

| 用户要求 | 审查结论 | PRD 处理 |
|---|---|---|
| 仿照后端结构创建 shared 模块 | 合理，与 `src/inline_domain/*/shared/` 先例一致 | 新建 `app/sections/inline_domain/shared/`，四个 section 委托复用 |
| ctq 与 spc 保持一致：折线/箱线决策 | 合理，且现状存在缺陷 | 统一为配置驱动的 `resolve_chart_type(param_name, tokens)`；CTQ 改用与 SPC 相同的调用方式，修复问题 3 |
| ctq 与 spc 保持一致：LSL 为空或 0 仅绘上限 | 已在 spc 实现 | 原样下沉 shared，行为不变 |
| ctq 与 spc 保持一致：By 过货时间横轴替换为时间 | 已在 spc 实现（`type="date"` + `tickformat`） | 原样下沉 shared，行为不变 |
| aoi_tt 与 aoi_rs 统一绘图逻辑 | 合理 | 趋势图/点线图/规格线 trace 参数化后下沉 shared，差异（规格口径、code 列、文案）由调用方注入 |
| 无需 E2E 测试 | 接受 | 仅单元测试 + import smoke；UI 验收由用户自行完成 |

## 3. CONSTRAINTS

### 3.1 行为保持规则

1. 除「CTQ 图表类型解析修复」外，四个页面的渲染行为、图表结构、筛选交互、session key、文案一律不变。
2. 四个 dashboard 模块的**现有公开函数名与签名全部保留**（页面 `app/pages/*.py` 与现有测试按名导入），内部实现改为委托 shared；允许保留薄封装。
3. `app/components/distribution_charts.py`（trace 工厂）保持原位不动，shared 复用它而非取代它。
4. 不动 `monitor` section、`app/pages/`、后端 `src/` 任何文件。
5. session State key 前缀（`spc_`、`ctq_`、`aoi_rs_`、`aoi_tt_`）必须保持，避免用户筛选状态丢失。

### 3.2 工程约束

1. shared 内部按职责拆分小模块，通过 `shared/__init__.py` 暴露公共 API；section 只从 `shared` 包级导入，不跨进子模块私有路径。
2. shared 模块中的绘图/筛选函数保持可单测：不依赖 `st.session_state` 的纯函数与 UI 函数分离（沿用现有 RenderGate 两阶段约定）。
3. 配置读取（`ConfigLoader.get_spc_line_chart_param_name_contains()`）由调用方在组合层完成并注入，shared 纯函数不直接读配置，便于测试。
4. 新代码风格对齐现有文件：`from __future__ import annotations`、类型注解、中文业务注释。

## 4. IMPLEMENTATION CONTRACT

### 4.1 目标结构

```text
app/charts/inline/           # 绘图层（section 不直接绘制）
├── __init__.py              # 公共 API 出口（显式 re-export）
├── constants.py             # 调色板、月周天周期标签/配色/分隔符
├── chart_type.py            # CHART_TYPE_BOX / CHART_TYPE_LINE、
│                            # resolve_chart_type(param_name, line_param_name_contains)
├── spec_lines.py            # 规格值格式化、首行规格提取、规格线绘制
│                            # （LSL 为空或 0 → 仅 USL/UCL 上限）、y 轴范围推导
├── sheet_charts.py          # 月周天分布图（period 轴 + display label + 箱线）、
│                            # Sheet 点位图（By 腔室 / By 过货时间；过货时间 chart_type=line
│                            # 时横轴为真实时间轴 type="date"）
└── aoi_charts.py            # spec trace 工具、月周天趋势图（分组 x 轴 + 零宽分隔 +
                             # 次 Y 轴柱 + 比值线）、By Lot/By Sheet 点线图；
                             # 规格线经 AoiSpecLine 列表注入：
                             #   RS → 单值规格线；TT → USL/UCL 双上限

app/sections/inline_domain/shared/    # 组装层共享（对齐后端 application 层）
├── __init__.py              # 公共 API 出口
├── constants.py             # INLINE_FACTORY_OPTIONS（筛选级联使用）
├── filters.py               # unique_sorted、normalise_selection、filter_signature、
│                            # get_available_factories / get_steps_for_factory /
│                            # get_options_for_factory_steps（第三级列名参数化：
│                            #   param_name / rs_code / tt_name）、
│                            # render_cascade_filters（key_prefix、第三级标签参数化）、
│                            # apply_report_filter（列名参数化）
└── decoration_admin.py      # Sheet OOS 修饰后台 UI（key_prefix/report_name 参数化）
```

> 2026-08-19 结构补充：按维护者指示，section 定位为组装层（对齐后端
> application 层），图表绘制从 `sections/inline_domain/shared/` 迁至
> `app/charts/inline/`；上方结构为最终落地形态。

### 4.2 各 section 改造方式

- `spc_dashboard.py`：删除已下沉的私有函数，改为从 `shared` 导入；保留 `render_spc_filters`、`filter_spc_report`、`render_spc_indicator_sections`、`render_cpk_alert_*`、CPK/SPC 修饰后台等公开 API；`render_spc_indicator_sections` 内 `ConfigLoader` 读取与 chart key/memo 逻辑留在 spc。
- `ctq_dashboard.py`：删除对 `spc_dashboard` 的私有导入，全部改从 `shared` 导入；`_resolve_chart_type` 调用修正为 `resolve_chart_type(param_name, ConfigLoader.get_spc_line_chart_param_name_contains())`（与 spc 同口径）。
- `aoi_rs_dashboard.py` / `aoi_tt_dashboard.py`：筛选与图表构建委托 shared；各自保留 code 显示名（RS 的 `code_desc`）、spec 装配（`attach_spec_values` 调用）与 render 编排。

### 4.3 明确拒绝的设计

1. 不把四个 dashboard 合并成一个巨型通用 dashboard——差异语义保留在各 section。
2. 不在 shared 中引入对具体 section 的反向依赖。
3. 不改动 `src/` 后端任何模块，不新建后端 shared。
4. 不趁机重排页面布局、改配色或改文案。

## 5. 接口与数据影响

### 5.1 预计修改文件

- 新增：`app/sections/inline_domain/shared/{__init__,constants,filters,chart_type,spec_lines,sheet_charts,aoi_charts}.py`
- 修改：`app/sections/inline_domain/{spc,ctq,aoi_rs,aoi_tt}/*_dashboard.py`
- 新增测试：`tests/unit/app/sections/shared/`（对 shared 纯函数的直接覆盖）
- 现有测试：`tests/unit/app/sections/{spc,ctq,aoi_rs,aoi_tt}/`、`tests/unit/app/pages/` 应保持不修改即通过；仅当测试断言了被修复的 CTQ 缺陷行为时才允许调整（当前评估为不需要，见 2.2 问题 3）。

### 5.2 向后兼容

- 页面 import 路径不变；session key 不变；图表输出结构不变。

## 6. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 下沉过程中无意改变图表细节（颜色、hover、坐标轴） | 现有 4 组 section 单测 + pages 单测全部必须通过；对关键图（规格线、时间轴）补 shared 级断言 |
| CTQ chart_type 修复改变线上表现 | 配置 token `UNI` 覆盖现有 CTQ 关注参数；PRD 明确记录该行为变更，由用户 UI 验收确认 |
| session key 漂移导致筛选状态丢失 | key 前缀作为参数显式传入 shared，测试断言前缀 |

## 7. NON-GOALS

1. monitor 看板、equipment/yield 域的共享化。
2. E2E / 浏览器自动化测试（用户自行完成）。
3. 性能优化、缓存策略调整。

## 8. 验收标准

1. `app/sections/inline_domain/shared/` 存在且四个 dashboard 不再包含逐字重复的筛选/绘图实现；`ctq_dashboard.py` 不再 import `spc_dashboard`。
2. `grep` 验证：`spc`、`ctq`、`aoi_rs`、`aoi_tt` 中不再各自定义 `_unique_sorted`、`_normalise_selection`、`_filter_signature`、`_add_spec_trace`、PERIOD 常量。
3. CTQ 的 chart_type 由 `resolve_chart_type(param_name, tokens)` 决定：参数名含配置 token → line，否则 box。
4. LSL 为空或等于 0 时，spc/ctq 的分布图仅出现 USL/UCL 上限规格线（现有测试已覆盖，继续通过）。
5. By 过货时间趋势图（chart_type=line）横轴为时间轴（`xaxis.type == "date"`）。
6. `pytest tests/unit -q` 全绿；`streamlit` 四个页面可正常 import（无循环依赖）。

## 9. 测试计划

### 9.1 单元测试（新增，`tests/unit/app/sections/shared/`）

- `chart_type`：token 命中/未命中/大小写不敏感/空配置。
- `spec_lines`：LSL 为空、LSL=0、正常双侧规格、极小值科学计数格式化。
- `filters`：级联选项推导、normalise、signature；`render_cascade_filters` 的 key 前缀隔离。
- `sheet_charts`：过货时间 line 图 x 轴为 date 类型；箱线图按腔室着色。
- `aoi_charts`：趋势图分组 x 轴含分隔位、次 Y 轴柱；RS 单规格线与 TT 双规格线经 spec_provider 注入。

### 9.2 回归测试

- 现有 `tests/unit/app/sections/{spc,ctq,aoi_rs,aoi_tt}` 与 `tests/unit/app/pages/` 全部原样通过。

### 9.3 UI 验收（用户自行完成）

- 四个页面筛选、查询、图表展示与重构前一致；CTQ 参数名含 `UNI` 时为折线图，其余为箱线图。

## 10. 实施顺序

1. 新建 `shared/` 骨架与 constants，迁移纯函数（filters、chart_type、spec_lines），同步建立单测。
2. 迁移 sheet_charts（spc/ctq 共用图），spc 切换到 shared，跑 spc/ctq 测试。
3. ctq 切换到 shared 并修复 chart_type 调用，跑 ctq 测试。
4. 迁移 aoi_charts（spec_provider 参数化），aoi_rs、aoi_tt 切换，跑两组测试。
5. 全量 `pytest tests/unit -q` + import smoke，更新 `ARCHITECTURE.md`/`references` 中涉及前端结构的描述（如有）。

## 11. OPEN QUESTIONS

1. CTQ 是否应拥有独立于 SPC 的 `line_param_name_contains` 配置段？（本期共用 `spc.chart.line_param_name_contains`，与"保持一致"诉求相符；如后续分化再拆配置。）

## 12. HANDOFF

- 开发入口：按第 10 节顺序实施；每步完成后跑对应 section 单测。
- 关键陷阱：`_resolve_chart_type` 的旧签名错配不要原样保留；session key 前缀必须参数化传入；`shared/__init__.py` 只 re-export 公共符号。
