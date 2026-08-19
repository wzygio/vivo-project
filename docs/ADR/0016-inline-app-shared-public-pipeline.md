# ADR-0016：Inline APP 层 Shared 公共管线——四报表 section 的筛选与绘图统一

- Status: Accepted
- Date: 2026-08-19
- Scope: `app/sections/inline_domain/`、`tests/unit/app/sections/`、`config/inline_config.yaml`
- Trace: Issue `.scratch/inline-app-shared/issues/01-inline-app-shared-pipeline.md`、
  PRD `docs/PRD/PRD-2026-08-19-Inline-APP层Shared公共管线重构.md`、
  Plan `.planning/2026-08-19-inline-app-shared/`、
  需求 `docs/dev_docs/dev_spec/Inline_domain/refactor-app_shared.md`

## Context

`app/sections/inline_domain/` 四个报表 section（spc/ctq/aoi_rs/aoi_tt）的公共前端
逻辑分散复制：级联筛选面板与 report filter 四份逐字重复；AOI 双模块的趋势图、
点线图、规格 trace、常量大面积重复；ctq 跨模块导入 spc 的私有函数形成隐性耦合。
同时发现实际缺陷：ctq 以错误签名调用 spc 的 `_resolve_chart_type`（传入 DataFrame
而非参数名与配置 token），导致 CTQ 图表类型恒为 line，配置
`spc.chart.line_param_name_contains` 在 CTQ 路径从未生效。

后端已有 `src/inline_domain/{core,application}/shared/` 先例（ADR-0014），前端
需要同层的公共管线（public pipeline）。

## Decision

1. **新建 `app/sections/inline_domain/shared/` 包**，按职责拆分并经包级
   `__init__.py` 显式 re-export：
   - `constants`：厂别顺序、调色板、月周天周期标签/配色/分隔符；
   - `filters`：级联选项推导、签名门控、`render_cascade_filters`
     （`key_prefix`/`third_kind` 参数化，session key 与历史逐字一致）、
     `apply_report_filter`；
   - `chart_type`：`resolve_chart_type(param_name, tokens)` 折线/箱线决策；
   - `spec_lines`：规格线绘制（LSL 为空或 0 → 仅 USL/UCL 上限）与 y 轴范围；
   - `sheet_charts`：月周天分布图、Sheet 点位图（By 腔室 / By 过货时间，
     line 时横轴为 `type="date"` 真实时间轴）；
   - `aoi_charts`：月周天趋势图与 By Lot/By Sheet 点线图，规格线经
     `AoiSpecLine` 列表注入（RS 单值「规格」；TT USL 虚线 + UCL 点线），
     code 列名/显示名/文案参数化；
   - `decoration_admin`：Sheet OOS 修饰后台 UI（`key_prefix`/`report_name` 参数化）。
2. **四个 dashboard 变为委托层**：保留全部现有公开函数名与签名（页面与测试的
   导入面不变），内部委托 shared；`ctq` 不再导入 `spc_dashboard`。
3. **修复 CTQ chart_type 缺陷**：与 SPC 同口径调用
   `resolve_chart_type(param_name, ConfigLoader.get_spc_line_chart_param_name_contains())`；
   CTQ 本期共用 spc 配置段，后续分化再拆配置。
4. **shared 纯函数不读配置、不碰 session**：`ConfigLoader` 读取与 session key
   前缀由调用方在组合层注入，保证可单测。

## Alternatives considered

- **四个 dashboard 合并为一个通用 dashboard**：拒绝——各报表的指标语义、规格
  口径与文案是真实业务差异，合并会以配置爆炸换取虚假统一；保留 section 薄壳，
  仅下沉机制。
- **公共逻辑放 `app/components/`**：拒绝——`app/components/` 是跨域 UI 组件
  （页头、上传、告警）；本管线只服务 inline 四个报表，放 `inline_domain/shared/`
  与后端 shared 对齐，内聚更高。
- **CTQ 独立配置段**：暂缓——本期诉求就是 ctq 与 spc 保持一致，共用
  `spc.chart.line_param_name_contains`；PRD OPEN QUESTION 留档。

## Consequences

- 正面：公共绘图/筛选规则单一来源，修改一处四页面同时生效；CTQ 图表类型真正
  受配置控制（含 `UNI` token 的参数画折线，其余箱线）；ctq→spc 私有耦合消除；
  新增 `tests/unit/app/sections/shared/` 32 条直接覆盖。
- 负面/约束：shared 与 section 之间以「模块属性别名」保留既有 monkeypatch 锚点
  （如 `spc_dashboard._create_period_overview_chart`），后续重命名需同步测试；
  session key 前缀是公开契约，shared 内禁止硬编码。
- 验证：`pytest tests/unit -q` = 7 failed（既有基线：hot_reload、aoi_rs portal
  nav、code_selector×2、yield×3）/ 482 passed，无新增失败；grep 验收与四页面
  import smoke 通过；E2E 由维护者自行验收（需求明确排除）。
