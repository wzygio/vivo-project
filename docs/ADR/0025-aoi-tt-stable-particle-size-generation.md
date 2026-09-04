# ADR-0025：AOI_TT 按站点比例稳定生成 Particle Size

- Status: Accepted
- Date: 2026-09-04
- Scope: `src/inline_domain/{application,core,infrastructure}/`、AOI_TT 页面与 Inline 配置
- Supersedes: ADR-0024 中仅 ARRAY/O/L 的 Particle Size 来源、范围与页面选项；Total、修饰顺序及聚合分母决策继续有效

## Context

Task1-1-1 将 Particle Size 范围调整为 ARRAY/TP 的 S/M/L/H，OLED 暂不区分，并要求默认把每片 Total 按站点比例规格生成各 Size。各 Sheet 的比例需要围绕站点规格变化，但同一 Sheet 重复加载时不能变化；同时仍需保留从真实缺陷明细计算的模式。

## Decision

1. `Total` 继续来自 SPC `param_value`，完成既有三态修饰后再拆分 Particle Size。
2. 默认启用“比例生成”模式。站点基础比例来自 `AOI_TT-比例规格表.xlsx` 的“比例规格表”，仅完整且合计为 1 的 S/M/L/H 分布有效。
3. 每片以 `factory + prod_code + step_id + sheet_id + tt_name` 为稳定业务键，对四档基础比例施加可配置的确定性扰动，再归一化并分配 Total。同一业务键、规格和扰动幅度不变时结果不变，四档合计保持等于该片 Total。
4. 配置可切回“真实缺陷明细”模式：ARRAY 从 `ARRAY_DEFECT_T.item119` 取得 S/M/L/H，并保留 `item51='AOI'`；TP 从 `TSP_DEFECT_T.item2` 取得 S/M/L/H，以 `cut_id` 对齐 SPC 的 `glass_id`，不使用 ARRAY 的 `item51` 条件；OLED 始终 Total-only。
5. 比例规格缺失、无效或不可读时，对应报表降级为 Total-only；缺少某站点比例时不得套用其他站点比例。
6. 模式、扰动幅度和比例规格文件版本均进入报表缓存身份，配置或规格变化后必须重建结果。
7. 页面保留原 Particle Size 多选框与 Expander 布局。ARRAY/TP 选项为 Total/S/M/L/H，OLED 仅为 Total。

## Alternatives considered

- 运行时调用普通随机数并缓存结果：拒绝。缓存刷新、进程重启或查询窗口变化会使历史 Sheet 比例漂移。
- 将每个 Sheet 的随机比例另存为台账：拒绝。增加写入、并发和生命周期管理成本；当前稳定业务键可以无状态复现。
- 所有站点共用一个比例：拒绝。比例规格明确按站点维护。
- 删除真实缺陷明细模式：拒绝。任务明确要求配置开关保留两种模式，且实表模式仍用于核验。

## Consequences

- 正面：默认模式不再依赖大体量缺陷明细查询；结果可重复，站点差异和 Total 守恒均明确。
- 正面：ARRAY 与 TP 的真实来源差异被隔离，OLED 不产生无意义的粒径选项。
- 代价：生成的 S/M/L/H 是模拟分配值，不代表缺陷明细表中的真实分类计数。
- 风险：比例规格表若配置不完整或合计不为 1，将触发 Total-only 降级；维护者需要先修正规格表。
- 约束：稳定业务键或扰动算法发生变化会改变既有 Sheet 的生成结果，必须通过新 ADR 和缓存版本升级管理。

## Verification

- Core/Infrastructure/Application/UI 定向测试：60 passed。
- 生产比例规格只读验证：24 条规则、6 个站点，每站 S/M/L/H 合计为 1。
- PostgreSQL 元数据与只读聚合验证：TP 标识列为 `cut_id`，可与 SPC `glass_id` 命中。
- Playwright E2E：M678 / ARRAY / 11620 默认五档渲染 15 图，Total-only 渲染 3 图。

## Traceability

- Requirement: `docs/dev_docs/dev_spec/Inline_domain/feat-AOI_TT.md#task1-1-1aoi_tt报表优化-数据生成`
- Domain flow: `references/domain/Inline_domain/aoi-tt-report-data-lineage.md`
- Key tests: `tests/unit/inline_domain/**/aoi_tt/`、`tests/unit/app/sections/aoi_tt/`、`tests/e2e/aoi_tt_report.js`
