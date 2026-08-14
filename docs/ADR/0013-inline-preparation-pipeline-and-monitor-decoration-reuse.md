# ADR-0013：Inline 制备管线归属 measurement 与 monitor 分类型修饰复用

- Status: Accepted
- Date: 2026-08-14
- Scope: `src/inline_domain/{application,infrastructure}/`、`src/inline_domain/composition.py`、`references/domain/Inline_domain/`

## Context

ADR-0012 将三厂测量 DAO 与原始快照集中到 `infrastructure/measurement/`，但
`infrastructure/spc/spc_repository.py` 仍承载跨模块共享的制备逻辑（白名单 +
data_type 注入、异常点过滤、主制程追溯、规格 YAML 覆盖），CTQ 与 monitor 均经
spc 模块派生，spc 因此不是与其他模块平行的业务模块。同时 monitor 对全部
data_type 统一走 SPC 修饰口径，与 CTQ 报表页的 ctq 口径不一致；修饰 + Sheet
特征计算在 spc/ctq/monitor 三处重复执行。

已确认的决策（见 issue）：D2 —— monitor 的 CTQ 行切换到 ctq 修饰口径（ctq 修饰
已汇总为 `resources/ctq_sheet_oos_decoration.xlsx`，每产品一个 sheet）；
D3 —— monitor 的 AOI 行免于 SPC 修饰（对齐 aoi_tt 的无修饰口径），aoi_tt 在
范围内，aoi_rs 不在。

## Decision

1. **共享制备管线归属 `infrastructure/measurement/`**：新增
   `measurement_preparation.py`（`InlineMeasurementPreparationRepository`，实现
   `MeasurementPreparationPort`），承载清洗/去重、排除参数过滤、白名单 merge +
   data_type 注入、异常点过滤、维度过滤、主制程追溯与规格 YAML 覆盖。
   `main_process_trace.py`、`measurement_preprocessor.py` 一并迁入（命名去 SPC 化）。
2. **管线顺序是行为契约**：排除参数（LOSS）在 data_type 过滤之前，异常点过滤在
   其后；迁移逐行平移保序。该约束写入
   `references/domain/Inline_domain/spec-infrastructure-architecture.md`。
3. **业务模块退化为薄投影**：`SpcRepository`（524→29 行）与 `CtqRepository`
   对称，各自只做投影；报废逻辑（`get_scrap_data` + 厂别推断）迁入
   `infrastructure/monitor/scrap_repository.py`——它仅 monitor 消费且与
   measurement 零耦合。模块 repository↔service 的 1:1 结构保持不变。
4. **段 2 不建共享服务漏斗**：新增无状态缓存计算函数
   `application/shared/decorated_features.py::fetch_decorated_features`
   （`st.cache_data`，key = prod_code + scope + 时间窗口 + snapshot_signature；
   scope ∈ spc/ctq/none）。spc/ctq service 以各自 scope 调用；monitor 按
   data_type 分组路由（SPC→spc、CTQ→ctq、AOI→none）后合并。窗口一致时跨模块
   命中同一缓存条目，跨模块一致性由此保证。审计文件落盘语义为缓存 miss 时写一次。
5. 三个报表页面的 `funcs_to_clear` 登记共享缓存函数，强刷链路完整。

## Alternatives considered

- 共享 `SheetFeaturesService` 汇总各模块 repository 再回供 service（V2 方案）：
  拒绝。它把 repository→本模块 service 的 1:1 编排改为漏斗，违背模块平行的
  DDD 结构（用户修正点 2）。
- 共享逻辑留在 spc_repository：拒绝（用户修正点 1）。spc 必须是平行业务模块，
  ctq/monitor 不应反向依赖兄弟模块。
- 统一 monitor-AOI 与 aoi_tt 的参数识别集（白名单 NULL→AOI vs 规格表
  param_type NULL）：拒绝，属行为变更，单独立项。
- 重构 `_apply_outlier_filters` 的 COM 解密写 CSV 副作用：拒绝，仅平移，
  后续单列优化。

## Consequences

- 正面：spc/ctq/monitor 同源同口径（同一制备管线 + 同一修饰缓存条目）；
  修饰+特征计算每 (prod, scope, 窗口) 只执行一次；模块边界清晰可单测。
- 代价/注意：monitor 的 AOI 报警数因免修饰可能变化（D3 预期内）；缓存 key 含
  时间窗口，窗口不一致时跨模块不共享（正确性优先）；审计文件只在缓存 miss
  时刷新。
- 约束：制备管线顺序变更视为口径变更；新模块消费测量数据必须经
  measurement 制备 port 与组合根。

## Verification

- 特征化安全网 `tests/unit/inline_domain/test_pipeline_characterization.py`
  14 例先行，迁移全程保持绿。
- 定向：tests/unit/inline_domain 140 passed（含新增 scope 路由 9 例）。
- 全量：7 failed / 414 passed，7 个失败均为既有基线（加密 xlsx 诊断 ×3、
  Yield 配置 ×2、Code selector ×2），无新增。
- E2E（playwright-cli，localhost:8503）：SPC/CTQ/AOI_TT/自动预警四页加载
  无 traceback、图表渲染正常；自动预警完成 CTQ 模式切换；服务端日志
  0 Traceback。signoff 截图存 `output/test-results/`。
- 有意排除：四页「刷新缓存」按钮点击验证未执行（用户评估现有证据后决定跳过）。

## Traceability

- Issue: `.scratch/inline-pipeline-reuse/issues/01-shared-pipeline-to-measurement-and-monitor-reuse.md`
- Plan: `.planning/2026-08-13-inline-pipeline-reuse/`
- 设计文档: `docs/dev_docs/generated/Inline_domain/monitor_data_reuse_evaluation_and_design.md`（V3 定稿）
- 架构规范: `references/domain/Inline_domain/spec-infrastructure-architecture.md`
- Supersedes: ADR-0012 中"共享逻辑暂留 spc_repository"的过渡形态
