# ADR-0024：AOI_TT Particle Size 的分源计数与聚合边界

- Status: Superseded by ADR-0025
- Date: 2026-09-03
- Scope: `src/inline_domain/{application,core,infrastructure}/`、`app/pages/AOI_TT监控报表.py`、`app/sections/inline_domain/aoi_tt/`
- Extends: ADR-0008 的 TT 参数、趋势分母与规格口径；ADR-0012 的共享事实和薄业务投影边界继续有效。

## Context

AOI_TT 原有 `Total` 单片缺陷数来自 SPC 测量明细的 `param_value`。Task1-Opt 要求 ARRAY/TDSUM 进一步区分 `O`、`L` 两类 Particle Size，两类计数来自 `eda.ARRAY_DEFECT_T.item119`。

需求参考查询把每条 defect 直接按 `glass_id=sheet_id` 连接完整的 `eda.spc_tzbjx_array`。SPC 同一 Sheet 存在多个站点、参数或点位记录，因此一个 defect 会命中多条 SPC 行，形成连接乘法。实测同一 Sheet/站点的 defect 原始计数与 SPC `TDSUM.param_value` 可以一致，但直接连接后的计数会被 SPC 匹配行数放大。

O/L 数据源只覆盖 ARRAY 缺陷明细；现有报表还包括 OLED、TP 以及非 TDSUM 参数。新增能力必须保持 Total、三态单片修饰和报表可用性。

## Decision

1. `Total` 继续以 SPC `param_value` 为权威值；不从 defect 明细重算，也不要求 `O + L = Total`。
2. O/L 只扩展 ARRAY/TDSUM。缺陷明细只保留 `item51='AOI'` 且标准化后的 `item119 IN ('O','L')`，每条缺陷事实计 1 个点，并按产品、Sheet、站点、Particle Size 汇总。
3. 产品限定先从 ARRAY SPC 与产品字典得到唯一 `(sheet_id, productcode)` 映射，再与 defect 连接。只去重映射，不去重缺陷事实。
4. 原始 ARRAY defect SQL 归 `infrastructure/shared/array_defect_data_loader.py`；`infrastructure/aoi_tt/particle_size_loader.py` 仅应用 AOI_TT 查询条件和源时间/显示时间策略。AOI_TT 应用端口暴露 Particle Size 计数能力，由组合根注入。
5. Total 完成既有 Delete/False/True 三态修饰后，才以剩余 ARRAY/TDSUM Total Sheet 为基准组合 O/L。每个有效 Sheet 都补齐 O、L，缺失计数为 0；这样三类粒径共用同一检测片集合。
6. `particle_size` 进入月周天、By Lot、By Sheet 的分组键。趋势检测片数仍按厂别、站点、周期去重 Sheet，规格线仍按站点、TT 名称复用现有 USL/UCL；O/L 不执行 Total 的超规修饰。
7. 页面 Particle Size 多选固定为 `Total/O/L` 且默认全选。同一站点与 TT 仍只有一个 Expander，其中每个选中粒径各展示月周天、By Lot、By Sheet 三图。
8. Particle 源异常时退化为 Total-only。页面缓存签名升级为 `aoi_tt_report_v2_particle_size`，禁止复用旧版不含粒径维度的 payload。

## Alternatives considered

- defect 直接连接完整 SPC 明细：拒绝。多参数 SPC 行会放大 defect 计数。
- 对连接结果中的 defect 行执行去重：拒绝。缺陷表没有已确认的业务唯一键，去重可能删除真实的重复点位事实；应只去重用于产品限定的 Sheet 映射。
- 使用 `O + L` 作为 Total：拒绝。需求明确 Total 的权威源仍是 SPC `param_value`，且 defect 表可能包含其他粒径等级。
- 只保留存在 O/L 缺陷的 Sheet：拒绝。会把均值分母变为“发生该类缺陷的片数”，不再代表全部 AOI 检测片。
- 把 SQL 留在 `infrastructure/aoi_tt`：拒绝。现有架构边界要求 SPC/AOI_TT 业务适配器不直接拥有数据库查询，共享事实访问应位于 shared infrastructure。

## Consequences

- 正面：Total 口径完全兼容；O/L 不受 SPC 多行连接放大；三张图的粒径数据严格隔离，且零缺陷 Sheet 仍进入平均值分母。
- 正面：OLED、TP、非 TDSUM 和 Particle 源故障均保持原有 Total 报表能力。
- 代价：AOI_TT 冷加载增加一次 ARRAY defect 聚合查询；O/L 长表会为每个有效 Total Sheet 增加两行。
- 约束：未来增加其他 Particle Size、粒径专属规格或其他厂别数据源时必须另行确认权威表和业务口径，不能从当前 O/L 规则推断。

## Verification

- 查询契约测试使用同一 Sheet 的两条 SPC 记录，验证 O/L defect 计数不被放大，并覆盖 AOI/O/L 过滤和显示时间映射。
- Core/Application/UI/Page 定向与架构边界回归：65 passed。
- Inline 邻接回归：298 passed，14 warnings；既有 Excel COM 回退测试会打印 Windows COM 诊断，但进程成功完成。
- 变更模块定向覆盖率：85%。Python 编译通过；Ruff correctness（E/F，忽略项目既有 E402/E501）通过。
- Playwright E2E：M678 / ARRAY / 11620 / TDSUM 默认全选渲染 9 图，切换 Total-only 后渲染 3 图；Total/O/L 同属一个 Expander。视觉证据位于 `output/test-results/aoi-tt-particle-size/`。

## Traceability

- Requirement: `docs/dev_docs/dev_spec/Inline_domain/feat-AOI_TT.md#task1-optaoi_tt报表区分particle_size`
- Issue: `.scratch/aoi-tt-report/issues/02-distinguish-particle-size.md`
- Plan: `.planning/2026-09-03-aoi-tt-particle-size/`
- Domain docs: `references/domain/Inline_domain/aoi-tt-report-data-lineage.md`、`references/domain/Inline_domain/spec-data_source-aoi_tt.md`
- Key code: `src/inline_domain/infrastructure/shared/array_defect_data_loader.py`、`src/inline_domain/infrastructure/aoi_tt/particle_size_loader.py`、`src/inline_domain/core/aoi_tt/aoi_tt_calculator.py`、`src/inline_domain/application/aoi_tt/aoi_tt_service.py`、`app/sections/inline_domain/aoi_tt/aoi_tt_dashboard.py`
- Tests: `tests/unit/inline_domain/**/aoi_tt/`、`tests/unit/app/sections/aoi_tt/`、`tests/unit/app/pages/test_aoi_tt_page.py`、`tests/e2e/aoi_tt_report.js`
