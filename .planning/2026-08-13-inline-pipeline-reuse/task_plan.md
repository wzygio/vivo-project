# Task Plan: Inline Pipeline 复用（段1下沉 + 段2复用）

- Plan ID: `2026-08-13-inline-pipeline-reuse`
- Issue: `.scratch/inline-pipeline-reuse/issues/01-shared-pipeline-to-measurement-and-monitor-reuse.md`（ready-for-agent）
- 设计文档: `docs/dev_docs/generated/Inline_domain/monitor_data_reuse_evaluation_and_design.md`（V3 定稿）
- Created: 2026-08-13

## Goal

将 spc_repository 中的跨模块制备逻辑下沉 `infrastructure/measurement/`（spc 变为平行薄模块）；
monitor 按 data_type 复用 spc/ctq 修饰口径（D2: CTQ→ctq 口径；D3: AOI→免修饰），
通过共享缓存计算函数消除修饰+特征的重复执行；直至 SPC/CTQ/AOI_TT/自动预警 E2E 全部通过。

## 已确认决策

- D2：monitor CTQ 行切 ctq 修饰口径（`resources/ctq_sheet_oos_decoration.xlsx`，每产品 sheet）。
- D3：monitor AOI 行免 SPC 修饰；aoi_tt 在范围内（结构核查），aoi_rs 不在。
- 段1：可复用逻辑全部入 measurement；scrap 逻辑入 monitor infrastructure。
- 段2：不建共享服务漏斗；新增无状态共享缓存计算函数，模块 repository↔service 1:1 保留。
- 保序约束：LOSS→白名单/data_type→异常点→维度过滤→追溯。

## Phases

### Phase 1: 特征化测试安全网
- [x] 1.1 固定 prod+结束日期，对 SPC payload、CTQ payload、monitor 三输出 DF 写快照断言测试（行数/关键列/OOC 汇总），先跑绿（验证基线）
- 验证: `pytest tests/unit/inline_domain -k characterization` 通过

### Phase 2: 段1 — 制备逻辑下沉 measurement
- [x] 2.1 迁 `main_process_trace.py`、`measurement_preprocessor.py` → `measurement/`，更新引用与测试
- [x] 2.2 新增 `measurement/measurement_preparation.py`：清洗/去重→LOSS→白名单+data_type→异常点→维度过滤→追溯 + 规格线/YAML 覆盖；暴露 `MeasurementPreparationPort`
- [x] 2.3 `SpcRepository` 改薄投影（组合制备 port，filter 语义不变）；`CtqRepository`/monitor 门面重接线
- [x] 2.4 scrap（`get_scrap_data`+`_infer_factory_from_step`）→ `infrastructure/monitor/scrap_repository.py`
- [x] 2.5 `composition.py` 重接线 + 修 `:18` 缺失的 `src.` 前缀
- [x] 2.6 运行 Phase 1 特征化测试 + 现有定向测试，逐断点确认口径零漂移
- 验证: `pytest tests/unit/inline_domain tests/integration -k "measurement or spc or ctq or monitor"` 通过

### Phase 3: 段2 — monitor 复用与缓存共享
- [x] 3.1 新增 `application/shared/decorated_features.py`：`fetch_decorated_features`（st.cache_data，key=(prod, scope, signature)，scope∈spc/ctq/none，只返回原生结构）
- [x] 3.2 spc/ctq service 改走共享缓存函数；审计文件落盘保留在模块薄包装（缓存外）
- [x] 3.3 monitor 主流程按 data_type 分组路由修饰（CTQ→ctq 口径，AOI→none），再合并判定
- [x] 3.4 monitor 下钻 `get_monitor_defect_details` 走同一路径，删除内联取数/修饰副本
- [x] 3.5 页面 `funcs_to_clear` 登记共享缓存函数（自动预警看板/SPC/CTQ 页）
- 验证: 定向测试通过；SPC 页与 monitor-SPC 部分输出一致（同一缓存条目）

### Phase 4: 文档
- [x] 4.1 `references/domain/Inline_domain/` 新增 infrastructure 架构规范（制备管线契约、保序约束、模块矩阵）
- [x] 4.2 设计文档已是 V3 定稿；ARCHITECTURE.md 如有所有权变化则同步

### Phase 5: 全量验证（完成门）
- [x] 5.1 全量 pytest 不引入新失败（基线 7 个既有失败除外）
- [x] 5.2 E2E（playwright）：SPC/CTQ/AOI_TT/自动预警四页无 traceback，图表渲染正常
- [x] 5.3 强刷链路验证：清缓存覆盖共享缓存函数

## 完成门（对应 Issue Acceptance Criteria）

全部 checklist 勾选 + E2E 通过 → 进入模块 4（ADR）。

## 批准记录

- 2026-08-13 用户批准：「批准，按计划执行」（含 AOI 免修饰的行为变化确认）。

## 收尾记录（2026-08-14）

- 全部 checklist 完成；5.3 强刷验证经用户裁定跳过（有意排除，记录于 progress.md 与 ADR-0013）。
- E2E 以现有证据 signoff（四页截图 + 0 Traceback）。
- ADR-0013 已产出。
