# Progress: Inline Pipeline 复用

## 2026-08-13 — Session 1

- 模块1（需求）：issue 创建并 triage 为 ready-for-agent：
  `.scratch/inline-pipeline-reuse/issues/01-shared-pipeline-to-measurement-and-monitor-reuse.md`
- 段2 方案终审完成：采纳用户修正点 2，V2 共享服务漏斗 → V3 无状态共享缓存计算函数；
  设计文档定稿 V3：`docs/dev_docs/generated/Inline_domain/monitor_data_reuse_evaluation_and_design.md`
- 模块2（计划）：task_plan/findings/progress 创建，待用户批准。

## Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| （暂无） | | |

## 2026-08-13 — Phase 1 完成（特征化安全网）

- 新增 `tests/unit/inline_domain/test_pipeline_characterization.py`：14 例
  - 制备管线 7 例（清洗/去重/LOSS/白名单/data_type/窗口/维度/追溯兜底/异常点真实规则路径）
  - 规格 YAML 覆盖 2 例、scrap 契约 2 例、monitor 聚合口径 1 例
  - monitor CTQ 修饰口径与 AOI 免修饰为有意变更点，未锁现状
- 证据：`.venv/Scripts/python.exe -m pytest tests/unit/inline_domain -q` → 130 passed
- 注意：monitor dummy 基座行偏移已被锁定，重构改基座语义会变红（预期守护）

## 2026-08-13 — Phase 2 完成（段1 下沉 measurement）

- 新增：measurement/main_process_trace.py、measurement/measurement_preprocessor.py（去 SPC 化命名）、
  measurement/measurement_preparation.py（407 行 InlineMeasurementPreparationRepository）、
  monitor/scrap_repository.py（141 行 InlineScrapRepository）
- SpcRepository 524→29 行薄投影；MonitorSpcDataPort 契约不变；composition 重接线 + 修 import 前缀
- 保序：以代码为准逐行平移（LOSS 在 dedup 之前，与计划文字略有出入，未按文字重排）
- 证据：tests/unit/inline_domain 131 passed；tests/unit 4 failed/387 passed（=基线）；
  tests 全量 7 failed/405 passed（=基线，无新增失败）
- 跟进：references/domain/Inline_domain/inline_domain.md 旧路径描述待 Phase 4 同步

## 2026-08-13 — Phase 3 完成（段2 monitor 复用）

- 新增 application/shared/decorated_features.py：fetch_decorated_features(prod, scope, start, end, signature)，
  scope∈{spc,ctq,none}；key 含窗口，窗口一致时跨模块命中；persist 语义=miss 落盘一次
- spc/ctq service 改走共享函数（scope 各自）；monitor 按 data_type 分组路由（CTQ→ctq 口径=D2，AOI→none=D3），
  下钻同路径、删除内联副本；三页面 funcs_to_clear 登记共享函数
- 特征化测试无变红（monitor fixture 仅 SPC 行）
- 新增测试 9 例（scope 路由 7 + monitor 分组路由 2）
- 证据：tests/unit/inline_domain 140 passed；全量 7 failed/414 passed（=基线，无新增）

## 2026-08-14 — Phase 4/5 收尾

- Phase 4 完成：新增 references/domain/Inline_domain/spec-infrastructure-architecture.md；
  inline_domain.md 全面同步（分层图、CTQ 修饰路径、SpcQueryConfig 位置、第 5/7/8 节）
- Phase 5 E2E（用户裁定"直接收尾"）：
  - 四页 signoff 截图产出：SPC 11:12 / CTQ 11:23 / AOI_TT 11:26 / 自动预警 11:35
  - 自动预警看板完成 CTQ 模式切换与 top10/bottom 明细截图；服务端日志 0 Traceback；
    console 仅有公司网络固有噪音（healthz/host-config/metrics）
  - 单页耗时 3-11 分钟，主要为查询+plotly 渲染
  - 【有意排除】「刷新缓存」按钮点击验证未执行——用户评估现有证据后决定跳过
- 全量 pytest 最终态：7 failed / 414 passed（7 个失败均为既有基线，与本任务无关）
