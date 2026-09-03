# Findings & Decisions: 报表制造事实日期前推

## Requirements

- 客户报表显示日期比源制造事实日期晚四天，统一由 `config/global.yaml` 开关控制。
- 覆盖 equipment、indicator、yield、inline 四个 domain。
- 三个月原始快照从截止日往前第三个月的第一天加载至截止日。
- 内部服务可关闭前推查看真实日期；不复制业务源码。
- 完成 development-flow 并迭代到 E2E 通过。

## Research Findings

- 仓库没有 `.codegraph/`，已按指令使用 `rg` 和定向读取。
- Yield `PanelRepository` 维护增量 Parquet，按 `warehousing_time` 判断新鲜、裁剪和过滤；`array_input_time` 是同一分析中的相关绝对时间。
- Inline 共享 `InlineMeasurementSnapshotRepository` 为 SPC/CTQ/AOI_TT 提供原始 `start_time`；当前起点为 `end - relativedelta(months=3)` 同日。
- AOI_RS 有独立产品快照和元数据，内部 `_filter_window` 按 `start_time` 过滤；同样使用三个月前同日。
- Q-Time 按 `[start_time,end_time)` 查询 varchar `timekey`；IJP 直接查询明细并按 `GLASS_START_TIME` 聚合 `day`，筛选选项还会查询 `EVENT_TIME`。
- Equipment 查询 90 天真实事实，但应用服务先用 `measurement_max_age_days=3` 按 `glass_start_time` 判新鲜，再用仿造快照补缺。
- 原始快照策略 ADR 要求缓存跨原生 payload；因此把显示时间写回 Parquet 会破坏既有边界。
- 当前工作树基于干净 master 的独立 worktree `D:/wzy/Python/vivo-project-data-forward`；主工作区的任务、Excel 和其他文档改动不会被覆盖。

## Technical Decisions

| Decision | Rationale |
|---|---|
| 共享不可变策略 + 纯函数 | 四域一致、易固定时钟测试，不把配置/Streamlit 依赖扩散到 core。 |
| 快照保存源时间，读取返回副本平移 | 同一快照支持客户/内部模式且不会累加偏移。 |
| 日期列映射显式声明 | 防止误平移日志、TTL、人工修饰或非制造元数据。 |
| 配置缺失默认关闭，启用配置严格校验 | 保持升级兼容，同时避免生产配置拼写错误被静默忽略。 |
| 缓存签名包含策略签名 | 防止开关或天数变化后复用旧派生 payload。 |

## Risks and Boundaries

- 只平移输出、不换算直接查询窗口会丢失头部四天；必须两步执行。
- 查询/快照中可能包含平移后晚于显示截止日的记录；必须在显示边界二次过滤。
- Equipment 若先平移再判新鲜，会因 `+4 天` 产生未来时间误判；顺序固定为源时间新鲜度 → 平移。
- IJP 日聚合若只修改明细会与趋势分桶不一致；两者必须使用相同显示日。
- 全量 pytest 可能改写受版本控制的 Excel 工作簿；测试前后需检查并仅恢复本 worktree 中测试产生的变化。

## Resources

- `D:/wzy/Python/vivo-project/docs/dev_docs/dev_spec/others/task-data_forward.md`
- `D:/wzy/Python/vivo-project/docs/PRD/PRD-2026-09-02-报表数据日期前推.md`
- `.scratch/data-forward/issues/01-report-data-forward.md`
- `ARCHITECTURE.md`, `CONTEXT.md`, `references/domain/GLOSSARY.md`
- ADR-0001、ADR-0012、ADR-0015

## Visual/Browser Findings

- 本阶段未读取新的图片或网页；E2E 将使用仓库现有 Streamlit fixture + Playwright 模式，产物写入 `output/test-results/data-forward/`。

