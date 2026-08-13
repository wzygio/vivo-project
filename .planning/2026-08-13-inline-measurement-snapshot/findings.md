# Findings & Decisions

## Requirements

- DAO 与产品级原始快照归属 `inline_domain/infrastructure/monitor`。
- SPC、CTQ、AOI_TT 的专属预处理分别归属其 infrastructure 子模块。
- 应用层经 ports 调用数据能力；Streamlit app 不直接读取快照。
- 保持业务口径并迭代到相关 E2E 通过。

## Research Findings

- 三个报表使用 `eda.spc_tzbjx_array/oled/tsp`；factory 对应 ID/时间列可统一映射。
- SPC 底层 DAO 当前未按 data_type 限制，因此 SPC/CTQ 的 Parquet 已是多类型测量集合，但缺少 AOI_TT 必需的 `lot_id`。
- `SpcRepository` 目前把 snapshot、白名单分类、异常值过滤、维度过滤和主制程追溯混在一起。
- CTQ 已调用 `SpcRepository.get_spc_measurements()` 并强制 `data_type_filter=CTQ`。
- AOI_TT 独立 SQL 使用规格表 `param_type IS NULL` 的 `(step_id,param_name)` 组合，并保留 `lot_id`。
- SPC SQL 按 `(product,sheet,step,param,site)` 保留最新行；AOI_TT 领域探查确认每 `(step,sheet,param)` 一行。迁移需保持现有去重口径并用特征测试验证。
- 共享快照当前必须含主制程追溯列；新设计需要独立 raw snapshot policy，SPC trace 成为派生层。
- 工作树已有 monitor compliance 相关改动，涉及 application/core/app 页面与测试；必须以增量修改兼容，不能还原。
- `infrastructure/monitor` 与 `infrastructure/ctq` 目录当前存在但没有 Python 文件，适合承接新 adapters，不需要移动用户已有模块。
- app 页面没有直接 `read_parquet`，但 SPC/CTQ/AOI_TT/自动预警页面直接从 infrastructure 导入查询配置 DTO；应用服务也直接构造 `SpcRepository`。需要把 DTO/ports/composition 收回 application 边界，而不是只改快照路径。
- 现有 application/spc/dtos.py 与 infrastructure/spc/data_loader.py 各自定义 `SpcQueryConfig`，且字段不完全一致；迁移需消除双定义并保留 `data_type_filter` 序列化兼容。
- 本地存在 5 个产品的旧 `spc_snapshot_<prod>.parquet`，规模约 2–10 MB；M626 约 73 万行。旧快照含 SPC trace 字段但不含 `lot_id`，不能无损转换为 AOI_TT 原始快照。
- 新快照首次生成需要数据库；为避免一次部署导致 SPC/CTQ 离线回归，可在共享 adapter 中提供只读旧快照兼容降级，但该降级不能向 AOI_TT 伪造 `lot_id`，AOI_TT 应返回明确空数据并记录诊断，直至新快照生成。
- 三个应用服务当前都在 `st.cache_data` 函数内部创建/调用具体 infrastructure；这是实际的 hexagonal dependency violation。最小清晰迁移是缓存函数接收以下划线命名的 port 参数（Streamlit 不参与 hash），query JSON 与 snapshot signature 继续作为稳定缓存键。
- 现有 service 测试已经使用 fake repository，但通过 monkeypatch 具体类构造器；可直接演进为传入 fake port，使测试更贴近公共能力而不是内部构造细节。
- 三个页面直接从 infrastructure 导入 DTO；应改为 application-owned DTO。页面的 DB manager 属于 composition root 可接受，但具体 adapter 构造应集中到 `inline_domain/composition.py`。
- SPC/CTQ 页的刷新 handler 当前借用 `MonitorAnalysisService.safe_refresh_snapshots`；应改为 composition 提供的共享 snapshot refresh capability，AOI_TT 同样注册该 handler，确保三个页面刷新同一个 raw snapshot。
- 现有异常值过滤是长 I/O+规则解析方法，且在当前 `SpcRepository` 中对所有 data_type 统一执行。为保持口径又避免复制，应先提炼为基础设施纯 adapter/helper，再由 SPC 与 CTQ repositories 显式调用；AOI_TT 不调用。
- 规格覆盖属于 SPC/CTQ 消费契约；共享 metadata DAO 只返回数据库事实，YAML 覆盖仍由 SPC/CTQ adapter 应用。
- 共享 metadata 需要两个能力：完整参数规格（含 `param_type/main_step_id/main_eqp_type`）和 IMP 参数目录（raw `data_type`）。AOI_TT 从完整规格筛 `param_type IS NULL`，SPC/CTQ 从参数目录分类。
- `references/dev_references/coding_spec` 在仓库中不存在；本任务以根 `AGENTS.md`、现有 pyproject/测试风格和 `$ecc-production-rules` common+python 为编码规范来源。
- TDD 约束要求按公共行为逐个 RED→GREEN；数据库与文件系统只在系统边界替换，不能用 mock 锁死内部调用顺序。
- ECC 安全规则要求新 SQL 参数化；迁移共享 DAO 时应消除现有产品/日期字符串插值，而不是原样复制注入风险。

## Technical Decisions

| Decision | Rationale |
|---|---|
| 原始规范列统一使用 `start_time`，消费 adapter 再映射 `sheet_start_time` | 避免共享层带 SPC 命名，同时兼容 AOI_TT。 |
| 快照以产品为键并覆盖最大三个月窗口 | 一份事实集覆盖 SPC/CTQ/AOI_TT 当前窗口。 |
| 元数据查询不与大表强行 join 成一个大 SQL | 白名单与规格表体量小且分类语义不同，避免多对多放大。 |
| 原子写采用同目录临时文件替换 | 避免并发/中断留下半成品 Parquet。 |
| composition 保持显式工厂而非 service locator | 符合 hexagonal architecture，便于 fake port 测试。 |

## Issues Encountered

| Issue | Resolution |
|---|---|
| ARCHITECTURE/CONTEXT 在当前 PowerShell 输出中出现乱码 | 只进行最小 UTF-8 patch，避免机械重写整文件。 |
| 旧 planning active 指向已完成的 SPC 主制程计划 | 为本任务创建隔离计划并切换 `.planning/.active_plan`。 |

## Resources

- Issue: `.scratch/inline-measurement-snapshot/issues/01-centralize-inline-measurement-snapshot.md`
- ADR-0001: `docs/ADR/0001-streamlit-cache-native-payload-boundary.md`
- SPC source spec: `references/domain/Inline_domain/spec-data_source-spc.md`
- AOI_TT source spec: `references/domain/Inline_domain/spec-data_source-aoi_tt.md`

## Visual/Browser Findings

- 尚未执行浏览器验证；将在 Phase 4 记录。
