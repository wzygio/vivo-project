# Findings & Decisions: AOI_RS 本地快照

## Requirements

- 仿照 AOI_TT 为 AOI_RS 增加本地数据快照。
- 先判断 AOI_RS DAO 是否适合并入共享 measurement；不适合则建立专属 infrastructure。
- 保持三厂表/视图映射、字段归一和既有提取逻辑。
- 用户已批准专属 infrastructure 方案，并授权计划生成后直接开发。

## Research findings

- AOI_TT 读取 `eda.spc_tzbjx_{array,oled,tsp}` 连续量测事实，经共享 measurement 快照提供 `param_name/param_value` 等稳定字段。
- AOI_RS 读取 `eda.spc_tzbjx_rs_{array,oled,tsp}` 的 RS Code 计数事实，字段为 `rs_code/code_qty`；趋势和 By Lot 还需要独立过货视图形成分母。
- 两条链路都按三厂三元组统一 sheet/glass 和时间字段，并通过产品字典过滤，但“提取形态相似”不足以建立同一事实契约。
- ADR-0012 只允许同源原始连续量测在 measurement 共享，且要求派生规则不回写共享快照。
- ADR-0007 曾因数据量较小拒绝 AOI_RS Parquet；本任务是明确的新需求，新 ADR 需要部分取代该决定。
- AOI_RS service 当前直接导入 infrastructure loader，是接入可替换仓储前需要纠正的依赖边界。
- 页面已有产品级 Streamlit cache revision，但没有 AOI_RS `refresh_handlers`，因此管理员“刷新数据”目前只提示无独立任务。
- 当前工作树已有四个修饰工作簿变更，归用户所有，必须保持原样。

## Technical decisions

| Decision | Rationale |
|---|---|
| 专属 AOI_RS snapshot repository | 保持 RS 事实内聚，避免污染 measurement 连续量测 schema |
| 明细/分母分文件、同一产品目录 | 两类事实 schema 不同，独立失效与降级更可诊断 |
| sidecar 保存策略版本与覆盖结束日期 | 空数据或生产停线时不能仅靠最大 `start_time` 判断快照覆盖范围 |
| 仓储读取后再裁剪请求窗口 | 数据库用三个月滚动窗口刷新，页面仍保持上一自然月起的现有输出 |
| 规格不快照 | 属于小型元数据，且 issue 明确排除；避免把易变规格和原始事实绑在同一 TTL 中 |

## Issues encountered

| Issue | Resolution |
|---|---|
| issue/ADR 引用的旧领域 spec 路径已失效 | 使用 `references/index.md` 路由和文件检索，定位现路径 `references/domain/Inline_domain/spec-data_source-aoi_rs.md` |
| development-testing 要求的 `references/dev_references/coding_spec` 不存在 | 记录为仓库缺失；继续遵循根 AGENTS、现有 pyproject/tooling、ADR 与 ECC common+python 规则，不创建未经请求的本地规范副本 |

## Resources

- `.scratch/aoi-rs-local-snapshot/issues/01-add-aoi-rs-local-snapshot.md`
- `docs/ADR/0007-aoi-rs-report-data-source-and-counting.md`
- `docs/ADR/0012-shared-inline-measurement-snapshot.md`
- `references/domain/Inline_domain/spec-data_source-aoi_rs.md`
- `src/inline_domain/infrastructure/measurement/measurement_snapshot_repository.py`
- `src/inline_domain/infrastructure/aoi_rs/data_loader.py`
- `src/inline_domain/application/aoi_rs/aoi_rs_service.py`
- `app/pages/AOI_RS监控报表.py`
- `pyproject.toml`（下一步确认仓库实际 lint/test 命令）

## Visual/browser findings

- 现有 8503 长驻进程缓存旧 `composition` 模块，页面热重载先执行新页面 import、后执行共享页头 deep reload，导致新增组合根符号在旧进程中 ImportError；保留该进程不动，另起 8504 冷启动进程后页面正常。这是现有热重载顺序限制，不是部署冷启动错误。
- 8504 冷启动页面正常：M626/ARRAY 数据加载，选择站点 11629 后自动带出 4 个 Code，查询渲染 12 张 Plotly 图（4 Code × 月周天/By Lot/By Sheet），无 traceback。
- 冷启动生成 `data/M626/aoi_rs_details_M626.parquet` 与 `aoi_rs_pass_through_M626.parquet` 及各自 sidecar；管理员“刷新数据”更新明细快照 mtime，页面上 12 图保持，证明未隐式清除 Streamlit payload。
- 管理员“刷新缓存”更新产品 revision，页面重载后 heading 存在且无 alert/traceback。
- 768×900 viewport：`bodyScrollWidth == documentElement.scrollWidth == innerWidth == 768`，12 图均存在，无水平溢出或 traceback。
- 宽/窄截图视觉检查：标题、产品与管理员卡片、筛选门控、Code chips、分组标题和三列图表均无遮挡或裁切；窄屏三列图表较紧凑但仍在视口内且标签可辨。
- 浏览器控制台 5 个错误均为 Streamlit 子路径 `_stcore/health`/`host-config` 404 和被环境阻止的 `data.streamlit.io/metrics.json`，与页面业务代码无关；服务器日志无应用异常。
- QA 证据位于 `output/tmp/aoi-rs-local-snapshot-qa/`：`cold-start.yaml`、`query-result.yaml/png`、`after-data-refresh.yaml`、`after-cache-refresh.yaml`、`narrow-768.yaml/png` 与 Streamlit 日志。
- 实机查询会经企业 Excel 组件保存 AOI_RS 修饰工作簿，导致加密二进制重新封装。通过 file-decryption 对比当前文件与 Git HEAD 后，确认二者 sheet 名、9 个字段及两行业务数据完全一致；审计临时副本已清理。其余三个用户工作簿未由本任务触碰。
