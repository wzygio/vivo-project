# Progress Log

## Session: 2026-08-13

### Phase 1: Requirements and architecture assessment

- **Status:** complete
- Actions taken:
  - 阅读项目 Harness、架构、领域词汇、SPC/AOI_TT 数据源规范和 ADR-0001。
  - 比较 SPC、CTQ、AOI_TT SQL、字段、去重、参数识别、快照和应用服务调用流。
  - 通过 hexagonal architecture 评估确认共享 raw adapter + 独立 consumer adapters 的设计。
  - 创建并 triage 本地 issue 至 `ready-for-agent`。
- Files created/modified:
  - `.scratch/inline-measurement-snapshot/issues/01-centralize-inline-measurement-snapshot.md`

### Phase 2: Approved implementation plan

- **Status:** complete
- Actions taken:
  - 创建隔离 planning 目录和 acceptance checklist。
  - 记录用户条件式批准、TDD 次序、E2E 完成门及 dirty-worktree 保护边界。
- Files created/modified:
  - `.planning/.active_plan`
  - `.planning/2026-08-13-inline-measurement-snapshot/task_plan.md`
  - `.planning/2026-08-13-inline-measurement-snapshot/findings.md`
  - `.planning/2026-08-13-inline-measurement-snapshot/progress.md`

### Phase 3: TDD implementation

- **Status:** complete
- Actions taken:
  - 加载 `$tdd`、`$playwright-cli` 和 ECC common+python 的实现、测试、安全与性能规则。
  - 确认仓库没有额外 `references/dev_references/coding_spec`，采用 Harness 与 pyproject 约束。
  - 下一步先写共享 DAO 公共行为的 tracer-bullet RED test。
- Files created/modified:
  - None yet.

## Test Results

| Test | Input | Expected | Actual | Status |
|---|---|---|---|---|
| Requirements gate | issue metadata/content scan | ready-for-agent + Agent Brief + executable AC | 条件齐全 | PASS |
| Shared DAO tracer RED | `uv run pytest -q tests/unit/inline_domain/infrastructure/monitor/test_measurement_data_loader.py` | 新公共 DAO 尚不存在，测试失败 | `ModuleNotFoundError: ...measurement_data_loader` | RED confirmed |
| Shared DAO tracer GREEN | same command after minimal adapter | 三厂字段统一并保留 lot | `1 passed` | PASS |
| Shared snapshot reuse RED | `uv run pytest -q tests/unit/inline_domain/infrastructure/monitor/test_measurement_snapshot_repository.py` | repository 尚不存在 | `ModuleNotFoundError` | RED confirmed |
| Shared snapshot reuse GREEN | same command after minimal repository | 首次写入且重复读取不重复调用 DAO | `1 passed` | PASS |
| Raw snapshot policy RED | same test module with policy case | 无 policy 快照应刷新 | calls=0, expected 1 | RED confirmed |
| Raw snapshot policy GREEN | same test module after policy marker | 缺失 policy 触发刷新并写版本 | `2 passed` | PASS |
| Snapshot refresh fallback RED | same test module with failing DAO | 有历史快照时降级 | RuntimeError propagated | RED confirmed |
| Snapshot refresh fallback GREEN | same test module after fallback boundary | 刷新异常读取已有快照 | `3 passed` | PASS |
| Concurrent first read RED | same test module with 2 threads | 同产品首次读取只调用一次 DAO | calls=2, expected 1 | RED confirmed |
| Shared snapshot complete GREEN | DAO + snapshot modules | 并发合并、原子写、策略、fallback | `5 passed` | PASS |
| AOI_TT raw projection RED | `uv run pytest -q tests/unit/inline_domain/infrastructure/aoi_tt/test_aoi_tt_repository.py` | application DTO/adapter 尚不存在 | `ModuleNotFoundError: ...aoi_tt.dtos` | RED confirmed |
| AOI_TT raw projection GREEN | same adapter test after DTO/port/repository | 混合 raw 仅投影 TT pair 并保留 lot | `1 passed` | PASS |
| AOI_TT application port RED | `pytest ...test_aoi_tt_service.py -k application_data_port` | service 尚不接收 data port | unexpected keyword `_data_port` | RED confirmed |
| AOI_TT application port GREEN | same focused service test + AOI subdomain suite | service 仅经 fake port 取数据 | focused PASS; AOI suite `20 passed` | PASS |
| Shared metadata DAO RED | `pytest ...monitor/test_measurement_metadata_loader.py` | metadata module 尚不存在 | `ModuleNotFoundError` | RED confirmed |
| Shared adapters | monitor + SPC + CTQ + AOI_TT infrastructure tests | 四个边界共享 raw、各自派生 | `9 passed` | PASS |
| Related regression | Inline application/infrastructure + report pages/sections | 无回归 | `180 passed` | PASS |
| SPC smoke | `uv run python tools/smoke.py spc` | SPC/CTQ/AOI_TT/monitor smoke 全绿 | `169 passed` | PASS |
| Full pytest | `uv run pytest -q --tb=line -p no:warnings` | 记录最大安全测试集 | `382 passed, 7 failed`；失败均为既有跨域/资源状态不一致 | BASELINE |
| Browser E2E | playwright-cli against localhost:8503 | 三报表真实筛选与查询 | SPC 9 charts；CTQ 6 charts；AOI_TT 3 charts | PASS |

### Boundary cleanup follow-up: 2026-08-13

- **Status:** complete
- 用户指出 SPC/AOI_TT 旧 `data_loader.py` 仍保留 SQL；复核确认上一阶段仅迁移生产主路径，未彻底关闭兼容入口。
- 新增 AST 边界测试，RED 捕获 SPC/AOI_TT 中的 SQLAlchemy/read_sql；完成后转绿。
- 删除两个旧 data loader、重复 DTO 引用和旧 DAO 测试；SPC 专属 LOSS 过滤迁入 `measurement_preprocessor.py`。
- 将主制程履历 SQL 移入 monitor history repository；SPC trace 模块改为纯 DataFrame 变换。
- `SpcRepository` 改为 raw/metadata/history 三端口强制注入，删除旧 SPC 快照及 SQL 回退分支。
- 验证：定向 50 passed；SPC smoke 161 passed；全量 374 passed / 7 个相同基线失败；SPC/CTQ/AOI_TT 浏览器 E2E 通过。

### Phase 4: Regression, integration and E2E

- **Status:** complete
- Actions taken:
  - `compileall`、定向回归与 SPC smoke 通过。
  - 全量测试的 7 个失败已隔离为非本任务基线：3 个加密 xlsx 诊断假设与当前标准 xlsx 冲突，2 个 Code selector 测试缺少既有新增参数，2 个 Yield 全局配置预期落后于当前配置。
  - 使用 `$playwright-cli` 在真实 Streamlit/数据库/快照链路完成 SPC、CTQ、AOI_TT 查询；分别渲染 9、6、3 张图，无 traceback。

### Phase 5: Architecture record and delivery gate

- **Status:** complete
- Actions taken:
  - 更新 `ARCHITECTURE.md` 的共享 raw snapshot、端口、组合根与派生适配器所有权。
  - 新增 `docs/ADR/0012-shared-inline-measurement-snapshot.md`。
  - 关闭本地 issue 和计划 checklist；完成静态边界、diff 与测试证据审查。

## Error Log

| Timestamp | Error | Attempt | Resolution |
|---|---|---:|---|
| 2026-08-13 | AOI_TT 规范旧路径不存在 | 1 | 用 `rg --files` 定位实际路径，后续使用新路径。 |
| 2026-08-13 | Shared DAO tracer test collection failed because module did not exist | 1 | 预期 RED；下一步写最小公共 DAO。 |
| 2026-08-13 | Shared snapshot reuse test collection failed because module did not exist | 1 | 预期 RED；实现最小产品快照 repository。 |
| 2026-08-13 | 并发 GREEN patch 将 `_fallback` 的 try/except 错放到 `_lock_for` 后 | 1 | 通过行号检查发现，立即移动回 `_fallback`，未运行错误实现。 |

## 5-Question Reboot Check

| Question | Answer |
|---|---|
| Where am I? | Phase 3 — TDD implementation |
| Where am I going? | Shared adapter → three consumer adapters → regression/E2E → ADR |
| What's the goal? | 同源一次提取、清晰端口、业务口径不变且 E2E 全绿 |
| What have I learned? | See findings.md |
| What have I done? | Requirements and approved plan complete |
