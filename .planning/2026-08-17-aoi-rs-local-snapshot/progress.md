# Progress Log: AOI_RS 本地快照

## Session: 2026-08-17

### Phase 1: Requirements, architecture and plan

- **Status:** complete
- Actions taken:
  - 检查 AOI_TT/AOI_RS 页面、service、DAO、composition、共享 measurement snapshot、现有测试和 ADR。
  - 创建并 triage 本地 issue；用户批准 `enhancement` → `ready-for-agent`。
  - 评估并决定 AOI_RS 使用专属 infrastructure 快照，不并入共享 measurement。
  - 创建本计划、findings 和 progress；记录用户“生成计划后直接开发”的批准。
- Files created/modified:
  - `.scratch/aoi-rs-local-snapshot/issues/01-add-aoi-rs-local-snapshot.md`
  - `.planning/2026-08-17-aoi-rs-local-snapshot/task_plan.md`
  - `.planning/2026-08-17-aoi-rs-local-snapshot/findings.md`
  - `.planning/2026-08-17-aoi-rs-local-snapshot/progress.md`

### Phase 2: TDD tracer

- **Status:** complete
- Actions taken:
  - 已读取 development-testing、TDD 及相关测试设计资料，并加载 ECC common+python 的编码、模式、安全和测试规则。
  - 发现仓库未提供 `references/dev_references/coding_spec`，已记录并改用项目 AGENTS、ADR、现有 tooling 与 ECC 规则。
  - RED：新增仓储公共接口测试，首次运行因 `snapshot_repository` 模块不存在而在收集期失败。
  - GREEN：实现 AOI_RS 明细快照的最小公共路径；首次读取以三个月窗口加载并写 Parquet/sidecar，第二次命中新鲜快照且 loader 仅调用一次。
  - RED→GREEN：application service 改为通过 AOI_RS-owned fake port 获取明细、分母和规格；查询 DTO 迁入 application，DAO 保留兼容导出。service 全套 `7 passed`。
  - RED→GREEN：分母拥有独立产品快照；明细与分母仓储测试 `2 passed`。
  - RED→GREEN：损坏但元数据新鲜的 Parquet 会记录错误并重载修复；仓储测试 `3 passed`。
  - RED→GREEN：页面通过 composition root 注入 data port，并注册底层刷新 handler；页面定向测试通过。
  - RED→GREEN：显式刷新能区分真实刷新与 fallback；任一 loader 异常/空结果返回 False 且保留两份已有快照。仓储+页面 `6 passed`。
  - RED→GREEN：缺失覆盖日期的 sidecar 不再被 `NaT` 误判为新鲜；仓储测试 `5 passed`。
- Files created/modified:
  - `tests/unit/inline_domain/infrastructure/aoi_rs/test_aoi_rs_snapshot_repository.py`
  - `src/inline_domain/infrastructure/aoi_rs/snapshot_repository.py`
  - `src/inline_domain/application/aoi_rs/dtos.py`
  - `src/inline_domain/application/aoi_rs/ports.py`
  - `src/inline_domain/application/aoi_rs/aoi_rs_service.py`
  - `src/inline_domain/infrastructure/aoi_rs/data_loader.py`
  - `src/inline_domain/composition.py`
  - `app/pages/AOI_RS监控报表.py`
  - `tests/unit/inline_domain/application/aoi_rs/test_aoi_rs_service.py`
  - `tests/unit/app/pages/test_aoi_rs_page.py`

### Phase 3: 快照完整语义与页面刷新

- **Status:** complete
- Actions taken:
  - 补齐 TTL、策略版本、覆盖日期、并发、原子临时文件、时间裁剪和数据库失败降级矩阵。
  - RED→GREEN：产品值 SQL 注入测试证明旧 DAO 可被引号绕过；三类查询均改为 SQLAlchemy 绑定参数，DAO `5 passed`。
  - RED→GREEN：可读但缺稳定字段的旧快照不再通过 `reindex` 静默降级，改为触发重载；仓储 `12 passed`。
  - 增加 application 无 infrastructure 导入、AOI_RS 不扩展 measurement 模块的 AST 边界测试；边界 `8 passed`。
  - Phase 3 汇总：Python compileall 通过；snapshot/application/DAO/boundary/page 合计 `34 passed`，2 条既有 pandas FutureWarning。
- Files created/modified:
  - `src/inline_domain/infrastructure/aoi_rs/snapshot_repository.py`
  - `src/inline_domain/infrastructure/aoi_rs/data_loader.py`
  - `src/inline_domain/application/aoi_rs/{dtos.py,ports.py,aoi_rs_service.py}`
  - `src/inline_domain/composition.py`
  - `app/pages/AOI_RS监控报表.py`
  - 对应 AOI_RS 仓储、DAO、service、boundary、page 测试。

### Phase 4: 回归、静态边界与浏览器 QA

- **Status:** complete
- Actions taken:
  - AOI_RS 全范围 `45 passed, 6 warnings`；整个 inline_domain `173 passed, 6 warnings`；相邻页面 `8 passed`。
  - Ruff/Pyright 未安装且 pyproject 未配置相应命令；静态证据使用 compileall 与 AST 边界测试，不临时安装工具改变环境。
  - 全量 pytest 首次运行出现 8 个跨域失败：加密 Excel 诊断 3、专项资料页未接共享页头 1、Code selector 参数 2、Yield 配置预期 2；并伴随 Windows Excel COM `0x80010108` 噪声。均不涉及本任务改动，待无 traceback 模式复取精确汇总。
  - 精确 deselect 上述 8 个基线后，剩余 `451 passed, 8 deselected, 23 warnings`，exit 0；Excel COM 仍输出宿主噪声但不影响结果。
  - Playwright：8504 冷启动成功，M626/ARRAY/11629/4 Code 渲染 12 图；刷新数据更新 Parquet mtime且图保留，刷新缓存更新 revision；768px 无溢出/traceback；宽窄截图视觉通过。
  - 关闭测试浏览器和 8504 隔离进程，确认用户原有 8503 仍监听。
- Files created/modified:
  - 待持续记录。

## Test results

| Test | Command | Expected | Actual | Status |
|---|---|---|---|---|
| Planning artifact validation | 检查 issue/plan/checklist/批准记录 | 三个文件同目录、AC 全映射、批准已记录 | issue 验收项与计划 checklist 全部关闭 | pass |
| Snapshot tracer RED | `uv run pytest -q tests/unit/inline_domain/infrastructure/aoi_rs/test_aoi_rs_snapshot_repository.py` | 新行为在实现前失败 | `ModuleNotFoundError: ...snapshot_repository` | pass |
| Snapshot tracer GREEN | 同上 | 首次落盘、二次命中，loader 一次 | `1 passed` | pass |
| Application port tracer RED | service 单测单项 | `_data_port` 在实现前不被接受 | `TypeError: unexpected keyword argument '_data_port'` | pass |
| Application port tracer GREEN | service 单测单项 | fake port 产出兼容 ViewModel | `1 passed` | pass |
| AOI_RS service regression | `uv run pytest -q tests/unit/inline_domain/application/aoi_rs/test_aoi_rs_service.py` | 端口化后行为不变 | `7 passed, 2 existing warnings` | pass |
| Page composition RED | AOI_RS 页面测试单项 | 页面旧链路暴露 `_db_manager` | `KeyError: '_data_port'` | pass |
| Page composition GREEN | 同上 | 注入 data port + 一个刷新 handler | `1 passed` | pass |
| Snapshot failure/metadata slices | AOI_RS snapshot test file | 损坏恢复、刷新失败保护、sidecar 严格校验 | `5 passed` | pass |
| AOI_RS DAO injection RED | loader 单项 | 恶意 product code 不得扩展查询 | 返回 5 行，确认字符串插值漏洞 | pass |
| AOI_RS DAO injection GREEN | AOI_RS DAO 两文件 | 三类值使用绑定参数 | `5 passed` | pass |
| Phase 3 aggregate | compileall + AOI_RS infra/app/boundary/page | 编译与行为全绿 | compile exit 0；`34 passed, 2 warnings` | pass |
| AOI_RS regression | AOI_RS infra/app/core/section/page | 全部行为兼容 | `45 passed, 6 warnings` | pass |
| Inline regression | `uv run pytest -q tests/unit/inline_domain` | SPC/CTQ/AOI_TT/RS/measurement 无回归 | `173 passed, 6 warnings` | pass |
| Adjacent page regression | SPC/CTQ/AOI_TT/AOI_RS page tests | 页面接线无回归 | `8 passed` | pass |
| Full pytest attempt 1 | `uv run pytest -q` | 全量通过或精确登记既有失败 | 8 个跨域失败；Excel COM 噪声，需简化输出复取 | partial |
| Full non-baseline regression | 全量 pytest + 8 个精确 deselect | 所有非基线测试通过 | `451 passed, 8 deselected, 23 warnings` | pass |
| Playwright functional | M626/ARRAY/11629/4 Code 查询 | 页面冷启动、筛选、12 图、无 traceback | pass；证据在 `output/tmp/aoi-rs-local-snapshot-qa/` | pass |
| Playwright admin refresh | 刷新数据/刷新缓存 | Parquet mtime/revision 分别更新，页面无异常 | pass | pass |
| Playwright visual/viewport | wide + 768×900 | 无遮挡、裁切、横向溢出 | `scrollWidth=innerWidth=768`；截图通过 | pass |
| Final targeted regression | AOI_RS + 相邻页面定向套件 | 新增行为及邻接页面全绿 | `53 passed, 6 warnings` | pass |
| ADR/architecture trace | ADR-0015、ADR-0007、ARCHITECTURE 路径/术语检查 | 边界与取代关系可追溯 | `git diff --check` 与文档交叉核对通过 | pass |

## Error log

| Timestamp | Error | Attempt | Resolution |
|---|---|---:|---|
| 2026-08-17 | `references/domain/aoi_rs/spec-data_source.md` 不存在 | 1 | 经 references 路由定位到 `references/domain/Inline_domain/spec-data_source-aoi_rs.md` |
| 2026-08-17 | `references/dev_references/coding_spec` 不存在 | 1 | 记录缺失，采用仓库 AGENTS/ADR/tooling 与 ECC common+python 规则 |
| 2026-08-17 | 全量 pytest 8 个跨域失败 + Excel COM `0x80010108` | 1 | 定向套件已全绿；以 `--tb=no` 复取计数并登记任务外基线 |
| 2026-08-17 | pandas/openpyxl 不能直接读取企业加密的 AOI_RS 修饰工作簿，Excel COM 会话又出现宿主噪声 | 1 | 使用 file-decryption 工具分别解密当前文件与 Git HEAD；sheet、9 个字段和两行业务数据一致，确认仅为加密二进制重新封装；临时副本已清理 |

### Phase 5: 文档沉淀与交付

- **Status:** complete
- Actions taken:
  - 新增 ADR-0015，记录 AOI_RS 专属双快照、端口/组合根、刷新与降级语义；仅部分取代 ADR-0007 Decision 8，ADR-0012 不变。
  - 更新 `ARCHITECTURE.md` 的 AOI_RS 数据流和所有权说明，并逐项关闭 issue/plan 验收项。
  - 审计工作树与企业加密工作簿：其余三个用户工作簿未由本任务改动；AOI_RS 工作簿在实机 QA 中被加密组件重新封装，解密后的可见内容与 HEAD 一致。
- Files created/modified:
  - `docs/ADR/0015-aoi-rs-product-local-snapshot.md`
  - `docs/ADR/0007-aoi-rs-report-data-source-and-counting.md`
  - `ARCHITECTURE.md`
  - `.scratch/aoi-rs-local-snapshot/issues/01-add-aoi-rs-local-snapshot.md`
  - `.planning/2026-08-17-aoi-rs-local-snapshot/{task_plan,findings,progress}.md`

## 5-question reboot check

| Question | Answer |
|---|---|
| Where am I? | Complete |
| Where am I going? | 交付给维护者；既有常驻 Streamlit 进程需刷新缓存或重启一次以加载新 composition 符号 |
| What's the goal? | AOI_RS 专属产品级 Parquet 快照与安全降级，不改变现有报表口径 |
| What have I learned? | 见 `findings.md` |
| What have I done? | 实现、自动化回归、浏览器 QA、ADR、架构文档与验收闭环均已完成 |
