# Task Plan：Inline Sheet OOS 修饰刷新与决策持久化

- Issue：`.scratch/inline-sheet-oos-refresh/issues/01-sheet-oos-decoration-refresh-decision-persistence.md`（ready-for-agent）
- PRD：`docs/PRD/PRD-2026-08-18-Inline-Sheet-OOS修饰刷新与决策持久化.md`
- 开发分支/worktree：新建 worktree + 分支 `feat/inline-sheet-oos-refresh`（见 Phase 0）
- 批准记录：用户 2026-08-18 在计划呈现后指示“请继续完成任务”，按推荐默认批准：
  worktree 于主仓同级目录 `../vivo-project-inline-sheet-oos-refresh`，
  分支 `feat/inline-sheet-oos-refresh`，含 UI 验收全量执行。

## Goal

建立可预测的 Sheet OOS 修饰生命周期：页头“刷新数据”L1 全成功后失效产品 L2；
修饰工作簿拆分为 产品 sheet（当前明细）/ `<产品>__flags`（决策台账）/ `__refresh_meta__`
（生成状态）；以 4h 周期 + 产品 revision + 决策内容签名做生成门控；多 sheet 原子写入
且仅在成功后才更新 meta 与页面状态。人工决策跨窗口不丢失，普通 rerun 不重写工作簿。

## 已确认设计（PRD 定案，无需新增决策）

- 决策 sheet 命名 `<产品>__flags`；内部 meta sheet `__refresh_meta__`（同工作簿，非 sidecar）。
- 上传语义 = 该产品完整决策集覆盖；空表 = 清空显式决策。
- 4h 自最近一次成功生成起算；L1 保持 8h TTL，不加 revision 文件、不入页面签名。
- meta 存共享产品 revision（`page_header.get_product_cache_revision`），不存页面 base signature。
- 决策签名两阶段：`file_stat_signature=(mtime_ns,size)` 探针 → 变化时重读 `__flags`
  规范化内容 SHA-256；探针结果按 file_stat 缓存，避免反复启动 COM。
- 企业加密 COM 整体重写为明文的既有行为不变，但日志明确记录。

## 公开接口变化

1. `excel_tools.py`：新增 `WorkbookWriteResult(written, path, updated_sheets, error)` 与
   多 sheet 原子写函数（临时文件验证后替换 + 进程内锁）；`replace_workbook_sheet` 保留
   旧签名委托新实现（调用方兼容），但修饰链路改用明确结果。
2. `sheet_oos_decoration.py`：新增 `RefreshDecision`、`should_regenerate_detail()`、
   决策 sheet 读写/迁移/签名函数；`SheetOosDecorationResult` 新增
   `decision_sheet`/`decision_df`/`refresh_reason`（既有字段保留）。
3. `decorated_data.py` / `decorated_features.py`：分离“计算/读取决策”与“允许持久化”，
   `fetch_decorated_features` 接受决策签名参数，仅在门控通过时写工作簿。
4. `page_header.py`：`_refresh_data_callback` 全成功后 `invalidate_page_cache(product_code=...)`。
5. `ctq_service.py`：外层缓存补 `ttl=4h`。

## Checklist（TDD：每切片先写/改测试）

### Phase 0 — 基线与 worktree
- [x] 0.1 创建 worktree + 分支 `feat/inline-sheet-oos-refresh`；确认测试运行方式
  （worktree cwd + 主仓 `.venv` python，必要时 PYTHONPATH 指 worktree `src`）；
  验证：`git worktree list` + 一条收集命令成功。
- [x] 0.2 记录基线：`pytest tests/unit -q`（对照既有失败基线）；
  验证：输出存档于 progress.md。

### Phase 1 — 页头“刷新数据”L1+L2 契约（tracer bullet）
- [x] 1.1 测试先行：全成功 → 调用产品级 invalidation 且提示含“L1 快照与 L2 缓存已刷新”；
  任一失败 → 不 invalidation、提示失败；无产品作用域 → 保持函数清理路径；
  不触发模块卸载/配置重读。验证：`tests/unit` 页头测试先红。
- [x] 1.2 实现 `_refresh_data_callback` 改造；验证：转绿。
  对应 AC：11.1 全部。

### Phase 2 — Excel 写入契约（WorkbookWriteResult + 原子写 + 锁）
- [x] 2.1 测试先行：成功写、PermissionError、COM 失败、临时保存失败、替换失败、
  多 sheet 单事务（任一失败无“部分新明细+新 meta”）、其他 sheet 保留；
  验证：先红。
- [x] 2.2 实现多 sheet 原子写（同目录临时文件 + openpyxl 可读性验证 + 原子替换 +
  进程内锁；加密回退 COM 读全表后临时文件整体重写并日志记录）；
  `replace_workbook_sheet` 委托保持兼容；验证：转绿。
  对应 AC：11.4 全部。

### Phase 3 — 决策 sheet / meta / 迁移 / 生成判定（core）
- [x] 3.1 测试先行：`should_regenerate_detail` 全分支（missing / ttl 边界 / revision 变化 /
  决策变化 / unchanged）；验证：先红。
- [x] 3.2 测试先行：旧表迁移（全部 flag 保留、重复键取最后、空表、幂等、失败保留原文件）、
  当前明细 LEFT JOIN 决策（消失/重现恢复 flag、历史键不进当前明细）；
  验证：先红。
- [x] 3.3 实现 core 层；验证：转绿。
  对应 AC：11.2 全部、11.3 之 revision/决策分支。

### Phase 4 — 决策签名与共享缓存接线
- [x] 4.1 测试先行：行序变化不改 hash；flag 变化改 hash；产品明细 sheet 变化不改决策 hash；
  file_stat 未变不重读（COM 不被反复调用）；`__flags` 不可读显式失败。验证：先红。
- [x] 4.2 `fetch_decorated_features`/`prepare_decorated_data` 改造：persist 由门控决定，
  决策签名入缓存键；SPC/CTQ 页面传入共享 revision + 决策签名；验证：转绿。
  对应 AC：11.3 之 rerun/重启/淘汰/自写 mtime/其他产品 sheet 分支。

### Phase 5 — 管理员下载/上传
- [x] 5.1 测试先行（service 层可测部分）：下载含“当前明细+决策台账”两 sheet；
  上传完整集覆盖 `__flags`、兼容旧单 sheet、键列/flag 枚举/重复键校验、
  内容一致不重写、失败不显成功不更新 meta；验证：先红。
- [x] 5.2 `spc_dashboard.py` 管理区改造（CTQ 页面如有同等区块一并处理）；验证：转绿。
  对应 AC：11.2 兼容项与 11.4 上传项。

### Phase 6 — CTQ/SPC/Monitor 4h 可达性
- [x] 6.1 测试先行：CTQ 外层缓存带 4h TTL（时钟测试：4h 后再次进入共享判定）；
  验证：先红。
- [x] 6.2 `ctq_service.py` 补 TTL；核查 SPC/Monitor 外层 TTL ≤ 4h；验证：转绿。

### Phase 7 — 回归与验收
- [ ] 7.1 集成测试：临时多 sheet 工作簿迁移；改 `__flags` 后 payload 同步；系统写明细
  mtime 变化无二次写；模拟 `force_refresh` 成功推进 revision、失败保留旧 L2。
- [ ] 7.2 全量 `pytest tests/unit -q` 无新失败（对照 0.2 基线）+ `tests/integration`。
- [ ] 7.3 SPC 报表 smoke + 管理员 UI 验收（浏览器）：刷新数据提示、下载双 sheet、
  上传生效、占用显失败、连续 rerun 不改 mtime；产物存 `output/test-results/`。
  （真实 `resources/` 工作簿操作前先备份，验收后恢复。）

### Phase 8 — 沉淀（模块 4）
- [ ] 8.1 `docs/ADR/` 新增 ADR；issue 勾选 AC、状态 → complete。

## 非目标（与 issue 一致）

OOS 判定/截断算法/flag 解析、L1 8h TTL 与快照策略、`spc_cpk_cpm_decoration.xlsx`、
AOI_TT/AOI_RS 工作簿迁移、历史 OOS 事实仓库、用户身份/审批/审计库。

## Errors Encountered

| Error | Attempt | Resolution |
|---|---|---|
| （暂无） | | |
