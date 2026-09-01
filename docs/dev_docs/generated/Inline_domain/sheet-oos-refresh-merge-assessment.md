# Inline Sheet OOS 修饰刷新分支：合并评估报告

- 评估日期：2026-08-27
- 评估对象：worktree 分支 `feat/inline-sheet-oos-refresh`（HEAD = `62e35f1`，基于 `e1f0af4`，相对 master 单提交、+3276/−116 行、32 个文件）
- 评估依据：`docs/PRD/PRD-2026-08-18-Inline-Sheet-OOS修饰刷新与决策持久化.md`、`references/domain/Inline_domain/sheet-oos-decoration-mechanism.md`
- 评估方式：PRD 逐条对照评审（核心层 / 应用层）+ 与 master 的 merge-tree 冲突分析 + 测试运行验证

## 0. 总体结论

**当前不能直接合并，需要"先修缺陷、再 rebase 适配"两步走。**

- **实现质量**：PRD 主线（三 sheet 结构、决策持久化、两阶段签名、4h 生成门控、原子写契约）实现忠实，**分支测试全绿**（改动相关 13 个测试文件 123 passed；`tests/unit/inline_domain` 整目录 223 passed，0 失败 0 跳过）。
- **但存在 4 个高严重度缺陷**，集中在企业加密工作簿路径和生产链路接线，会直接破坏 PRD 的核心承诺（决策不丢失、失败可感知）。
- **与 master 的合并存在真实冲突**：master 已前进 15+ 提交（TTL 配置化、PPA 快照层修正、自动预警中心、app 目录重构、resources 按域拆分），merge-tree 显示 6 个内容冲突 + 1 个目录改名冲突 + 1 个 modify/delete，其中 3 处是**语义冲突**而非机械冲突。

## 1. 测试验证结果

| 运行范围 | 结果 |
|---|---|
| 改动相关 13 个测试文件 | 123 passed / 0 failed / 0 skipped |
| `tests/unit/inline_domain` 整目录回归 | 223 passed / 0 failed / 0 skipped |

- 集成测试全部使用 `tmp_path` 临时工作簿，未触碰 `resources/` 真实文件。
- 既有测试 `test_cpk_decoration.py` 的 COM 回退用例打印 `RPC_E_DISCONNECTED` faulthandler 噪声，属预期行为，测试本身通过。
- 注意：本次验证在项目 `.venv` 内由 uv 现场创建环境完成，未做全局安装。

## 2. 必须修正的缺陷（合并前）

### 2.1 【高】企业加密工作簿路径：生成门控失效 + `__flags` 会被系统覆写（✅ 已修复 2026-08-28，见 §6）

- 位置：`src/inline_domain/core/shared/sheet_oos_decoration.py:354-356, 642-643, 672-674`，`migrate_legacy_flags_if_needed`（`:431-463`）
- 问题：`_workbook_sheet_names` 对加密工作簿返回 `None` → `current_sheet_exists` 恒为 False → `should_regenerate_detail` 恒返回 `missing` → **每次调用都重写**，PRD 4.2/11.3 生成门控对加密文件完全失效；同时每次 persist 都用旧产品 sheet 的迁移结果**覆写 `__flags`**，管理员对决策台账的编辑会被系统冲掉。而真实 SPC 工作簿当前正是企业加密文件。
- 无测试覆盖 `sheet_names=None` 路径。
- 修正方向：通过 COM 实际探测 `__flags` 是否存在并优先读取；`names=None` 时不得无条件走旧表迁移。

**"每次 persist 都用旧产品 sheet 的迁移结果覆写 `__flags`"的具体含义**（执行链如下）：

1. `_workbook_sheet_names()`（`sheet_oos_decoration.py:347-360`）用 openpyxl 列举 sheet 名；企业加密文件 openpyxl 打不开，返回 `None`。
2. `migrate_legacy_flags_if_needed()`（`:447-453`）的两个守卫都是 `names is not None and ...` 形式。`names=None` 时两个守卫都不成立，代码直接落到 `:452-453` 的兜底分支——**从旧产品 sheet（如 `M678`）提取键列+flag 作为决策来源**。也就是说：即使 `M678__flags` 已经存在、且管理员刚在里面改过 flag，这一步也完全无视它，只认产品明细 sheet 里那份"上次系统生成时留下的旧 flag"。
3. `_persist_sheet_oos_decoration()` 里 `decision_sheet_exists = sheet_names is not None and ...`（`:642`），`names=None` 时恒为 `False`。于是每次进入写入分支（`:670-674`）都会执行 `sheets_to_write[decision_sheet] = decisions`——把第 2 步那份"从旧产品 sheet 迁移来的决策"当作首次迁移结果，**整个覆写 `M678__flags`**。
4. 同时 `current_sheet_exists` 也恒为 `False`（`:643`），`should_regenerate_detail` 恒返回 `missing`，门控失效，**每次调用都重写**，第 3 步的覆写因此每次都会发生。

净效果：只要工作簿处于加密状态，管理员对 `__flags` 的任何直接编辑都会在下一次 persist 时被系统用旧产品 sheet 里的过时 flag 冲掉；历史键（已不在当前明细中的决策）也同样丢失，因为产品 sheet 只保存当前明细。

**"names=None 时不得无条件走旧表迁移"的含义**：`names=None` 只说明"openpyxl 列举不了 sheet"，**并不代表 `__flags` 不存在**。正确做法是把它当作"未知"而非"不存在"：走 COM 回退实际探测/读取 `__flags`——读得到就优先用它作为决策来源（不再迁移、不再覆写）；读不到且确认 sheet 缺失时才走旧表迁移；读取失败则按 PRD 5.4 显式抛错，不得静默降级。

注意缓解因素：新代码首次成功写入时，COM 回退会把整个工作簿重写为明文 xlsx，之后 openpyxl 就能正常列举 sheet，此缺陷暂时消失。但只要文件再次处于加密状态（例如管理员重新上传了加密工作簿），问题立即复发。

### 2.2 【高】决策台账下载恒空，覆盖语义上传可清空全部决策（✅ 已修复 2026-08-28，见 §6）

- 位置：`src/inline_domain/application/shared/decorated_features.py:175-179`、`spc_service.py:147-152`、`ctq_service.py:63-71`、`app/sections/spc/sheet_oos_admin.py:74-76`
- 问题：`fetch_decorated_features` 的缓存 payload 只带 `decoration_df/decoration_path/decoration_sheet`，**不含 `decision_df`/`refresh_reason`**；service 重建 ViewModel 时这些字段全丢。后果：
  - 管理员下载的"决策台账"永远是空表（违反 PRD 5.9）；
  - 在"上传即完整决策集覆盖"语义下，下载（空台账）再上传会**清空该产品全部显式决策**，且界面无提示——数据丢失脚枪；
  - 管理界面"本次载荷重建原因"永远不显示（PRD §8 可观测性打折扣）。
- 修正方向：payload 与两个 service 的 `_view_model_from_payload` 携带 `decision_df`（ADR-0001 下 DataFrame 可过缓存边界，无技术障碍）。

### 2.3 【高】页头"刷新数据"存在假成功路径（✅ 已修复 2026-08-28，见 §6）

- 位置：`app/components/page_header.py:128-144`、`src/inline_domain/composition.py:104-109`、`src/inline_domain/infrastructure/measurement/measurement_snapshot_repository.py:70-71`
- 问题：`refresh_raw_measurements` 以 `not result.empty` 当成功信号；L1 仓储 DB 失败时降级返回旧快照（非空），被报为成功并推进 revision、失效 L2——PRD 11.1"任一失败则不失效"被架空。monitor 侧 `safe_refresh_snapshots`（`monitor_service.py:694-710`）`success_flag` 恒 True，同类问题。
- 修正方向：composition 层区分"刷新成功 / 降级旧快照 / 真空数据"三种结果，仅真成功才推进 revision。

### 2.4 【高】加密 + `__flags` 缺失时，两个决策读取入口行为矛盾（✅ 已修复 2026-08-28，见 §6）

- 位置：`sheet_oos_decoration.py:380-382`（`load_sheet_oos_decisions`）vs `:452-453`（`migrate_legacy_flags_if_needed`）
- 问题：前者因 `names is None` 跳过存在性检查，COM 读缺失 sheet 抛 `SheetOosDecorationReadError`（违反其自身"决策 sheet 不存在返回空台账"契约）；后者无条件走旧表迁移。全新加密工作簿的首次签名读取会向页面抛错而非优雅走空签名。
- 修正方向：与明文路径语义对齐，返回空台账；两个入口行为统一。

### 2.5 中低严重度问题（建议同批修正）

| 严重度 | 问题 | 位置 |
|---|---|---|
| 中 | `SheetOosDecorationWriteError` 被 service 泛化 `except` 吞成空 payload，页面显示"暂无数据"而非"文件被占用请关闭 Excel"（违反 11.4 失败可见性） | `spc_service.py:278-280`、`ctq_service.py:135-137` |
| 中 | `get_cached_alarm_detail_tables` 无 TTL 且缓存键缺 revision/决策签名，可永久遮挡共享 4h 判定；未纳入 TTL 结构测试 | `app/sections/monitor/monitor_dashboard.py:422` |
| 中 | meta 读-改-写在进程内锁之外，同工作簿多产品并发写可能丢失更新；多进程文件级锁未实现（需确认部署形态） | `sheet_oos_decoration.py:684` vs `excel_tools.py:141-142` |
| 中 | 首次迁移时 meta 落 `"empty"` 签名，下一轮真实 hash 触发一次多余重写 | 应用层 `get_decision_signature` 与 core 落盘值未对齐 |
| 低 | `_refresh_data_callback` 未捕获 handler 异常（fail-safe 但无失败提示） | `page_header.py:129-133` |
| 低 | `should_regenerate_detail` naive/aware datetime 混用无防御；决策签名对重复键不去重；`__flags` 缺列静默降级无告警 | `sheet_oos_decoration.py:577, 401-428, 398` |
| 低 | `_cached_decision_signature` 带 4h TTL，到期后即使 file_stat 未变也会重读 `__flags`（可能重启 Excel COM），略超 PRD 规则 2 | `decision_signature.py:44` |

## 3. 与 master 的合并冲突评估

master 自 merge-base 以来前进 15+ 提交，其中与本分支强相关：`680d2c0`（TTL 配置化）、`a96b4b1`（PPA 修正移到快照层）、`c3f284e`（自动预警中心）、`f3d2256`（app 目录重构）、`32c9c89`（resources 按域拆分）。

### 3.1 语义冲突（合并时最容易"合完看起来能跑但其实错了"）

1. **【最大风险】master 新增的共享后台 `app/sections/inline_domain/shared/decoration_admin.py` 仍是旧写模型**：上传直接覆盖产品明细 sheet 的 flag 列 + 全局 `st.cache_data.clear()`，与本分支"只写 `__flags`、绝不碰明细"的持久化不变量正面矛盾。合并时若顺手采用 master 版 UI，决策持久化被静默绕过。**适配**：保留 master 的共享 UI 壳，写入路径换成本分支 `sheet_oos_admin.py` 的纯逻辑层。
2. **clip_rules 链路必须整条摘除**：master 已把 `_apply_clip_rules`/`clip_rules` 从 core 和 `decorated_data.py` 完全删除（PPA 修正上移到快照层，`git grep clip_rules master -- src/` 零命中）；本分支仍保留 3 处引用。rebase 后必须跟随 master 删除，否则 TypeError。注意本分支 core 文件 +505 行中仍含 `_apply_clip_rules`，合并时极易把 master 的删除"撤销"回去。
3. **`excel_tools.py` 两套原子写实现择优**：master 实现单 sheet 暂存 + `.bak` 备份 + 表头校验、返回 bool；本分支实现多 sheet 事务 + `WorkbookWriteResult` + 进程锁 + 回读验证。本分支版本是 `__flags`+明细+meta 原子提交的设计前提，**必须以本分支为底**，可移植 master 的 `.bak` 备份与表头校验。
4. **既有决策数据搁浅风险**：master 把修饰工作簿迁到 `resources/inline_domain/`（master 侧副本不含 `__flags`/`__refresh_meta__`）。若本分支试用期间已在旧路径积累真实用户决策，rebase 后需一次性数据迁移把旧文件 `__flags` 并入新路径文件。

### 3.2 机械冲突（按方叠加即可）

- `ARCHITECTURE.md`：双方改不同段落，取 master 补本分支门控段落。
- `app/pages/SPC监控报表.py`、`CTQ监控报表.py`：以 master 为底，把本分支的 `get_product_cache_revision` + `get_scope_decision_signature` 参数块重放进调用处。
- `spc_service.py`：master 配置化 TTL + 本分支两个缓存键参数，直接叠加；本分支 `ctq_service.py` 硬编码的 `ttl=4*60*60` 应并入 master 的 `ConfigLoader.get_service_cache_ttl_seconds()` 体系。
- `tests/unit/test_excel_tools_workbook_sheets.py`：保留本分支 8 个新测试，移植 master 的锁定工作簿用例。

### 3.3 位置映射与资源搬迁

- `app/sections/spc/spc_dashboard.py` → master 新位置 `app/sections/inline_domain/spc/spc_dashboard.py` 且被重写（715 行变动）：本分支 58 行修改需**人工移植**，不能靠 git 自动合并。
- 本分支新增的 `sheet_oos_admin.py` 应落到 `app/sections/inline_domain/spc/`。
- `resources/override_rates.xlsx`（modify/delete）实质是 master 搬迁至 `resources/yield_domain/`；本分支对 `override_rates.xlsx`、`scrap_sheets.xlsx`、`入库不良率规格.xlsx` 的二进制修改需重新应用到新路径。
- `app/components/page_header.py` master 未动，本分支改动直接保留。

### 3.4 确认无风险项

- master 的自动预警中心消费的是**同一个** `fetch_spc_report_payload` 缓存 payload，不绕过 4h 门控，无需额外接入签名机制；其 `_is_false_flag` 语义与本分支 `_parse_flag` 已对齐。
- TTL 数值两侧一致（4h），master 管配置化、本分支管缓存键与门控，语义互补。

**合并工作量估算**：约 1 个人日。6 个文本冲突中 4 个机械；重活三处——excel_tools 原子写择优合并、spc_dashboard 人工移植、decoration_admin 写模型替换。

## 4. PRD 符合性摘要

| PRD 条目 | 结论 | 备注 |
|---|---|---|
| 5.1 三 sheet 结构 / meta schema | 符合 | 列结构与 schema 完全一致 |
| 5.2 旧工作簿迁移 | 部分符合 | 明文路径符合（幂等、保留全部 flag、单事务）；加密路径有缺陷 2.1/2.4 |
| 5.3 合并语义（LEFT JOIN + 默认 True） | 符合 | 历史键不进当前 sheet，测试覆盖消失/重现恢复 |
| 5.4 两阶段签名 | 符合 | mtime 只作探针；`__flags` 不可读显式失败；有 2.4 与低severity偏差 |
| 5.5 `should_regenerate_detail` 纯函数 | 符合 | 五枚举齐全，revision/decision 优先于 TTL |
| 5.6 刷新数据 L1+L2 | 部分符合 | 主链路符合；假成功缺陷 2.3 |
| 5.7 刷新缓存 hard reset | 符合 | 保留模块重载与配置重读 |
| 5.8 4h 可达性 | 基本符合 | SPC/CTQ/Monitor 主缓存已对齐；`get_cached_alarm_detail_tables` 漏网 |
| 5.9 管理员下载/上传 | 部分符合 | 纯逻辑层全部符合；生产链路 `decision_df` 丢失（缺陷 2.2） |
| 5.10 写入契约与原子性 | 符合 | `WorkbookWriteResult`、临时文件验证 + 原子替换、失败不更新 meta；锁粒度有保留 |
| 4.3 被拒绝设计残留检查 | 符合 | 无 mtime 直接入键、无 outer join、无纯内存生成状态 |

## 5. 建议行动顺序

1. ~~在本分支先修 4 个高严重度缺陷（§2.1–2.4）~~ **已完成（2026-08-28，见 §6）**；中 severity 的 WriteError 可见性与 `get_cached_alarm_detail_tables` TTL 仍待修。
2. 确认部署形态（是否存在多进程 Streamlit worker），决定是否需要文件级锁。
3. rebase 到 master，按 §3.1 处理语义冲突：摘除 clip_rules 链路、decoration_admin 写模型替换、excel_tools 以本分支为底择优合并、spc_dashboard 人工移植。
4. 处理 resources 路径搬迁与既有 `__flags` 数据迁移（若试用期间已积累真实决策）。
5. 合并后执行：`tests/unit/inline_domain` 全量回归 + 集成测试 + SPC smoke + 管理员 UI 验收（PRD 12.3）。

## 6. 修复记录（2026-08-28）

4 个高严重度缺陷已在本分支工作树修复（**未提交**，改动基于 HEAD `3a0588e`；顺带修复了 §2.5 中"handler 异常未捕获"低危项）。

### 6.1 改动摘要

**缺陷 2.1 + 2.4（加密工作簿门控与读取语义）**
- `excel_tools.py` 新增 `list_workbook_sheet_names()`：openpyxl 枚举失败时回退 Excel COM 枚举 sheet 名，双失败才返回 `None`；`_read_encrypted_xlsx_via_com` 对缺失 sheet 返回空 DataFrame（与明文读取语义对齐，避免首次 persist 读 `__refresh_meta__` 崩溃）。
- `sheet_oos_decoration.py`：`_workbook_sheet_names` 委托新枚举函数；`load_sheet_oos_decisions` 与 `migrate_legacy_flags_if_needed` 在 `names=None`（文件不可读）时统一抛 `SheetOosDecorationReadError`，不再静默迁移/覆写；`__flags` 存在性判断对加密文件真实生效，4h 门控恢复。

**缺陷 2.2（决策台账过缓存边界）**
- `decorated_features.py` payload 的 `sheet_oos_decoration` 字典新增 `decision_sheet`/`decision_df`/`refresh_reason`（DataFrame + str，符合 ADR-0001；缓存键不受影响）。
- `spc_service.py` / `ctq_service.py` 的 `_view_model_from_payload` 透传三字段，旧缓存条目缺键时降级为默认值不抛错。下载决策台账、上传一致性检测、重建原因 caption 随之接通（`sheet_oos_admin.py`/`spc_dashboard.py` 无需改动）。

**缺陷 2.3（刷新假成功）**
- `measurement_snapshot_repository.py` 新增 frozen dataclass `MeasurementRefreshResult(measurements, refreshed_from_db)` 与 `refresh_measurements()`；`get_measurements` 对外行为不变，新增实例状态 `last_refresh_from_db`。
- `composition.py` 的 `refresh_raw_measurements` 改返回 `refreshed_from_db`：空窗口算成功，DB 失败降级旧快照返回 False。
- `monitor_service.py` 的 `safe_refresh_snapshots`：单产品异常 → 整体 False；经 `_resolve_raw_refresh_status` 沿组合链读到底层 raw 仓储的 `last_refresh_from_db`，降级即 False。
- `page_header.py`：handler 抛异常按失败处理（不推进 revision、不清 L2、走失败提示）。

### 6.2 测试验证

- 新增测试：`test_sheet_oos_refresh_encrypted.py`（加密场景 4 例，COM 全程 mock）、`test_composition_refresh.py`、`test_monitor_safe_refresh.py`，以及 page_header / sheet_oos_admin / spc / ctq / excel_tools / repository 既有测试文件的补充用例。
- 全量回归 `uv run pytest tests/unit tests/integration`：**560 passed / 9 failed / 3 skipped**。
- 9 个失败经 `git stash` 在干净 HEAD 上复跑确认**全部为既有失败，与本次修复无关**：`test_hot_reload`、`test_aoi_rs_page`、`test_spc_dashboard`（箱线图）、`test_code_selector_filter`×2（`count_threshold` 签名漂移）、`test_yield_dashboard_plotly_keys`、`test_yield_global_data_policy`×2（config 断言漂移）、`test_spc_db`（真实 DB 集成测试，环境/数据依赖）。

### 6.3 遗留问题（不阻塞，建议后续工单）

1. monitor 侧刷新状态经 `_resolve_raw_refresh_status` 沿私有属性链下探，属务实但有耦合的写法；理想做法是 `MeasurementPreparationPort`/`SpcRepository`/`InlineMonitorRepository` 逐层透传刷新状态。
2. §2.5 中危项仍开放：WriteError 被 service 吞成"暂无数据"（失败可见性）、`get_cached_alarm_detail_tables` 无 TTL、meta 读-改-写在锁外、首次迁移 `"empty"` 签名多写一次。
3. `load_refresh_meta` 对加密工作簿 COM 短暂不可用时按"meta 缺失 → 触发一次重写"处理，属既有自愈语义。
4. 上述 9 个既有失败测试与本分支无关，建议另行安排修复（其中 yield config 断言漂移可能随 master 演进已变化）。

## 7. 合并记录（2026-08-28，merge commit `ec63f3e`）

master 已合入本分支。用户裁定与执行结果：

### 7.1 裁定执行对照

| 事项 | 裁定 | 执行 |
|---|---|---|
| decoration_admin 写模型 | 保留分支模型 | master 的共享 UI 壳（key_prefix/report_name 参数化）保留，写路径换为 `sheet_oos_admin` 逻辑（只写 `__flags`、失败不清缓存不 rerun）；`sheet_oos_admin.py` 无 spc 硬编码，直接落于 `app/sections/inline_domain/shared/` 供 SPC/CTQ 共用 |
| clip_rules 链路 | 整条摘除 | 合并后全仓 `clip_rules` 零引用（master 删除自动生效，分支侧引用随冲突解决消除） |
| excel_tools 原子写 | 采用分支方案 | 多 sheet 事务 + `WorkbookWriteResult` 为底；移植了 master 的 `.bak` 备份（替换前备份正式文件）与表头/行列校验（并入 `_verify_temp_workbook`）；单 sheet `replace_workbook_sheet` 保持 bool 返回兼容 master 调用方 |
| 修饰工作簿迁至 resources/inline_domain/ | 保留 master 方案 | 分支新增的 `decision_signature.py` 默认资源路径已对齐 `ConfigLoader.get_domain_resource_dir("inline_domain")`（修复了会导致决策签名静默恒为空的断链） |
| override_rates / scrap_sheets / 入库不良率规格 | 采用 master 版 | 分支提交与工作树的二进制变更全部放弃 |
| TTL 12h vs 4h | 保留 master 12h | 周期重建实为 12h；手动刷新/决策上传经缓存键即时生效；PRD 5.8 已加注记；TTL 守卫测试改为"必须配置且 ≤12h" |
| PPA_FALLBACK_VALUE_OFFSET | 保持 -1 | 修正 master 侧测试期望值（9.5→9.0）与模块 docstring（-0.5→-1） |

### 7.2 冲突解决明细

- 页面（SPC/CTQ）：master 为底（新 import 路径 + 预警中心接线），重放分支的 revision/决策签名参数与 `SheetOosDecorationReadError` 处理。
- `spc_service.py`：master 配置化 TTL + 分支两个缓存键参数 + decision payload 透传三方叠加；`ctq_service.py` 硬编码 TTL 并入配置体系（`global.yaml` 新增 `inline_ctq_report_payload: 4`）。
- `measurement_snapshot_repository.py`（master 迁至 `infrastructure/shared/`）：`measurement_corrector` + 配置化 TTL + 分支 `MeasurementRefreshResult`/`last_refresh_from_db` 三族能力叠加；分支侧 import 全部对齐新路径。
- dashboard 移植：分支对旧 `spc_dashboard.py` 的改动全部落在 admin 函数，已并入共享壳 `decoration_admin.py`；旧目录 `app/sections/spc/` 删除。
- `ARCHITECTURE.md`：master 结构 + 分支门控段落。

### 7.3 合并后回归

`uv run pytest tests/unit tests/integration`：**702 passed / 6 failed / 3 skipped**。
6 个失败均为 HEAD 或 master 的既有失败，与合并无关（页头覆盖断言、AOI_RS 门户导航、yield config 断言漂移 ×3、真实 DB 集成测试）。合并引入的 4 个失败（test 适配问题）已全部修复。

### 7.4 合并后遗留

1. CPK 修饰后台（`render_cpk_decoration_admin`）仍是"覆盖写 + 全局清缓存"旧模型——`spc_cpk_cpm_decoration.xlsx` 是 PRD 明确 NON-GOAL，未动；如需统一另行决策。
2. §6.3 遗留项仍开放（monitor 刷新状态属性链下探、WriteError 页面可见性、`get_cached_alarm_detail_tables` 无 TTL 等）。
3. `_cached_decision_signature` 的 4h TTL 仍为硬编码，未并入 `service_cache` 配置体系（影响小，仅签名重读频率）。
4. 6 个既有失败测试建议另行修复。
5. 建议后续执行 SPC smoke 与管理员 UI 验收（PRD 12.3）后再合并回 master。
