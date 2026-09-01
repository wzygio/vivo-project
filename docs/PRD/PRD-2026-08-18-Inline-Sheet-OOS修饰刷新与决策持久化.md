# PRD：Inline Sheet OOS 修饰刷新与决策持久化

- 日期：2026-08-18
- 状态：待评审（未实施）
- 适用范围：SPC、CTQ、自动预警中复用的 Sheet OOS 修饰链路
- 重点资源：`resources/spc_sheet_oos_decoration.xlsx`、`resources/ctq_sheet_oos_decoration.xlsx`
- 关联代码：`app/components/page_header.py`、`app/sections/spc/spc_dashboard.py`、`src/inline_domain/application/shared/`、`src/inline_domain/core/shared/sheet_oos_decoration.py`、`src/inline_domain/infrastructure/measurement/measurement_snapshot_repository.py`、`src/shared_kernel/utils/excel_tools.py`

## 1. CAPABILITY

报表使用者和管理员需要一套可预测的 Sheet OOS 修饰生命周期：页头“刷新数据”成功后同时更新底层 L1 快照并失效对应产品的 L2 报表缓存；人工维护的 `flag` 作为独立决策长期保留；系统只在约定的刷新事件发生时重算并更新当前 OOS 明细，普通 rerun、进程重启或无关文件写入不得反复覆盖工作簿。

交付后，工作簿中的产品 sheet 表示“当前 OOS 明细”，产品决策 sheet 表示“跨窗口保留的人工决策”。底层快照刷新、4 小时修饰刷新周期、有效决策变更或点击“刷新缓存”会触发明细重建；写入失败不会被记录为成功。

## 2. 背景与现状

### 2.1 当前数据层级

```text
L1：data/<product>/inline_measurements_<product>.parquet
    - InlineMeasurementSnapshotRepository 管理
    - 当前 TTL：8 小时
    - force_refresh=True 时从数据库重建

L2：Streamlit st.cache_data
    - fetch_decorated_features 当前 TTL：4 小时
    - SPC 外层 payload TTL：4 小时
    - CTQ 外层 payload 当前没有 TTL

用户状态：resources/*_sheet_oos_decoration.xlsx
    - 当前产品 sheet 同时保存当前明细和 flag
```

### 2.2 当前问题

1. 页头“刷新数据”只执行 L1 refresh handler，不失效 L2；刷新后页面仍可能继续使用旧载荷。
2. 当前明细通过“当前 OOS LEFT JOIN 旧 flag”重建。旧记录一旦不再属于当前 OOS 集合，就从产品 sheet 消失；以后重新出现时原人工决策已丢失。
3. `fetch_decorated_features` 在任意缓存 miss 时都以 `persist=True` 运行；TTL、缓存淘汰、进程重启、调用参数差异都可能重写工作簿。
4. 整个 xlsx 的 mtime 不能直接作为业务缓存键。系统自己重写产品 sheet 也会改变 mtime；若直接入键，会形成“写文件 → mtime 变化 → cache miss → 再写文件”的循环。
5. `replace_workbook_sheet()` 遇到 `PermissionError` 只记录告警，调用方无法区分“成功写入”和“被占用而跳过”。
6. 企业加密工作簿的 COM 回退会整体重写文件，当前缺少多 sheet 原子提交和明确成功结果。

## 3. 需求审查结论

| 用户要求 | 审查结论 | PRD 处理 |
|---|---|---|
| “刷新数据”同时更新 L1 和 L2 | 合理 | L1 全部成功后失效对应产品 L2；任一失败则不失效 |
| 复用当前 `data/` L1 快照 | 合理 | 不新增 L1 revision 文件；保留现有 Parquet、策略版本和 8 小时 TTL |
| 同工作簿增加 `<产品>__flags` | 合理 | 产品 sheet 只保存当前明细；决策 sheet 长期保存人工 flag |
| 4 小时 TTL 或 signature 变化时重建 | 合理，但需区分 L1/L2 TTL | 这里的 4 小时定义为“修饰明细刷新周期”，不是 L1 已发生变化的证明 |
| 根据文件修改时间检测管理员上传 | 方向合理，mtime 不得直接作为最终业务签名 | mtime/size 只触发重新读取；最终使用决策 sheet 的规范化内容哈希 |
| 点击“刷新缓存”强制重算明细 | 合理 | 产品 revision 变化，强制 L2 miss 并允许重写当前明细 |
| 不把底层快照版本加入缓存签名 | 接受 | 不读取 Parquet mtime/hash 作为页面缓存键；“刷新数据”成功后主动推进现有产品 revision |
| 只有成功写入后更新状态 | 必须 | 写入 API 返回明确成功结果；明细和内部 meta 同一事务提交 |

## 4. CONSTRAINTS

### 4.1 固定业务规则

1. 修饰动作继续使用 `True / False / Delete` 三态语义，不修改截断算法。
2. 决策匹配键保持：

   ```text
   (prod_code, step_id, param_name, sheet_id)
   ```

3. 产品当前明细必须来自本次原始数据计算得到的当前 OOS 集合，不允许把历史明细混入图表或能力计算。
4. 历史人工决策不得因日期窗口缩短、OOS 暂时消失、缓存淘汰或进程重启而丢失。
5. `resources/` 工作簿仍是人工业务状态；`data/` Parquet 仍是 L1 测量事实快照，修饰结果不得回写 L1。
6. SPC 与 CTQ 使用各自工作簿，决策不得跨 scope 串用。

### 4.2 缓存与刷新规则

允许重新生成产品当前明细的事件只有：

1. 产品明细 sheet 或内部生成状态不存在；
2. 距上一次成功生成已达到 4 小时；
3. 共享产品刷新 revision 发生变化；
4. 当前产品决策 sheet 的内容签名发生变化；
5. 用户点击“刷新缓存”；该操作通过推进产品 revision 落入规则 3；
6. 用户点击“刷新数据”且所有 L1 refresh handler 成功；该操作同样推进产品 revision，随后 L2 重算。

以下事件本身不得导致工作簿重写：

- 普通 Streamlit rerun；
- 进程重启，但距上次成功生成不足 4 小时且其他签名未变化；
- L2 条目因容量被淘汰，但持久化生成状态仍有效；
- 系统仅重写当前产品明细 sheet 导致整个 xlsx mtime 变化；
- 对工作簿中其他产品 sheet 的更新；
- 页面级 base signature 不同但共享产品 revision 未变化。

### 4.3 明确拒绝的设计

- 不新增 `inline_measurements_<product>.revision` 文件；
- 不把 L1 Parquet 的 mtime、大小或内容 hash 加入页面 `snapshot_signature`；
- 不用整个 xlsx 的 mtime 直接作为 `fetch_decorated_features` 缓存键；
- 不用 outer join 把历史明细混入当前产品 sheet；
- 不依赖纯内存状态判断是否已经成功生成，因为进程重启后会丢失。

## 5. IMPLEMENTATION CONTRACT

### 5.1 工作簿结构

每个产品使用两个业务 sheet；每个工作簿另有一个系统内部状态 sheet。

以产品 `M678` 为例：

| sheet | 所有者 | 内容 | 更新方式 |
|---|---|---|---|
| `M678` | 系统 | 当前查询口径下的 OOS 明细及解析后的 `flag` | 仅在允许刷新事件发生时重建 |
| `M678__flags` | 管理员/迁移逻辑 | 持久化人工决策台账 | 管理员上传或首次迁移时更新；系统明细刷新不得删除旧决策 |
| `__refresh_meta__` | 系统 | 各产品/scope 最近一次成功生成状态 | 与产品明细同一成功事务更新 |

产品 sheet 保持当前 13 列结构：

```text
factory, prod_code, step_id, param_name, sheet_id,
sheet_start_time, sheet_max, sheet_min, sheet_mean,
usl, lsl, oos_type, flag
```

决策 sheet 最小结构：

```text
prod_code, step_id, param_name, sheet_id, flag
```

可选审计列：

```text
decision_updated_at, decision_source
```

若加入审计列，不能把操作人身份作为本 PRD 的强制字段；当前系统没有可靠用户身份来源。

内部 meta sheet 最小结构：

```text
scope
prod_code
last_generated_at
product_revision
decision_signature
detail_row_count
```

`__refresh_meta__` 不作为用户下载或编辑界面的一部分。

### 5.2 旧工作簿迁移

首次读取某产品时，如果 `M678` 存在但 `M678__flags` 不存在：

1. 从旧产品 sheet 读取四列键和 `flag`；
2. 重复键保留最后一行；
3. 写入 `M678__flags`；
4. 用当前 OOS 明细重新生成 `M678`；
5. 仅在上述内容全部成功写入后写入或更新 `__refresh_meta__`；
6. 不删除其他产品 sheet，也不改变其他产品的决策。

为避免丢失无法识别的显式 True 决策，首次迁移保留旧表中的全部 `flag`，不只迁移 False/Delete。

迁移必须幂等：重复执行不能增加重复决策或改变已有 flag。

### 5.3 当前明细与决策合并

运行时仍以当前明细为左表：

```text
current_oos_detail
LEFT JOIN
durable_flags
ON (prod_code, step_id, param_name, sheet_id)
```

- 命中决策：使用持久化 `flag`；
- 未命中决策：默认 `True`；
- 决策表中的历史键即使当前不存在，也继续保留在 `__flags` 中；
- 历史键不进入当前产品 sheet，也不参与本次修饰；
- 以后同键重新出现时恢复原决策。

### 5.4 文件变化与决策签名

为满足“检测文件改动”且避免自触发循环，采用两阶段签名：

```text
阶段 1：file_stat_signature = (mtime_ns, size)
阶段 2：decision_signature = hash(canonicalized <product>__flags rows)
```

规则：

1. 每次页面运行可以廉价读取 xlsx 的 mtime 和大小；
2. `file_stat_signature` 未变化时复用上次决策内容 hash；
3. mtime 或大小变化时，重新读取目标 `__flags` sheet；
4. 对键列与 flag 规范化、稳定排序后计算 SHA-256；
5. 进入页面和共享 L2 缓存键的是 `decision_signature`，不是 xlsx mtime；
6. 系统重写 `M678` 会改变 mtime，但 `M678__flags` 内容不变，因此最终业务签名不变；
7. 管理员上传或直接编辑 `M678__flags` 后，内容 hash 变化，触发 L2 miss 和当前明细重建；
8. 直接修改系统拥有的 `M678` 不视为人工决策变更，下次允许刷新时会被覆盖；
9. `__flags` 无法读取时必须显式失败，不得降级为空决策，否则可能把历史 flag 全部解释为默认 True。

企业加密工作簿的决策读取继续使用现有 Excel COM 回退。实现应缓存由 `file_stat_signature` 索引的决策 hash，避免普通 rerun 重复启动 Excel COM。

### 5.5 统一生成判定

新增纯函数式判定，概念接口如下：

```python
should_regenerate_detail(
    *,
    current_sheet_exists: bool,
    last_generated_at: datetime | None,
    stored_product_revision: str | None,
    current_product_revision: str,
    stored_decision_signature: str | None,
    current_decision_signature: str,
    now: datetime,
) -> RefreshDecision
```

`RefreshDecision` 至少返回：

```text
should_write: bool
reason: missing | ttl_expired | product_revision_changed |
        decision_changed | unchanged
```

日志必须记录刷新原因，不能只记录“cache miss”。

注意：页面当前传递的完整 `snapshot_signature` 含页面 base 名称。SPC 页面、CTQ 页面和自动预警页面的 base 不同，不能直接把完整字符串写入共享 meta，否则跨页面访问会互相触发重建。生成判定使用共享的产品 revision 部分；完整页面 signature 仍可保留用于各自 L2 缓存隔离。

### 5.6 “刷新数据”同时更新 L1 与 L2

修改 `render_page_header()` 中 `_refresh_data_callback`：

```text
执行所有 refresh_handlers
    ├── 任一失败：保留现有 L2，不推进产品 revision，提示失败
    └── 全部成功：
          1. 失效对应产品 L2（推进共享产品 revision）
          2. 清理必要的 session ViewModel memo
          3. 由按钮回调后的自然 rerun 重新计算页面
          4. 提示“L1 快照与 L2 缓存已刷新”
```

产品页面调用：

```python
invalidate_page_cache(cached_funcs, product_code=product_cache_scope)
```

无产品作用域页面继续使用已注册缓存函数的全量清理方式。

“刷新数据”不执行代码模块卸载或配置重读；这些行为仍只属于“刷新缓存”的 hard reset 路径。

### 5.7 “刷新缓存”语义

保持现有 hard reset 能力：

- 推进当前产品 revision；
- 失效 L2；
- 清理页面 ViewModel/session memo；
- 重载代码和配置。

revision 改变必须使 `should_regenerate_detail()` 返回 `product_revision_changed`，即使距上次成功生成不足 4 小时，也要重新计算并写入当前产品明细。

### 5.8 4 小时刷新周期

4 小时是修饰明细的最大复用周期，不修改 L1 仓储现有 8 小时 TTL。

> **2026-08-28 合并裁定（修订）**：周期 TTL 上限随 master 的
> `service_cache.ttl_hours` 体系统一，`inline_spc_report_payload` 与
> `inline_decorated_features` 当前配置为 12h，因此"到点自动重建"的实际周期
> 为 12h 而非 4h。手动刷新（推进产品 revision）与决策上传（决策签名变化）
> 经缓存键即时生效，不受周期 TTL 影响。core 层 `should_regenerate_detail`
> 的 4h 门控保留，作为进入共享函数后的最小复用周期。

- `fetch_decorated_features` 保持 4 小时 TTL；
- 所有可能在外层遮挡共享缓存的 SPC/CTQ/Monitor payload 缓存必须保证最多 4 小时后能再次进入共享生成判定；
- CTQ 当前外层缓存无 TTL，需要补齐 4 小时 TTL，或调整边界确保共享层的 4 小时判定可达；
- 即使因进程重启或容量淘汰提前进入共享函数，也必须读取 `__refresh_meta__`，不足 4 小时且其他签名不变时只计算/读取页面所需载荷，不写工作簿；
- 达到 4 小时后第一次有效调用重写一次，成功后重置 `last_generated_at`。

本 PRD 不声称 4 小时到期等价于 L1 Parquet 已变化。它只是用户批准的周期性重建条件。

### 5.9 管理员下载与上传

管理员下载文件包含两个业务 sheet：

```text
当前明细
决策台账
```

上传规则：

1. 优先读取“决策台账”sheet；兼容旧单 sheet 上传时，从其中提取四列键和 `flag`；
2. 校验键列、flag 枚举和重复键；
3. 上传内容更新 `<产品>__flags`，不得直接覆盖系统拥有的产品当前明细 sheet；
4. 上传成功后 rerun；决策内容签名变化自然触发 L2 miss；
5. 如果上传内容与现有决策规范化后完全一致，不强制重写当前明细；
6. 写入失败时显示错误，不清缓存、不显示成功、不更新 meta。

决策台账采用“上传文件为该产品完整决策集”的覆盖语义。管理员需要删除历史决策时，应从下载的决策台账中删除对应行后上传；空表表示清空该产品的显式决策，后续全部按默认 True。

### 5.10 成功写入与原子性

`replace_workbook_sheet()` 当前返回 `None` 且吞掉 `PermissionError`，不能继续作为成功判定依据。需要提供明确契约，例如：

```python
WorkbookWriteResult(
    written: bool,
    path: Path,
    updated_sheets: tuple[str, ...],
    error: str | None,
)
```

写入要求：

1. 当前产品 sheet、首次迁移产生的 `__flags`、`__refresh_meta__` 必须在一个工作簿事务中提交；
2. 先在同目录临时文件完成工作簿保存和可读性验证，再原子替换正式文件；
3. `__refresh_meta__` 只能与成功生成的当前明细一起进入最终文件；
4. 文件被占用、COM 读取失败、临时文件保存失败或正式替换失败时，`written=False` 或抛出明确业务异常；
5. 调用方只有在 `written=True` 后才能返回“已更新”状态；
6. 同一工作簿写入需要进程内锁；如部署存在多进程 worker，还需文件级锁或等效互斥；
7. 其他产品 sheet、决策 sheet和 meta 行必须保留。

企业加密文件回退仍可能把最终文件保存为标准明文 xlsx。该既有行为不在本 PRD 中改变，但必须在日志中明确记录。

## 6. 状态与转换

```text
[稳定]
  │ 普通 rerun / 提前 cache miss / 系统明细写导致 mtime 变化
  └──────────────────────────────────────────────► [稳定，不写文件]

[稳定]
  │ 4h 到期 / 产品 revision 变化 / 决策内容变化 / 首次迁移
  ▼
[待生成]
  │ 计算当前 OOS + 合并持久化决策
  ▼
[待写入]
  ├── 写入成功 ──► [稳定，更新 meta]
  └── 写入失败 ──► [失败，保留旧文件与旧 meta，允许重试]
```

## 7. 接口与数据影响

### 7.1 预计修改文件

| 文件 | 预计改动 |
|---|---|
| `app/components/page_header.py` | L1 成功后产品级 L2 失效；刷新反馈文案 |
| `app/sections/spc/spc_dashboard.py` | 下载当前明细+决策台账；上传写入 `__flags`；严格成功反馈 |
| `app/pages/SPC监控报表.py` | 计算并传递决策签名/共享产品 revision |
| `app/pages/CTQ监控报表.py` | 同上 |
| `src/inline_domain/application/shared/decorated_features.py` | 4h 缓存入口、刷新判定、决策签名参数 |
| `src/inline_domain/application/shared/decorated_data.py` | 分离“计算/读取决策”和“允许持久化” |
| `src/inline_domain/core/shared/sheet_oos_decoration.py` | 决策 sheet、迁移、meta、刷新判定、当前明细合并 |
| `src/inline_domain/application/ctq/ctq_service.py` | 防止无 TTL 外层缓存遮挡 4h 共享刷新 |
| `src/shared_kernel/utils/excel_tools.py` | 多 sheet 原子写、明确 WriteResult、锁和失败语义 |

`measurement_snapshot_repository.py` 原则上无需改变数据模型或缓存签名；仅在测试或组合接口需要时复用现有 `force_refresh=True` 行为。

### 7.2 向后兼容

- 旧产品 sheet 自动迁移，不要求用户手工拆分；
- `SheetOosDecorationResult` 可新增 `decision_sheet`、`decision_df`、`refresh_reason`，但现有字段继续保留；
- 旧上传文件保持可读，但写入目标改为决策 sheet；
- SPC/CTQ 三态修饰结果和图表接口保持不变；
- AOI_TT、AOI_RS 自有修饰工作簿不在本 PRD 自动迁移范围内。

## 8. 可观测性与运维

每次生成判定记录结构化日志：

```text
product
scope
decision_signature 前 12 位
product_revision
last_generated_at
refresh_reason
write_attempted
write_succeeded
detail_row_count
```

不得记录完整业务明细、用户上传文件内容或敏感路径信息。

管理员界面应显示：

- 当前工作簿和产品 sheet；
- 决策 sheet 名；
- 上次成功生成时间；
- 本次载荷是缓存命中还是因何重建；
- 写入失败时的可操作提示，例如关闭 Excel 后重试。

## 9. 风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| 整体 mtime 直接入键导致无限重写 | 高 | mtime 只触发重算决策 hash；最终 key 使用 `__flags` 内容签名 |
| SPC/Monitor 的页面 base signature 不同造成交替重写 | 高 | meta 使用共享产品 revision，不存完整页面 base signature |
| CTQ 外层无 TTL 遮挡 4h 刷新 | 高 | 对齐外层 TTL 或调整调用边界，增加时钟测试 |
| 旧 flag 迁移失败导致人工决策丢失 | 高 | 迁移与当前明细/meta 单事务提交；失败保留原文件 |
| Excel 占用却显示成功 | 高 | 写入 API 返回明确结果，失败不更新 meta、不清缓存 |
| 企业加密整体重写中断 | 高 | 临时文件验证后原子替换；保留原文件直到最终替换 |
| 多页面同时更新同一工作簿 | 中 | 进程内锁；多进程部署增加文件级锁 |
| 决策 sheet 不断增长 | 低 | 只保存一行/业务键；管理员可通过完整台账上传删除历史决策 |
| 直接编辑当前明细不生效 | 低 | 明确产品 sheet 为系统所有；后台只引导编辑决策台账 |

## 10. NON-GOALS

- 不修改 OOS 判断、稳定哈希截断、PPA clip rule 或三态 flag 解析；
- 不修改 L1 Parquet 的 8 小时 TTL、三个月窗口、数据库失败降级或策略版本；
- 不把实际 L1 快照版本加入页面缓存签名；
- 不把工作簿改造成完整历史 OOS 事实仓库；只持久化人工决策；
- 不修改 `spc_cpk_cpm_decoration.xlsx`；
- 不在本次迁移 AOI_TT/AOI_RS 专用修饰文件；
- 不引入用户身份、审批流或决策审计数据库。

## 11. 验收标准

### 11.1 页头刷新

- “刷新数据”所有 handler 成功后，当前产品 revision 必须变化，下一次页面运行 L2 必须 miss；
- handler 任一失败时 revision 不变、L2 不清除、旧页面载荷保留；
- 成功提示明确为“L1 快照与 L2 缓存已刷新”；
- “刷新数据”不执行代码模块卸载和配置重读；
- “刷新缓存”继续执行 hard reset，并强制生成当前明细。

### 11.2 决策持久化

- 旧产品 sheet 首次迁移后生成 `<产品>__flags`，所有旧 flag 保留；
- 某 OOS 行消失后，决策仍存在于 `__flags`；
- 同键以后重新出现时恢复原 False/Delete/True；
- 历史决策不得进入当前产品明细或图表；
- SPC 与 CTQ 决策互不影响。

### 11.3 生成门控

- 相同 revision、相同决策签名、距成功写入不足 4 小时时，普通 rerun 不改工作簿；
- 进程重启后满足上述条件，工作簿仍不改；
- 缓存容量淘汰后满足上述条件，工作簿仍不改；
- 满 4 小时后的第一次有效调用只重写一次；
- 产品 revision 改变时，不等待 4 小时，立即重写一次；
- 决策内容改变时，不等待 4 小时，立即重写一次；
- 系统重写产品明细造成 xlsx mtime 改变，但决策 hash 不变时，不触发第二次重写；
- 修改其他产品 sheet 不触发当前产品重写。

### 11.4 写入可靠性

- 文件被占用时页面显示失败，旧文件与旧 meta 保持不变；
- 任一 sheet 保存失败时不得出现“部分新明细 + 新 meta”；
- 写入成功后能由 openpyxl 或现有 COM 回退重新读取；
- 其他产品业务 sheet、决策 sheet 和 meta 行保持不变；
- 同产品并发刷新不会生成损坏工作簿。

## 12. 测试计划

### 12.1 单元测试

1. `page_header`：L1 全成功后调用产品级 invalidation；部分失败不调用；无产品作用域走函数清理。
2. 刷新判定：缺文件、4h 边界、revision 变化、决策变化、unchanged 全部分支。
3. 决策迁移：旧表到 `__flags`、重复键、空表、幂等。
4. 合并：当前明细消失/重现仍恢复 flag。
5. 签名：行序变化不改变 hash；flag 变化改变 hash；当前明细 sheet 变化不改变决策 hash。
6. 写入结果：成功、PermissionError、COM 失败、临时保存失败、原子替换失败。
7. CTQ/SPC/Monitor：外层缓存不会永久遮挡 4h 生成判定。

### 12.2 集成测试

1. 使用临时多 sheet 工作簿执行旧格式迁移；
2. 修改 `__flags` 后验证页面 payload 和当前明细同步变化；
3. 模拟系统写当前明细后 mtime 变化，验证无二次写；
4. 模拟 `refresh_raw_measurements(force_refresh=True)` 成功，验证产品 revision 推进并重算；
5. 模拟刷新失败，验证旧 L2 和工作簿均保留；
6. 企业加密样本执行只读/临时目录回归，不覆盖真实业务文件。

### 12.3 UI 验收

- 点击“刷新数据”一次后显示 L1+L2 成功提示，报表重新加载；
- 下载文件同时包含当前明细和决策台账；
- 上传决策后对应图表按新 flag 生效；
- Excel 文件打开占用时上传或刷新显示失败，不显示成功；
- 连续普通筛选/rerun 不改变工作簿修改时间。

## 13. 实施顺序

1. 先修正 `page_header.py` 的 L1+L2 刷新契约及测试；
2. 为 Excel 写入增加明确结果、多 sheet 原子提交和锁；
3. 增加决策 sheet、内部 meta、旧表迁移和刷新判定纯函数；
4. 增加 mtime 探针 + 决策内容签名，并接入页面/共享缓存；
5. 改造管理员下载上传；
6. 对齐 CTQ/SPC/Monitor 的 4 小时可达性；
7. 执行定向单测、Inline 回归、SPC smoke 和管理员 UI 验收。

## 14. OPEN QUESTIONS

当前没有阻塞实施的产品问题。以下采用本 PRD 默认值，若评审时有异议再调整：

- 决策 sheet 命名固定为 `<产品号>__flags`；
- 决策上传采用“该产品完整决策集覆盖”语义；
- 系统内部状态使用同工作簿 `__refresh_meta__`，不新增 sidecar JSON；
- 4 小时从最近一次成功生成时间计算，不采用自然时钟固定分桶；
- L1 TTL 保持 8 小时，L2/修饰明细刷新周期保持 4 小时。

## 15. HANDOFF

本 PRD 已达到实现前架构评审状态。下一步应先确认三个技术契约：

1. `WorkbookWriteResult` 与多 sheet 原子写接口；
2. `__refresh_meta__` 的最小 schema；
3. SPC/CTQ/Monitor 外层缓存如何保证 4 小时共享判定可达。

评审通过后按 TDD 顺序实施：先写页头刷新和刷新判定测试，再实现业务逻辑，最后执行工作簿迁移与企业加密回归。

