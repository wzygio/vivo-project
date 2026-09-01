# Sheet OOS 修饰管线算法（新旧对照）

> 重写日期：2026-08-31（合并 `ec63f3e` 之后）。本文取代 2026-08-14 版
> （旧版只描述共享管线 `decorated_features.py`，且持久化还是旧模型）。
>
> 覆盖范围：inline_domain 全部 Sheet 级超规修饰——SPC/CTQ/monitor 共享管线
> 与 AOI_TT/AOI_RS 自有管线。
>
> **2026-08-31 缓存机制统一**：inline_domain 全部子模块（spc / ctq / aoi_tt /
> aoi_rs / monitor）的缓存刷新机制已统一为同一套——缓存键含
> `product_revision` + `decision_signature`，TTL 由 `config/global.yaml` 的
> `service_cache.ttl_hours` 统一配置（周期上限 12h），工作簿写入全部由 core
> 门控 `should_regenerate_detail` 决定。AOI 不再走"总是写"旧语义；monitor 的
> parquet mtime 签名（`compute_snapshot_signature`）确认为死代码并删除；
> 预警明细表缓存补齐 TTL 与 revision/决策签名键。
>
> **2026-09-01 简化**：删除旧表迁移机制（`migrate_legacy_flags_if_needed`）——
> `__flags` 只记录人为决策，缺失即空台账，旧产品 sheet 的 flag 永不生效；
> SPC/CTQ 与 AOI 的决策来源完全一致。
>
> 阅读约定：以**统一后的新链路为主干**逐步展开；与旧模型（2026-08-18 前）
> 不同的步骤用【旧】标注差异，相同处不重复描述。

## 0. 一张图看懂两套入口、一个引擎

```text
SPC 页面 / CTQ 页面 / 自动预警看板(monitor)
    │  scope=spc / ctq / none（monitor 按 data_type 路由）
    ▼
service 层 payload 缓存（st.cache_data，TTL 配置化）
    │  缓存键含 product_revision + decision_signature
    ▼
application/shared/decorated_features.py  fetch_decorated_features（共享 L2 缓存）
    ▼
application/shared/decorated_data.py      prepare_decorated_data(scope→工作簿)
    ▼
core/shared/sheet_oos_decoration.py       引擎：识别→迁移/读取决策→合并→门控→原子写→三态应用
    ▼
重算 Sheet 特征 → 图表 / CPK / 预警

AOI_TT / AOI_RS 页面
    │  不经共享管线（aoi 数据不做 Sheet 特征），但缓存键机制相同：
    │  payload 缓存键含 product_revision + decision_signature，TTL 配置化（12h）
    ▼
aoi_tt_service / aoi_rs_service
    ▼
core/aoi_tt/aoi_tt_decoration.py / core/aoi_rs/aoi_rs_decoration.py
    │  调用同一引擎 persist_sheet_oos_decoration（传 scope=aoi_tt/aoi_rs → 门控生效）
    ▼
core/shared/auto_decoration.py            apply_tri_state_decoration（三态应用）
```

三态语义（True 截断 / False 释放真实值 / Delete 剔除）与截断算法
（稳定哈希、线内 5%~15% span）**新旧完全一致、各模块单一来源**，不在本文差异范围。

## 1. 触发与缓存（回答"缓存 miss 是否还重写工作簿"）

**新（全部子模块统一）**：缓存 miss 只意味着**重新计算**，不再意味着重写工作簿。

- 页面运行 → service payload 缓存（`fetch_spc_report_payload`、
  `fetch_aoi_tt_report_payload`、`fetch_dashboard_data_dict` 等，
  TTL 由 `config/global.yaml` 的 `service_cache.ttl_hours` 统一配置，
  当前各链路均为 12h）→ miss 才进入计算；
- 共享层 `fetch_decorated_features` 缓存键 =
  `(prod_code, scope, start_date, end_date, snapshot_signature,
  product_revision, decision_signature)`（`decorated_features.py:75-92`）；
  AOI 的 payload 缓存键同样含 `product_revision` + `decision_signature`
  （2026-08-31 统一）；预警明细表缓存键含产品 revision 与决策签名的
  稳定序列化（`monitor_dashboard.get_cached_alarm_detail_tables`）；
- 键中两个成员是"主动失效开关"：页头「刷新数据/刷新缓存」推进
  `product_revision`，管理员改动决策台账改变 `decision_signature`——
  两者都必然制造 miss，从而进入第 5 步的门控判定。

【旧】缓存键只有 `(prod, scope, 窗口, snapshot_signature)`，任何 miss
（TTL 到期、容量淘汰、进程重启、参数差异）都会走到"重建并覆写产品 sheet"，
工作簿写入完全不受控；且「刷新数据」只刷 L1 快照、不失效 L2。
monitor 曾额外使用 parquet mtime/size 聚合签名（`compute_snapshot_signature`），
统一后作为死代码删除——revision/决策签名已覆盖其失效场景。

## 2. 取数与窗口过滤

共享管线：`_features_source.get_spc_measurements` + 规格表 →
`sheet_start_time` 窗口过滤（对已窗口化数据幂等）。新旧一致。

【AOI】各自 service 取 TT 明细 / RS 图表点帧，不做 Sheet 特征。

## 3. OOS 明细识别（回答"AOI 有没有自动修饰、与 SPC 有何不同"）

**AOI 有完整的超规片自动修饰**，三态 flag 语义与 SPC 同源；不同在于
**修饰对象、规格形态和键维度**：

| | SPC / CTQ | AOI_TT | AOI_RS |
|---|---|---|---|
| 明细来源 | Sheet 特征：`sheet_max > usl` 或 `sheet_min < lsl`（双边，严格不等式，等线不算） | TT 明细：`tt_qty > usl`（单边） | 两张图的点帧：`value > spec`，By Lot 用 LOT_RATIO 规格、By Sheet 用 SHEET_ID/GLASS_ID 规格 |
| 被修饰的值 | 点位测量值 `param_value` | `tt_qty`（数量） | 图点 `value`（lot 比值 / sheet rs_qty） |
| 决策键 | `(prod_code, step_id, param_name, sheet_id)` | `(prod_code, step_id, tt_name, sheet_id)` | `(prod_code, factory, step_id, rs_code, chart_kind, point_id)` |
| 明细产出 | 13 列（含 max/min/mean、oos_type） | 9 列（含 lot_id、tt_qty、usl） | 键 + `value/spec/sheet_start_time` |

识别函数：SPC 走引擎 `build_sheet_oos_detail`；AOI 走各自模块的
`build_aoi_tt_oos_detail` / `build_aoi_rs_oos_detail`（含 `chart_kind` 维度，
解决一行 flag 无法分别控制两张图的问题）。此步新旧一致。

## 4. 决策来源与合并（回答"新增 sheet_ID 时 __flags 会不会自动同步"）

**新**：决策的唯一来源是决策台账 `<产品>__flags` sheet。

1. `load_sheet_oos_decisions()`：`__flags` 存在 → 读取返回；不存在 → 空台账
   （**不做旧表迁移**：2026-09-01 起全局如此，旧产品 sheet 里的 flag 永不生效）；
   工作簿不可读（openpyxl 与 COM 均失败）→ 抛
   `SheetOosDecorationReadError`，不降级为空。
2. 合并：`当前 OOS 明细 LEFT JOIN __flags`，命中用持久化 flag，未命中默认 True。

**新增 sheet_ID 不会自动同步进 `__flags`——这是设计而非疏漏**：

- 新出现的 OOS 键只进入产品明细 sheet（以默认 flag=True 生效），
  `__flags` 只保存**显式人工决策**，系统刷新永远不会向其中添加行
  （`_persist_sheet_oos_decoration` 仅在 `__flags` 不存在的那一次写入它）；
- `__flags` 的写入入口只有两个：首次写入（自动、一次性，物化**空**台账）
  与人工维护（SPC/CTQ 走共享后台 `decoration_admin` 上传，覆盖语义：
  上传文件 = 该产品完整决策集；AOI 无上传 UI，直接编辑 `__flags` sheet）；
- 推论：想删除一条决策，从台账下载中删行再上传（SPC/CTQ）或直接删
  `__flags` 的行（AOI）；删除后该键回到默认 True。

【旧】没有 `__flags`：决策寄生在产品 sheet 里，每次重建时"当前 OOS
LEFT JOIN 旧 sheet 的 flag"。新增键同样是默认 True（新旧一致），但
**OOS 消失即丢决策**（键随明细一起被重建掉）；新模型下消失的键留在
`__flags`，重现时自动恢复决策。

AOI 侧说明：合并、决策来源与门控与主干完全同享（同一个
`persist_sheet_oos_decoration`），无差异。注意行为变化：所有工作簿产品
sheet 里的 flag 列一律只是"当前生效状态的物化结果"，手工改它不生效，
决策必须写入 `<产品>__flags`。

## 5. 写入门控（回答"SPC 现在的更新逻辑是什么"）

**新（全部子模块统一，含 AOI）**：核心是纯函数 `should_regenerate_detail()`
（`sheet_oos_decoration.py`），输入当前状态与 `__refresh_meta__` 中
上次成功生成状态，输出 `should_write + reason`。只有以下事件重写工作簿：

1. `missing`：产品明细 sheet 或生成状态不存在（含首次迁移）；
2. `product_revision_changed`：页头「刷新数据」（全部 L1 handler 成功才推进）
   或「刷新缓存」推进了产品 revision——立即重写，不等周期；
3. `decision_changed`：`__flags` 内容签名变化（两阶段：file_stat 探针
   mtime_ns+size 廉价过滤，变化才重读 `__flags` 算 SHA-256；系统自己重写
   明细只动 mtime 不动内容签名，不会自触发循环）——立即重写；
4. `ttl_expired`：距上次成功生成 ≥ 4h（core 门控周期；实际周期受外层
   12h 缓存配置调节，见 2026-08-28 合并裁定与 PRD 5.8 注记）。

不满足以上任一条 → `unchanged`：只计算/读取页面所需载荷，**不写文件**。
日志记录 reason、write_attempted、write_succeeded、行数等结构化字段。

【旧】无门控：任何缓存 miss 都重建并覆写产品 sheet；`PermissionError`
（文件被 Excel 占用）只记告警，上层拿到的内存结果与文件状态脱节。

AOI 侧说明：2026-08-31 统一后，`prepare_aoi_tt/aoi_rs_decoration` 以
`scope="aoi_tt"/"aoi_rs"` 调用 `persist_sheet_oos_decoration`，门控与
`__refresh_meta__` 对 AOI 同样生效（meta 行按 (scope, prod_code) 隔离，
AOI 与 SPC/CTQ 互不干扰）。AOI 仍无上传 UI（`decoration_admin` 仅接
SPC/CTQ），维护方式是直接编辑 `__flags` sheet；编辑经决策签名变化
触发立即重写，与 SPC/CTQ 一致。

## 6. 写入执行

**新（全部模块共享，含 AOI）**：`replace_workbook_sheets()` 多 sheet 原子事务
（`excel_tools.py`）：

- 同目录临时文件完成保存 → openpyxl 回读校验（sheet 存在、行数、表头/列数
  一致）→ 正式文件存在则先备份为 `.bak` → `os.replace` 原子替换；
- 产品明细、首次创建的 `__flags`、`__refresh_meta__` 同事务提交，
  绝不出现"部分新明细 + 新 meta"；
- 返回 `WorkbookWriteResult(written, path, updated_sheets, error)`；
  文件被占用返回 `written=False`，core 层上抛 `SheetOosDecorationWriteError`，
  失败不更新 meta、不清缓存（AOI service 层有兜底降级，不崩页面）；
- 企业加密工作簿经 COM 全量读出后整体重写（会转为明文 xlsx，日志有告警），
  sheet 枚举 openpyxl 失败时回退 COM（`list_workbook_sheet_names`）。

【旧】单 sheet 替换直接保存原文件；加密回退"先删原文件再整体重写"，
无备份、无回读校验、无成功契约。

## 7. 三态应用与出口

- SPC/CTQ：`apply_sheet_oos_decoration` 对点位 `param_value` 按合并后的
  flag 处理（Delete 剔除该键全部点位 / False 保留真实值 / True 稳定哈希
  截断到线内 5%~15% span），随后**用修饰后的点位重算 Sheet 特征**，
  供图表、CPM/CPK 与自动预警消费——预警中心因此与报表天然同源；
- AOI：`auto_decoration.apply_tri_state_decoration` 对 `tt_qty` / 图点
  `value` 应用同一三态与同一截断算法（截断语义仿自引擎 `_clip_inside_spec`，
  稳定哈希 `_stable_fraction` 单一来源），另有**参数豁免**配置
  （`exempt_param_name_contains`，优先级低于 Delete、高于 True）。

payload 契约（ADR-0001）：`sheet_oos_decoration` 携带 `decoration_df /
decoration_path / decoration_sheet / decision_sheet / decision_df /
refresh_reason`——决策台账与重建原因因此能到达管理界面与下载文件
（下载 = 当前明细 + 决策台账双 sheet）。

## 8. 速查：事件 → 行为

| 事件 | SPC/CTQ/monitor | AOI |
|---|---|---|
| 普通 rerun / 筛选 | 缓存命中，不重算不写文件 | 同左（各自缓存） |
| 缓存 miss 且门控 unchanged | 重算载荷，**不写**工作簿 | 同左 |
| 满周期（core 4h / 外层 12h）后首次有效调用 | 重写一次，更新 meta | 同左 |
| 页头「刷新数据」全成功 | 推进 revision → 立即重写一次 | 同左（revision 按产品共享） |
| 页头「刷新缓存」 | hard reset + 立即重写一次 | 同左 |
| 上传决策台账 / 编辑 `__flags` | 签名变化 → 立即重写一次 | 无上传 UI；编辑 `__flags` 后签名变化 → 立即重写一次 |
| 直接编辑产品明细 sheet | 无效，下次重写被覆盖 | 同左（迁移完成后） |
| 新增 sheet_ID（新 OOS 键） | 进明细 sheet 默认 True，**不进** `__flags` | 同左 |
| OOS 键消失 | 明细行消失，决策留 `__flags`，重现即恢复 | 同左 |
| 工作簿被 Excel 占用 | 写失败上抛、页面报错、meta 不动 | 写失败由 service 兜底降级 |

## 9. 源码索引

- 引擎与门控：`src/inline_domain/core/shared/sheet_oos_decoration.py`
  （`prepare_sheet_oos_decoration` / `persist_sheet_oos_decoration` /
  `should_regenerate_detail` / `migrate_legacy_flags_if_needed`）
- 三态应用：`core/shared/auto_decoration.py`（AOI）、引擎内
  `apply_sheet_oos_decoration`（SPC/CTQ）
- 共享管线：`application/shared/decorated_features.py`、
  `application/shared/decorated_data.py`、
  `application/shared/decision_signature.py`
- AOI 链路：`core/aoi_tt/aoi_tt_decoration.py`、
  `core/aoi_rs/aoi_rs_decoration.py`
- 写入：`src/shared_kernel/utils/excel_tools.py`
- 管理后台：`app/sections/inline_domain/shared/decoration_admin.py` +
  `sheet_oos_admin.py`（仅 SPC/CTQ）
- 页头刷新：`app/components/page_header.py`
