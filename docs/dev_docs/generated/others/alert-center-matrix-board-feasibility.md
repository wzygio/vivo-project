# Task-1 自动预警看板矩阵化改造 — 可行性分析报告

- 日期：2026-09-02
- 需求来源：`docs/dev_docs/dev_spec/Inline_domain/feat-alert_center.md`（Task-1：自动预警看板）
- 分析样例：`indicator_domain/qtime`（`app/pages/Q_Time监控报表.py`）
- 结论：**可行，且比预期容易** —— 矩阵看板所需的"全产品预计算"管线已在现有 `自动预警看板` 中跑通，本次改造以呈现层为主；真正的新增风险集中在缓存键设计、qtime 无缓存直查生产库、以及跨域预警口径统一三处。

---

## 1. 需求重述

客户希望把自动预警看板从"超规项列表 + 自动渲染图像"升级为**矩阵看板（By 子模块）**：

- 列 = 各产品（当前 7 个：M626 / M673 / M678 / Z517 / Z553 / Z571 / Z576）
- 行 = 各监控参数（aoi_rs / aoi_tt / spc / ctq 的 sheet 超规，spc 的 cpk 超规，yield 的 lot 超规与良率波动）
- 单元格 = 达标绿点 / 不达标红点

已知难点（需求方自述）：
1. 与现有 Header"By 产品筛选"模式相违背；
2. 计算量巨大，需要预先计算所有产品的后台数据。

可接受的妥协：不再自动渲染图像，改为点击单元格后再查询详情。

## 2. 以 qtime 为例的模式分析

### 2.1 qtime 的数据流现状

- 页面极薄（`app/pages/Q_Time监控报表.py`，44 行）：组合根 → 页头 → `render_qtime_dashboard`（`app/sections/indicator_domain/qtime/dashboard.py`）。
- 数据源为**单一集中式数据库表** `mdw.qtime_tzbjx`（决策见 `docs/ADR/0019-qtime-report-data-source-and-ui-boundary.md`），`data/` 目录下**没有任何 qtime 本地文件**。
- **没有任何缓存**：全链路无 `st.cache_data`，页头 `cached_funcs=[]`；每次点击"查询"都是一次实时库查询，仅靠 session_state 签名 `(shop, step_options, product)` 防止同条件重查（`dashboard.py:21-22, 82-86`）。
- 计算链路非常轻：`build_qtime_oos_detail`（布尔过滤 + 一列减法，`core/qtime/decoration.py:37-51`）→ 决策台账 merge（`decoration.py:80-100`）→ `build_qtime_alerts`（取 flag=False，`core/qtime/alerts.py:19-28`）。无 SPC 规则引擎、无特征管道。

### 2.2 关键事实：qtime 页面目前也是 By 产品模式

`dashboard.py:135` 传的是 `products=(selected_product,)`。但服务层签名为 `products: tuple[str, ...] = ()`（`application/qtime/service.py:60`），仓储仅在非空时追加 `AND prodcode IN :products`（`infrastructure/qtime/repository.py:116-118`）。

**即 qtime 切全产品在架构上只是少传一个 WHERE 条件，一行代码的事。**

### 2.3 qtime 与 inline 模式的本质差异

| 维度 | qtime | inline（aoi_rs / aoi_tt / spc / ctq） |
|---|---|---|
| 数据物理布局 | 单表含全部产品，产品是行内普通列 | 按产品分目录分文件：`data/{PROD}/inline_measurements_{PROD}.parquet` 等 |
| 全产品代价 | SQL 少一个 WHERE，一次查询 | 循环 7 产品 × 每产品建 repo、读快照、跑特征管道 |
| 修饰键 | `(prodcode, step_desc, lot_id, timekey)`，产品天然在键里 | 修饰工作簿按产品分 sheet，跨产品需逐产品预算决策签名 |
| 计算 | 过滤 + merge，O(行数) | sheet 级特征聚合 + SPC 规则 + 修饰持久化 |
| 缓存 | 无 | L1 parquet 快照（TTL 12h）+ L2 `@st.cache_data`（TTL 12h/4h） |

**结论：qtime"可以全产品"的真正原因不是页面设计，而是数据架构**（集中式单表 + 产品作为行内维度）。inline 的 By 产品是被"每产品一套快照 + 每产品一个修饰 sheet + 每产品一份 spec/config"的物理边界决定的。因此 qtime 只能证明"全产品查询在交互上可行"，不能直接照搬到 inline——但好消息是 inline 侧的全产品管线已经存在（见下节）。

## 3. 现状盘点：最难的部分已经做了

`app/pages/自动预警看板.py` **已经是全产品模式**：

- 固定以 `prod_code="ALL"` 查询（`自动预警看板.py:138-153`），`MonitorAnalysisService.fetch_dashboard_data_dict`（`src/inline_domain/application/monitor/monitor_service.py:281`）内部通过 `discover_monitor_products` 扫描全部产品目录逐产品加载（`monitor_service.py:311-314`）。
- Header 的"产品筛选"selectbox 虽被渲染，但页面数据流完全忽略它，改用自带的控制台（监控类型 + 产品 multiselect + 厂别 multiselect，`app/sections/.../monitor_dashboard.py:44-69`），前端对已缓存的 ALL 数据做切片，**不重查后端**。这意味着"与 Header 筛选模式相违背"的顾虑已有先例解法：矩阵页不参与 Header 单产品筛选即可。
- 缓存键已包含逐产品 revision 与 (prod, scope) 决策签名（`自动预警看板.py:107-117`），即"7 产品 × 多 scope 签名预算"已有生产先例。
- 渲染层面，`render_monitor_summary_chart` 与 Top10 图已使用 `app/manager/render_gate.py` 的两阶段模式（`stage()` 注册纯计算，`collect()`/`collect_memoized` 统一执行 + 签名命中跳过重建）。

因此矩阵看板的核心工作是把现有 ALL payload 从"时间桶聚合表"**重排为"产品 × 参数"红绿矩阵**，属于呈现层改造，不是从零造模式。

## 4. 计算量卡点分析

### 4.1 数据规模实测（`data/` 与 `resources/`）

| 域 | 规模 | 说明 |
|---|---|---|
| inline 量测 | ~246 万行 / ~30MB parquet | M626 490K、M673 185K、M678 614K、Z517 304K、Z553 5K、Z571 858K、Z576 0.7K |
| yield 快照 | ~354 万行 | 最大 Z571 986K + M678 995K |
| aoi_rs | ~43 万行 | 两类快照合计 |
| inline 修饰工作簿 | spc_cpk_cpm 392K、spc_sheet_oos 192K，其余 ≤ 40K | `resources/inline_domain/`，全部很小 |
| qtime | 本地零文件 | 纯生产库查询，估万~十万行量级 |

### 4.2 卡点清单

1. **首次冷启动**：7 产品 × 3 scope（SPC/CTQ/AOI）全量跑特征 + 规则 + 修饰，是全链路最重的一步；之后靠 L1（parquet 快照，TTL 12h）+ L2（`st.cache_data`，TTL 12h/4h）秒开。矩阵化不改变这个总量——预警看板已在承担同样的成本。
2. **L2 缓存条目数抖动**：`fetch_decorated_features` 设 `max_entries=12`（`src/inline_domain/application/decorated_features.py:79`），键为 (prod, scope, window, 签名)。矩阵模式 7 产品 × 2~3 scope = 14~21 个条目，**会超过 max_entries 产生淘汰抖动**，必须调大。
3. **ALL payload 单点重算**：`fetch_dashboard_data_dict` `max_entries=1`（`monitor_service.py:276`），任何签名变化全量重算——矩阵若复用此入口，任一产品修饰决策变更都会触发 7 产品重算。
4. **决策签名预算的 IO 放大**：`get_scope_decision_signature` 需 stat 7 产品 × 多 scope 的工作簿（`decision_signature.py:35-41`）；企业加密 xlsx 存在 Excel COM 回退路径（`sheet_oos_decoration.py:217-233`），COM 调用落在页面 rerun 路径上需警惕（目前已有 file_stat 两阶段门控缓解）。
5. **qtime 直查生产库**：无缓存，接入矩阵意味着每次页面加载实时打库；ADR-0019 还记录过目标表权限与资源治理超时的历史。接入前必须补缓存层。
6. **yield 域冷加载**：354 万行快照为全域最重，但已有 `@st.cache_data` + snapshot_signature 机制，可复用。
7. **自动渲染图像（若保留）**：SPC 页现状是每个预警指标 3 张 Plotly 图（`spc_dashboard.py:688-759`），矩阵全产品展开后图像数量不可控——**这正是需求方 hint 中"改为点击后查询"要消除的卡点，建议采纳**。

## 5. 架构风险点分析

1. **Session 单产品模型**：`SessionManager` 以单产品 active_config 为中心（`session_manager.py:15-40`），CPK 计算依赖每产品 `get_spc_spec_limits(prod)`。矩阵页需要同时解析 7 份产品配置。风险可控：预警看板已在 ALL 模式下逐产品拉 spec（`monitor_service.py:360-361`），沿用即可，不要改动 Session 模型本身。
2. **缓存失效联动**：单产品页用 `product_cache_scope` revision（`page_header.py:88-97`）；矩阵页必须沿用逐产品 revision 字典进缓存键的模式（`monitor_service.py:288-289` 已支持 `product_revisions`/`decision_signatures` 参数）。否则"刷新缓存"按钮只能清 inline 或误伤其他页面。
3. **降级语义耦合**：monitor 的 `safe_refresh_snapshots` 任一产品失败即整体不推进 revision（`monitor_service.py:655-712`）。全产品模式下"一个产品坏了拖垮整板"会被放大，矩阵页需要按单元格/按产品做降级展示（失败产品显示灰点 + 提示），而不是整页报错。
4. **跨域口径不统一**：qtime 台账是单工作簿行内 prodcode；inline 是按产品分 sheet + 分 scope 文件；yield 走 `abnormal_detector.py` 的环比/激增规则。矩阵单元格红/绿的判定必须在所有域的决策签名预算完成后统一渲染，否则出现半新半旧的混合状态。建议先定义统一的"单元格状态契约"（达标 / 不达标 / 无数据 / 加载失败四态），各域适配到该契约。
5. **渲染卡顿**：需继续遵守 render_gate 的两阶段模式——所有 payload 计算完成后一次性渲染；矩阵单元格点击后的详情查询走独立的懒加载路径，不阻塞矩阵本体。

## 6. 解决方案建议

### 6.1 推荐方案（采纳 hint 的妥协）

- **矩阵本体**：复用 `fetch_dashboard_data_dict`（或抽出其逐产品循环为共享服务），产出"产品 × 参数 × 四态"的轻量 payload；单元格只渲染红/绿/灰点，不渲染图像。
- **点击详情**：点击单元格后按 (prod, scope, 参数) 懒查询明细与图像——此时退回单产品粒度，可直接复用现有 SPC 页预警图像管线（RenderGate + `collect_memoized`，签名含产品 revision + 预警内容指纹，`spc_dashboard.py:762-782`）。
- **缓存调整**：
  - 调大 `fetch_decorated_features` 的 `max_entries`（12 → ≥ 32）；
  - 给 qtime 补一层 `@st.cache_data`（TTL 与 inline 对齐，键含产品集合 + 时间窗 + 决策签名）后再接入矩阵；
  - yield 沿用现有 snapshot_signature 机制，仅需把决策签名纳入矩阵缓存键。
- **失效语义**：矩阵缓存键 = 逐产品 revision 字典 + 逐 (prod, scope) 决策签名 + 时间窗（上一 ISO 周），全部沿用 `自动预警看板.py:107-117` 的既有先例。
- **降级**：单产品加载失败时该列显示灰点并标注原因，不阻断其他产品。

### 6.2 工作量分布估计

| 部分 | 性质 | 预估占比 |
|---|---|---|
| 矩阵呈现层（含四态契约、点击懒查询） | 新增，纯前端/ViewModel | ~50% |
| qtime 缓存层补齐 | 新增，小 | ~15% |
| yield 域接入矩阵键 | 适配 | ~15% |
| 缓存参数调整 + 降级逻辑 | 适配，小 | ~10% |
| E2E 验证 | 测试 | ~10% |

### 6.3 不推荐的方向

- 保留矩阵内自动渲染图像：图像数量随 产品 × 参数 爆炸，且与 hint 允许的妥协相悖。
- 改动 Session 单产品模型或 Header 筛选机制：冲击面过大且无必要，矩阵页像预警看板一样绕开即可。
- 为矩阵新建一套并行缓存体系：会与现有 L1/L2 失效语义打架，沿用并调参即可。

## 7. 最终意见

**可行，建议立项实施。** 理由：

1. 全产品预计算管线已在 `自动预警看板` 生产验证，矩阵化是呈现层重排 + 两个域（qtime、yield）的接入适配，不存在颠覆性架构改动；
2. 需求方自述的两个难点均已有先例解法：Header 筛选冲突（矩阵页自带控制台、忽略 Header 产品筛选）、全产品计算（ALL 模式 + 逐产品签名预算）；
3. 采纳"点击后再查询详情/图像"的妥协后，计算量与渲染压力均落在现有系统已证明可承受的范围内。

需在 PRD 阶段锁定的三个前置决策：

1. **单元格四态契约**（达标 / 不达标 / 无数据 / 加载失败）的精确定义与各域映射；
2. `fetch_decorated_features` 缓存容量调大与 qtime 缓存层的 TTL 取值；
3. 单产品失败时的矩阵降级样式（灰点 + tooltip）。

## 附：关键代码索引

- 矩阵数据基础：`src/inline_domain/application/monitor/monitor_service.py:281`（`fetch_dashboard_data_dict`）、`:311-314`（逐产品循环）
- 全产品签名先例：`app/pages/自动预警看板.py:107-153`
- 渲染门：`app/manager/render_gate.py:32-67`
- 预警图像懒加载复用点：`app/sections/inline_domain/spc/spc_dashboard.py:762-828`
- qtime 全产品切换点：`app/sections/indicator_domain/qtime/dashboard.py:135`、`src/indicator_domain/infrastructure/qtime/repository.py:116-118`
- qtime 预警判定：`src/indicator_domain/core/qtime/alerts.py:19-28`
- yield 波动判定：`src/yield_domain/core/abnormal_detector.py:22-148`
- 缓存容量隐患：`src/inline_domain/application/decorated_features.py:79`
- 相关 ADR/PRD：`docs/ADR/0019-qtime-report-data-source-and-ui-boundary.md`、`docs/PRD/PRD-2026-08-25-Inline自动预警中心.md`、`docs/PRD/PRD-2026-08-18-Inline-Sheet-OOS修饰刷新与决策持久化.md`
