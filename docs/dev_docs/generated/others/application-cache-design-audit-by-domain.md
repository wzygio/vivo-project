# Application 层缓存设计审计（By Domain）

- 审计日期：2026-09-04
- 审计范围：`src/*_domain/application/` 中的 Streamlit application cache，以及页面对这些缓存的调用、签名和失效链路
- 运行版本：Streamlit 1.60.0
- 审计基准：`docs/ADR/0001-streamlit-cache-native-payload-boundary.md`、`ARCHITECTURE.md`、`config/global.yaml`、Streamlit 1.60 随包性能指引
- 非目标：本报告不修改业务代码，不评价 infrastructure 层 SQL 本身是否高效，也不把所有“未加 `st.cache_data`”都视为问题

## 1. 结论摘要

当前 application 缓存设计处于两代模式并存状态：

1. **Inline 域整体最成熟**：普遍采用“缓存原生 payload、缓存外构造 ViewModel、TTL + revision/signature”的正确边界；主要问题是容量偏小，以及异常被转换为空 payload 后形成长时间负缓存。
2. **Yield 域缓存最多，但治理最弱**：10 个缓存函数均未设置 TTL 或 `max_entries`，并缓存多份相互重叠的大 DataFrame；同时动态时间窗口和部分资源版本没有成为显式缓存键。这是当前最需要治理的域。
3. **Equipment 域的 payload 边界正确，但缓存作用域错误**：报表计算本身不是产品级，页面却把产品 revision 放入缓存键，导致同一全局报表按产品重复存储；刷新底层全局快照后，又只使当前产品的 L2 条目失效。
4. **Q-Time 的键设计较完整，但违反 ADR-0001**：`st.cache_data` 直接返回项目 dataclass `QTimeMonitoringResult`，存在模块热重载期间的 pickle 类身份竞态。
5. **IJP 缺少的不是“所有查询缓存”**：结果查询由按钮门控并写入 `st.session_state`，单 Session 不会在普通 rerun 中重复执行；真正明确的缺口是级联筛选选项——每次 widget rerun 都会直接执行多条 SQL，且不同用户不能共享结果。

总体评级：**需要整改，但不需要推倒重建**。建议优先处理 4 个 P0/P1 问题：Q-Time payload 边界、Equipment 缓存作用域、Inline/Yield 负缓存、Yield 无界缓存。

## 2. 判定标准

本报告按以下规则判断缓存是否合理：

- 昂贵数据库读取、文件读取和 DataFrame 聚合适合使用 `st.cache_data`。
- 可序列化数据使用 `st.cache_data`；数据库 Engine、客户端等共享资源才使用 `st.cache_resource`。
- application 缓存只返回 DataFrame、原生容器和原生标量；项目 dataclass/Pydantic ViewModel 必须在缓存外构造。
- 参数化查询必须设置 TTL 或 `max_entries`，避免历史日期、旧 revision 和筛选组合永久占用内存。
- 缓存键必须覆盖会改变结果的全部输入：产品、查询窗口、配置/策略版本、快照版本、人工决策版本和资源文件版本。
- `_repository`、`_data_port`、`_service` 等下划线参数不会参与 Streamlit 哈希；只有当其语义能被其他显式参数完整代表时才安全。
- 瞬时数据库、文件或 COM 错误不应被转换成正常空结果后缓存数小时；让异常越过缓存边界，Streamlit 就不会保存失败调用。
- 不对廉价纯函数、UI ViewModel 组装、按钮门控的高基数明细查询盲目增加跨用户缓存。

严重度定义：

| 等级 | 含义 |
|---|---|
| P0 | 可能造成错误数据、跨模块热重载故障或刷新语义明显错误，应先修复 |
| P1 | 会造成长时间空报表、明显缓存抖动或无界内存增长，应在上线前处理 |
| P2 | 优化项或低概率一致性风险，可结合压测和监控处理 |

## 3. 缓存清单

| Domain | application 缓存 | 当前 TTL | `max_entries` | 初步判断 |
|---|---:|---:|---:|---|
| Yield | 10 | 全部无 | 全部无 | 无界且存在隐式输入，需重点整改 |
| Inline / SPC | 1 个报表缓存 + 共享特征缓存 | 12h + 12h | 3 + 32 | 边界合理；外层容量偏小、负缓存风险高 |
| Inline / CTQ | 1 个报表缓存 + 共享特征缓存 | 4h + 12h | 1 + 32 | `max_entries=1` 在多产品并发下易抖动 |
| Inline / AOI_TT | 1 | 12h | 3 | 键较完整；容量偏小、负缓存风险高 |
| Inline / AOI_RS | 1 | 12h | 3 | 键较完整；容量偏小、负缓存风险高 |
| Inline / Monitor | 1 个 application 大盘缓存 | 12h | 1 | 单一 ALL 聚合键场景下合理 |
| Inline / Shared | 特征缓存 1；决策签名缓存 1 | 12h；4h | 32；64 | 设计合理，签名与容量均有界 |
| Equipment | 1 | 12h | 无 | payload 边界正确；作用域错误且无界 |
| Indicator / Q-Time | 1 | 12h | 32 | 键较完整；返回类型违反 ADR-0001 |
| Indicator / IJP | 0 | 无 | 无 | 报表按钮门控合理；筛选选项缺少共享缓存 |

配置值来自 `config/global.yaml:14-26`。装饰器中的 `default_hours` 只是配置缺失时的后备值，当前运行以全局配置为准。

## 4. 跨域主要发现

### F-01（P0）：Q-Time 缓存直接返回项目 dataclass

证据：

- `src/indicator_domain/application/qtime/cached_monitoring.py:73-96` 的 `_cached_monitoring()` 被 `st.cache_data` 修饰，并直接返回 `QTimeMonitoringResult`。
- `QTimeMonitoringResult` 定义于 `src/indicator_domain/application/qtime/service.py:33-39`，属于项目自定义 dataclass。
- ADR-0001 明确禁止项目 dataclass/Pydantic/ViewModel 直接作为 `st.cache_data` 返回值，并要求 payload/facade 两层接口。

影响：页头刷新会卸载并重新导入项目模块。若缓存填充与模块重载并发，旧类实例在 pickle 时可能解析到新类，产生 `UnserializableReturnValueError`；该问题已经在 SPC 和关键备件历史回归中被验证过。

建议：将 `_cached_monitoring()` 改为返回原生 dict：DataFrame、字符串路径或 `None`；新增无缓存 facade，在当前模块中构造 `QTimeMonitoringResult`。补充“缓存填充期间 reload 模块”的并发回归测试。

### F-02（P0）：关键备件全局报表被错误地按产品缓存和失效

证据：

- `PartsReportService.fetch_report_payload()` 的业务输入只有数据库管理器、全局 baseline 路径和签名，未消费产品配置，见 `src/equipment_domain/application/parts_service.py:83-155`。
- 页面却用当前活动产品构造 `parts_report_cache_signature`，见 `app/pages/关键备件报表.py:74-79`。
- 强刷调用 `safe_refresh_snapshots()` 刷新的是同一套全局备件快照，见 `app/pages/关键备件报表.py:80-89`；页头随后只推进当前产品 revision。

影响：

- 相同全局报表会按 M626、M678 等产品重复缓存，浪费 DataFrame 内存。
- 用户在产品 A 下刷新全局快照后，产品 B 对应的旧 L2 条目不会失效；切换到 B 可能继续看到旧报表，直到 12h TTL 到期。

建议：二选一并明确语义：

1. 若关键备件报表确实是全局报表，使用设备域全局 revision / baseline 文件签名 / 快照签名，不使用产品 revision；刷新时使唯一全局 L2 条目失效。
2. 若应为产品级报表，则把 product code 纳入 service 查询和计算，不能只把产品放进缓存键。

### F-03（P1）：Inline 把瞬时异常缓存成 4～12 小时空报表

证据：

- SPC：`src/inline_domain/application/spc/spc_service.py:286-297` 捕获宽泛异常并返回 `_empty_payload()`。
- CTQ：`src/inline_domain/application/ctq/ctq_service.py:148-150` 同样返回空 payload。
- AOI_TT：`src/inline_domain/application/aoi_tt/aoi_tt_service.py:185-187` 同样返回空 payload。
- AOI_RS：`src/inline_domain/application/aoi_rs/aoi_rs_service.py:177-179` 同样返回空 payload。
- 以上函数都位于 `st.cache_data` 边界内，TTL 分别为 4h 或 12h。

影响：一次短暂数据库断连、Excel COM 失败、配置文件被占用或意外代码异常，会被当作一次“成功的空报表”保存。即使依赖几秒后恢复，相同缓存键仍会持续返回空数据，造成误导性的长时间无数据状态。

建议：

- 确定性的“查询成功但无数据”可以缓存。
- 数据访问、工作簿读取和未知异常应记录上下文后重新抛出稳定 application error，让失败调用不进入缓存。
- 页面负责显示安全错误文案；不要用空 DataFrame 同时表达“无数据”和“读取失败”。

### F-04（P1）：Yield 的缓存条目全部无界

证据：`src/yield_domain/application/yield_service.py:85-86,132-133,223-224,248-249,293-294,332-333,380-381,425-426,454-455,483-484` 的 10 个缓存均只有 `show_spinner=False`，没有 TTL 或 `max_entries`。

影响：产品 revision、配置对象、历史时间窗口、缩放倍率和资源签名每次变化都会留下永久孤儿条目。`get_raw_panel_details`、`get_modified_panel_details`、Code/Group 趋势、Lot、Sheet 和 Mapping 又保存多份相互重叠的大 DataFrame，长期运行时内存增长不可控。

建议：

- 把 Yield 纳入 `config/global.yaml.service_cache`，至少给所有参数化缓存配置 TTL。
- 对高基数函数增加 `max_entries`。初始值应根据 7 个启用产品和同时保留的窗口/revision 数计算，而不是统一设为 1 或 3。
- 先保留昂贵的 derived cache；通过命中率和对象大小监控判断是否删除某一层，避免仅凭“层数多”直接移除缓存。

### F-05（P1）：Yield 的动态时间窗口不是显式缓存输入

证据：

- `_build_panel_request()` 在缓存函数内部调用 `get_time_window()`，见 `src/yield_domain/application/yield_service.py:118-129`。
- `get_time_window()` 依赖 `datetime.now()` 或类变量 `_custom_end_date`，见同文件 `:53-71`。
- 页面传入的基础签名是固定字符串 + 产品 revision，见 `app/pages/入库不良率分析看板.py:38-46`；页面还明确声明配置资源只靠人工刷新失效，见 `:77-78`。

影响：跨日后，在 snapshot signature/revision 未变化时，缓存键不变，滚动窗口可能继续返回前一天结果；调用 `set_analysis_end_date()` 后也可能命中旧窗口结果。因为 Yield 又没有 TTL，该状态不会自动修复。

建议：在进入第一个缓存边界前计算并序列化 `start_date/end_date`，让窗口成为键的一部分；人工锁定日期也走同一路径。TTL 只用于条目回收，不应代替正确的时间窗口键。

### F-06（P1）：Inline 产品页外层缓存容量低于产品集合

证据：全局启用产品有 7 个，见 `config/global.yaml:28-36`；SPC、AOI_TT、AOI_RS 外层缓存均为 `max_entries=3`，CTQ 为 `max_entries=1`，分别见：

- `src/inline_domain/application/spc/spc_service.py:173-180`
- `src/inline_domain/application/ctq/ctq_service.py:87-94`
- `src/inline_domain/application/aoi_tt/aoi_tt_service.py:113-120`
- `src/inline_domain/application/aoi_rs/aoi_rs_service.py:123-130`

影响：少于 10 个用户同时查看不同产品时，外层完整报表 payload 会频繁相互驱逐。共享特征缓存仍可能命中，因此正确性不受影响，但 CPK、图表点位和 payload 组装会重复执行。CTQ 的 `max_entries=1` 尤其容易形成产品切换抖动。

建议：以“启用产品数 × 常用窗口数 + revision 余量”确定容量。若页面通常只有一个标准滚动窗口，可先评估 8～16；共享特征的 32 条容量可容纳 7 产品 × SPC/CTQ/AOI scope 并留有余量，当前较合理。

### F-07（P1）：IJP 级联筛选在每次 rerun 上执行多条数据库查询

证据：

- `IjpReportService.get_filter_options()` 一次调用最多组合 `list_product_codes`、`list_product_names`、`list_sub_prod_types`、`list_picis`、`list_cycles` 五次数据端口访问，见 `src/indicator_domain/application/ijp/service.py:41-71`。
- 当前 IJP repository 这些方法直接执行 SQL，没有 L1 快照，见 `src/indicator_domain/infrastructure/ijp/repository.py:97-180,289-293`。
- 页面在所有筛选 widgets 之前、每次脚本 rerun 都调用该方法，见 `app/sections/indicator_domain/ijp/dashboard.py:111-125`。
- IJP 页明确注册 `cached_funcs=[]`，见 `app/pages/IJP溢流监控报表.py:35-40`。

影响：任何 multiselect、日期或文本输入变化都会重新打生产库；10 个并发用户操作筛选时，选项查询数量会被显著放大。

建议：

- 优先缓存稳定选项（产品 code、产品名、工单类型），TTL 可从 15～60 分钟起步。
- 对依赖时间和上游选择的批次/Cycle 选项设置较短 TTL 和有界 `max_entries`，键包含时间窗、产品集合、批次集合与 enabled-product 配置签名。
- 报表明细已经由“查询按钮 + `st.session_state` 结果”门控，见 `dashboard.py:186-206,258-285`。其筛选组合高、结果大，不建议在没有命中率证据时直接做长 TTL 全局缓存；可先只缓存日比率或设置小容量、短 TTL 的组合 payload。

## 5. By Domain 详细评价

### 5.1 Yield Domain

评价：**缓存覆盖充分，但存在无界、隐式键和重复驻留；属于“缓存过度保留”，不是单纯缓存过多。**

合理部分：

- 原始查询与派生计算分层，复杂 Code/Group、Lot、Sheet、Mapping 计算能够复用结果。
- `_db_manager` 排除哈希是正确的，查询 DTO JSON、配置和 snapshot signature 承担结果语义。
- 返回值为 DataFrame、dict 或标量，符合 ADR-0001 的稳定 payload 边界。
- `read_only` 进入缓存键，矩阵只读消费不会复用到会写修饰表的条目。

问题与建议：

| 项目 | 判断 | 说明 |
|---|---|---|
| 10 个缓存均无 TTL/容量 | P1 | revision 和窗口变化会永久留下大对象 |
| 时间窗口在缓存内部读取 | P1 | `datetime.now()` / `_custom_end_date` 不是显式键 |
| raw + modified + 多级派生均缓存 | P2 | 显著重复驻留；是否删层应由对象大小和命中率决定 |
| `load_static_warning_lines()` 用 panel snapshot signature 失效 | P2 | 警戒线文件变化与 panel 快照并非同一事件；当前只能靠产品人工刷新 |
| Lot/Sheet 在缓存函数内读取 rate override | P1 | override 文件内容/mtime 未成为独立键；且 `get_lot_defect_rates()` 没有 `modifier_signature` 参数 |
| `get_modifier_context()` 在缓存 miss 中同步并可能写文件 | P2 | 这是有意的 write-on-miss，但需要并发/失败回归；签名 JSON 写入当前不是原子提交 |
| 异常降级为空并被永久缓存 | P1 | `load_static_warning_lines()` 失败返回 `{}`；缺陷倍率异常也可能缓存未修饰结果 |

建议的目标结构：

1. `fetch_raw_panel_payload(query_json, policy_json, snapshot_revision)`：有 TTL/容量，显式时间窗。
2. `build_modified_panel_payload(..., policy_signature)`：若内存压力高，可与 raw 合并或只保留一层。
3. 各重计算派生函数：保留缓存，但统一 TTL/容量，并把 modifier、warning、override 的内容签名作为相关函数的键。
4. 缓存函数保持纯计算；需要同步台账时，最好由显式 orchestration 在缓存边界外执行，然后把稳定输入签名传入纯缓存函数。

### 5.2 Inline Domain

评价：**整体合理，是其他 Domain 应参考的基线；需要修复负缓存与外层容量。**

合理部分：

- SPC、CTQ、AOI_TT、AOI_RS 均缓存原生 payload，并在 facade 中构造 ViewModel，符合 ADR-0001。
- 查询 JSON、snapshot signature、product revision、decision signature 均进入键。
- `fetch_decorated_features()` 把产品、scope、起止日期、快照、产品 revision、决策签名全部纳入键，见 `src/inline_domain/application/shared/decorated_features.py:77-102`。
- 决策签名采用 file stat 廉价探针 + 内容 hash 二阶段缓存，见 `src/inline_domain/application/shared/decision_signature.py:37-73`，避免普通 rerun 反复读取 Excel。
- Monitor 页面获取 ALL 数据后在前端切片，application 大盘缓存 `max_entries=1` 与当前单一聚合主键相符；这里不应机械地调大。

需要调整：

- 先修复 F-03 的异常负缓存。
- 按 F-06 调整产品页外层容量。
- SPC/CTQ 外层 payload 与共享 features payload 同时保存 raw/features DataFrame，存在双份序列化。SPC 外层还有昂贵能力计算，保留合理；CTQ 外层主要做图表类型赋值和 indicators 去重，是否保留应以命中率与对象大小为依据，可列为 P2 压测项。
- `get_monitor_defect_details()` 没有 application 装饰器，但它复用了 `fetch_decorated_features()`，且管理表路径另有 app 层 `get_cached_alarm_detail_tables()`。明细参数基数很高，因此当前“不缓存最终明细、只缓存底层特征”的做法合理，不构成明确缺失。

### 5.3 Equipment Domain

评价：**缓存函数本身设计良好，但页面提供的缓存作用域与真实业务作用域不一致。**

合理部分：

- `fetch_report_payload()` 只返回 DataFrame 和标量，`get_report_data()` 在缓存外构造 ViewModel。
- TTL 12h 有配置来源；数据库/计算异常没有被吞成可缓存空 payload。
- baseline、真实/仿造快照、规则计算在一次 payload 构建中完成，缓存粒度合适。

需要调整：

- 按 F-02 统一全局或产品级语义。
- 增加 `max_entries`，防止 revision 迭代产生无限历史条目。
- baseline 内容和运行策略来自文件/配置，但当前键只有路径和页面签名。若保留全局缓存，应加入 baseline file stat/content signature 与 equipment policy signature。
- `as_of=pd.Timestamp.now()` 在缓存内部读取，12h TTL 使其最多 12h 更新一次；若业务要求按自然日变化，应把 `as_of_date` 显式放入键。

### 5.4 Indicator Domain / Q-Time

评价：**缓存键设计细致、容量合理，但 payload 类型是 P0 架构违规。**

合理部分：

- `as_of=None` 在进入私有缓存前归一为当天日期，避免 `None` 与显式 today 形成两个条目。
- shop、站点、产品、as-of、决策文件 mtime/size 均在键中。
- 厂别全站点/全产品入口让 Q-Time 页面和预警矩阵共享同一条缓存，避免增加产品筛选基数。
- `max_entries=32` 与 3 个厂别、日期和决策版本余量相符。

需要调整：

- 按 F-01 改为原生 payload + facade。
- L2 键没有 L1 快照版本；只靠 12h TTL。若 Q-Time 快照被外部刷新，L2 在 TTL 内看不到变化。建议加入 snapshot revision/signature，或让成功强刷推进对应 revision。
- 当前页头仅清 L2，未注册 Q-Time snapshot 强刷 handler，见 `app/pages/Q_Time监控报表.py:39-44`。若“刷新缓存”期望读取最新数据库，这一语义目前不完整。
- `get_filter_options()` 每次页面 rerun 调用，但 intended repository 已有 24h L1 option snapshot，读取成本主要是小 Parquet；application 再加一层不是优先事项。

审计限制：当前工作区的 tracked 文件 `src/indicator_domain/infrastructure/qtime/repository.py` 处于已删除状态，且同目录 snapshot 代码和测试存在未提交修改。本报告没有恢复或修改这些用户工作。上述 L1 行为仅以 `HEAD` 中的 repository 作为背景，不代表当前工作树能够成功运行 Q-Time。

### 5.5 Indicator Domain / IJP

评价：**存在针对筛选选项的明确缺失缓存；报表明细不应直接照搬其他 Domain 的长 TTL 大 payload。**

合理部分：

- 点击“查询”后才执行 `get_daily_ratios()` 和 `get_details()`。
- 结果与筛选签名保存在当前 Session State；普通渲染和无关 rerun 不会重复执行大查询。
- repository 错误会上抛为稳定 application error，不会形成负缓存。

缺失部分：

- 每次 rerun 的五类筛选 SQL 没有 application L2，也没有 infrastructure snapshot。
- 不同 Session 无法共享相同筛选选项，正是 10 用户并发时最容易放大的负载。
- `config/global.yaml.service_cache` 尚无 IJP 配置键，页头也未注册缓存函数。

建议先新增两个层次：

1. 稳定元数据 payload：产品 code/name、工单类型，TTL 30～60 分钟，较小容量。
2. 级联窗口 payload：批次和 Cycle，TTL 5～15 分钟，`max_entries` 依据常用日期/产品组合设置。

报表结果是否跨 Session 缓存应通过日志确认重复查询率；若需要，优先缓存较小的 daily ratio，并谨慎缓存带 `detail_limit` 的明细。

## 6. 哪些未缓存是合理的

以下对象当前不需要因为“application 层函数”身份而添加缓存：

- `SpcReportService.get_spc_report_data()`、`CtqReportService.get_ctq_report_data()`、`PartsReportService.get_report_data()`：它们是缓存外的轻量 ViewModel facade，保持无缓存正是 ADR-0001 的要求。
- `AlertService`：消费已缓存趋势数据，主要进行小规模规则筛选；继续缓存会复制输入依赖并增加失效复杂度。
- `FileManagerService`：目录分类成本较低，而且需要及时反映文件增删。
- `PDFService` / `PPTService`：转换由用户动作触发，并将页面图片落入输出目录；应依赖源文件签名和生成物复用，而不是直接缓存 service 实例。
- IJP 最终明细：已有查询按钮和 Session State 门控，筛选组合高、结果较大；在无命中率证据时不应默认做 12h 全局缓存。
- Monitor 最终 drill-down：底层特征已有缓存，最终筛选参数基数高；按需计算比无界缓存每个明细组合更安全。

## 7. 推荐整改顺序

### 第一批：正确性与稳定性

1. Q-Time 改为 native payload + facade，并补模块重载竞态测试。
2. 明确关键备件是全局还是产品级，修正缓存 key、刷新 revision 与底层快照的作用域。
3. Inline 和 Yield 区分“成功但无数据”与“读取/计算失败”，禁止把瞬时故障保存为正常空结果。
4. Yield 把 `start_date/end_date` 提前到缓存键。

### 第二批：容量与资源签名

1. 为 Yield 全部参数化缓存增加 TTL 和 `max_entries`。
2. 调整 SPC/CTQ/AOI 外层 `max_entries`，至少覆盖常用产品集合。
3. 给 Yield warning、modifier、rate override 以及 Equipment baseline/policy 建立独立内容签名。
4. 为 IJP 筛选元数据和级联选项增加短 TTL、有界缓存。

### 第三批：以指标驱动去重

记录每个缓存函数的 miss 次数、计算耗时、返回 DataFrame 内存、条目驱逐情况；重点验证：

- Yield raw/modified/derived 多层缓存是否需要全部保留。
- CTQ 外层 payload 是否比只保留 shared features 有明显收益。
- IJP daily ratio/明细是否存在足够跨用户重复度，值得增加全局缓存。

## 8. 建议的验证用例

| 用例 | 应验证的行为 |
|---|---|
| 相同 key 连续调用 | repository/重计算只执行一次 |
| 产品、窗口、策略、快照或决策签名变化 | 只使相关条目 miss |
| 一个产品刷新 | 不清空其他产品缓存；全局报表例外应全部一致失效 |
| 数据库首次失败、第二次恢复 | 第二次必须重新访问数据源，不能命中空负缓存 |
| 缓存填充期间 reload service 模块 | 不出现项目类 pickle 身份错误 |
| 连续推进 revision 超过容量 | 旧条目被淘汰，进程内存趋于稳定 |
| 7 产品、10 Session 交错访问 | CTQ/SPC/AOI 不出现持续重复计算抖动 |
| 跨日不手动刷新 | Yield/Q-Time 的新日期窗口自动产生正确 key |
| 修改 warning/modifier/override/baseline 文件 | 只重建真正依赖该资源的 payload |
| Q-Time/Equipment 强刷 | L1 成功更新后，所有对应 L2 视图一致更新 |

## 9. 最终判断

- **缓存确实缺失**：IJP 级联筛选选项；Q-Time/Equipment 的部分 source revision 传播。
- **缓存确实过度**：Yield 的无 TTL/无容量历史保留；Equipment 同一全局报表按产品重复缓存。Inline 的 CTQ 外层双层 payload 可能过度，但需要测量后再删。
- **缓存正确但参数不合理**：SPC/CTQ/AOI 产品页外层容量低于 7 产品并发使用需求。
- **缓存存在正确性风险**：Q-Time 缓存项目 dataclass；Inline/Yield 异常空结果负缓存；Yield 隐式动态时间窗口与资源签名缺口。
- **不应盲目补缓存**：轻量 facade、AlertService、按钮门控的 IJP 大明细和高基数 Monitor drill-down。

建议以 ADR-0001 的 payload/facade 模式和 Inline shared cache 的显式签名模式作为全项目统一模板，再分别针对报表更新频率设置 TTL 和容量，而不是按 Domain 复制同一个数字。
