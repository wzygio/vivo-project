# adr-0006：rerun 减重——图层 memo 与 payload 缓存容量

- Status: Accepted
- Date: 2026-08-06
- Scope: `app/manager/render_gate.py`、`app/sections/spc/spc_dashboard.py`、`src/inline_domain/application/spc/spc_service.py`

## Context

核心使用场景：用户在同一份数据上反复切换筛选条件、深入研究细节。
对缓存语义的分析结论：

1. `st.cache_data` 为进程级共享（跨会话/跨标签/跨用户），是否重算由缓存 key
   （产品 + 日期窗口 + revision + sigma 来源）决定；`max_entries` 是内存中保留的
   payload 份数。
2. 筛选切换在前端对已缓存 payload 做 pandas 过滤，不改变 key——数据层缓存
   对该场景一直有效。
3. 两条失效路径：
   - `max_entries=1`：多标签/多产品互相驱逐，交替使用时每次 rerun 都是
     30~90s 全量重建；
   - 图层无缓存：payload → Plotly Figure 的构建每次 rerun 重做，其中
     "自动预警指标图像"位于查询闸门之外，任何筛选点击都会触发全部预警图重建。

## Decision

1. **图层 memo（可复用机制，入 `app/manager/render_gate.py`）**：
   `RenderGate.collect_memoized(state_key, signature)`——签名命中
   `session_state` 时直接返回缓存 payload（无 spinner、不执行构建任务）；
   未命中时 `collect()` 并写入。签名 = 产品缓存 revision（
   `build_product_cache_signature`）+ 预警内容指纹（行数 + 内容哈希），
   保证点"刷新缓存"后必变、必重建，不存在旧数据复用路径。
   `render_spc_indicator_sections` 增加可选 `memo_signature` 参数，
   自动预警区（`render_cpk_alert_indicator_sections`）接入；手动查询区不传，
   行为不变。
2. **payload 缓存容量**：`fetch_spc_report_payload` 从 `max_entries=1` 改为
   `max_entries=3, ttl=4h`。不设无穷的原因：key 含日期窗口与 revision，
   跨日与每次"刷新缓存"都会产生新 key，无穷缓存使孤儿条目永不释放、内存
   单调增长；TTL 负责兜底回收。
3. 不采用"换 key → 显式 clear()"的失效重构（改动全站共用失效机制，回归面大）。

## Consequences

- 内存实测（2026-08-06，M626 当前查询窗口）：单条 payload 约 25 MB
  （raw_measurements 43k 行 24 MB 为主），3 条约 76 MB，内存有界且充裕。
- 同一版数据下，筛选点击的 run 时长从秒级降到亚秒级（预警区不再重建图）。
- 手动查询区图表仍在每次 rerun 重建（渲染顺序已由 RenderGate 批量化）；
  如后续需要可复用同一 memo 机制。
- CTQ service 的同类 `max_entries` 未调整，列为后续项。
- 配套测试：`tests/unit/app/manager/test_render_gate.py`（memo 命中/未命中/
  签名变化）、`tests/unit/app/sections/spc/test_spc_dashboard.py`
  （预警区重复渲染不重建）。
