# ADR-0022：自动预警看板产品×参数矩阵与 Q-Time 缓存层

- Status: Accepted
- Date: 2026-09-03
- Scope: `app/pages/自动预警看板.py`、`app/sections/inline_domain/monitor/alert_matrix*.py`、`src/indicator_domain/application/qtime/`、`src/inline_domain/application/shared/decorated_features.py`、`src/yield_domain/`
- Trace: Issue `.scratch/alert-matrix-board/issues/01-alert-matrix-board.md`、
  Plan `.planning/2026-09-02-alert-matrix-board/`、
  PRD `docs/PRD/PRD-2026-09-02-自动预警看板矩阵化.md`、
  可行性分析 `docs/dev_docs/generated/others/alert-center-matrix-board-feasibility.md`、
  前置 ADR-0017（inline 预警中心）、ADR-0001（cache 原生载荷边界）、
  ADR-0021（indicator_domain 对等子模块）

## Context

自动预警看板（ADR-0017 后续）需要把"超规项列表 + 自动渲染图像"升级为
"产品 × 监控参数"的达标矩阵：列 = 7 个启用产品，行 = 8 个监控参数
（aoi_rs/aoi_tt/spc/ctq 单片异常、spc 趋势波动 cpk、yield lot 超规、
yield 良率波动、qtime 单片异常），单元格只表达"是否达标"。可行性分析确认：
全产品预计算管线已在预警看板生产验证（`prod_code="ALL"` + 逐产品 revision/决策签名），
但存在三个结构性障碍——qtime 无缓存直查生产库（ADR-0019 有权限/超时前科）、
`fetch_decorated_features max_entries=12` 在矩阵规模（7 产品 × 2~3 scope = 14~21 条目）
下淘汰抖动、跨域组装（inline + yield + indicator）在 src 层无落点。

## Decision

1. **矩阵组装落 app 层**：`app/sections/inline_domain/monitor/alert_matrix_service.py`
   （纯计算：8 行注册表、四态 evaluator、payload 构建、签名摘要，不 import streamlit）
   + `alert_matrix_cache.py`（st.cache_data 入口与生产装配）+
   `alert_matrix.py`（UI）+ `alert_matrix_detail.py`（点击详情懒加载）。
   src 域间互导会破坏分层，app 层可自由组合各 src 域（先例：SPC 页同时组装
   monitor + spc 服务）。
2. **单元格四态契约**：`ok / alert / no_data / error`（⬜ 带 message tooltip）。
   每个 evaluator 独立 try/except，单产品/单域失败只降级对应单元格，整板不失败；
   签名分量采集失败降级为确定性 `"unavailable"` 标记，不产生每次 rerun 都变化的脏键。
3. **矩阵本体不渲染图像**（需求方确认的妥协）：点击 🔴 单元格后按
   `detail_key = f"{row_key}|{prod}"` 懒加载明细与图像，复用各域既有预警渲染管线
   （SPC/CTQ/Yield/Q-Time 走 RenderGate `collect_memoized`，chart key 前缀
   `matrix_detail`，页头硬清理覆盖该前缀）；未点击连 loader 注册表都不构建。
4. **矩阵只读**：sheet OOS 四行只读 scope 工作簿（`load_sheet_oos_decoration` +
   `build_sheet_oos_alerts`），绝不走 `prepare_*`（会写盘）；yield 域新增
   `read_only` 开关穿透至 `sync_modifier_table`（内存口径不变，仅跳过工作簿与
   `.sig.json` 写回；默认 False，既有页面行为不变），矩阵传 True。
   例外说明：spc CPK 行复用 `fetch_spc_report_payload`，其内部 capability 修饰
   台账写盘是 SPC 页既有行为，矩阵未新增写路径。
5. **Q-Time 缓存层**：`cached_monitoring.py` 以 `@st.cache_data`（max_entries=32，
   TTL 读 `service_cache.ttl_hours.qtime_monitoring: 12`）包装
   `get_current_monitoring`；键 = (shop, step_descriptions, products, as_of,
   决策文件 mtime_ns, size)；`as_of=None` 在公开包装层归一为当天 date 再进键
   （装饰器在函数体前算键，函数体内归一无影响）；决策上传后 mtime 变化自然 miss，
   无需显式 clear。矩阵侧按 shop ×3 全站点 union 后按 prodcode 拆列。
6. **缓存容量**：`fetch_decorated_features max_entries` 12 → 32（覆盖 21 条目 + 余量），
   由行为式 LRU 测试验证（streamlit bare 模式确实执行 max_entries 淘汰）。
7. **矩阵缓存键集中组装**：`build_default_signature_components` 一处预算
   逐产品 revision + 逐 (prod, scope) 决策签名 + qtime 决策 stat + 周标签
   （同 ISO 周内命中）；页面"刷新缓存"经 `get_alert_matrix_cached_funcs()`
   显式清理矩阵链路缓存。
8. **时间口径**：单元格统一语义为"上一 ISO 周有预警"；yield lot 与 qtime 在适配层
   做呈现层过滤（`warehousing_time` / `timekey` ∈ 上一 ISO 周），不改探测算法；
   yield 良率波动保持 period 制口径（记录非空即红），图例注明。

## Alternatives considered

- **矩阵内自动渲染图像**：拒绝。图像数量随 产品×参数 爆炸且不可控；需求方明确
  接受"点击后再查询"的妥协（决策 3）。
- **改动 SessionManager/Header 支持多产品**：拒绝。冲击面过大；矩阵页沿用预警
  看板先例绕开 Header 单产品筛选（`ConfigLoader.get_enabled_products()` 取列）。
- **直接读 `spc_cpk_cpm_decoration.xlsx` 判 CPK 行**：拒绝。该工作簿是用户修饰
  台账（`cpk_corrected` 快照 + opt-in flag），不含"低于 1.33"判据语义，必须经
  `fetch_spc_report_payload` 重算后走 `build_weekly_cpk_alerts`。
- **矩阵数据服务落 src/inline_domain/application/monitor/**：拒绝。矩阵是跨域
  组装（inline + yield + indicator），落 src 会造成域间互导。
- **aoi_tt/aoi_rs 详情图像也做 RenderGate memo 化**：暂缓。两个域的渲染器本身无
  memo 机制（其原页面同样每 rerun 重建），数据层已全部命中缓存；如需 memo 化
  属两个域内渲染器的两阶段重构，建议单独立项。

## Consequences

- 正面：矩阵首屏成本 = 一次 payload 计算（缓存命中时秒开）；点击详情的计算与
  渲染全部懒加载且可缓存；qtime 页面查询不再每次直打生产库；单产品故障被隔离
  为单元格级灰点。
- 负面/约束：
  - 矩阵口径依赖 scope 工作簿的新鲜度——aoi 工作簿只在对应页面运行时刷新，
    长期未打开的产品矩阵单元格可能反映旧数据（只读约束的既定取舍）；
  - `fetch_aoi_rs/tt_report_payload` 的 `max_entries=3` 在逐个点击 7 产品 aoi
    详情时会互相驱逐 L2 条目（有 TTL 兜底），未在本次调整；
  - yield 详情图像在"数据变了但命中 Code 集合不变"的极端场景可能复用旧图，
    已由 `matrix_detail_` 前缀硬清理兜底刷新场景；
  - 矩阵行清单（8 行）硬编码在注册表中，新增监控参数需改 `alert_matrix_service.py`。
- 顺带修复：`自动预警看板.py` 补齐了其它页面都有的 sys.path bootstrap
  （pyproject 锚定 + root/src 注入），修复"新进程直接访问该页 URL 未过 Home 时
  ModuleNotFoundError"的预存在惯例缺陷（矩阵首次把 yield_domain 顶层导入带进该页
  才暴露）。

## Verification

- 单元：`pytest tests/unit` 855 passed / 5 failed（5 项均为基线预存在失败，
  逐项核对与本改动无关）；新增矩阵服务 36 项、矩阵 UI AppTest 15 项、
  qtime 缓存 10 项、yield 只读 5 项、max_entries 行为式淘汰 1 项。
- 集成：`pytest tests/integration` 23 passed（含矩阵口径与单域判据一致性 +
  只读字节校验）。
- E2E（playwright-cli，假数据隔离）：`tests/e2e/alert_matrix_board.js`、
  `tests/e2e/qtime_report.js` 全部通过；截图证据在 `output/test-results/`
  （四态渲染、点击懒加载、缓存命中与刷新重建、qtime 回归）。
- 受限验证：真实数据手工抽查由需求方决定跳过（以集成测试口径一致性证据替代）；
  qtime 生产库不可用时的 ⬜ 整行降级有单测覆盖但未做真实环境验证。
- 合并：分支 `feat/alert-matrix-board` 经需求方确认后合入 master
  （commit d8141a3，fast-forward），合并后 master 复验同上结果。
