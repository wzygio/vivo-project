# ADR-0032：全产品良率看板复用矩阵只读 Group 趋势缓存

- Status: Proposed (implemented and verified on feature branch; merge approval pending)
- Date: 2026-09-24
- Scope: 自动预警页面的全产品 Group 月周日趋势

## Context

用户要求在全指标预警下增加全产品良率看板，复用 Yield application 缓存和入库分析页的 Group 趋势图。图表实际展示的是按 Group 堆叠的入库不良率及入库数，不是各 Group 的独立合格率。

已有两种 application 消费契约：预警矩阵使用文件资源版本与 `read_only=True`；入库分析页使用手动资源版本与默认可写模式。它们共享产品指标刷新版本，但仅共享基签名不能保证完整 L2 命中。不能为了命中可写缓存而让一个只读总览触发维护表同步。

## Decision

1. 新看板直接消费 `YieldAnalysisService.get_mwd_trend_data` 的原生载荷，与矩阵保持相同配置、资源目录、日期窗口、文件版本、指标刷新版本、只读标志及调用参数顺序。不增加整板缓存、源查询或快照。
2. 缓存基签名由 Yield 展示层统一持有；矩阵与单产品入库页继续使用该值。TTL、淘汰、跨日和定向刷新行为由原应用服务负责。单产品入库页的完整 L2 条目仍独立，不能承诺跨进程、过期或淘汰后的命中。
3. 新看板有独立查询门控和产品范围。空选择表示所有启用产品，按配置顺序呈现；过期选择先清理。初次进入不触发全产品计算。
4. 沿用现有 Group 图表计算、颜色、入库数折线和近 3 月／3 周／7 日窗口。共享渲染接口只增加可选 key 前缀，保持旧调用兼容。
5. 外层 fragment 隔离看板与矩阵；每产品 fragment 隔离 Group 交互。产品异常不停止整页。普通界面只展示业务提示，不暴露内部健康元数据、异常内容、路径或备用数据入口。

### 双向交互隔离补充（2026-09-24）

初版只在良率看板上建立 fragment。矩阵单元格仍属于页面主流程，因此点击它会整页重跑；慢详情加载期间，下方尚未重新执行的良率内容被 Streamlit 标记为 stale 并变灰。fragment 只能约束其内部控件触发的重跑，不能阻止外部控件发起整页执行。

矩阵的筛选、查询、刷新、单元格、详情和收起控件现统一位于页面局部 `render_alert_matrix_panel` fragment，与良率 fragment 同级。每日快照自动打开检查在矩阵 fragment 内执行；收起回调使用当前日期，避免长会话跨日后沿用旧日期。每产品趋势位于默认展开的产品 Expander 中。

## Consequences

- 已热的矩阵 Group 缓存可直接命中，冷缓存仍执行既有只读加载/聚合。
- 数据可能需要滚动较长页面查看；产品筛选可缩小范围。
- 保留了入库分析页与矩阵各自的资源刷新契约，二者在资源变动期间可能属于不同缓存版本。
- 每个产品的 Group 筛选仅重新读取该产品的缓存并绘图，不重跑预警矩阵。

## Verification

- 相关单元、Streamlit AppTest、矩阵回归与架构测试：141 passed。
- 实际 `get_mwd_trend_data` 缓存测试：矩阵填充后总览命中；刷新 M678 不影响 M626；文件版本和日期窗口改变时重新计算；只读标志始终为 True。
- Playwright 合成数据页面：查询门控、6 张真实 Plotly 图、产品筛选、Group fragment 隔离、空/失败/过期/不可用状态通过；1440/768/375 视口无横向溢出；浏览器 pageerror/console error 为 0。
- 编译检查与限定范围 `git diff --check` 通过。
- 双向隔离修复：真实页面入口搭配 4 秒慢详情加载的 Playwright 回归，修复前断言下方变灰失败，修复后通过；整页执行次数保持 1，矩阵交互不增加良率加载次数，反向查询、关闭详情和收起矩阵也通过。相关页面/矩阵/良率回归 47 passed。脚本 `tests/e2e/warning_yield_fragments.js`，浏览器工作目录与证据位于 `output/test-results/warning-yield-fragments/`。
- 未连接生产数据库验证真实数据；未运行仓库完整业务测试集；无已有截图基线，视觉检查为人工审阅而非基线差异验证。

## Traceability

- Local spec: `.scratch/all-product-yield-board/PRD.md`
- Local ticket: `.scratch/all-product-yield-board/issues/01-all-product-group-trends.md`
- UI: `app/sections/yield_domain/all_product_board.py`
- Shared cache consumer: `app/sections/yield_domain/group_trend_data.py`
- Tests: `tests/unit/app/sections/yield_domain/test_all_product_board.py`, `test_group_trend_cache_reuse.py`
- Browser script: `tests/e2e/all_product_yield_board.js`
- Local browser artifacts: `output/test-results/all-product-yield/`
- Related: [ADR-0001](0001-streamlit-cache-native-payload-boundary.md), [ADR-0006](0006-rerun-slimming-cache-semantics.md)
