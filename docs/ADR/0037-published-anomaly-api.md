# ADR-0037：本地发布结果驱动的异常片／Lot 只读 API

- Status: Accepted（用户于2026-11-02确认合并并采纳已审阅草案）
- Date: 2026-11-02
- Scope: 异常片／Lot HTTP消费、共同公开投影、本地元数据和人工释放证据；不合并各域原始分析管路

## Context

用户要求其他系统通过API读取与自动预警看板同源的异常，包含各模块Lot、统一六项业务字段及明确False释放。现有共同展示入口和底层文件缓存依赖Streamlit；部分发布结果遗漏Flag证据，Yield缺少人工释放和站点／厂别信息，站点描述字典只存在数据库查询及页面进程内缓存中。

用户已确认四类Inline同时支持OOS/OOC，必要字段资料不足时来源不可用，Q-Time发生站点与层别采用起点。后续明确Yield厂别必需、站点可NULL；没有站点时仍需维护描述以满足层别完整性。API请求必须只读本地，不得用原始分析或数据库查询补齐缺口。

## Decision

1. 增加独立FastAPI入口及上层跨域组合，保持原域生产、发布协议和来源有效性。HTTP只消费本地结果；不新增异常识别引擎，不从application/core反向依赖app。
2. 提取Inline共同SheetAlertQuery、纯公开显示规则与可注入工作簿读取适配器，保留页面兼容包装。API使用有上限的独立进程缓存及短期公开分页快照。
3. Q-Time沿用原人工台账，在发布中保存明确False证据。Yield增加产品持久决定表、默认不释放并跨发布保留历史，读写在既有文件锁和原子发布边界内完成。
4. 工序字典及Yield归属是维护资源。独立有界同步命令产生本地站点映射，API只读；Yield由操作者维护真实厂别、可选站点及必要描述，不推断缺失位置。
5. API在当前授权及公开政策下投影、计数和分页，严格白名单输出；完整性不足时安全失败。游标绑定身份、授权、查询、周与公开结果，不暴露内部来源签名。

## Alternatives considered

### 直接导入Streamlit页面／透传共同读取字典

改动较少，但页面初始化、会话和图像依赖不适合作为HTTP生命周期；透传内部records还会遗漏公开政策并泄露内部列。选择保留页面包装、提取共同业务与纯规则。

### API请求时重算报表或查询字典

可按需取得更多事实，却违反本地发布结果只读约束，也把源数据耗时和写入副作用带进请求。选择离线有界字典同步和既有正常报表生产，缺结果明确不可用。

### 缺字段填固定厂别、站点或NULL后继续

当前Yield结果不能推断缺陷发生位置；固定ARRAY／10000会造成错误事实。用户只批准Yield站点可NULL，厂别及层别仍需可核验维护资料；资料不足不伪装成完整记录。

### 为API另设Q-Time人工决定／仅编辑生成结果Flag

第二套Q-Time台账会与原页面决策分叉；直接编辑Yield生成明细又会被下一次发布覆盖。选择原Q-Time台账，以及Yield独立持久决定sheet与生成结果分离。

### 无版本offset分页／立即增加Redis或新数据库

文件改写期间offset可造成漏行或重复；新增基础设施又超出当前小范围内网需求。选择单worker、有界不可变公开快照和认证游标；扩展多worker前需重新设计共享或路由。

## Consequences

复用既有异常规则与域发布协议，HTTP失败不会启动生产处理。公开政策先于数据访问，隐藏记录不会通过错误、计数或游标暴露。代价是需要维护站点／Yield资料，首次无发布或无释放证据时不可用；分页重启与进程内缓存需要调用方理解。

Excel／Office读取依旧具有平台和账号约束。缓存和并发受限，但HTTP超时不能终止卡住的COM，部署前仍需真实账号验证与运维恢复安排。当前不承诺公网、跨进程缓存、高可用或源数据实时。

## Traceability

- 需求：[feat-API](../dev_docs/dev_spec/inline_domain/monitor/feat-API.md)、[获批规格](../../.scratch/sheet-anomaly-api/PRD.md)、[执行计划](../../.planning/2026-11-02-sheet-anomaly-api/task_plan.md)。
- 实现：[HTTP入口](../../api/main.py)、[跨域组合](../../api/composition.py)、[公开投影](../../api/service.py)、[Inline共同查询](../../src/inline_domain/application/monitor/sheet_alert_query.py)、[公开规则](../../src/shared_kernel/core/alert_visibility.py)、[本地维护元数据](../../src/shared_kernel/infrastructure/api_alert_metadata.py)。
- 测试：[本地文件到HTTP集成](../../tests/integration/test_anomaly_api_files.py)、[Inline查询](../../tests/unit/inline_domain/application/monitor/test_sheet_alert_query.py)、[元数据](../../tests/unit/shared_kernel/test_api_alert_metadata.py)、[缓存](../../tests/unit/shared_kernel/test_api_workbook_cache.py)、[依赖守卫](../../tests/architecture/test_backend_dependencies.py)。
- 使用：[异常片／Lot API使用说明](../../references/domain/inline_domain/monitor/guide-sheet-anomaly-api.md)。

## Verification and acceptance

- 实现提交 `800ffbc2` 已按用户2026-11-02授权快进合并至 `master`；收尾修复 `1e22d029` 保证Q-Time不覆盖的授权厂别返回合法空结果。
- 专项267项通过，覆盖率92.37%；Standards／Spec具体阻断项均已关闭。合并后126项验证通过；新增Q-Time边界回归后127项相关验证通过，该变更Pyright为0 errors、0 warnings。
- 完整回归2708项通过、24项失败；24项在同条件起始相关源码对照中仍复现。用户明确接受另行跟踪，不把它们标记为通过，亦不把质量例外推广到后续变更。
- 正式服务部署、真实服务账号的企业保护文件可读性和生产字典／Yield归属维护属于后续运行准备，本次不代替完成。
- 验证与限制：[交付记录](../dev_docs/generated/inline_domain/monitor/api-delivery-2026-11-02.md)；后续基线问题：[独立Issue目录](../../.scratch/api-baseline-regressions/issues/)。
