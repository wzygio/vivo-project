# ADR-0037：本地发布结果驱动的异常片／Lot 只读 API

- Status: Accepted（用户于2026-11-02确认合并并采纳原草案；同日确认2.2的日期、部分成功口径与执行计划）
- Date: 2026-11-02
- Scope: 异常片／Lot HTTP消费、共同公开投影、本地元数据和人工释放证据；不合并各域原始分析管路

## Context

用户要求其他系统通过API读取与自动预警看板同源的异常，包含各模块Lot、统一六项业务字段及明确False释放。现有共同展示入口和底层文件缓存依赖Streamlit；部分发布结果遗漏Flag证据，Yield缺少人工释放和站点／厂别信息，站点描述字典只存在数据库查询及页面进程内缓存中。

用户已确认四类Inline同时支持OOS/OOC，必要字段资料不足时来源不可用，Q-Time发生站点与层别采用起点。初版允许Yield站点为空、由人工归属提供厂别和层别；用户2026-11-02明确修订需求2.1：厂别在发布时按可配置的分组映射产生，站点／层别均NULL，不再维护Yield归属表。API请求必须只读本地，不得用原始分析或数据库查询补齐缺口。

## Decision

1. 增加独立FastAPI入口及上层跨域组合，保持原域生产、发布协议和来源有效性。HTTP只消费本地结果；不新增异常识别引擎，不从application/core反向依赖app。
2. 提取Inline共同SheetAlertQuery、纯公开显示规则与可注入工作簿读取适配器，保留页面兼容包装。API使用有上限的独立进程缓存及短期公开分页快照。
3. Q-Time沿用原人工台账，在发布中保存明确False证据。Yield增加产品持久决定表、默认不释放并跨发布保留历史，读写在既有文件锁和原子发布边界内完成。
4. 工序字典是站点类来源的维护资源。独立有界同步命令产生本地站点映射，API只读。根据用户需求2.1修订，Yield厂别由全局Defect Group/factory映射注入原计算并随Lot结果发布，API只读该厂别，站点／层别固定NULL，不再维护或查询Yield归属；旧归属表保留而不删除。
5. API在当前授权及公开政策下投影、计数和分页，严格白名单输出。按用户2026-11-02的2.2确认，所有查询参数可选；默认上一ISO周全部启用且授权的产品／分类OOS、不按条数截断，显式limit启用全局分页。自定义日期按上海时区包含首尾，仍由各域发布结果选择并检查原有有效性；删除重复起始站点。
6. 用户2.2选择返回可用部分：单个产品／分类／事件来源失败时丢弃该来源的全部行，成功来源仍返回200，并以meta.complete=false及failed_sources明确标记；全部选中来源失败仍503，附安全失败来源列表。成功空来源与失败分开表示。请求级超时／容量／配置故障仍为安全错误。游标绑定身份、授权、实际启用范围、查询日期、公开结果和失败来源状态，不暴露内部来源签名。

## Alternatives considered

### 直接导入Streamlit页面／透传共同读取字典

改动较少，但页面初始化、会话和图像依赖不适合作为HTTP生命周期；透传内部records还会遗漏公开政策并泄露内部列。选择保留页面包装、提取共同业务与纯规则。

### API请求时重算报表或查询字典

可按需取得更多事实，却违反本地发布结果只读约束，也把源数据耗时和写入副作用带进请求。选择离线有界字典同步和既有正常报表生产，缺结果明确不可用。

### 缺字段填固定厂别、站点或NULL后继续

当前Yield结果不能推断缺陷发生位置；固定站点／层别会造成错误事实。用户2.1明确要求两者均NULL，厂别则来自已知Defect Group与全局配置，不能以统一默认厂别代替；缺厂别时仍不可用。

### 为API另设Q-Time人工决定／仅编辑生成结果Flag

第二套Q-Time台账会与原页面决策分叉；直接编辑Yield生成明细又会被下一次发布覆盖。选择原Q-Time台账，以及Yield独立持久决定sheet与生成结果分离。

### 无版本offset分页／立即增加Redis或新数据库

文件改写期间offset可造成漏行或重复；新增基础设施又超出当前小范围内网需求。选择单worker、有界不可变公开快照和认证游标；扩展多worker前需重新设计共享或路由。

### 2.2跨来源整次失败／返回可用部分

原API单来源完整性不足即失败；若直接推广到全部产品／分类，某个过期发布会让其余来源一并不可用。用户明确选择返回可用部分，因此按产品／分类／事件隔离已知来源失败，用完整性标记防止调用方将部分结果误认为全量。失败来源的已投影行整批丢弃，全部失败仍503，程序错误仍安全500；这不是忽略失败后默认成功。

### 2.2强制指定分类／全部可选

强制分类或固定行数可缩小单次负担，但不符合用户直接提取全部上周异常的使用方式。选择默认授权与启用范围内全选、无行数截断，同时保留字节、线程、缓存、超时等有限资源约束；不新增外部服务。

## Consequences

2.2将“所有来源一起成功”改为显式部分成功；这是用户选择的可用性取舍。调用方必须检查meta.complete，不能仅凭HTTP200或status=ok判断全量无异常。失败列表只含已授权的产品／分类／事件与固定错误码。默认不限行仍受原有字节／超时等资源约束；分页也需要完整公开快照。未来实测值、阈值、分组等业务字段仅作建议，本次不添加。

复用既有异常规则与域发布协议，HTTP失败不会启动生产处理。公开政策先于数据访问，隐藏记录不会通过错误、计数或游标暴露。站点类来源仍需维护字典；Yield不再需要人工归属，但全局分组映射变更需正常重新发布，无厂别／无释放证据仍不可用。分页重启与进程内缓存需要调用方理解。

Excel／Office读取依旧具有平台和账号约束。缓存和并发受限，但HTTP超时不能终止卡住的COM，部署前仍需真实账号验证与运维恢复安排。当前不承诺公网、跨进程缓存、高可用或源数据实时。

## Traceability

- 需求：[feat-API](../dev_docs/dev_spec/inline_domain/monitor/feat-API.md)、[获批规格](../../.scratch/sheet-anomaly-api/PRD.md)、[执行计划](../../.planning/2026-11-02-sheet-anomaly-api/task_plan.md)。
- 实现：[HTTP入口](../../api/main.py)、[跨域组合](../../api/composition.py)、[公开投影](../../api/service.py)、[Inline共同查询](../../src/inline_domain/application/monitor/sheet_alert_query.py)、[公开规则](../../src/shared_kernel/core/alert_visibility.py)、[本地维护元数据](../../src/shared_kernel/infrastructure/api_alert_metadata.py)。
- 测试：[本地文件到HTTP集成](../../tests/integration/test_anomaly_api_files.py)、[Inline查询](../../tests/unit/inline_domain/application/monitor/test_sheet_alert_query.py)、[元数据](../../tests/unit/shared_kernel/test_api_alert_metadata.py)、[缓存](../../tests/unit/shared_kernel/test_api_workbook_cache.py)、[依赖守卫](../../tests/architecture/test_backend_dependencies.py)。
- 使用：[异常片／Lot API使用说明](../../references/domain/inline_domain/monitor/guide-sheet-anomaly-api.md)。
- 2.2：[获批查询规格](../../.scratch/anomaly-api-query/PRD.md)、[执行计划](../../.planning/2026-11-02-api-query/task_plan.md)、[查询协调](../../api/query.py)、[HTTP查询回归](../../tests/unit/api/test_query_options_http.py)、[交付记录](../dev_docs/generated/inline_domain/monitor/api-query-optimization-2026-11-02.md)。

## Verification and acceptance

- 实现提交 `800ffbc2` 已按用户2026-11-02授权快进合并至 `master`；收尾修复 `1e22d029` 保证Q-Time不覆盖的授权厂别返回合法空结果。
- 专项267项通过，覆盖率92.37%；Standards／Spec具体阻断项均已关闭。合并后126项验证通过；新增Q-Time边界回归后127项相关验证通过，该变更Pyright为0 errors、0 warnings。
- 完整回归2708项通过、24项失败；24项在同条件起始相关源码对照中仍复现。用户明确接受另行跟踪，不把它们标记为通过，亦不把质量例外推广到后续变更。
- 初版开发验收时未代替正式部署或真实服务账号的保护文件可读性验证。后续已授权部署和需求2.1变更分别由对应交付记录跟踪；2.1不再要求维护Yield归属。
- 用户明确要求的2.1修订及global.yaml可配置要求、实施范围和新验证见 [Yield Lot优化记录](../dev_docs/generated/inline_domain/monitor/api-yield-lot-optimization-2026-11-02.md)。此处只修订用户已明确选择的业务决定，其他API边界保持原决定。
- 验证与限制：[交付记录](../dev_docs/generated/inline_domain/monitor/api-delivery-2026-11-02.md)；后续基线问题：[独立Issue目录](../../.scratch/api-baseline-regressions/issues/)。
