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
4. 工序字典是站点类来源的维护资源。独立有界同步命令产生本地站点映射，API只读。2026-11-03按用户澄清，`--initialize` 应探查规模后从数据库完整读取站点、描述并生成层别，不能只创建空模板；`--step` 定向刷新，离线模板另用 `--template-only`。全量读取须有超时、容量限制、冲突检查和原子写入，失败保留旧字典；源描述的层别缺口明确报告，不伪造值。根据用户需求2.1修订，Yield厂别由全局Defect Group/factory映射注入原计算并随Lot结果发布，API只读该厂别，站点／层别固定NULL，不再维护或查询Yield归属。2026-11-03用户授权删除`api_alert_metadata.xlsx`内的旧归属表，保留删除前备份；不批量清理其它历史文件。
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

## 2026-11-03：扩展 SPC 周度能力项目

用户要求兼容维护工作簿中的 CPK／CPM 不达项、保留必要字段、更新调用程序和文档，并授权所有设计决策采用默认项。采用现有路径显式增加 `event_type=CPK/CPM`，仍归属 SPC 授权；原默认 OOS 及 Sheet/Lot 的十一字段记录保持兼容。能力记录使用独立模型与 `capability` 对象身份，增加有限指标值、达标下限、周期类型、ISO 周、统计首尾日期。

Inline monitor 提供薄只读应用查询和可注入的现有 workbook reader，复用标准化释放、严格 `<1.33`、实际跨度、周期日期排除；HTTP 不调用会更新汇总的 dashboard 服务。默认上一完整周，自定义日期只包含已结束的完整 ISO 周；不把源数据首末时间伪造为周边界。缺少周期记录不证明已经计算且无异常，继续标记来源不可用。

不新增独立分类或修改全部查询默认值，以保留现有授权配置和调用行为；不把能力项伪装成单片或 Lot，公开键摘要作为稳定对象编号。旧新模型联合验证，CSV 则统一追加六列。CPK 与 CPM 分别使用矩阵政策，并在读取来源前执行；分页来源身份增加事件，继续绑定公开结果变化。

11-03旧配置缺少catalog已声明的CPM行，自动审批当时拒绝新增公开政策，未以回退CPK或缺行默认放行绕过。用户于11-04明确批准后，已备份并仅追加CPM行、初始逐产品复制CPK设置，核对原单元格保留；之后两行独立维护。

同日已在原地址及授权下加载正式1.2.0，部署配置原字节保持一致，旧进程退出和新监听就绪均已核实。正式HTTP已返回M626／2026-W44的CPK、CPM各1条，必要字段完整，与本地台账一致，并完成真实多页和CSV／JSON行数验证。见[正式验收记录](../dev_docs/generated/inline_domain/api/api-capability-deployment-2026-11-04.md)。

规格及任务：[能力 API 规格](../../.scratch/anomaly-api-capability/PRD.md)；核心应用：[只读能力查询](../../src/inline_domain/application/monitor/capability_alert_query.py)；HTTP 验证：[能力文件集成](../../tests/integration/test_anomaly_api_capability.py)。验证证据与真实来源预检位于 `output/test-results/api-cpk-cpm-20261103/`。

## 2026-11-04：能力项目返回完整 Sheet ID 名单

用户明确将 LOWER 和未明确边界统一为 `spec`，并要求能力项目的对象编号返回对应统计周全部 Sheet ID。1.3.0 保持 `对象类型=capability`、每行一个能力项目，`对象编号` 改为非空、去重、文本升序的字符串数组；片／Lot 仍使用字符串。`entity_id` 对能力项目按成员匹配并返回完整名单；分页及计数仍以项目行为单位，完整名单进入游标指纹。JSON 保留数组，CSV 在单个单元格保存标准 JSON，不截断片号。

SPC 应用提供成员发布及只读查询接口。来源是现有报表 By Sheet 的制备点位，按产品／厂别／站点／参数和完整 ISO 周筛选，不从 OOS 名单反推，也不把整月的片都放进上周。正常 SPC 报表在 native payload 缓存外自动发布产品／周 JSON 分片；对应策略进入上游缓存键。独立同步命令调用同一个正常报表流程以初始化，不引入 HTTP 数据库回退。成员文件属于 `data/inline_domain/spc/sheet_membership/` 的可重建快照，维护工作簿及原始数据生命周期不变。

成员快照验证完整日期覆盖、策略、业务键、真实点位跨度和指标／来源跨度锚点；资料不匹配时沿用来源不可用语义，不返回假成功的空编号。真实来源覆盖及代际在原始快照事务锁内捕获，经显示时间映射后随同批制备点位及原生缓存传递，当前 manifest 不得给旧缓存扩大覆盖。正常发布只覆盖本次完整覆盖的周，保留其它历史周；完整覆盖但无点位的周必须覆盖为空，不能继续保留旧名单。缺成员或发布失败不在 HTTP 中补算。

复用完整 SPC 报表服务作为 HTTP 查询接口虽然能取得片号，却会在缓存未命中时查数据库并维护台账，因此不采用。直接切原始 Parquet 会遗漏制备阶段的清洗、分类、去重和物理过滤，也不采用。新增成员 port 是既有报表生产与独立 HTTP 读取两种运行时之间的必要边界。

规格与任务见 [片号优化规格](../../.scratch/anomaly-api-sheet-membership/PRD.md)。文件／HTTP 回归和 [真实网络 E2E](../../tests/e2e/test_anomaly_api_sheet_membership_e2e.py) 验证完整名单、按片号查询、跨页、导出、部分失败及成功／失败／中断的监听清理。536 项相关验证通过，覆盖率 92.21%；正式 1.3.0 已核对真实 M626 上周各指标的 3 个片号及历史两页，原部署配置保留，见 [交付记录](../dev_docs/generated/inline_domain/api/api-sheet-membership-delivery-2026-11-04.md)。新契约是已授权的对象编号类型调整，调用方需按 1.3.0 说明迁移，不再依赖旧 `capability:` 摘要。

## 2026-11-04：默认查询全部适用事件

用户要求省略 `event_type` 时全选。1.4.0 将查询参数默认值改为未指定，并在授权范围内按分类展开：SPC 为 OOS／OOC／CPK／CPM，CTQ／AOI 为 OOS／OOC，Yield／Q-Time 仅 OOS。显式选择保持单事件语义；非 SPC 调用方的默认查询不要求 SPC 权限。每个事件仍是独立来源，失败列表保留具体事件，公开政策、完整性、合并排序、容量和游标约束继续生效。

用户同时询问能否取消密钥或默认填写。本次保留当前认证及授权，没有启用匿名访问；客户端从进程环境的 `API_KEY` 或兼容名称 `VIVO_API_KEY` 自动取得密钥，GUI 星号显示，查询后清空输入框并可从环境再次读取，不落盘、不加载共享默认密钥。项目本机启动器在没有进程凭证时，仅从已授权使用的项目 `.env` 取得 `API_KEY`，注入本次调用并在退出时恢复，独立客户端仍不读密钥文件。API Key 是本项目选择的认证方式，不是 HTTP 的强制要求；取消认证及匿名范围不作为这次默认查询调整的一部分。

## 2026-11-04：简化查询程序启动入口

用户明确要求删除 `query_anomaly_api.cmd` 与 `tools/launch_anomaly_api_query.py`，改为直接使用 `uv run --no-sync python tools/fetch_sheet_anomalies.py`。两份启动文件及仅验证已移除启动器的专用测试一并删除，保留客户端的 HTTP 封装、GUI、分页、导出和环境变量密钥支持。此前启动器读取项目 `.env` 的行为随组件移除而终止，客户端不新增密钥文件读取；本次不调整正式 API 服务及其启动入口。

## 2026-11-13：一键入口改为重启已有 API

用户明确要求一键 API 脚本与项目启动入口一样，先停止旧进程再启动新实例。`start_anomaly_api.cmd` 继续调用 PowerShell 入口，由后者执行托管工具新增的 `restart` 动作。重启前验证部署配置，已有状态必须属于本项目且地址、端口一致；只按记录的 PID 和创建时间清理本部署进程并核实监听释放，保留调用方凭证。独立重启锁避免并发重启操作相互停止新实例。首次没有状态记录时直接启动；停止失败不再创建第二套服务，替代实例启动失败或 Python 异常中断时复用现有启动清理流程。

这是用户明确授权的运行方式调整，取代一键入口此前“已运行则复用”的约定。底层 `start` 与登录启动项仍保留幂等启动行为。重启会短暂中断调用、使旧游标失效；新版本启动失败时可能需要维护恢复。验证使用独立项目副本和最小健康检查页面，不连接数据库，不操作正式服务。

## Traceability

- 需求：[feat-API](../dev_docs/dev_spec/inline_domain/monitor/feat-API.md)、[获批规格](../../.scratch/sheet-anomaly-api/PRD.md)、[执行计划](../../.planning/2026-11-02-sheet-anomaly-api/task_plan.md)。
- 实现：[HTTP入口](../../api/main.py)、[跨域组合](../../api/composition.py)、[公开投影](../../api/service.py)、[Inline共同查询](../../src/inline_domain/application/monitor/sheet_alert_query.py)、[公开规则](../../src/shared_kernel/core/alert_visibility.py)、[本地维护元数据](../../src/shared_kernel/infrastructure/api_alert_metadata.py)。
- 测试：[本地文件到HTTP集成](../../tests/integration/test_anomaly_api_files.py)、[Inline查询](../../tests/unit/inline_domain/application/monitor/test_sheet_alert_query.py)、[元数据](../../tests/unit/shared_kernel/test_api_alert_metadata.py)、[缓存](../../tests/unit/shared_kernel/test_api_workbook_cache.py)、[依赖守卫](../../tests/architecture/test_backend_dependencies.py)。
- 部署：[API服务部署说明](../../references/domain/inline_domain/api/guide-sheet-anomaly-api.md)；调用：[API调用说明](../../references/domain/inline_domain/api/api-caller-guide-2026-11-03.md)；实现：[API程序介绍](../../references/domain/inline_domain/api/architecture-sheet-anomaly-api.md)。
- 2.2：[获批查询规格](../../.scratch/anomaly-api-query/PRD.md)、[执行计划](../../.planning/2026-11-02-api-query/task_plan.md)、[查询协调](../../api/query.py)、[HTTP查询回归](../../tests/unit/api/test_query_options_http.py)、[交付记录](../dev_docs/generated/inline_domain/api/api-query-optimization-2026-11-02.md)。

## Verification and acceptance

- 实现提交 `800ffbc2` 已按用户2026-11-02授权快进合并至 `master`；收尾修复 `1e22d029` 保证Q-Time不覆盖的授权厂别返回合法空结果。
- 专项267项通过，覆盖率92.37%；Standards／Spec具体阻断项均已关闭。合并后126项验证通过；新增Q-Time边界回归后127项相关验证通过，该变更Pyright为0 errors、0 warnings。
- 完整回归2708项通过、24项失败；24项在同条件起始相关源码对照中仍复现。用户明确接受另行跟踪，不把它们标记为通过，亦不把质量例外推广到后续变更。
- 初版开发验收时未代替正式部署或真实服务账号的保护文件可读性验证。后续已授权部署和需求2.1变更分别由对应交付记录跟踪；2.1不再要求维护Yield归属。
- 用户明确要求的2.1修订及global.yaml可配置要求、实施范围和新验证见 [Yield Lot优化记录](../dev_docs/generated/inline_domain/api/api-yield-lot-optimization-2026-11-02.md)。此处只修订用户已明确选择的业务决定，其他API边界保持原决定。
- 验证与限制：[交付记录](../dev_docs/generated/inline_domain/api/api-delivery-2026-11-02.md)；后续基线问题：[独立Issue目录](../../.scratch/api-baseline-regressions/issues/)。
