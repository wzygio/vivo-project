# 异常片／Lot API 实现与验证记录

日期：2026-11-02。模式：development-flow complex。开发分支：`feat/sheet-anomaly-api`；起始基准：`b494db8e23ef4e9df1dfb0aefeaf3e24b9fed0c5`。

本文保留开发交付时的验证与边界。用户随后另行授权的本机正式部署及跨设备验证见 [部署记录](api-deployment-2026-11-02.md)，不将部署前的状态当成当前运行状态。

状态：实现提交800ffbc2已按用户2026-11-02确认快进合并到master，收尾修复1e22d029及127项相关验证通过，正式ADR-0037已采纳。用户确认24项已独立复现的基线失败另行跟踪，因此**不宣称全套通过**；本次开发流程在该明确质量例外下交付完成，未部署正式服务。

## 1. 需求逐项核验

| 需求／确认项 | 实现证据 | 可观察验证 |
|---|---|---|
| 只读取本地发布结果，复用共同业务规则 | API上层组合、SheetAlertQuery、现有域结果reader；页面保留兼容包装 | 六来源真实临时工作簿到HTTP；禁止DB／保护文件fallback替身，所有源字节不变 |
| 四Inline同时OOS/OOC | event_type校验、同一reader alarm_type，默认OOS | 八种分类事件集成路径、缺来源与错误schema |
| 包含各模块Lot，真实区分Sheet／Lot | RS chart_kind、Yield/QTime lot_id；统一对象类型／编号 | RS两类及前导零；不展开Lot推断异常片 |
| 六项必要业务字段 | 明确中文DTO及类型／长度约束 | 分类、站点、部门、层别、参数、事件逐字段断言 |
| Q-Time站点、层别都使用起点 | 公开投影使用f_step，本地字典解释该站点 | 终点刻意配置不同描述，仍输出起点及其层别 |
| Yield厂别必需，站点可NULL | 本地Yield归属，精确Lot优先、产品参数默认归属次之 | 缺厂别503；有厂别、空站点和明确维护描述时成功；缺层别503 |
| 仅明确Flag=False释放，缺控制补齐 | QTime原台账＋发布证据；Yield持久__flags，默认True | 旧缺证据503，True/Delete/缺失／非法不放行，False释放，历史跨发布保留 |
| 层别来自描述首个下划线前缀 | 本地字典与显式维护描述；独立有界同步命令 | 前缀、无下划线、缺失／歧义、源表回退、截断／超时策略及失败保留 |
| 公开投影和可见性规则 | 纯规则共用，政策先于来源访问 | 隐藏且来源已删除仍为公开无预警；政策读取失败503；全响应不含flag／路径／内部状态 |
| 独立只读接口及必要服务边界 | create_app、独立客户端授权、当前启用范围、分页／限流／有界读取 | HTTP鉴权、撤销、越权、无效参数、稳定／失效游标、安全500／503、超时／429 |
| 空表不得掩盖坏schema | API启用strict_empty，原页面默认兼容 | RED6项复现后修复；OOS/OOC空白／缺Flag／缺身份503，合法表头空表200 |
| Q-Time不覆盖的授权厂别返回合法空结果 | 无匹配shop时构造含flag表头的空表，不访问来源 | CELL查询RED503→GREEN200，公开data为空、meta.status为ok，来源builder禁止调用 |
| 使用说明位于指定领域目录 | [正式使用说明](../../../../../references/domain/inline_domain/monitor/guide-sheet-anomaly-api.md) | 45个文档本地链接、JSON／Python示例、围栏检查通过；环境模板及命令帮助有效 |

相关设计与实现入口见[获批方案](sheet-anomaly-api-development-plan.md)、[PRD](../../../../../.scratch/sheet-anomaly-api/PRD.md)和[执行状态](../../../../../.planning/2026-11-02-sheet-anomaly-api/task_plan.md)。

## 2. 专项与静态验证

- 最终专项 **267 passed**，覆盖API、真实临时文件集成、Inline读取、Q-Time／Yield发布与Flag、维护命令、元数据、缓存、依赖守卫及真实监听生命周期。
- 语句覆盖率 **92.37%**，要求的80%门通过。完整命令与结果见 [专项JUnit](../../../../../output/test-results/api-verification-final/junit.xml)、[覆盖率明细](../../../../../output/test-results/api-verification-final/coverage.json)及下方命令范围。
- 修复4项新增兼容性失败及P2后，HTTP／文件集成、页面夹具和依赖守卫的相关集合 **126 passed**。
- 合并收尾发现Q-Time无匹配shop空结果与严格Flag检查冲突；新增集成回归先复现503，再修复空表结构。提交 **1e22d029** 后上述相关集合 **127 passed**（25.87秒），见 [最终相关JUnit](../../../../../output/test-results/api-merge/final-junit.xml)；`uvx --offline pyright api/composition.py`为0 errors、0 warnings，编译及差异检查通过。未重跑完整2732项；267项／92.37%及完整回归证据对应主体实现800ffbc2。
- Pyright限定API及修改读取／元数据／缓存模块：**0 errors、0 warnings**；编译、`git diff --check`、`uv lock --check`通过。
- 双轴评审各发现同根因P2一项，均已复核关闭；无剩余具体评审阻断项。见 [Standards／Spec分列报告](api-review-2026-11-02.md)。

专项命令范围：

```text
.venv/Scripts/python.exe -m pytest -q
  tests/unit/api
  tests/unit/shared_kernel/test_api_alert_metadata.py
  tests/unit/shared_kernel/test_api_workbook_cache.py
  tests/unit/shared_kernel/test_alert_visibility.py
  tests/unit/inline_domain/application/monitor/test_sheet_alert_query.py
  tests/unit/inline_domain/application/monitor/test_excel_alarm_reader.py
  tests/unit/indicator_domain/application/qtime/test_alert_results.py
  tests/unit/indicator_domain/application/qtime/test_published_release_flags.py
  tests/unit/yield_domain/application/test_alert_results.py
  tests/unit/yield_domain/application/test_lot_release_flags.py
  tests/unit/yield_domain/application/test_initialize_lot_controls_cli.py
  tests/integration/test_anomaly_api_files.py
  tests/integration/test_api_server_lifecycle.py
  tests/architecture/test_backend_dependencies.py
  --cov=api
  --cov=src.shared_kernel.infrastructure.api_alert_metadata
  --cov=src.shared_kernel.infrastructure.api_workbook_cache
  --cov=src.shared_kernel.core.alert_visibility
  --cov=src.inline_domain.application.monitor.sheet_alert_query
  --cov=src.inline_domain.application.monitor.excel_alarm_reader
  --cov=src.indicator_domain.application.qtime.alert_results
  --cov=src.indicator_domain.infrastructure.qtime.alert_workbook
  --cov=src.yield_domain.application.alert_results
  --cov=src.yield_domain.infrastructure.alert_result_workbooks
  --cov=tools.sync_api_alert_metadata
  --cov=tools.initialize_yield_alert_controls
  --cov-fail-under=80
  --cov-report=term-missing
  --cov-report=json:output/test-results/api-verification-final/coverage.json
  --junitxml=output/test-results/api-verification-final/junit.xml
```

上面的换行用于展示参数范围，执行时作为同一命令；`COVERAGE_FILE`设置在 `output/test-results/api-verification-final/.coverage`，未把覆盖率产物写到源目录。

## 3. 全量回归及归因限制

最终运行 `tests/unit`＋`tests/architecture`，**2732项：2708通过、24失败、0 errors、0 skipped**，349.45秒。运行有临时数据库／Office／维护目录写入护栏，Python子进程继承，不通过关闭护栏或标记跳过获得结果。

首轮为2703通过／28失败。定向换回任务起点的相关Python源码、保持同一当前配置／数据／安全护栏，仅重跑这28项后：24仍失败、4通过。四项新增兼容性候选为TT详情两项、Inline工作簿解析复用一项及Yield释放规则夹具一项；修复接口替身参数及明确False维护步骤后，最终四项均通过。**最终失败集合与已复现的24项完全一致，新增失败节点为0。**

这些24项涉及既有显示文案、配置夹具、CTQ／CPK／TT断言、资源登记及只读Yield用例。归因是同条件下的定向源码对照，不是历史完整环境快照；子集执行顺序、动态加载／源码检查仍有限制，因此不夸大为所有失败的绝对历史结论，也不把它们标为成功。

- [最终全量JUnit](../../../../../output/test-results/api-full-regression-final/junit.xml)
- [最终失败集合与基线逐项比较](../../../../../output/test-results/api-full-regression-final/failure-comparison.json)
- [24项完整失败消息](../../../../../output/test-results/api-full-regression-final/failures.json)
- [源码对照方法与限制](../../../../../output/test-results/api-baseline-attribution/REPORT.md)
- [原28项与起始源码的逐项结果](../../../../../output/test-results/api-baseline-attribution/paired-outcomes.json)

用户于2026-11-02明确确认合并、采纳ADR并把这24项基线失败单独跟踪。本次依此通过质量例外与合并门，完整回归结果仍保持失败事实；不将例外推广至后续变更。已建立14张独立needs-triage Issue，逐项覆盖全部24个失败节点，见下方跟踪索引。

## 4. 数据与进程边界

最终完整回归对243个 `resources/`／`data/` 文件做前后SHA256核对，**0差异**；观察窗口内836个Python文件也无变化。数据库连接尝试0、维护目录写入尝试0。一个既有只读Yield测试尝试Office创建，在创建前被护栏阻止；未当作真实Office可用性验证。

新增API集成与专项使用临时工作簿，SQL及真实保护文件fallback均被禁止。本轮未初始化／迁移正式维护工作簿，未生成真实访问凭证；原始用户配置、需求及业务资产改动保持原样。

真实最小API分别覆盖正常结束、模拟失败及模拟KeyboardInterrupt；每次记录启动前意图、PID／创建时间、子进程、临时端口，并按已核验身份清理，最后验证进程退出与监听释放。证据位于 `output/test-results/api-lifecycle/`。未停止、重启或更改现有正式服务。

早期扩大页面回归时，旧夹具曾走真实只读Q-Time文件读取并触发COM诊断，随后进程正常结束，检查无Excel存活，但没有事前Office身份清单，不能声称完整证明那次Office归属。现已修复夹具并设置禁止真实I/O护栏；27项页面用例干净通过，零禁止调用，记录进程全部退出。

## 5. 使用与交付状态

入口：`api.main:create_app`；建议按使用说明在单worker、本机／正式内网入口下运行。启动前应分配独立凭证、维护本地字典与Yield真实厂别／必要描述，并由原生产流程发布带有效释放证据的结果。本轮未代替运维部署；真实服务账号下的企业保护文件读取仍需部署验证，HTTP超时不能强制终止Office调用。

提交仅包含本次代码、测试、说明及新增资源注册行，排除用户原有产品注释、需求改动与业务资产。具体提交由Git与最终交付回复标识。

已确认：用户同意基线问题独立跟踪，并将实现800ffbc2快进合并到master；正式 [ADR-0037](../../../../../docs/ADR/0037-published-anomaly-api.md) 已采纳。合并后126项针对性验证通过，见 [合并后JUnit](../../../../../output/test-results/api-merge/junit.xml)。21个原有未提交文件在快进及分支切换前后逐字节核对无变化，见 [合并证据](../../../../../output/test-results/api-merge/fast-forward.json)。开发分支保留，未推送远端、未部署正式服务。

收尾修复1e22d029同样位于master，最终127项相关验证通过。最终审计再次核对21个用户文件无变化、14张卡片完整覆盖24项、已记录测试服务进程全部退出。文档链接核对未发现新增断链；references/index.md原有GLOSSARY链接目标缺失，保留原路由，未在API交付中虚构术语文件。见 [最终审计](../../../../../output/test-results/api-merge/final-audit.json)。

## 6. 独立基线问题索引

用户仅确认另行跟踪，以下卡片为needs-triage；尚未授权实施修复，不是API交付的未完成子任务。

| Issue | 范围 | 失败节点数 |
|---|---|---|
| [01](../../../../../.scratch/api-baseline-regressions/issues/01-parts-cvd-empty-state.md) | 备件CVD空结果提示一致性 | 1 |
| [02](../../../../../.scratch/api-baseline-regressions/issues/02-ijp-error-message-isolation.md) | IJP孔区与AA区失败提示及夹具隔离 | 2 |
| [03](../../../../../.scratch/api-baseline-regressions/issues/03-inline-isolated-global-config.md) | Inline隔离夹具的全局配置完整性 | 4 |
| [04](../../../../../.scratch/api-baseline-regressions/issues/04-aoi-tt-weekly-empty-density.md) | AOI_TT周报零分母与缺数据提示 | 2 |
| [05](../../../../../.scratch/api-baseline-regressions/issues/05-ctq-report-lifecycle.md) | CTQ报告构建及模块重载稳定性 | 2 |
| [06](../../../../../.scratch/api-baseline-regressions/issues/06-capability-ledger-refresh.md) | SPC能力台账同周变更的一致投影 | 2 |
| [07](../../../../../.scratch/api-baseline-regressions/issues/07-capability-span-policy.md) | SPC能力最小数据跨度边界 | 3 |
| [08](../../../../../.scratch/api-baseline-regressions/issues/08-aoi-tt-excluded-reinspection.md) | AOI_TT排除日期与复测选择 | 1 |
| [09](../../../../../.scratch/api-baseline-regressions/issues/09-aoi-tt-exemption-projection.md) | AOI_TT参数豁免与有效释放投影 | 2 |
| [10](../../../../../.scratch/api-baseline-regressions/issues/10-yield-monthly-output-contract.md) | Yield周期输出monthly_full契约 | 1 |
| [11](../../../../../.scratch/api-baseline-regressions/issues/11-maintained-resource-registry.md) | 维护资源登记与历史文件分类 | 1 |
| [12](../../../../../.scratch/api-baseline-regressions/issues/12-cutoff-factory-projection.md) | 共享量测截止规则夹具的厂别字段 | 1 |
| [13](../../../../../.scratch/api-baseline-regressions/issues/13-yield-alert-health-forwarding.md) | Yield预警中心源健康透传 | 1 |
| [14](../../../../../.scratch/api-baseline-regressions/issues/14-yield-readonly-office-fixture.md) | Yield只读服务测试禁止真实Office回退 | 1 |

共14张卡片、24个节点，集合核对无重复或遗漏。后续triage需核查现行业务政策，不得只为通过测试而弱化断言或修改生产资料。
