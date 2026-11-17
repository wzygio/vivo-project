# 0046 — 独立 CPK/CPM 周度汇总及达标率趋势

日期：2026-11-17。状态：已采纳并实施；正式维护文件迁移、获准历史数据修复及真实后端核验已完成，证据见[执行记录](../dev_docs/generated/inline_domain/monitor/capability-summary-task0-task3-20261117.md)。

## 背景

[Task0–Task3](../dev_docs/dev_spec/inline_domain/monitor/task-cpk_cpm_summary.md)要求把报警率、CPK、CPM 三种维护历史拆分为三个资源文件，按 SPC 现有计算逻辑重算指定历史周，并把能力看板改为总项目数与达标率趋势。原查询仅用维护分母扣减最新周预警，不向上更新月、季、年；其缺失周期补零、零分母0%及最新项目明细已不满足本次需求。

## 决定

1. 报警率继续使用资源键 `monitor_summary_workbook`，CPK、CPM 分别使用 `cpk_summary_workbook` 和 `cpm_summary_workbook`。独立文件保存各自的产品 Sheet，资源路径仍由全局注册表拥有。原始快照、维护预警台账及报警率统计规则保留原职责。
2. CPK/CPM 以完整 ISO 周为能力项目计数单位。分母由共享测量快照重放既有 SPC 最终点位和能力计算管线，使用已捕获的参数目录与规格；页面不查询数据库。显式元数据捕获命令读取数据库时使用只读事务、时间谓词及规模检查。
3. 预警数直接统计维护预警表中按业务键去重的 `flag=False` 周度项目。能力阈值和跨度门槛由预警生产负责，汇总不再次应用状态、阈值或跨度过滤。达标数为总数减预警数，零分母达标率为100%；矛盾计数显式失败，不自动截断。
4. ISO 周整体归入其周日所属自然月，由月到季度、季度到自然年加和计数并重算达标率。已有部分月度基线通过旧／新周贡献差额维护；缺少可靠旧贡献时，要求该月已结束周的完整覆盖后再重建，避免擦除范围外维护历史。
5. Task0 按明确补全顺序修复缺项，并从月度重建季度和年度。Task1 重算最近五个已结束周的 CPK；Task2 重算上上个月1日至统计日范围内已结束周的 CPM。常规页面仅刷新上一个完整周及其祖先期间，当前未结束周不生产能力汇总。
6. 持久字段保留规范时间标签，显示标签在读取时派生。能力历史使用独立跨进程事务锁和原子替换，缓存身份覆盖输入、日期及政策代次，失败保留维护文件。
7. 页面使用总项目数柱图和右轴达标率折线，周期类型分别着色并断开折线。原最新项目明细和厂别筛选移除；汇总范围为全部厂别，产品查询与 CPK/CPM fragment 保持独立。CPM、AOI_RS 的管理员显示约定及筛选面板默认折叠继续有效。
8. 用户明确授权“仅补算这些异常历史周”后，历史扩展采用稀疏产品/周范围：CPK为Z576/W39，另核算同月缺失的W36、W37覆盖；CPM为M626、M678、Z517、Z571的8月异常周，并核算同月缺失的M626/W31、Z571/W35。最终附加范围为3条CPK和20条CPM产品/周结果，既有健康周保持原值。补齐缺失月份覆盖是为了计算受影响祖先，不把缺失输入假定为零。

## 替代方案与取舍

- **继续共用一个工作簿**：不同看板的维护边界和发布失败相互影响；采用独立资源文件。
- **用预警行数推断全部项目数**：缺少达标项目，不能建立分母；采用同一 SPC 计算管线的可计算项目数。
- **在页面缺输入时查询原始数据库**：隐藏输入维护失败并扩大界面查询范围；采用显式捕获元数据和已验证本地快照，失败由业务可理解的不可用状态呈现。
- **把跨月周拆成两段**：破坏完整周能力项目的一致性；整周向后归属到周日所在自然月。
- **直接用本次五周数据覆盖所有月度历史**：没有完整月覆盖时会丢失未修复周贡献；采用可验证的周贡献替换与完整月重建两种明确入口。
- **把零总项目数显示0%**：与本次用户指定语义不一致；统一显示100%，同时保留总项目数0，呈现分母规模。

## 后果与维护边界

元数据捕获反映捕获时已知的参数与规格，不构造不存在的历史版本。回放统计必须绑定所用元数据代次及原始快照覆盖。Z622 当前没有可用于能力计算的规格，计数为零；以后引入规格属于独立需求。

拆分会改变三个看板的工作簿路径和缓存身份。维护命令必须保留原始备份，迁移结果应核验产品 Sheet、时间范围、未修改期间及报警率内容。超出授权范围的历史矛盾值不能以“修复”为名改写。

本次已正式发布北极星报警率汇总.xlsx、北极星CPK汇总.xlsx、北极星CPM汇总.xlsx，并在全局资源注册表中切换运行路径；原合并文件作为归档输入保留。正式核验确认报警率内容不变，CPK的267行和CPM的177行均与计划的结构、计数一致。正式文件副本上的实际查询首次、重复执行均未产生写回，未访问数据库。历史缺失原始输入只补充到隔离目录，不覆盖共享事实快照。

## 实现与验证路由

- 业务规则与运维入口：[CPK/CPM 周度汇总规则](../../references/domain/inline_domain/monitor/rules-cpk-summary-workbook.md)。
- 纯历史维护：[`capability_trend.py`](../../src/inline_domain/core/monitor/capability_trend.py)。
- 离线分母：[`capability_snapshot_source.py`](../../src/inline_domain/infrastructure/monitor/capability_snapshot_source.py)；显式元数据捕获：[`refresh_capability_metadata.py`](../../tools/refresh_capability_metadata.py)。
- 独立存储：[`capability_summary_store.py`](../../src/inline_domain/infrastructure/monitor/capability_summary_store.py)；页面查询编排：[`cpk_workbook_service.py`](../../src/inline_domain/application/monitor/cpk_workbook_service.py)。
- 批量维护：[`rebuild_capability_summaries.py`](../../tools/rebuild_capability_summaries.py)。
- 页面与图表：[历史预警看板](../../app/pages/历史预警看板.py)、[`capability_trend_chart.py`](../../app/charts/inline_domain/capability_trend_chart.py)。
- 隔离浏览器验收：[`capability_trends_browser.py`](../../tests/e2e/capability_trends_browser.py)；执行证据位于 `output/test-results/capability-trends/`。
- 正式发布与数据核验：[`rebuild-plan.json`](../../output/test-results/capability-summary/delivery-final/rebuild-plan.json)、[`formal-data-verification.json`](../../output/test-results/capability-summary/delivery-final/formal-data-verification.json)、[`real-backend.json`](../../output/test-results/capability-summary/delivery-final/real-backend.json)。
