# SPC CPK/CPM 明细工作簿更新与预警规则

本文维护 `spc_cpk_cpm_decoration.xlsx` 的字段语义、新增、刷新、历史保留、统一预警来源及筛选规则。核验日期：2026-11-17。能力计算公式及缺失值准入见 [SPC 能力计算规则](rules-spc-cpk&cpm.md)，服务调用路径见 [SPC 数据流](data-flow-spc.md)。

## 1. 工作簿用途与字段

该工作簿保存不达标项目的能力值替换决策及尚未解决的历史记录。2026-11-17起，正常持久化移除已确认达标（对应 corrected 值 ≥ 1.33）的行，不再将达标行当作历史决策长期保留；无法判定的空值行和未重新覆盖的不达标历史仍保留。默认位置为 `resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx`，实际路径由全局资源配置解析。

CPK 使用产品编码同名 sheet，例如 `M678`；CPM 使用 `<产品>_cpm`，例如 `M678_cpm`。两类指标独立匹配、独立维护。

| 字段 | 含义 |
|---|---|
| `prod_code`、`factory`、`step_id`、`param_name` | 产品、厂别、站点和参数。 |
| `period_type`、`period_label` | 周期类型及标签；周度采用 ISO 周标签 `YYYY-Www`。 |
| `period_sort` | 周期排序。 |
| `period_start`、`period_end` | 对应项目有效 Sheet 特征的最早、最晚检出时间，不是统一的日历周边界。本次计算覆盖同一键时，与能力值一起刷新；未覆盖的历史行保留原时间。 |
| `cpk_corrected` / `cpm_corrected` | 由完成点位修饰的输入计算得到的能力值，位于独立能力值替换步骤之前。 |
| `flag` | 默认 `False`，表示不启用能力值替换；`True` 表示使用已保存的替换值。它不是是否不达标的状态。 |
| `cpk_replacement` / `cpm_replacement` | 能力值替换目标；保存的有效目标复用，缺失或越界时补齐。 |

匹配键为 `prod_code + factory + step_id + param_name + period_type + period_label`。同一项目跨周是不同记录，不合并为一条。

## 2. 新增规则

`build_capability_anomaly_detail()` 只选取参考日期的**上一完整 ISO 周**，且对应能力值严格小于 `1.33`、有效 Sheet 时间跨度达到配置门槛的记录作为新增候选。SPC 主服务以查询截止日期作为参考日期。

门槛由 `config/domain/inline_domain.yaml` 的 `spc.spc_cpk.alert_min_span_hours` 配置，默认及缺省值为 **48 小时**；支持有限非负数及小数小时，拒绝负数、布尔值、空值、NaN、无穷大和无法转为数字的值。设为 `24` 即采用 24 小时门槛；设为 `0` 取消最小时长限制，但起止时间仍须有效且顺序正确。该配置统一控制新增预警明细、SPC 页面、全指标矩阵及 CPK/CPM 监控，并进入能力计算缓存和明细源签名。修改配置后，读取按新门槛判断，不批量补录未计算的历史周。下文的 48 小时边界及示例均以默认配置为准；此门槛不限制能力计算。

- 不收录当前未结束周、其他历史周或月度记录作为新增候选。
- 等于或大于 `1.33` 的值不成为新增候选。
- 缺失或无法转为数值的值不成为新增候选。
- 时间跨度为每个“产品＋厂别＋站点＋参数＋周”中 `period_end - period_start`，不使用日历周长或全部点位的首末时间替代。少于配置门槛不新增预警；恰好达到门槛可新增。
- 起止时间缺失、无法解析或先后颠倒时，不能确认跨度达到门槛，不新增预警。
- 已存在的键不重复追加；新键默认 `flag=False`。

跨度门槛只控制预警。短跨度项目仍正常计算 CPK/CPM，仍保留在能力结果表及常规图表中；没有新增候选时不为该产品新建空报警 sheet 或空工作簿。

当前 SPC 主服务也仅计算上一完整周。通用计算函数或直接应用调用可以提供更广的完整计算结果，但不改变新增候选的上述约束。

## 3. 已有记录的刷新与保留

持久化时读取已有工作簿，将完整计算结果与历史记录按上述键匹配，再追加不存在的新增候选。

| 本次计算情况 | 已有记录如何处理 |
|---|---|
| 匹配到同一键，仍低于 `1.33` | 更新 `cpk_corrected` / `cpm_corrected`，保留记录和人工决策。 |
| 匹配到同一键，已达到 `1.33` | 更新后从异常修饰台账移除，不再沿用该行 flag 替换已达标的计算值。 |
| 匹配到同一键，新的能力值为缺失 | 新的缺失值覆盖旧能力值；记录和人工 flag 仍保留。 |
| 本次计算没有该键 | 不重算或置空；原值仍不达标或未知时保留。已存 corrected ≥ 1.33 的历史达标行也在本次保存前剔除。 |

更新已有键不受“新增只取上一周”的限制：完整计算结果覆盖到旧周时，旧周数值也会刷新。保留行的已有 flag 和有效替换值不变；无效或缺失的替换目标会补齐并保存。旧行全部达标被剔除时，仍会追加本次新发现的不达标项，不漏写。工作簿内容未变化时不重复写入；已有sheet删至零行时保留表头。

历史刷新函数同时覆盖对应的 `cpk_corrected` / `cpm_corrected` 和 `period_start`、`period_end`，保证门槛使用与本次能力值一致的样本范围；`period_sort`、flag 和有效替换目标保留。未重新计算的历史时间可能仍是旧版本首次保存的范围，不能把这些字段当作最后刷新时间。

例如某项目某周首次计算为 `0.9`，进入工作簿后，再次计算为 `1.6`，原行会被移除，最终能力值保留 `1.6`。如果尚存的异常行再次计算为空，则仍保留空值行供后续判定；不把空值视为达标。若已删除的项目再次不达标且满足上一周新增条件，将作为新异常写入，flag 默认 False。

用户同日澄清：`flag=True` 的替换行为无需调整，故 `apply_capability_decoration()` 保留原有按flag匹配并应用replacement的逻辑，不增加计算值门槛。上述达标行的处理发生在保存台账时。`*_corrected`保存能力替换前的计算值，`*_replacement`保存替换目标；最终展示值不会因本次替换自动回写为corrected。例如 corrected=1.10、flag=True、replacement=1.37 时，最终能力为1.37，台账中的corrected仍为1.10。

这是对旧“刷新达标值但保留历史行”规则的修正，并非统计窗口滚动导致结果自然改变。同一完整周在测量快照、去重后的有效点、规格、点位修饰决策／规则或统计配置变化后，重新计算值仍可能不同；输入与策略完全相同时结果不因日期推移变化。当前台账没有逐次变更审计，不能从现存值断言每一条达标行的历史原因。

数值为空可能源于样本不足导致标准差无法计算、必要输入缺失或规格无效等情况。完全没有匹配计算行与“有匹配行但结果为空”是两种处理，前者保留旧值，后者覆盖为空。仅凭工作簿当前内容不能逐条还原历史变更原因。

这些刷新发生在服务实际计算并调用持久化时。报表命中缓存或本次计算范围没有该历史周，不会主动重算整份历史工作簿。SPC 页面的底层计算缓存签名包含当前产品原始测量快照及其 `.policy` 文件版本；两者有变化时，同一天、同一周的查询也会重新经过共享 Sheet 特征和能力计算，再更新明细。页头“刷新缓存”继续刷新原始快照并更换产品 revision。

“同周更新”沿用上一完整 ISO 周的统计范围，不表示开始计算当前尚未结束的周，也不是后台实时轮询数据库。没有新的快照、窗口或决策版本且缓存未过期时继续复用；数据库取数仍按原有 TTL 和显式刷新机制执行。

### 页面与监控预警

SPC 页面、全指标矩阵和 CPK/CPM 监控统一读取 `spc_cpk_cpm_decoration.xlsx` 已保存的产品 sheet。SPC 主服务先完成能力计算及严格持久化，然后页面通过 composition 的 `build_capability_latest_reader()` 读取与监控相同的 `CpkLatestExcelStore` / `CpmLatestExcelStore`；不使用本次周期能力表或未落盘的候选记录决定预警。

读取后使用 `normalize_latest_cpk()` 的统一状态规则。SPC 页面的 `build_weekly_cpk_alerts()` / `build_weekly_cpm_alerts()` 与矩阵的 `build_latest_cpk_alerts()` 共用 core 的 `latest_capability_alerts()`，选取上一完整 ISO 周、`flag=False`、对应 `*_corrected < 1.33`、有效 Sheet 跨度至少 48 小时的预警，并应用相同的日期排除。预警明细显示已保存的 corrected 值；周期能力表仍用于常规能力展示和命中指标的图表。

以下条件须同时满足，才能成为周度能力预警：

| 条件 | CPK | CPM |
|---|---|---|
| 已保存的明细来源 | 产品同名 sheet | `<产品>_cpm` sheet |
| 统计周期 | `period_type=week`，`period_label` 为参考日期的上一完整 ISO 周 | 同 CPK |
| 人工决策 | 规范化后的 `flag=False` | 同 CPK |
| 能力值 | `cpk_corrected < 1.33` | `cpm_corrected < 1.33` |
| 样本时间跨度 | 起止时间有效，`period_end - period_start ≥ 48h` | 同 CPK |
| 日期排除 | 当前厂别及周期未命中排除规则 | 同 CPK |

等于 `1.33` 不预警；恰好 48 小时满足跨度条件。`flag=True`、`Delete` 或空决策不计入预警；能力值为空或无法转为数值也不作为已确认不达标。无需依赖周期能力表中的 `*_decorated` 标记再次决定是否预警。

同一项目同一周的更新示例如下，已有记录均保持相同业务键，不重复追加：

| 更新顺序 | 能力值 / Sheet 跨度 | 明细更新结果 | 预警结果（flag=False） |
|---|---|---|---|
| 首次计算 | 0.9 / 18h | 不新增记录，能力结果仍保留 0.9 | 不预警 |
| 后续数据覆盖扩大 | 0.9 / 48h | 新增该周明细 | 预警 |
| 后续计算达标 | 1.6 / 72h | 原行删除，计算值保留 1.6 | 停止预警 |
| 后续再次不达标 | 0.8 / 72h | 满足新增条件时重新写入，flag 默认 False | 恢复预警 |
| 有匹配计算行但能力值为空 | 空值 / 72h | 空值覆盖原能力值，历史行保留 | 无法判定，不生成预警 |

若本次计算没有匹配业务键，则沿用前述历史保留规则，不把“未重新计算”当作达标或空值。上述状态转换发生在明细成功保存后；预警入口读取保存后的结果。

全指标矩阵和 CPK/CPM 监控通过 `normalize_latest_cpk()` 读取工作簿。历史周度行即使仍为 `flag=False`、值低于 1.33，跨度不足或无法确认时也标为“数据跨度不足”，不计入预警项目数、不生成矩阵预警详情。数值仍保留，历史行不自动删除。该规则不修改 Sheet OOS/OOC 的报警逻辑。

能力结果缓存及矩阵源签名包含跨度策略，SPC 结果签名额外标识统一明细来源及异常专用台账规则。Excel 原始内容按文件版本缓存，规范化及页面读取发生在能力计算缓存之外，保证保存后的明细或人工 flag 变化可被读取。

2026-11-17另按用户明确要求，对当前工作簿执行一次严格 `> 1.33` 的数据清理，等于 1.33、空值及其余记录在该次清理中不动；先做独立完整备份，在副本中用 Excel 原生删行并逐表验证后替换。此一次性阈值与之后正常保存的 `≥ 1.33` 达标剔除规则分别记录；不清理 replacement 字段、不更改能力公式、源快照或汇总历史。

## 4. 不达标风险台账的筛选口径

按 2026-10-10 确认，不达标风险台账同时检查人工 flag 和对应的计算值：

| 类型 | 保留条件 |
|---|---|
| CPK | `flag=False` 且 `cpk_corrected < 1.33`。 |
| CPM | `flag=False` 且 `cpm_corrected < 1.33`。 |

空值、等于 `1.33`、大于 `1.33` 或 `flag=True` 的记录不进入这份不达标风险台账。空值不按零处理；如需追踪，可单列数据缺失清单，不计作已确认不达标。

能力记录的发生日期使用 `period_label` 周度。总台账与型号分页是相同记录的两种视图，统计时只计总台账。同一项目跨周分别列示；删去不符合条件的风险台账行不回写或删除源明细工作簿的历史决策。

此处规定的是风险台账导出口径，不自动改变监控汇总工作簿的维护和日期排除策略；后者见 [CPK/CPM 汇总工作簿规则](../monitor/rules-cpk-summary-workbook.md)。

## 5. 读写失败及实现依据

SPC 主服务启用 `strict_persistence=True`：已有工作簿或 sheet 清单读取失败、写入失败均向应用层抛出异常，不返回候选作为成功结果，失败结果也不进入报表缓存。原文件保留；页面提示加载失败。预警明细再次读取失败时，页面显示“CPK/CPM 预警数据读取失败”，停止该次展示，不回退到计算结果生成预警。数值缺失、NaT 和旧字段缺失在保存时转为空单元格，不因 pandas 缺失值类型而静默更新失败。

兼容调用默认 `strict_persistence=False`，保留原有读取失败返回候选、写入失败仅记录异常的行为；它们不作为 SPC 页面预警的数据来源。

| 责任 | 实现 |
|---|---|
| 新增候选、flag 合并、替换值、历史数值刷新与达标剔除 | [cpk_decoration.py](../../../../src/inline_domain/core/spc/cpk_decoration.py)：`build_capability_anomaly_detail()`、`merge_capability_detail_with_decoration_flags()`、`refresh_existing_capability_values()`、`remove_passing_capability_rows()`、`_append_missing_detail_rows()`。 |
| 传入完整计算结果及新增候选 | [capability_decoration_service.py](../../../../src/inline_domain/application/spc/capability_decoration_service.py)：`prepare_capability_decoration()`。 |
| 加载、保留历史、追加、比较及保存 | [capability_decoration_repository.py](../../../../src/inline_domain/infrastructure/spc/capability_decoration_repository.py)：`persist_capability_decoration()`。 |
| 查询截止日期与上一完整周范围 | [spc_service.py](../../../../src/inline_domain/application/spc/spc_service.py)：`SpcReportService.fetch_spc_report_payload()`。 |
| 统一明细读取、筛选及页面接入 | [cpk_latest.py](../../../../src/inline_domain/core/monitor/cpk_latest.py)：`normalize_latest_cpk()`、`latest_capability_alerts()`；[composition.py](../../../../src/inline_domain/composition.py)：`build_capability_latest_reader()`；[SPC监控报表.py](../../../../app/pages/SPC监控报表.py)。 |
| 同周保存/读取生命周期、写入失败、正常/空/失败页面验证 | [test_spc_capability_ledger.py](../../../../tests/unit/inline_domain/application/spc/test_spc_capability_ledger.py)、[test_spc_page_alerts.py](../../../../tests/unit/app/pages/test_spc_page_alerts.py)。 |
| 48 小时门槛、页面/监控一致性、正常能力计算与历史时间刷新验证 | [test_capability_alert_span.py](../../../../tests/unit/inline_domain/core/spc/test_capability_alert_span.py)。 |
| 新增与历史决策保留验证 | [test_cpk_decoration.py](../../../../tests/unit/inline_domain/core/spc/test_cpk_decoration.py)。 |
