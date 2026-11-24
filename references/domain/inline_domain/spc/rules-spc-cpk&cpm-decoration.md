# SPC CPK/CPM 明细工作簿维护与预警规则

本文维护 `spc_cpk_cpm_decoration.xlsx` 的字段语义、上周重算后的维护、人工维护及预警筛选规则。核验日期为 2026-11-24。能力计算公式及缺失值准入见 [SPC 能力计算规则](rules-spc-cpk&cpm.md)，服务调用路径见 [SPC 数据流](data-flow-spc.md)。

## 1. 工作簿用途与字段

该工作簿保存能力不达标项目的修饰决策及历史记录。自动报表与巡检按服务器今天之前的上一完整 ISO 周维护该周记录。默认位置为 `resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx`，实际路径由全局资源配置解析。

CPK 使用产品编码同名 sheet，例如 `M678`；CPM 使用 `<产品>_cpm`，例如 `M678_cpm`。两类指标独立匹配、独立维护。

| 字段 | 含义 |
|---|---|
| `prod_code`、`factory`、`step_id`、`param_name` | 产品、厂别、站点和参数。 |
| `period_type`、`period_label` | 周期类型及标签；周度采用 ISO 周标签 `YYYY-Www`。 |
| `period_sort` | 周期排序。 |
| `period_start`、`period_end` | 本次有效计算所用 Sheet 特征的最早、最晚检出时间，随匹配的上周计算值一同刷新。 |
| `cpk_corrected` / `cpm_corrected` | 由完成点位修饰的输入计算得到、尚未应用独立能力值替换的能力值。 |
| `flag` | 默认 `False`，表示不启用能力值替换；`True` 表示使用已保存的替换值。`Delete` 隐藏该指标，保留决策及另一指标。 |
| `cpk_replacement` / `cpm_replacement` | 创建该记录时生成并保存的能力值替换目标；后续读取复用原值。 |

匹配键为 `prod_code + factory + step_id + param_name + period_type + period_label`。同一项目跨周是不同记录，不合并为一条。人工扩展列、备注及额外单元格值也属于已有内容，自动追加时保留。

## 2. 新增规则

`build_capability_anomaly_detail()` 只选取**上一完整 ISO 周**，且对应能力值严格小于 `1.33`、有效 Sheet 时间跨度达到配置门槛的记录作为新增候选。SPC 主服务以服务器今天所在 ISO 周的周一作为参考日期。

门槛配置项为 `spc.spc_cpk.alert_min_span_hours`，当前值为 **0 小时**，配置缺失时默认 **48 小时**。配置接受有限非负数，可用小数表示不足一小时的跨度。设为 `0` 时，起止时间仍须可解析，结束时间可以等于开始时间。新增记录、SPC 页面、全指标矩阵及 CPK/CPM 监控共用这一门槛。修改门槛会使相关缓存失效，读取时按新门槛筛选已保存记录。能力值计算和已保存的样本边界保持原口径。[Inline 域配置](../../../../config/domain/inline_domain/spc.yaml)

- 不收录当前未结束周、其他历史周或月度记录作为新增候选。
- 等于或大于 `1.33` 的值不成为新增候选。
- 缺失或无法转为数值的值不成为新增候选。
- 时间跨度为每个“产品＋厂别＋站点＋参数＋周”中 `period_end - period_start`，不使用日历周长或全部点位的首末时间替代。少于配置门槛不新增预警；恰好达到门槛可新增。
- 起止时间缺失、无法解析或先后颠倒时，不能确认跨度达到门槛，不新增预警。
- 只追加工作簿中尚不存在的键。已有键按下节规则处理。
- 只有新创建的记录生成随机 replacement，范围为 `1.331～1.399` 的三位小数；新记录默认 `flag=False`。

跨度门槛只控制预警。短跨度项目仍正常计算 CPK/CPM，仍保留在能力结果表及常规图表中；没有新增候选时，普通报表路径不为该产品新建空报警 sheet 或空工作簿。巡检为正式空结果创建缺失的产品 sheet 和表头属于 Create；已有 sheet 的表头、记录或字段不自动补列、补值或修复。

当前 SPC 主服务也仅计算上一完整周。通用计算函数或直接应用调用可以提供更广的完整计算结果，但不改变新增候选的上述约束。

## 3. 上周已有记录的更新与保留

自动维护只匹配服务器今天之前的上一完整 ISO 周内、产品和指标相同的业务键。以本次计算得到的 corrected 判定，先维护台账，再应用 flag 和 replacement。

同一次写入在工作簿副本中完成。普通 XLSX 编辑对应工作表，企业保护文件通过 Excel 原生接口编辑。自动写入和管理员上传共享发布锁；提交前检查源版本并保留备份。

| 后续查询情况 | 已有记录如何处理 |
|---|---|
| 重算值小于或等于 `1.33` | 刷新 corrected、period_start 和 period_end，保留 flag、replacement、排序及人工扩展列。恰好等于 `1.33` 时保留已有行，不新增，也不预警。 |
| 重算值严格大于 `1.33` | 刷新该指标的 corrected 与样本时间，保留 flag、replacement 和备注。 |
| 本次该指标无法计算 | 保留原记录及决策，完整周重算后将其 corrected 留空。 |
| 完整周重算中缺少该键 | 其 corrected 留空，保留人工决策。其他周的记录保留原有字段。 |
| replacement 缺失、非法或越界 | 读取时不生成新随机值，不写回修补；该次能力值保留计算值，修饰标记为 False，交由人工维护。 |
| 本次有其他新异常键 | 通过跨度门槛的缺失键新增记录。 |
| 没有新增或修改 | 保留原文件版本和备份。 |

只有新建记录生成 replacement。已有行的刷新依据上周重算值，不受新增记录所用的跨度门槛限制。预警读取时仍检查更新后的样本跨度。

SPC 按 ISO 周选择各 Sheet 点位的最新测量，再截取上一完整周的最终点位重建 Sheet 均值和样本边界。本周复测归入本周。同一周的点位、规格及规则固定时，缓存失效后仍得到相同的计算结果。已有台账按键读取，计算值和已保存值分别用于报表展示与预警。[SPC 数据流](data-flow-spc.md)

业务键匹配、`flag=True` 且保存的 replacement 是严格介于 `1.33` 和 `1.40` 之间的有限数时，报表采用该目标。比如台账中的 corrected 为 `1.10`、replacement 为 `1.37` 且 flag 为 `True`，最终展示 `1.37`，台账继续保存 `1.10`。`1.33` 阈值用于新增异常和预警筛选，应用替换时按已保存的决策和目标。replacement 缺失或无效时，报表显示本次计算值，修饰标记为 `False`。

### 管理员手动上传

原始用户规格 [feat-SPC_CPM.md](../../../../docs/dev_docs/dev_spec/inline_domain/spc/feat-SPC_CPM.md) 中“Task2：制作cpk自动修正逻辑”要求提供与既有 uploader 相同的管理员维护入口。用户在本次澄清中确认继续保留该入口。管理员在 SPC 预警中心下载对应产品及指标的台账，再提交 flag 和 replacement 修改。写入时核对下载版本。系统保留当前事实和上传中省略的记录。用户维护后的 flag 和有效 replacement 从工作簿重新读取并应用。

### 页面与监控预警

历史能力看板按指标移除 `Delete` 项目的公开明细。对该周有可计算值的项目，公开周、月、季、年汇总同时扣除它的分母贡献，重新计算达标数和达标率。原始台账和维护用汇总保留完整基线，撤销决定后可恢复显示。

SPC 页面、全指标矩阵和 CPK/CPM 监控统一读取 `spc_cpk_cpm_decoration.xlsx` 已保存的产品 sheet。SPC 主服务先完成能力计算及严格持久化，然后页面通过 composition 的 `build_capability_latest_reader()` 读取与监控相同的 `CpkLatestExcelStore` / `CpmLatestExcelStore`；不使用本次周期能力表或未落盘的候选记录决定预警。

SPC 页面和矩阵通过 `read_alert_result()` 读取预警。读取器用 `normalize_latest_cpk()` 规范化状态，再由 `latest_capability_alerts()` 按上一完整 ISO 周、以下条件和日期排除规则筛选。预警明细显示已保存的 corrected 值，周期能力表展示本次计算并应用有效替换后的结果。

以下条件须同时满足，才能成为周度能力预警：

| 条件 | CPK | CPM |
|---|---|---|
| 已保存的明细来源 | 产品同名 sheet | `<产品>_cpm` sheet |
| 统计周期 | `period_type=week`，`period_label` 为参考日期的上一完整 ISO 周 | 同 CPK |
| 人工决策 | 规范化后的 `flag=False` | 同 CPK |
| 能力值 | `cpk_corrected < 1.33` | `cpm_corrected < 1.33` |
| 样本时间跨度 | 起止时间有效，`period_end - period_start` 达到配置门槛（当前 `0` 小时，缺省 `48` 小时） | 同 CPK |
| 日期排除 | 当前厂别及周期未命中排除规则 | 同 CPK |

等于 `1.33` 的记录不产生预警。跨度恰好等于配置门槛时通过检查。预警要求已保存的 flag 规范化为 `False`，因此 `True`、`Delete` 或空决策均不计入。能力值为空或无法转为数值时，未修饰项目的状态为“无法判定”。

下表使用当前 `0` 小时配置，能力不达标的项目以 `flag=False` 为例。

| 场景 | 能力值 / Sheet 跨度 | 明细处理 | 预警结果（flag=False） |
|---|---|---|---|
| 首次遇到时间边界有效的不达标项目 | 0.9 / 18h | 新增该周明细 | 预警 |
| 起止时间缺失或颠倒的新项目 | 0.9 / 无法确认 | 保留报表计算值，无台账记录 | 不预警 |
| 后续查询同一键 | 仍为 0.9 / 18h | 原行、corrected、样本边界及决策不变 | 继续按已保存记录预警 |
| 其他周新增样本 | 与该周无关 | 已结束周样本隔离，原行不变 | 该周预警不因其他周样本改变 |
| 同一项目上周重算恢复达标 | 1.45 / 18h | 更新 corrected、保留人工决策 | 不再预警 |

在已确认覆盖的完整周内，本次缺少匹配业务键时，台账保留人工决定，当前 corrected 留空。预警入口只读取已保存的结果，不用新计算的候选覆盖读取事实。

全指标矩阵和 CPK/CPM 监控通过 `normalize_latest_cpk()` 读取工作簿。历史周度行即使仍为 `flag=False`、值低于 1.33，跨度不足或无法确认时也标为“数据跨度不足”，不计入预警项目数、不生成矩阵预警详情。数值仍保留，历史行不自动删除。该规则不修改 Sheet OOS/OOC 的报警逻辑。

能力结果缓存及矩阵源签名包含跨度策略，SPC 结果签名额外标识统一明细来源及异常专用台账规则。Excel 原始内容按文件版本缓存，规范化及页面读取发生在能力计算缓存之外，保证保存后的明细或人工 flag 变化可被读取。

2026-11-17用户此前明确授权对当时工作簿执行一次严格 `> 1.33` 的 corrected 清理，共删除292条（CPK148、CPM144）；等于1.33、空值及其他记录在该次清理中不动。完整备份后，在副本中用 Excel 原生删行并逐表验证后替换，未依据 replacement 删除记录。2026-11-24 起，后续自动刷新保留已有决策行。

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

SPC 主服务启用 `strict_persistence=True`：已有工作簿或 sheet 清单读取失败、新增、更新或删除写入失败均向应用层抛出异常，不返回候选作为成功结果，失败结果也不进入报表缓存。原文件保留；页面提示加载失败。预警明细再次读取失败时，页面显示“CPK/CPM 预警数据读取失败”，停止该次展示，不回退到计算结果生成预警。新记录保存前将 pandas 缺失类型转换为空单元格；已有行的缺失字段不因此自动补值。

兼容调用默认 `strict_persistence=False`，保留原有读取失败返回候选、写入失败记录异常并返回原台账的行为；它们不作为 SPC 页面预警的数据来源。

| 责任 | 实现 |
|---|---|
| 新增候选、flag 匹配、创建时生成替换值与缺失键追加 | [cpk_decoration.py](../../../../src/inline_domain/core/spc/cpk_decoration.py)：`build_capability_anomaly_detail()`、`merge_capability_detail_with_decoration_flags()`、`_append_missing_detail_rows()`。 |
| 编排上周重算与能力替换 | [capability_decoration_service.py](../../../../src/inline_domain/application/spc/capability_decoration_service.py)：`prepare_capability_decoration()`。 |
| 匹配上周记录、计算增删改并跳过无变化的写入 | [capability_decoration_repository.py](../../../../src/inline_domain/infrastructure/spc/capability_decoration_repository.py)：`persist_capability_decoration()`。 |
| 原生单元格更新、整行删除、追加及原子提交 | [capability_workbook_writer.py](../../../../src/inline_domain/infrastructure/spc/capability_workbook_writer.py)：`write_capability_rows()`。 |
| 服务器日期与上一完整 ISO 周范围 | [spc_service.py](../../../../src/inline_domain/application/spc/spc_service.py)：`SpcReportService.fetch_spc_report_payload()`。 |
| 统一明细读取、筛选及页面接入 | [cpk_latest.py](../../../../src/inline_domain/core/monitor/cpk_latest.py)：`normalize_latest_cpk()`、`latest_capability_alerts()`；[composition.py](../../../../src/inline_domain/composition.py)：`build_capability_latest_reader()`；[SPC监控报表.py](../../../../app/pages/SPC监控报表.py)。 |
| 同周保存/读取生命周期、写入失败、正常/空/失败页面验证 | [test_spc_capability_ledger.py](../../../../tests/unit/inline_domain/application/spc/test_spc_capability_ledger.py)、[test_spc_page_alerts.py](../../../../tests/unit/app/pages/test_spc_page_alerts.py)。 |
| 人工上传维护例外 | [spc_dashboard.py](../../../../app/sections/inline_domain/spc/spc_dashboard.py)：`render_capability_decoration_admin()`。 |
| 缺省 48 小时门槛、页面与监控读取一致性、上周样本边界刷新 | [test_capability_alert_span.py](../../../../tests/unit/inline_domain/core/spc/test_capability_alert_span.py)。 |
| 跨周复测时上周点位及两种波动口径的能力结果保持一致 | [test_weekly_point_stability.py](../../../../tests/unit/inline_domain/core/spc/test_weekly_point_stability.py)。 |
| 追加新键、保护已有记录、读取目标时的替换规则 | [test_capability_ledger_append_only.py](../../../../tests/unit/inline_domain/core/spc/test_capability_ledger_append_only.py)、[test_cpk_decoration.py](../../../../tests/unit/inline_domain/core/spc/test_cpk_decoration.py)。 |
