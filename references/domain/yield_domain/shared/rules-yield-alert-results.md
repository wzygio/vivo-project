# Yield 预警结果发布与文件读取规则

更新日期：2026-10-21。适用：入库不良率分析看板、自动预警矩阵及矩阵预计算。结果以文件发布并跨进程共享，内存仅缓存文件解析结果。

## 1. 资源与所有权

全局资源注册表 `config/global.yaml → resources.yield_domain.files` 配置两份独立工作簿：

| 配置键 | 默认位置 | 内容 |
|---|---|---|
| `lot_oos_results` | `resources/yield_domain/sheet_lot/yield_lot_oos_results.xlsx` | Lot 超规记录 |
| `trend_alert_results` | `resources/yield_domain/mwd_trend/yield_trend_alert_results.xlsx` | 分析期间全部 Group/Code 月度、周度结构化趋势报警及当前判定标记 |

路径通过 `ConfigLoader.get_domain_resource_path` 和 Yield composition 解析；不根据产品重新拼固定文件名。首次成功生成时才创建文件，不以空模板伪装已经计算。每份工作簿的 `<产品>` sheet 保存当前结果，`<产品>__meta` sheet 保存该产品的发布元数据。

`application/alert_publication.py` 调用原报表服务并协调独立发布；`application/alert_results.py` 拥有发布/读取契约、有效性和预警范围；`infrastructure/alert_result_workbooks.py` 拥有整本解析缓存和 Excel 持久化。`composition.py` 装配端口。它们是 Yield 通用预警结果职责，未新增独立计算引擎或源快照层。

## 2. 生产规则与时机

- Lot 复用 `core/sheet_lot/lot_alerts.compute_lot_oos_records`，使用报表已计算的 Lot 不良率与已读取警戒线。文件发布传入 `time_period=None`，保留所选期间内原 Lot 计算输出的全部超规；页面仍按数据最大入库日回看 30 天展示。原 `app.components.alert_center` 保留同名导入兼容。
- 趋势使用 `AlertService.get_dashboard_alert_history`，逐月/逐周复用原 `AbnormalDetector.detect_system_trend_records` 判定，保存分析期间全部 monthly/weekly、group/code、周期、当前/前期值及命中规则。没有两个有效观察点时不虚构前期基线。`is_current` 由原 `get_dashboard_alert_records` 的最新逐序列判定生成，是程序计算的当前状态标记，不是人工决策字段。页面继续消费原最新预警；外部基准报表比较的文本报警不进入这份结果。
- 聚合输出的 `monthly_full`、`weekly_full` 供历史发布使用，保留完整计算序列；`monthly`、`weekly` 继续保持原最近 3 个图表周期。月标签保留原 `%Y-%m月`，窗口解析兼容该业务标签，不改写展示格式。仅重发某类结果时，生产用例的 `kinds` 参数限制对应分支，其他结果文件不变。
- 入库分析页先生成并发布两类预警，之后才加载 Sheet/Mapping 与图像筛选设置。Lot 和趋势分别捕获失败，一种失败不阻止另一种发布；两者所需的原有计算缓存与资源手动刷新参数保持不变。
- 发布在计算缓存外调用。计算命中内存但结果文件缺失时仍能落盘；指纹和记录内容相同则不重写，不延长旧来源的刷新时间。计算过程中配置版本变化时不发布为成功。
- 工作簿属于生成结果，不增加人工 flag 决策规则。每次成功替换当前产品的结果及元数据，保留其他产品与 sheet，不把历史报警累加进当前结果。

## 3. 元数据、有效性与失败

成功元数据包括 schema、产品、类型、参考日、显示时间覆盖、`has_data`、记录数、发布时间、来源健康/刷新时间与源时间覆盖、策略签名、计算签名和指纹。显示时间覆盖与来源健康中的源时间覆盖具有不同语义。

策略签名跟踪全局配置、产品配置、Yield 域配置和结果 schema 版本；计算签名跟踪实际传入报表的指标版本及 cache context。报表采用的手动资源刷新语义继续保留：规格/修饰表如何进入报表由原流程控制，不在矩阵重新加载这些输入后另算一次。

读取要求参考日一致，策略版本一致，来源为 fresh，发布时间和来源刷新时间均处于全局 TTL 内。源快照的无时区时间按服务器本地 `Asia/Shanghai` 解释，再与 UTC 比较；带时区时间保留其含义。重写结果不能让旧来源重新变 fresh。Lot 的扫描覆盖必须包含上一完整 ISO 周；趋势覆盖必须包含参考日。Lot 读取后筛选 `[上周一, 本周一)`；趋势文件保留全部历史记录，但矩阵只读取 `is_current=True` 的记录，历史预警不改变当前红绿灯。旧格式没有该列时按原“全部为最新结果”的契约读取。

| 读取状态 | 含义 | 矩阵行为 |
|---|---|---|
| ready + 有记录 | 有效结果存在预警 | alert |
| ready + 零记录 | 已完成分析，有业务数据且无报警 | ok |
| no_data | 已成功发布，来源正常但无可分析业务数据 | 保留既有 no_data 展示约定 |
| missing / stale / uncovered / failed | 未发布、过期、覆盖不足或计算失败 | error，提示结果暂不可用，避免被当作达标 |
| 文件/元数据损坏 | 无法验证结果 | error，不返回空表冒充成功 |

失败只更新该产品元数据中的安全错误码与尝试信息，保留最后成功的记录；成功零报警则发布合法的空结果 sheet。写入失败保留旧文件并向页面/CLI 报告失败；如果文件不可写，无法保证失败标记本身也可保存，不能宣称结果已经更新。

发布使用操作系统进程锁与原子工作簿替换；一个产品的结果和元数据同时提交。锁按文件路径隔离，进程退出后 OS 释放；稳定锁文件保留在系统临时目录，不删除仍可能被其他进程等待的锁 inode。成功更新沿用公共 Excel helper 的备份与原子替换规则，备份为相邻的 `.xlsx.bak`；默认备份路径同样列入全局资源注册表，实际备份位置随主文件路径派生。

## 4. 消费与缓存

矩阵生产上下文只调用公开 Yield 结果读取服务，不再调用 `get_data_health`、Panel、Lot、MWD 或阵列时间分析。缺文件、缺产品、失败或过期时也不回退数据库或原报表。显式注入的旧测试上下文可继续用于判据对照，不是生产缺失回退入口。

工作簿解析缓存按路径、mtime_ns、大小取键，最多 32 项，TTL 服从全局设置；标准 XLSX 直接解析，企业加密回退到既有 Excel COM 读取。文件读取不依赖任何完整报表 application payload 已经预热。

矩阵单元格签名包含结果文件版本、读取有效性、策略和参考日，因此来源 TTL 到期也能使先前成功状态失效。矩阵定向“刷新数据”仅清理对应结果工作簿解析缓存并推进该指标/产品 revision，不重新生成报表结果。`warm_alert_matrix.py` 保持现有 JSON 状态汇总机制；新的 Yield 来源让它在冷进程中也只读文件。

现有点击详情及下方全产品良率图像仍按需使用报表服务。本规则不改变 L1 更新、数据库生命周期、人工维护表或详情图像。

## 5. 全产品补齐与运维

页面一次发布当前产品。其他产品尚未生成时矩阵明确显示不可用，不在矩阵补算。需要首次补齐或批量更新时，调用同一 application 生产用例的 CLI：

```powershell
# 只验证注册路径，不读写业务数据、不查询数据库
.venv/Scripts/python.exe tools/publish_yield_alert_results.py --check

# 生成所有启用产品的结果；执行原报表所需的数据读取/计算
.venv/Scripts/python.exe tools/publish_yield_alert_results.py

# 仅更新指定启用产品
.venv/Scripts/python.exe tools/publish_yield_alert_results.py --products M678 Z571

# 明确指定包含首尾日的分析窗口（2026-10-21 的补齐范围）
.venv/Scripts/python.exe tools/publish_yield_alert_results.py --start-date 2026-08-01 --end-date 2026-10-21
```

CLI 沿用页面配置装配、手动资源版本和原报表服务，不新增任务计划或常驻服务；日期参数必须成对且起日不晚于止日，不改写服务全局时间或持久配置。明确指定窗口时向前读取一个自然月作为月/周比较基线，例如发布 8 月 1 日起的记录时从 7 月 1 日开始计算；基线月记录不写入发布期间，跨起日的周按周期区间相交保留。输出含产品、窗口、基线起日、成功/部分失败、可读状态和安全错误类型，发布后再次验证读取状态，不能把过期/覆盖不足称为成功。某产品失败不阻止其他产品，任一失败退出 1；全部成功退出 0。`--check` 不能证明真实数据已经生成。

警戒线 XLSX 由 `infrastructure/warning_workbook_reader.py` 读取：普通文件沿用 `header=None` 解析；`BadZipFile` 时走现有只读 COM，恢复首行后继续同一业务解析。不能因读取失败返回空规格；不改写维护文件。写入测试数据前必须把真实配置派生出的绝对路径重绑定到临时目录，并核实路径归属。

## 6. 验证入口

- `tests/unit/yield_domain/application/test_alert_results.py`：空结果、覆盖/日期/TTL、失败保留、原子写失败、损坏恢复。
- `tests/unit/yield_domain/application/test_alert_publication.py`：既有判据、独立失败、空来源与无图像依赖。
- `tests/unit/app/pages/test_yield_alert_publication.py`：真实页面在 Sheet 失败前发布，并在普通 rerun 保持文件版本。
- `tests/unit/app/sections/monitor/test_file_only_alert_matrix.py`：默认生产上下文禁止 Yield 计算/SQL，以及 4 工作簿 × 7 产品只解析 4 次、单文件更新只多解析一次。
- `tests/integration/test_yield_alert_result_files.py`：并发进程保留不同产品、冷进程读灯、预计算入口。

相关决策：[ADR-0035](../../../../docs/ADR/0035-published-alert-results-for-matrix.md)。
