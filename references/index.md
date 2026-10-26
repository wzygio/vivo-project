# Project Knowledge Router

## 按需阅读

先按问题选择下表中的文件，只读取命中的文档；理解整个模块时再读对应 overview，并沿相关链接展开。项目目的与硬边界见 [CONTEXT.md](../CONTEXT.md)，跨模块运行流见 [ARCHITECTURE.md](../ARCHITECTURE.md)，决策理由按需查询 `docs/ADR/`。

### 业务知识与当前实现

| 问题 / 关键词 | 首选文件 |
|---|---|
| Lot、Sheet、Glass、Panel、膜位、厂别等制造术语 | [术语表](domain/shared_kernel/shared/GLOSSARY.md) |
| Inline 范围、SPC / CTQ / Monitor / AOI 职责 | [Inline 概览](domain/inline_domain/shared/overview-inline.md) |
| Inline 共享测量、制备管线、快照、薄投影与装配 | [Inline 基础设施架构](domain/inline_domain/shared/architecture-inline-infrastructure.md) |
| Inline data_exclusion、厂别日期排除、去重与修饰之前过滤、快照与历史保留 | [Inline 日期排除规则](domain/inline_domain/shared/rules-inline-date-exclusion.md) |
| SPC 后端输入、分层处理、前端筛选、预警与图表数据流 | [SPC 数据流](domain/inline_domain/spc/data-flow-spc.md) |
| SPC 传统→专用点位修饰、中央规格区间、日期窗口、稳定哈希 margin、flag=False、修饰后能力输入 | [SPC 点位修饰规则](domain/inline_domain/spc/rules-spc-point-decoration.md) |
| SPC Cpk / Cpm、Sheet 均值 μ、点位标准差 σ、规格与能力计算边界；OOS 点位修饰、OOC 清单筛选与能力不达标的先后关系 | [SPC 能力计算规则](domain/inline_domain/spc/rules-spc-cpk&cpm.md) |
| SPC CPK / CPM 明细字段、同周更新与缓存失效、统一 Excel 预警来源、可配置跨度门槛（默认 48h）、历史数值刷新、flag 与风险台账筛选 | [SPC 能力明细工作簿规则](domain/inline_domain/spc/rules-spc-cpk&cpm-decoration.md) |
| 简要介绍 Cpk / Cpm 公式及各指标的计算方式 | [Cpk / Cpm 计算简要版](domain/inline_domain/spc/北极星-cpk_cpm计算公式.md) |
| 全指标预警矩阵、计划任务、跨进程预计算快照 | [预警看板架构](domain/inline_domain/monitor/architecture-alert-dashboard.md) |
| CPK / CPM 汇总补零基线、最新周写回、厂别筛选与日期排除 | [CPK / CPM 汇总工作簿规则](domain/inline_domain/monitor/rules-cpk-summary-workbook.md) |
| AOI_RS监控看板、半年季度月周密度、周期历史复用、无L1取数、按产品Sheet下载 | [AOI_RS 周期监控规则](domain/inline_domain/monitor/rules-aoi-rs-period-monitor.md) |
| SPC 主制程设备 / 腔室、最近 OUT、履历回退 | [SPC 主制程数据源](domain/inline_domain/spc/data-source-spc-main-process.md) |
| AOI_TT 来源表、字段与取数口径 | [AOI_TT 数据源](domain/inline_domain/aoi_tt/data-source-aoi-tt.md) |
| AOI_TT 聚合、Particle Size、趋势与单片异常链路 | [AOI_TT 数据流](domain/inline_domain/aoi_tt/data-flow-aoi-tt.md) |
| AOI_TT 专用 Sheet 修饰、厂别日期窗口、UCL 半规格触发、四分之一上限整数哈希、可配置比例 | [AOI_TT 专用修饰规则](domain/inline_domain/aoi_tt/rules-aoi-tt-special-decoration.md) |
| AOI_TT 上周平均每片 TT 个数、八产品 PPT、粒径及 Array 层别筛选 | [AOI_TT 周度报告](domain/inline_domain/aoi_tt/rules-aoi-tt-weekly-report.md) |
| AOI_TT 仅 Total 昨日趋势预警、五天均值、七天跨日比较与精简预警中心 | [AOI_TT 趋势预警规则](domain/inline_domain/aoi_tt/rules-aoi-tt-trend-alerts.md) |
| AOI_RS 来源表、RS Code、分母与计数口径 | [AOI_RS 数据源](domain/inline_domain/aoi_rs/data-source-aoi-rs.md) |
| AOI_RS 上一周平均每片 RS 个数、PPT 模板、无过货、Array 层别及站点筛选 | [AOI_RS 周度报告](domain/inline_domain/aoi_rs/rules-aoi-rs-weekly-report.md) |
| AOI_RS 昨日趋势预警、当日超规双条件、上次密度与五天均值、七天跨日比较、1.2倍、缺测与自动图像 | [AOI_RS 趋势预警规则](domain/inline_domain/aoi_rs/rules-aoi-rs-trend-alerts.md) |
| AOI_RS 专用顺序修饰、厂别日期窗口、Sheet 半规格、Lot 归零、周日1.3倍限幅 | [AOI_RS 专用修饰规则](domain/inline_domain/aoi_rs/rules-aoi-rs-special-decoration.md) |
| Inline Sheet OOS、三态修饰、人工台账与执行机制 | [Sheet OOS 规则](domain/inline_domain/shared/rules-sheet-oos-decoration.md) |
| Step_ID → Data_Type、SPC / CTQ / AOI 站点分类 | [站点类型映射](domain/inline_domain/shared/mapping-step-id-data-type.json) |
| Yield 良率分析职责与分层 | [Yield 概览](domain/yield_domain/shared/overview-yield.md) |
| Yield Lot/趋势预警结果文件、原报表发布、空结果/失败元数据、矩阵直读与全产品补齐 | [Yield 预警结果规则](domain/yield_domain/shared/rules-yield-alert-results.md) |
| Mapping 坐标、矩阵修饰、随机分布与守恒 | [Mapping 算法](domain/yield_domain/mapping/algorithm-mapping.md) |
| MWD 月周日趋势、修饰表、Code / Group 汇总与跨月平滑 | [MWD 趋势算法](domain/yield_domain/mwd_trend/algorithm-mwd-trend.md) |
| 全产品半年／季度／月度不良率、独立过货数、Excel历史快照、Code筛选及季报Top5 | [年度趋势历史规则](domain/yield_domain/mwd_trend/rules-period-trend-history.md) |
| Sheet / Lot 不良率、热点分配与覆盖率计算 | [Sheet / Lot 算法](domain/yield_domain/sheet_lot/algorithm-sheet-lot.md) |
| 共享内核、配置、日志、缓存与数据库公共能力 | [共享内核概览](design/shared_kernel/shared/overview-shared-kernel.md) |
| QTime 取数、重点站点修饰、蒸镀腔室停留时间 | [QTime 数据源](domain/indicator_domain/qtime/datasource-Q-Time数据源分析.md)、[站点修饰算法](design/indicator_domain/qtime/algorithm-qtime-data-decoration.md)、[腔室数据库规则](domain/indicator_domain/qtime/rules-chamber-database.md) |
| Indicator data_exclusion、Q-Time/IJP 显示日期过滤、聚合前排除、缓存失效 | [Indicator 日期排除规则](domain/indicator_domain/shared/rules-indicator-date-exclusion.md) |
| IJP 溢流数据源、站点与时间口径 | [IJP 数据源](domain/indicator_domain/ijp/datasource-IJP溢流报表分析.md)；[孔区月周汇总与占比规则](domain/indicator_domain/ijp_hole/rules-hole-period-report.md)；孔区输入按 `docs/dev_docs/dev_spec/indicator_domain/ijp_hole/` 定位 |
| IQC 寿命测试 JNCD、衰减曲线、实时取数与快照/缓存边界 | [寿命监控与取数规则](domain/iqc_domain/lifetime/rules-lifetime-monitoring.md)；原始来源分析按 `docs/dev_docs/dev_spec/iqc_domain/lifetime/` 定位，匿名边界见 `docs/ADR/0030-*` |
| IQC 蒸镀材料、物料／批次／产品多对多关联、已启用产品筛选、产品型号展示、默认检验日期范围 | [蒸镀材料产品范围规则](domain/iqc_domain/eva_materials/rules-evaporation-product-scope.md)；来源分析按 `docs/dev_docs/dev_spec/iqc_domain/eva_materials/` 定位，决策见 `docs/ADR/0029-*` |
| 关键备件规格、测量、快照与寿命报表；数据读取、通用仿造、CVD 特殊规则仿造、厂别-MMDD Sheet 更新、小数年限与自然月、95% 更换 | [关键备件数据流](domain/equipment_domain/parts/data-flow-critical-parts.md) |
| 日期前推、offset_days、源日期与显示日期、跨模块报表时间链路 | [报表日期前推数据链路](domain/shared_kernel/shared/data-flow-report-date-forward.md) |

### 跨域范式与功能设计

| 问题 / 关键词 | 首选文件 |
|---|---|
| 数据修饰统一语义、原始事实 / 决策台账 / 报表投影、分层职责 | [数据修饰架构范式](design/shared_kernel/shared/architecture-data-decoration.md) |
| Infrastructure 刷新、原始快照、增量回读、TTL、强刷、中午截止、Yield 精确入库时间；Streamlit 定时启动/重启、Windows 计划任务及进程清理 | [系统数据刷新机制](design/shared_kernel/shared/data-flow-infrastructure-refresh.md) |
| 多用户共享缓存、max_entries、同键并发、查询收起、定向失效与跨进程边界 | [看板共享缓存设计](design/shared_kernel/shared/architecture-shared-dashboard-cache.md) |
| 数据健康、空窗口、失败降级、应用出站端口、缓存隔离与架构守卫 | [数据健康与出站端口](design/shared_kernel/shared/architecture-data-health-and-outbound-ports.md) |
| 门户节点、Inline指标、寿命测试入口、专项资料合并、台账周报与解析报告上传/下载/预览 | [门户导航与专项资料](design/shared_kernel/shared/interaction-portal-documents.md) |
| Inline 报表滚动、Plotly 滚轮缩放冲突与交互约束 | [报表滚动交互设计](design/inline_domain/shared/interaction-inline-report-scroll.md) |
| Inline 周度 PPT 输出设计、density 规格判定、红绿字体、八产品分页与共用数据契约 | [周度 PPT 输出设计规范](design/inline_domain/shared/architecture-weekly-summary-ppt.md) |

共享工程标准按任务使用 `$ecc-production-rules`；文档职责与维护规则见 [CONTEXT.md](../CONTEXT.md#maintenance-and-document-ownership)。

## 文件命名规则

适用于 `references/domain/` 和 `references/design/` 中的知识文件：

```text
<类型前缀>-<具体程序或业务主题>.<扩展名>
```

使用小写英文、数字和连字符（kebab-case）。类型在前，程序或主题在后；保留 `spc`、`aoi-tt`、`mwd-trend` 等可检索业务词。正文标题可用中文，代码标识符保持原样。普通文档使用 `.md`，结构化映射使用 `.json`。

| 类型前缀 | 内容边界 | 示例 |
|---|---|---|
| `overview-` | 模块范围、职责与总体结构 | `overview-inline.md` |
| `architecture-` | 分层、依赖、架构约束与设计范式 | `architecture-data-decoration.md` |
| `data-source-` | 来源表、字段、关联与取数规则 | `data-source-aoi-rs.md` |
| `data-flow-` | 输入、处理阶段、层间产物与输出 | `data-flow-spc.md` |
| `rules-` | 业务判定、不变量、例外及执行机制 | `rules-sheet-oos-decoration.md` |
| `algorithm-` | 计算或数据处理过程 | `algorithm-mwd-trend.md` |
| `mapping-` | 标识、分类或字段对应关系 | `mapping-step-id-data-type.json` |
| `interaction-` | 用户操作、交互行为与约束 | `interaction-inline-report-scroll.md` |

`GLOSSARY.md` 保留为已约定的术语入口；路由文件 `index.md` / `README.md` 使用入口名称。目录统一为 `<domain>/<submodule>/`，迁移保留现有文件名；文件命名规则不改变 Python 包、代码符号或资源文件名称。新增文档先按用途选择类型，再按实际范围选择主题；持久知识文件用 Git 追踪版本，文件名不添加日期、任务编号、`final` 或 `v2`。

## 索引缺项时的定位方式

**此索引可能未及时更新。没有命中项或链接失效时，先按上述命名规则扫描文件名称，缩小到类型和主题，再读取候选文件的标题与适用范围；不要因此全量读取目录。**

在仓库根目录执行，例如：

```powershell
# 列出实际文件，核对路径
rg --files references/domain/inline_domain references/design/inline_domain
# 查 SPC 取数资料
rg --files references/domain/inline_domain/spc -g 'data-source-*spc*.md'
# 查某个主题的所有文档，或所有算法文档
rg --files references/domain/inline_domain/aoi_tt -g '*aoi-tt*'
rg --files references/domain/yield_domain references/design/yield_domain -g 'algorithm-*.md'
```

将用户用语映射到已知主题词，例如“月周日 → mwd-trend”“单片超规 → sheet-oos”“AOI_TT → aoi-tt”。若文件名仍未命中，在相关模块内搜索标题或关键词，再选择性读取正文。

## 维护约定

- 新增、重命名或调整文档范围时，同步更新本路由的问题关键词、路径和相关引用；本文件允许路由到具体文件。
- 当前领域知识放在 `domain/<domain>/<submodule>/`，功能设计放在 `design/<domain>/<submodule>/`；域内共享资料用 `<domain>/shared/`，跨域机制用 `shared_kernel/shared/`。归属按 [Domain Submodule Architecture](../ARCHITECTURE.md#domain-submodule-architecture) 确定；决策背景与取舍继续记录在 `docs/ADR/`。
- 每条规则维护一个权威位置，概览和其他文档通过链接引用。核验日期表示实际核验时间；设计意图与已验证实现应明确区分。
- 修改完成后检查新文件名、路由覆盖、链接目标及旧名称残留。发现索引遗漏时，补充已确认的入口。
