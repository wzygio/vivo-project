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
| SPC 专用点位修饰、中间50%规格区间、开始结束日期、flag=False、修饰后能力输入 | [SPC 点位修饰规则](domain/inline_domain/spc/rules-spc-point-decoration.md) |
| SPC Cpk / Cpm、Sheet 均值 μ、点位标准差 σ、规格与能力计算边界；OOS 点位修饰、OOC 清单筛选与能力不达标的先后关系 | [SPC 能力计算规则](domain/inline_domain/spc/rules-spc-cpk&cpm.md) |
| SPC CPK / CPM 明细字段、同周更新与缓存失效、统一 Excel 预警来源、48h 门槛、历史数值刷新、flag 与风险台账筛选 | [SPC 能力明细工作簿规则](domain/inline_domain/spc/rules-spc-cpk&cpm-decoration.md) |
| 简要介绍 Cpk / Cpm 公式及各指标的计算方式 | [Cpk / Cpm 计算简要版](domain/inline_domain/spc/北极星-cpk_cpm计算公式.md) |
| 全指标预警矩阵、计划任务、跨进程预计算快照 | [预警看板架构](domain/inline_domain/monitor/architecture-alert-dashboard.md) |
| CPK / CPM 汇总补零基线、最新周写回、厂别筛选与日期排除 | [CPK / CPM 汇总工作簿规则](domain/inline_domain/monitor/rules-cpk-summary-workbook.md) |
| SPC 主制程设备 / 腔室、最近 OUT、履历回退 | [SPC 主制程数据源](domain/inline_domain/spc/data-source-spc-main-process.md) |
| AOI_TT 来源表、字段与取数口径 | [AOI_TT 数据源](domain/inline_domain/aoi_tt/data-source-aoi-tt.md) |
| AOI_TT 聚合、Particle Size、趋势与单片异常链路 | [AOI_TT 数据流](domain/inline_domain/aoi_tt/data-flow-aoi-tt.md) |
| AOI_RS 来源表、RS Code、分母与计数口径 | [AOI_RS 数据源](domain/inline_domain/aoi_rs/data-source-aoi-rs.md) |
| AOI_RS 专用顺序修饰、厂别日期窗口、Sheet 半规格、Lot 归零、周日1.3倍限幅 | [AOI_RS 专用修饰规则](domain/inline_domain/aoi_rs/rules-aoi-rs-special-decoration.md) |
| Inline Sheet OOS、三态修饰、人工台账与执行机制 | [Sheet OOS 规则](domain/inline_domain/shared/rules-sheet-oos-decoration.md) |
| Step_ID → Data_Type、SPC / CTQ / AOI 站点分类 | [站点类型映射](domain/inline_domain/shared/mapping-step-id-data-type.json) |
| Yield 良率分析职责与分层 | [Yield 概览](domain/yield_domain/shared/overview-yield.md) |
| Mapping 坐标、矩阵修饰、随机分布与守恒 | [Mapping 算法](domain/yield_domain/mapping/algorithm-mapping.md) |
| MWD 月周日趋势、修饰表、Code / Group 汇总与跨月平滑 | [MWD 趋势算法](domain/yield_domain/mwd_trend/algorithm-mwd-trend.md) |
| Sheet / Lot 不良率、热点分配与覆盖率计算 | [Sheet / Lot 算法](domain/yield_domain/sheet_lot/algorithm-sheet-lot.md) |
| 共享内核、配置、日志、缓存与数据库公共能力 | [共享内核概览](design/shared_kernel/shared/overview-shared-kernel.md) |
| QTime 取数、重点站点修饰、蒸镀腔室停留时间 | [QTime 数据源](domain/indicator_domain/qtime/datasource-Q-Time数据源分析.md)、[站点修饰算法](design/indicator_domain/qtime/algorithm-qtime-data-decoration.md)；腔室规则按 `qtime` 下对应开发说明定位 |
| IJP 溢流数据源、站点与时间口径 | [IJP 数据源](domain/indicator_domain/ijp/datasource-IJP溢流报表分析.md)；孔区输入按 `docs/dev_docs/dev_spec/indicator_domain/ijp_hole/` 定位 |
| IQC 蒸镀材料与寿命测试来源和报表要求 | 按 `docs/dev_docs/dev_spec/iqc_domain/{eva_materials,lifetime}/` 定位；已接受口径见 `docs/ADR/0029-*` 与 `0030-*` |
| 关键备件规格、测量、快照与寿命报表 | [关键备件数据流](domain/equipment_domain/parts/data-flow-critical-parts.md) |
| 日期前推、offset_days、源日期与显示日期、跨模块报表时间链路 | [报表日期前推数据链路](domain/shared_kernel/shared/data-flow-report-date-forward.md) |

### 跨域范式与功能设计

| 问题 / 关键词 | 首选文件 |
|---|---|
| 数据修饰统一语义、原始事实 / 决策台账 / 报表投影、分层职责 | [数据修饰架构范式](design/shared_kernel/shared/architecture-data-decoration.md) |
| Infrastructure 刷新、原始快照、增量回读、TTL、强刷、中午截止、Yield 精确入库时间 | [系统数据刷新机制](design/shared_kernel/shared/data-flow-infrastructure-refresh.md) |
| 多用户共享缓存、max_entries、同键并发、查询收起、定向失效与跨进程边界 | [看板共享缓存设计](design/shared_kernel/shared/architecture-shared-dashboard-cache.md) |
| 数据健康、空窗口、失败降级、应用出站端口、缓存隔离与架构守卫 | [数据健康与出站端口](design/shared_kernel/shared/architecture-data-health-and-outbound-ports.md) |
| Inline 报表滚动、Plotly 滚轮缩放冲突与交互约束 | [报表滚动交互设计](design/inline_domain/shared/interaction-inline-report-scroll.md) |

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
