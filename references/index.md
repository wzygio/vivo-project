# Project Knowledge Router

## 按需阅读

先按问题选择下表中的文件，只读取命中的文档；理解整个模块时再读对应 overview，并沿相关链接展开。项目目的与硬边界见 [CONTEXT.md](../CONTEXT.md)，跨模块运行流见 [ARCHITECTURE.md](../ARCHITECTURE.md)，决策理由按需查询 `docs/ADR/`。

### 业务知识与当前实现

| 问题 / 关键词 | 首选文件 |
|---|---|
| Lot、Sheet、Glass、Panel、膜位、厂别等制造术语 | [术语表](domain/GLOSSARY.md) |
| Inline 范围、SPC / CTQ / Monitor / AOI 职责 | [Inline 概览](domain/Inline_domain/overview-inline.md) |
| Inline 共享测量、制备管线、快照、薄投影与装配 | [Inline 基础设施架构](domain/Inline_domain/architecture-inline-infrastructure.md) |
| SPC 后端输入、分层处理与输出 | [SPC 数据流](domain/Inline_domain/data-flow-spc.md) |
| SPC 主制程设备 / 腔室、最近 OUT、履历回退 | [SPC 主制程数据源](domain/Inline_domain/data-source-spc-main-process.md) |
| AOI_TT 来源表、字段与取数口径 | [AOI_TT 数据源](domain/Inline_domain/data-source-aoi-tt.md) |
| AOI_TT 聚合、Particle Size、趋势与单片异常链路 | [AOI_TT 数据流](domain/Inline_domain/data-flow-aoi-tt.md) |
| AOI_RS 来源表、RS Code、分母与计数口径 | [AOI_RS 数据源](domain/Inline_domain/data-source-aoi-rs.md) |
| Inline Sheet OOS、三态修饰、人工台账与执行机制 | [Sheet OOS 规则](domain/Inline_domain/rules-sheet-oos-decoration.md) |
| Step_ID → Data_Type、SPC / CTQ / AOI 站点分类 | [站点类型映射](domain/Inline_domain/mapping-step-id-data-type.json) |
| Yield 良率分析职责与分层 | [Yield 概览](domain/yield_domain/overview-yield.md) |
| Mapping 坐标、矩阵修饰、随机分布与守恒 | [Mapping 算法](domain/yield_domain/algorithm-mapping.md) |
| MWD 月周日趋势、修饰表、Code / Group 汇总与跨月平滑 | [MWD 趋势算法](domain/yield_domain/algorithm-mwd-trend.md) |
| Sheet / Lot 不良率、热点分配与覆盖率计算 | [Sheet / Lot 算法](domain/yield_domain/algorithm-sheet-lot.md) |
| 共享内核、配置、日志、缓存与数据库公共能力 | [共享内核概览](domain/shared_kernel/overview-shared-kernel.md) |

### 跨域范式与功能设计

| 问题 / 关键词 | 首选文件 |
|---|---|
| 数据修饰统一语义、原始事实 / 决策台账 / 报表投影、分层职责 | [数据修饰架构范式](design/feat_design/architecture-data-decoration.md) |
| Inline 报表滚动、Plotly 滚轮缩放冲突与交互约束 | [报表滚动交互设计](design/feat_design/interaction-inline-report-scroll.md) |

共享工程标准按任务使用 `$ecc-production-rules`；Harness 演进记录见 [retrospective.md](retrospective.md)。

## 文件命名规则

适用于 `references/domain/` 和 `references/design/feat_design/` 中的知识文件：

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

`GLOSSARY.md` 保留为已约定的术语入口；路由文件 `index.md` / `README.md` 使用入口名称。此次保留现有目录路径，文件命名规则不改变 Python 包、代码符号或资源文件名称。新增文档先按用途选择类型，再按实际范围选择主题；持久知识文件用 Git 追踪版本，文件名不添加日期、任务编号、`final` 或 `v2`。

## 索引缺项时的定位方式

**此索引可能未及时更新。没有命中项或链接失效时，先按上述命名规则扫描文件名称，缩小到类型和主题，再读取候选文件的标题与适用范围；不要因此全量读取目录。**

在仓库根目录执行，例如：

```powershell
# 列出实际文件，核对路径
rg --files references/domain references/design/feat_design
# 查 SPC 取数资料
rg --files references/domain references/design/feat_design -g 'data-source-*spc*.md'
# 查某个主题的所有文档，或所有算法文档
rg --files references/domain references/design/feat_design -g '*aoi-tt*'
rg --files references/domain references/design/feat_design -g 'algorithm-*.md'
```

将用户用语映射到已知主题词，例如“月周日 → mwd-trend”“单片超规 → sheet-oos”“AOI_TT → aoi-tt”。若文件名仍未命中，在相关模块内搜索标题或关键词，再选择性读取正文。

## 维护约定

- 新增、重命名或调整文档范围时，同步更新本路由的问题关键词、路径和相关引用；本文件允许路由到具体文件。
- 当前知识放在 `domain/`，跨域范式与功能设计放在 `design/feat_design/`；决策背景与取舍继续记录在 `docs/ADR/`。
- 每条规则维护一个权威位置，概览和其他文档通过链接引用。核验日期表示实际核验时间；设计意图与已验证实现应明确区分。
- 修改完成后检查新文件名、路由覆盖、链接目标及旧名称残留。发现索引遗漏时，补充已确认的入口。
