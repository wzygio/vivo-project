# 从计划与设计文档，到 Local Markdown Issues 与 ADR

本文是为 `vivo-project` 完整运行一次 `/setup-matt-pocock-skills` 而准备的操作手册。  
已选择的方案：**Local Markdown**。  
本文不代替 Skill 的逐步确认；真正执行时仍必须一项一项确认。

## 先校正三个概念

你的理解接近了，但需要把“文档的职责”和“生命周期”分开看。

```text
需求/缺陷的一次具体工作  ──> Issue
完成这项工作的实施路线    ──> Plan
系统目前怎样设计          ──> System Design
某个重要选择为何这样做    ──> ADR
```

### Issue 不是“带生命周期的 Plan”

Issue 是一张**工作卡**。它可以是 bug、功能、调查、重构或文档任务，通常包含：当前状态、讨论、补充信息、验收条件、负责人和关闭原因。

Plan 是一份**如何完成某项工作**的实施路线，例如模块拆分、步骤、风险和验证命令。一个 Issue 可以：

- 没有 Plan：很小的修复可直接完成；
- 附带一个 Plan：复杂工作在开始前先做计划；
- 拆成多个 Issue：一个大目标需要多张可独立交付的工作卡；
- 引用一个 PRD：较大的需求先形成产品需求文档。

因此，Issue 和 Plan 是互补关系：Issue 管“这项工作从提出到关闭的全过程”，Plan 管“执行阶段准备怎样完成”。

你过去用 `active/` 与 `completed/` 管理 Plan 的生命周期，是合理的轻量做法；它并不错误。Issue 能补上的是 Plan 通常未保存的部分：讨论、缺失信息、分类、当前阻塞、可委托性、关闭原因和历史追踪。

### ADR 不是“带生命周期的 System Design”

System Design 描述**当前系统是什么样**：模块边界、依赖方向、规则边界、运行流程。

ADR（Architecture Decision Record，架构决策记录）记录**一次重要选择为什么这样做**：当时有哪些可选方案、选择了哪一个、后果是什么，以及它日后是否被替代。

例如：

| 文档 | 合适内容 |
| --- | --- |
| System Design | “数据读取层、快照层和页面层如何分层” |
| ADR | “为什么快照刷新采用两天缓冲与降级策略，而不改为每次实时查询” |

ADR 可以有 `Proposed → Accepted → Superseded/Deprecated` 等状态，但它的“生命周期”服务于**一条决定**，不是管理整个系统设计文档。系统设计应保持为当前可用的说明；旧的架构取舍和被替代原因由 ADR 保留。

### 更准确的结论

你不一定“忽略了设计与计划生命周期管理”。你已经用 `active/`、`completed/` 做了 Plan 的基本管理。

更准确地说，当前 Harness 缺少的是两类补充记录：

1. **工作项生命周期**：需求从提出、澄清、可执行、完成/拒绝的过程；Issue 适合承担。
2. **重大决策生命周期**：一个难逆架构选择何时被接受、何时被替代；ADR 适合承担。

不要用 Issue 替代所有 Plan，也不要把每一篇 System Design 都拆成 ADR。

## 当前仓库的起点

本次探索到的事实：

| 项目 | 当前状态 | 含义 |
| --- | --- | --- |
| `AGENTS.md` | 存在 | setup 将更新它，不会创建 `CLAUDE.md` |
| `CONTEXT.md` | 存在 | 适合选择 single-context（单上下文） |
| `CONTEXT-MAP.md` | 不存在 | 当前不是多上下文/多子域路由模式 |
| `docs/agents/` | 不存在 | setup 确认后会创建三份 Matt Skills 配置文件 |
| `.scratch/` | 不存在 | 正常；第一次发布 Local Markdown 工作项时再按需创建 |
| `docs/adr/` | 不存在 | 正常；真正有重要架构决定时再按需创建 |
| GitHub remote | 有 GitHub push 地址 | 不影响本次 Local Markdown 选择 |
| `gh` | 当前不可用 | 不影响本次 Local Markdown 选择 |

还有一项与 setup 无关、但应知晓的现状：根 `AGENTS.md` 仍指向若干不存在的 `references/plans/`、`references/design/` 路由；实际存在的是 `references/plan_references/`。同时，`references/plan_references/plans/index.md` 又指向不存在的 `references/plans/active/` 和 `references/plans/completed/`。这是旧 Harness 路径残留，需要单独维护；**setup 不会自动修复它**。

## 不需要事先创建什么

为了运行 setup，以下目录都**不需要**预先手工创建：

| 路径 | 是否需要预建 | 原因 |
| --- | --- | --- |
| `.scratch/` | 不需要 | Local Markdown 规则规定：第一次创建 PRD/Issue 时才建立 `.scratch/<feature>/` |
| `docs/agents/` | 不需要 | setup 在最终确认后创建其中的三份配置文件 |
| `docs/adr/` | 不需要 | ADR 应在有真实、重要、难逆的架构取舍时按需创建 |
| “domain 目录” | 不需要 | 本次使用现有根 `CONTEXT.md`；setup 会创建的是 `docs/agents/domain.md`，它是路由说明，不是业务领域目录 |

本次唯一需要的准备是：确认下面三项选择，并在写入前审阅草稿。

## `/setup-matt-pocock-skills` 实际会做什么

它不是安装器，也不是创建 Issue 或 ADR 的工具。它是“接线器”。

```text
它读取现有仓库
    ↓
向维护者确认三个选择
    ↓
展示要写入的配置草稿
    ↓
得到确认后，更新 AGENTS.md 并创建 docs/agents/*.md
```

完成后，`triage`、`to-prd`、`to-issues`、`diagnose`、`tdd`、`improve-codebase-architecture` 和 `zoom-out` 就知道：

- 工作项存在哪里；
- 状态名称是什么；
- 先读哪个 `CONTEXT.md`、在哪里寻找 ADR。

它不会：

- 自动把 `docs/dev_prompt/` 转换成 Issue；
- 自动新建 `.scratch/`、`docs/adr/`；
- 自动创建第一条 ADR；
- 自动修正现有 Harness 的旧路由；
- 自动编写或运行代码。

## 本次运行：每一步怎样回答

Skill 规定三个选择必须**一次一个**地确认。下文提前给出推荐答案，是为了让你理解并准备；实际对话中仍按顺序回答。

### 第一步：选择 Issue tracker

Skill 会问：项目的工作卡放在哪里？

本次已选 Local Markdown，建议回答：

```text
选择 Local Markdown。
原始需求继续放在 docs/dev_prompt/；新的 PRD 和可追踪工作项放在
.scratch/<feature>/ 下，并在工作项中链接对应的 docs/dev_prompt 文件。
```

这样，目录结构会在第一次真正创建工作项时演变为：

```text
docs/dev_prompt/
└── feat-Indicator_Improvement.md        # 原始需求材料

.scratch/
└── indicator-improvement/
    ├── PRD.md                            # 已澄清的整体需求（可选）
    └── issues/
        ├── 01-parse-and-compare-specs.md
        └── 02-render-tightening-report.md
```

`docs/dev_prompt/` 的作用是“需求源材料”；`.scratch/` 的作用是“这一次工作目前到什么状态”。二者相互链接，但不互相取代。

### 第二步：确认 triage 状态名称

Skill 会解释五个状态角色，并问项目是否要改名。Local Markdown 没有既有状态约定时，建议先采用默认名称，回答：

```text
使用默认状态名称，不做映射：
needs-triage、needs-info、ready-for-agent、ready-for-human、wontfix。
```

含义如下：

| 状态 | 人话解释 | 典型下一步 |
| --- | --- | --- |
| `needs-triage` | 新工作，尚未判断怎么处理 | 阅读需求、检查代码和上下文 |
| `needs-info` | 缺关键信息，不能靠猜测继续 | 向报告者提出具体问题 |
| `ready-for-agent` | 范围、验收条件、边界足够清楚 | Agent 可独立领取实施 |
| `ready-for-human` | 需要人工权限、审美判断、线下验证或业务拍板 | 交给相应人员 |
| `wontfix` | 已有稳定理由，不打算处理 | 记录原因并关闭 |

在 Local Markdown 模式中，这些不是 GitHub 标签，而是 Issue 文件顶部的状态字段，例如：

```markdown
# G3 亮点月周天良损异常放大

Category: bug
Status: needs-triage
Source: docs/dev_prompt/fix-yield_domain.md
```

补充：`bug` 和 `enhancement` 是工作类别，不是上述五个状态之一。一条工作通常有一个类别和一个状态。

### 第三步：确认领域文档布局

Skill 会问当前项目是单上下文，还是多个子域各自有上下文。

当前只有根 `CONTEXT.md`，因此建议回答：

```text
选择 single-context。
使用根目录 CONTEXT.md；ADR 放在 docs/adr/，但现在不预先创建，
等有真实架构决策时再创建。
```

这不会改变现有 `CONTEXT.md` 的内容，也不会立刻产生 ADR。

### 第四步：审阅草稿并确认写入

前三项确认后，Skill 必须展示以下草稿：

1. 加入根 `AGENTS.md` 的 `## Agent skills` 区块；
2. `docs/agents/issue-tracker.md`；
3. `docs/agents/triage-labels.md`；
4. `docs/agents/domain.md`。

对本次选择，审阅重点是：

- `AGENTS.md` 只增加一个 `## Agent skills` 区块，不修改周边 Harness 规则；
- Issue tracker 明确写 Local Markdown 和 `.scratch/`；
- 标签表使用五个默认状态名称；
- Domain 路由指向根 `CONTEXT.md` 与将来的 `docs/adr/`；
- 不把 `docs/dev_prompt/` 错写成 Issue tracker；
- 不擅自处理前文提到的旧 Harness 路径残留。

确认无误后，回答：

```text
草稿确认。请只按展示的内容更新 AGENTS.md 并创建 docs/agents 下的三份配置文件；
不要修改 references/、docs/dev_prompt/、CONTEXT.md，也不要创建 .scratch 或 docs/adr。
```

这句话把本次写入范围说得很清楚。

## 确认后将发生的文件变化

```text
AGENTS.md                              # 更新：增加/更新 ## Agent skills
docs/agents/issue-tracker.md           # 新建：Local Markdown 工作项约定
docs/agents/triage-labels.md           # 新建：五个状态名称映射
docs/agents/domain.md                  # 新建：CONTEXT.md / ADR 的读取规则
```

不会立即出现：

```text
.scratch/                              # 首条 PRD/Issue 发布时才建立
docs/adr/                              # 首条 ADR 被确认时才建立
```

## setup 完成后，如何真正创建第一条 Local Markdown Issue

以 `docs/dev_prompt/feat-Indicator_Improvement.md` 为例，建议不要把整份文件原样复制成一张长期 Issue。先通过 triage 澄清范围，再按可交付成果拆分。

第一张工作卡可以从最小问题开始：

```markdown
# 解析并比较指标规格版本

Category: enhancement
Status: needs-triage
Source: docs/dev_prompt/feat-Indicator_Improvement.md

## Current context

规格文件位于 resources/project_files/。监控规格并非都是纯数字，
需要先确定哪些表述可以稳定比较。

## Desired outcome

为同一产品的不同版本建立可重复的规格比较规则，输出收严项及其名称。

## Questions to resolve

- 哪些文本规格可以判定为“收严”？
- 无法解析的表述是否一律按“不收严”处理？
- 数据表和图像的验收格式是什么？

## Comments

```

随后调用 `/triage`：它会读取该文件、`CONTEXT.md` 和相关项目规则，判断是补信息还是可以变成 `ready-for-agent`。在信息完整前，不要急着创建详细 Plan。

## 如何从零开始使用 ADR

### 什么时候值得写 ADR

只有同时满足以下条件时才写：

1. **难逆**：以后改变它的成本明显；
2. **需要背景**：未来读者不会自然知道为什么这样选；
3. **确有取舍**：认真比较过可行替代方案。

以下通常不值得写 ADR：临时命名、一次性 bug 修复、显而易见的代码整理、当前没有做只是因为“没时间”。

### 第一条 ADR 的建议格式

当真实决定出现时，再创建：

```text
docs/adr/0001-<short-decision-name>.md
```

例如：

```text
docs/adr/0001-preserve-parquet-snapshot-degradation.md
```

内容应回答：

```markdown
# ADR-0001：保留 Parquet 快照的缓冲与降级策略

Status: Accepted
Date: 2026-07-13

## Context

页面查询依赖外部数据源；在数据源波动时需要兼顾性能、可用性和数据时效。

## Decision

保留现有 TTL、两天缓冲与分层降级策略；不改为每次页面访问都实时全量查询。

## Alternatives considered

1. 每次实时查询：时效最高，但性能和外部依赖风险更高。
2. 仅使用静态快照：稳定，但数据时效不足。

## Consequences

- 页面在外部查询异常时仍有可用的降级数据。
- 刷新逻辑必须继续保留缓冲与异常处理。
- 需要监控快照时效，避免长期使用过旧数据。
```

以后若决定被新方案替代，不必删除旧 ADR；把旧 ADR 的 `Status` 改为 `Superseded`，并链接新 ADR。这样历史取舍可追踪，System Design 则更新为当前真实结构。

## Local Markdown 与 GitHub Issues：本次选择的边界

你选择 Local Markdown，意味着这次 setup **不会建立 GitHub Issues**。它只会在项目中约定 `.scratch/` 文件是工作卡。

以后如果希望改用 GitHub Issues，需要重新运行 setup 并选择 GitHub；同时还需要：

1. GitHub 仓库有可用的 Issues；
2. 本机安装并认证 `gh`，或改由浏览器手动操作；
3. 在 GitHub 创建/确认状态标签；
4. 把 `docs/agents/issue-tracker.md` 改为 GitHub 操作规则。

这是一种 tracker 迁移，不是 Local Markdown 的必经步骤。先用本地文件学会 Issue 生命周期，再决定是否需要线上协作，是完全合理的路径。

## 本次实际执行清单

```text
[x] 读取 setup Skill 的完整流程和 Local Markdown 模板
[x] 检查当前 AGENTS.md、CONTEXT.md、远端、docs/agents、.scratch、docs/adr
[x] 明确选择 Local Markdown
[ ] 确认是否采用五个默认状态名称
[ ] 确认 single-context 布局
[ ] 审阅 setup 生成的四份草稿
[ ] 确认后写入 AGENTS.md 与 docs/agents/*.md
[ ] 未来按需创建第一条 .scratch Issue
[ ] 未来有真实架构取舍时按需创建第一条 ADR
```

下一步不是创建目录，而是完成 setup 的第二个确认问题：是否接受五个默认状态名称。
