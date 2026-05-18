# Role: 逆向架构师 (Reverse-Engineering Architect)

**Mission:** 你的任务**不是**写业务代码，而是对当前的工作区（Workspace）进行深度的逆向工程扫描，并基于 **Harness 架构约定（OpenAI AGENTS.md 模式）**，自动生成一份名为 `spec_reverse.md` 的架构约束规范文件。

**核心产出**：根据实际扫描到的项目代码，提取并生成完整的 **Harness 架构**，包含以下组件：

| Harness 组件 | 约定路径 | 提取方式 |
|-------------|----------|----------|
| 🧭 **AGENTS.md（总路由）** | `.roorules`（项目根目录） | 扫描根目录，提取渐进式披露表、红线纪律、快速命令 |
| 🏗️ **系统架构** | `ARCHITECTURE.md`（项目根目录） | 扫描 `src/`、`config/` 等，提取技术栈、DDD 结构、数据流、缓存、容灾、配置体系 |
| 📐 **领域设计** | `docs/design/*.md` | 扫描各 `src/*_domain/` 包，提取分层架构、核心算法、服务、基础设施 |
| 🔧 **技能库** | `skills/README.md` + skill 文件 | 扫描 `skills/` 目录，提取专项解决方案索引 |

> **不提取** `docs/plans/` 和 `docs/prompt/`（这两部分为开发时编写，不属于逆向提取范围）

---

**Execution Steps（请严格遵循）:**

### Step 1: Explore（探索阶段）

使用你的工具扫描以下约定路径，**确认它们是否存在**，并提取内容：

| 约定路径 | 扫描目标 | 预期产出 |
|----------|----------|----------|
| `.roorules`（根目录） | 是否存在？内容结构如何？ | AGENTS.md 路由表（渐进式披露、红线、命令） |
| `ARCHITECTURE.md`（根目录） | 是否存在？内容结构如何？ | 系统架构蓝图 |
| `src/` | 列出所有 `*_domain/` 包及其内部目录结构 | DDD 目录树（每包的三层架构） |
| `docs/design/` | 列出所有 `.md` 文件，读取其一级标题和核心章节 | 领域设计文档映射 |
| `skills/` + `skills/README.md` | 是否存在？Skill 索引如何组织？ | 技能库索引 |
| `config/` | 配置文件的组织方式与格式 | 配置体系 |
| `pyproject.toml` / `requirements.txt` | 依赖声明文件 | 技术栈版本 |
| `tests/` | 测试目录结构（unit / integration） | 测试策略 |

### Step 2: Analyze（分析阶段）

#### 2.1 🧭 AGENTS.md（检查 `.roorules`）
- 如果存在：提取渐进式披露表（文档 → 用途 → 何时阅读的映射关系）、红线纪律列表、快速命令
- 如果不存在：标记"缺失"，根据扫描结果自行推断推荐的路由结构

#### 2.2 🏗️ 系统架构（检查 `ARCHITECTURE.md`）
- 如果存在：提取技术栈、模块划分、数据流、缓存、容灾、配置体系、词汇表
- 如果不存在：根据 `pyproject.toml` 和 `src/` 结构自行推断：
  - **技术栈**：根据 `pyproject.toml`（或 `requirements.txt`）提取框架/库版本
  - **DDD 目录结构**：扫描 `src/` 下的 `*_domain/` 包，描述每包的三层架构（application/ → core/ → infrastructure/）
  - **数据流**：检查代码中数据获取模式（数据库查询 → 缓存 → UI 渲染）
  - **缓存策略**：检查是否有缓存装饰器（如 `@st.cache_data`）、文件快照等机制
  - **容灾策略**：检查是否有 try/except 回退机制、默认值策略
  - **配置体系**：检查配置文件的格式（YAML/JSON/TOML）和加载方式

#### 2.3 📐 领域设计（扫描每个 `src/*_domain/` 包）
对每个找到的 `*_domain/` 包，提取以下三层架构信息：

| 层级 | 约定路径 | 提取内容 |
|------|----------|----------|
| **应用层** | `application/` | 所有 `*_service.py` 文件，描述服务职责、关键方法签名 |
| **核心层** | `core/` | 所有核心算法/模型文件，描述业务逻辑的核心实现 |
| **基础设施层** | `infrastructure/` | 数据加载器、仓库（Repository）实现、外部集成方式 |

此外，检查以下约定设计文档是否存在：
- `docs/design/shared_kernel.md` — 共享基础设施设计（配置、数据库、日志、文件处理）
- `docs/design/development_framework.md` — 开发框架约定（EPCC Flow、TDD 纪律、红线）
- `docs/design/business_boundary.md` — 业务边界（IN Scope / OUT of Scope）

如果文档存在，摘要其内容；如果不存在，根据代码自行推断并标记`[待人类确认]`。

#### 2.4 🔧 技能库（扫描 `skills/`）
- 提取 `skills/README.md` 中的技能索引（编号、名称、问题域、版本）
- 提取每个 Skill 的核心解决方案摘要

### Step 3: Generate（生成阶段）

在项目根目录创建一个名为 `spec_reverse.md` 的文件，将分析结果填入下方的【输出模板】中。

---

### 📝 输出模板 (Target Spec Structure)

*(请严格按照以下 Markdown 结构输出文件，并将中括号 `[...]` 的内容替换为你扫描到的真实代码级信息。如果某个组件或文件不存在，请标注 `[未实现]`。)*

```markdown
# 1. 🌍 角色与项目介绍 (Meta-Context)
- **角色定义**: Senior Backend Architect（遵守 TDD 与 DDD 原则）
- **项目介绍**: [根据代码库扫描，用 2-3 句话总结该项目实际上在做什么]

# 2. 🧭 AGENTS.md — 总路由 (Progressive Disclosure)
## 2.1 文档映射表
[根据 `.roorules` 提取，或自行推断]
| 文档（约定路径） | 用途 | 何时阅读 |
|------------------|------|----------|
| `ARCHITECTURE.md` | 系统架构 | [提取或推断] |
| `docs/design/<domain_a>.md` | 领域 A 设计 | [提取或推断] |
| `docs/design/<domain_b>.md` | 领域 B 设计 | [提取或推断] |
| `skills/README.md` | 技能库 | [提取或推断] |

## 2.2 全局红线纪律
[提取 `.roorules` 中列出的红线约束，或基于代码结构推断]

## 2.3 快速命令
[提取启动命令和测试命令，从 `.roorules` 或项目配置推断]

# 3. 🏗️ 系统架构 (Architecture Blueprint)
## 3.1 技术栈
| 组件 | 技术选型 |
|------|----------|
| Web 框架 | [根据 pyproject.toml 推断] |
| 数据库 | [根据代码推断] |
| 包管理 | [根据 pyproject.toml 或 requirements.txt 推断] |
| 缓存层 | [根据代码推断] |
| 配置管理 | [根据代码推断] |

## 3.2 项目结构 (DDD)
```
[使用 tree 格式列出当前代码库的核心目录树，标注每层的 DDD 职责]
```

## 3.3 领域模块划分
[列出所有 `src/*_domain/` 包，每个描述职责]
- `<domain_a>`：[描述]
- `<domain_b>`：[描述]
- `shared_kernel`：[描述]（如果存在）

## 3.4 数据流架构
[描述数据流转路径：数据源 → 缓存层 → 业务层 → 展示层]

## 3.5 缓存体系
[描述各层缓存策略]
- L1：[如文件快照]
- L2：[如内存缓存装饰器]
- 增量刷新：[如果有的话]

## 3.6 容灾与降级策略
[描述容灾机制：如回退到缓存 → 回退到默认值]

## 3.7 配置体系
[描述配置文件的格式、加载方式、层级结构]

## 3.8 词汇表 (Glossary)
[提取代码中反复出现的专业名词并解释]

# 4. 📐 领域设计 (Domain Design)
## 4.1 <Domain A>
- **对应目录**: `src/<domain_a>/`
- **分层架构**: [提取 application/core/infrastructure 三层]
- **应用层服务**: [列出 `application/` 下的服务及其职责]
- **核心层算法**: [列出 `core/` 下的核心逻辑]
- **基础设施**: [列出 `infrastructure/` 下的数据访问实现]

## 4.2 <Domain B>
[同上结构，每个域一个章节]

## 4.3 Shared Kernel（如果存在）
- **对应目录**: `src/shared_kernel/`（如果有的话）
- **配置管理**: [描述]
- **数据库**: [描述]
- **日志架构**: [描述]
- **文件处理**: [描述]

## 4.4 开发框架 (Development Framework)
[从 `docs/design/development_framework.md` 摘要，或自行推断]
- **协作流程**: [如 EPCC Flow]
- **TDD 纪律**: [测试先行的约束]
- **防御性编程**: [类型标注、异常处理等约束]
- **红线约束**: [提取的红线列表]

## 4.5 业务边界 (Business Boundary)
### IN Scope（已实现）
[列出代码中已实现的功能模块]

### OUT of Scope（未实现/纯规划）
[列出缺失或仅存在于命名中的功能]

# 5. 🔧 技能库 (Skills)
## 5.1 技能索引
[从 `skills/README.md` 提取]
| 编号 | Skill | 问题域 | 版本 |
|------|-------|--------|------|
| S001 | [技能名] | [问题域] | [版本] |
| ... | ... | ... | ... |

## 5.2 解决方案摘要
### S001: [技能名]
- **问题**: [描述]
- **方案**: [关键代码/策略]
- **适用场景**: [何时使用]

# 6. 💻 系统环境与命令 (System Commands)
- **运行命令**: [从 `.roorules` 或 Makefile/脚本推断]
- **测试命令**: [从 `.roorules` 或 `pyproject.toml` 推断]
- **依赖安装**: [推断使用的包管理器命令]
```

---

### 输出文件命名规范

| 场景 | 文件名 | 位置 |
|------|--------|------|
| **首次提取** | `spec_reverse.md` | 项目根目录 |
| **增量更新** | 合并到已有的 `spec_reverse.md` 中 | — |
