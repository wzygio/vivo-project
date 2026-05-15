# 企业级 Spec 框架体系设计方案

您好！作为大厂的 AI 编程工程师，我非常赞同您的观点：**一个真正复杂的企业级项目，其规范（Spec）绝对不应该全部塞在一个文件里。**

将所有内容堆砌在 `.roorules` 或单个 `project_spec.md` 中会导致：
1. **上下文超载 (Context Overflow)**：每次 LLM 交互都会带上大量不相关的规则，浪费 Token 且容易让 AI 产生幻觉。
2. **维护困难**：前端在看规范时，不需要看后端的数据库设计；新功能开发时，不需要重温整个架构的演进史。
3. **隔离性差**：多智能体（Multi-Agent）协作时，不同角色的 Agent 需要的 Context 是完全不同的。

因此，我们需要将大一统的规范拆解为 **"一主多从"（Hub-and-Spoke）** 的网状知识体系。

---

## 🎯 拟定的企业级 Spec 框架结构 (在 `spec/` 目录下)

我们将 `.roorules` 升级为一个真正的知识库系统：

```text
vivo-project/
├── .roorules                     # [入口文件] 仅保留极简的"路由"和最核心的红线警告，引导 AI 去读 spec/
├── spec/
│   ├── 00_project_hub.md         # 🟢 [总控中枢] 项目全景图、术语表、模块指引 (原 Meta-Context)
│   ├── 01_architecture_core.md   # 🔵 [架构核心] 目录结构约定、DDD分层说明 (原 Architecture Blueprint)
│   ├── 02_infrastructure.md      # 🟡 [基础设施] 配置管理、日志架构、缓存降级、数据库连接 (原 Core Infrastructure Patterns)
│   ├── 03_domain_rules.md        # 🟠 [业务领域] Yield 和 SPC 的核心业务规则、边界与防呆机制 (原 Negative Constraints & Scope)
│   ├── 04_agent_workflow.md      # 🟣 [AI协作] EPCC Flow、测试纪律、命令集 (原 多 Agent 协作)
│   └── roadmap.md                # ⚪ [演进路线] IN Scope 与 OUT of Scope 规划
```

### 为什么这样拆分？（渐进式揭示 Progressive Disclosure）

当 AI 拿到一个任务时，它的逻辑链路应该是：
1. **自动读取 `.roorules`** -> 知道自己是个 Backend Architect，且必须先看 `spec/00_project_hub.md`。
2. **读 `00_project_hub.md`** -> 了解项目是做什么的，以及各个子模块在哪。
3. **按需读取 (Lazy Loading)**：
   - 如果是修 Bug（比如这次的加密 xlsx 无法读取问题），它会去读 `02_infrastructure.md`（文件处理逻辑）。
   - 如果是改 SPC 计算逻辑，它会去读 `03_domain_rules.md`（业务防呆边界）。
   - 如果是改目录结构，它会看 `01_architecture_core.md`。

---

## 🛠️ 下一步行动计划

如果您同意这个分层架构，我将执行以下操作：

1. **清空旧结构**：基于 `spec_template.md` 和 `.roorules`，在 `spec/` 目录下创建上述的 6 个独立的 markdown 文件。
2. **内容迁移与重组**：将目前臃肿的 `.roorules` 内容，拆解并提纯到对应的文件里。
3. **精简 `.roorules`**：将其改写为一个"导流引物"，里面只留最重要的全局纪律和文件跳转链接（例如 `[架构规范请见](spec/01_architecture_core.md)`）。

这个方案您觉得如何？我们是否可以直接开始文件的拆分与写入？