# 计划总览

> **最后更新**: 2026-05-18  
> **查看活跃计划前，请先阅读本文件获取全景认知。**

---

## 1. 活跃计划 (Active)

| 计划 | 状态 | 负责人 | 说明 |
|------|------|--------|------|
| *暂无活跃计划* | — | — | — |

> 当前无活跃执行计划。如需要启动新计划，请遵循 EPCC Flow（Explore → Plan → Code → Commit）流程，先创建计划文档再执行。

---

## 2. 已完成计划 (Completed)

| 计划文件 | 完成日期 | 说明 |
|----------|----------|------|
| [`spec_architecture_plan.md`](./spec_architecture_plan.md) | 历史 | 架构规划与系统设计 |
| [`spec_关键备件报表.md`](./spec_关键备件报表.md) | 历史 | 关键备件报表模块开发计划 |

---

## 3. 计划模板

新计划请参考以下模板：

```markdown
# 计划：[计划名称]

## 目标
（一句话描述本次计划要达成的目标）

## 涉及文件清单
- `路径`: 变更说明

## 执行步骤
1. [ ] 步骤一
2. [ ] 步骤二
3. [ ] 步骤三

## 验收标准
- `uv run pytest tests/ -v --tb=short` 100% PASS
- 代码审查通过

## 回滚指南
（如果计划失败，如何安全回退）
```

---

## 4. 相关指引

- **系统架构**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md)
- **业务设计**: [`docs/design/`](../design/)
- **开发规范**: [`docs/design/development_framework.md`](../design/development_framework.md)
- **技能库**: [`skills/README.md`](../../skills/README.md)
