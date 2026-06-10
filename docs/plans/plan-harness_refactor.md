# 计划：Harness 架构重构

## 目标

将当前项目整理为轻量 Codex Harness：根级路由清晰、设计和计划可索引、生成事实可重建、可维护规则有独立入口。

## 涉及文件清单

- `AGENTS.md`: 补充 Harness 入口并修正渐进式披露路由。
- `.roorules`: 与根级 Harness 路由保持一致，避免指向缺失文档。
- `CONTEXT.md`: 新增项目上下文和边界速查。
- `docs/design/index.md`: 设计文档入口。
- `docs/plans/index.md`: 计划入口。
- `docs/references/README.md`: 外部参考入口。
- `docs/generated/README.md`: 可重建生成事实入口。
- `docs/generated/harness-audit.md`: Harness 审计结果。
- `specs/README.md`: 可维护规则和任务契约入口。

## 执行步骤

1. [x] 读取 `harness-builder` skill 并运行审计。
2. [x] 使用 repair 模式生成缺失索引层。
3. [x] 项目化 Harness 文档内容和根级路由。
4. [x] 验证引用路径和审计结果。

## 验收标准

- `harness-builder` audit 不再报告缺失 Harness 根目录。
- 新增/修改文档中的本地引用均存在，除非明确标记为未来路径。
- 不修改业务核心算法、数据库单例、缓存或 Parquet 刷新逻辑。

## 回滚指南

删除本计划新增的 Harness 文档，并恢复 `AGENTS.md` / `.roorules` 中本次新增的 Harness 路由段即可回退。
