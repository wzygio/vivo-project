# Task Plan: Skill Update Audit

## Goal

确认 codegraph-ast-grep、ecc-production-rules、development-flow 及其全部依赖 skill 的可信上游最新版，评估风险后安全更新并验证安装完整性。

## Current Phase

Complete

## Phases

### Phase 1: Inventory and dependency discovery

- [x] 枚举目标 skill、development-flow references 与全部直接依赖。
- [x] 记录当前版本、文件哈希、来源线索和本地定制。
- **Status:** complete

### Phase 2: Upstream research and risk assessment

- [x] 从可信上游确认最新版、发布日期或提交及变更内容。
- [x] 比较本地与上游差异，识别破坏性、高权限或来源风险。
- **Status:** complete

### Phase 3: Safe update

- [x] 备份本次将覆盖的 skill。
- [x] 仅更新通过风险门的目标；高风险目标留待用户判断。
- **Status:** complete

### Phase 4: Verification and delivery

- [x] 校验 SKILL.md、引用文件、脚本和依赖完整性。
- [x] 汇总已更新、已是最新版、未更新及回滚位置。
- **Status:** complete

## Key Questions

1. 每个本地 skill 的权威上游仓库和路径是什么？
2. development-flow 实际引用了哪些 skill，是否存在间接依赖需要纳入？
3. 上游最新版是否与 Codex、Windows 和当前 AGENTS.md 契约兼容？

## Decisions Made

| Decision | Rationale |
|---|---|
| 创建独立计划目录 | 原活动计划已经完成，避免覆盖其产物 |
| 更新前保留可恢复备份 | skill 更新会覆盖用户级全局文件，需要可回滚 |
| 外部网页内容只写入 findings.md | 遵守 planning-with-files 的外部内容安全边界 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| 首次并行读取输出发生截断 | 1 | 分开读取 skill 文件并使用明确 UTF-8 解码 |
| PowerShell 将 `foreach (...) { ... } | Format-Table` 解析为空管道 | 1 | 改为先赋值 `$results`，再单独格式化输出 |
| stark-agent-skills 首次完整检出触发 Windows 路径过长 | 1 | 保留已下载对象，改用新的 sparse clone 仅检出目标 skill |
| skill-installer 默认 ZIP 解压 Stark 仓库再次触发长路径 | 1 | 使用安装器支持的 `--method git` sparse checkout 通道重试 |
| planning-with-files 隐藏 `.agents` 路径的 auto 模式回退失败 | 1 | 使用明确的 `--method git` 安装通道重试 |
| planning-with-files Git 安装仍失败 | 2 | 发现安装器默认 `main`，仓库默认分支很可能不是 main；核实分支后显式传 `--ref` |
| 分支核实命令再次出现 foreach 直接管道 ParserError | 1 | 沿用 `$results` 中间变量写法，不再重复该语法 |
| 组合验证命令包含递归删除而被执行策略拒绝 | 1 | 先精确列出本次生成文件，再逐文件和空目录非递归清理；将验证命令拆开 |
| 非递归 Remove-Item 仍被执行策略拒绝 | 2 | 不删除任何文件；把含验证产物的目录移入备份，并从已验证暂存副本重新铺设干净目录 |
| Git Bash 不在已知系统路径，无法执行 `sh -n` | 1 | 保留 Shell 语法为未验证项；PowerShell/Python 与上游哈希验证继续执行 |
| 完成状态补丁首次因上下文匹配失败 | 1 | 重新读取当前 plan 后以更小上下文补丁更新 |
