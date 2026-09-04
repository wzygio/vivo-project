# Findings: Skill Update Audit

## Requirements

- 检查并更新 `codegraph-ast-grep`。
- 检查并更新 `ecc-production-rules`。
- 检查并更新 `development-flow` 中涉及的所有 skill。
- 搜索并理解最新版；若发现巨大风险则不更新，交由用户判断。
- 用户级 skill 目标目录固定为 `C:\Users\V0141351\.agents\skills`。

## Local Findings

- `codegraph-ast-grep` 本地 metadata 版本为 `0.1.2`。
- `ecc-production-rules` 的 SKILL.md 未声明版本。
- `development-flow` 的 SKILL.md 未声明版本；其四个模块依赖需要从 references 中枚举。
- `development-flow` 的直接 skill 依赖为 `create-local-markdown-issue`、`triage`、`planning-with-files`、`tdd`、`playwright-cli`。
- `coding_spec` 是仓库文档目录，不是 skill；不属于本次 skill 更新集合。
- 原活动计划 `2026-09-03-aoi-tt-particle-size` 已完成，因此本次使用独立计划目录。
- `create-local-markdown-issue` 是面向本仓库 Local Markdown tracker 的定制流程，没有版本 metadata。
- `triage` 与 `tdd` 的术语、文档结构和 Matt Skills 路由一致，候选上游为 Matt Pocock 的技能仓库，需进一步核验精确路径。
- `playwright-cli` 是完整 CLI 使用手册并包含 9 个 references；候选上游应以 Microsoft Playwright CLI 仓库的 agent skill 为准。
- `mattpocock/skills` 当前树中确认存在 `skills/engineering/triage` 与 `skills/engineering/tdd`，与本地目录结构匹配。
- Microsoft Playwright 当前权威路径是 `packages/playwright-core/src/tools/skills/playwright-cli`，包含 SKILL.md 和 9 个 references。
- `OthmanAdi/planning-with-files` 对 Codex 提供 `.codex/skills/planning-with-files`，对跨客户端标准安装提供 `.agents/skills/planning-with-files`；本地内容需与后者/前者做宿主差异比较。
- `development-flow` 是 2026-07-15 按用户专门需求用官方 skill-creator 创建的本地 skill，之后又按本项目工作方式演进；没有公共上游可定义另一个“最新版”。
- `create-local-markdown-issue` 同样是本地 Local Markdown tracker 适配，无权威公共同名来源。

## Research Findings

- `codegraph-ast-grep` 的权威公开来源已定位为 `stark-ai-de/agent-skills` 下 `skills/engineering-workflows/codegraph-ast-grep`。
- 上游当前 SKILL.md 与本地 0.1.2 已有实质变化：公开工作流收敛为 `setup/update/doctor`，普通代码探索不再触发该 skill；新增遥测关闭、来源保持、迁移/回滚与实验 ast-grep MCP 排除策略。
- `planning-with-files` 搜索线索与本地正文一致指向 `OthmanAdi/planning-with-files`；仍需用 GitHub API/仓库文件确认当前 tag 和内容。
- 精确检索没有找到 `development-flow`、`create-local-markdown-issue` 的公共权威副本；它们很可能是本地定制，需要结合本仓库历史确认。
- `ecc-production-rules` 内容来自 Everything Claude Code 规则体系，但本地 SKILL.md 是 Codex 渐进式路由适配层，不能直接用上游整个仓库覆盖。
- `planning-with-files` 最新权威版本为 3.16.0（本地 3.1.0）。3.12-3.15 包含 session-history 默认禁读、路径/selector 防逃逸、错误 selector 不再回退到其他 plan、Windows resolver 修复等安全更新；其 hooks 仍会自动运行，但风险较本地旧版下降。
- `codegraph-ast-grep` 最新 metadata 版本为 0.3.3（本地 0.1.2），新版本将公开操作收敛为 setup/update/doctor，并默认关闭 skill 驱动的 CodeGraph telemetry；属于安全和作用域改进。
- `playwright-cli` 最新版本相对本地只增加 `recording-start/recording-stop` 说明，并修正 trace 输出目录为 `.playwright-cli/traces/`，风险低。
- `triage` 最新版新增 PR triage、redundancy/prior-rejection 检查、`disable-model-invocation`，并依赖尚未安装的 `grilling` 与 `domain-modeling`。
- `tdd` 最新版引入“预先确认测试 seam”，删除本地 3 份设计/重构参考，并把重构移出 red-green loop、转交 `code-review`；还依赖尚未安装的 `codebase-design`。这与当前 development-flow 的 GREEN 后重构步骤存在直接契约冲突。

## Technical Decisions

| Decision | Rationale |
|---|---|
| 先比较后覆盖 | 安装器默认在目录已存在时中止，且无法识别本地定制 |
| 只纳入 development-flow 的直接依赖 | 用户所述“中涉及”对应其 SKILL.md 和四个 references 明确调用的 skills；无限递归依赖会无边界扩张 |
| Codegraph 改用 sparse clone | 完整仓库包含 Windows 超长路径，但目标 skill 路径正常；无需修改全局 Git 配置 |
| 暂不把新 Matt 依赖加入更新集合 | 最新 triage/tdd 的依赖与执行模型会改变用户日常开发流，应按“巨大风险交用户判断”处理，而非静默扩展 |
| 备份后替换 4 个低风险目标 | 更新内容已在隔离目录与权威上游逐文件核对，可通过完整目录备份回滚 |

## Final Status

| Skill | Result | Upstream |
|---|---|---|
| codegraph-ast-grep | Updated `0.1.2` → `0.3.3` | stark-ai-de/agent-skills `284d268` |
| ecc-production-rules | Updated embedded 122-file rules snapshot; adapter retained | affaan-m/ECC `e04ea0b` |
| planning-with-files | Updated `3.1.0` → `3.16.0` | OthmanAdi/planning-with-files `03128b2` |
| playwright-cli | Updated 2 changed docs files | microsoft/playwright `d1dcd6b` |
| development-flow | No public upstream; current local skill is the project-owned latest | local skill-creator history |
| create-local-markdown-issue | No public upstream; current local tracker adapter retained | local |
| triage | Not updated: breaking dependency/workflow change requires user decision | mattpocock/skills `6654f6b` |
| tdd | Not updated: seam approval/refactor ownership/dependency changes require user decision | mattpocock/skills `6654f6b` |

## Verification

- Updated CodeGraph, planning, Playwright and ECC rule directories match the pinned upstream sources byte-for-byte.
- `quick_validate.py` passes for CodeGraph, ECC, Playwright and all retained development-flow skills.
- Planning 3.16.0: 1 Python and 8 PowerShell scripts parse successfully; resolver selects this plan and completion check behaves correctly.
- Shell syntax check was unavailable because Git Bash/`sh` is not installed at the known paths; the installed shell files are byte-identical to upstream 3.16.0.
- Five link-scan findings are upstream placeholders or links to full-repository docs not bundled in the standalone skill, not runtime dependencies.
- Installed `playwright-cli` binary is 0.1.17 and does not advertise the new optional recording commands. Its process also reports `NODE_TLS_REJECT_UNAUTHORIZED=0`; neither binary nor environment was changed in this task.
- Rollback root: `C:\Users\V0141351\.agents\skill-backups\20260904-182500`.

## Resources

- `C:\Users\V0141351\.agents\skills`
- https://github.com/stark-ai-de/agent-skills/tree/main/skills/engineering-workflows/codegraph-ast-grep
- https://github.com/OthmanAdi/planning-with-files
- https://github.com/affaan-m/everything-claude-code
