# 📚 Skills 技能库

> **用途**：项目级技能索引。所有技能的完整内容已迁移到 **Roo Code 全局 Skills Store**（`C:\Users\V0141351\.roo\skills\`），AI Agent 会在需要时自动匹配加载。  
> **原则**：每个 Skill 聚焦 **一个具体问题**，包含根因分析 + 解决方案 + 关键代码片段。

---

## Skills 索引

| 编号 | Skill（全局路径） | 问题域 | 版本 | 最后更新 |
|------|------------------|--------|------|----------|
| S001 | [加密 Excel 的 COM 透明解密](file:///C:/Users/V0141351/.roo/skills/encrypted-xlsx-com-read/SKILL.md) | 文件处理 | 1.0 | 2026-05-14 |
| S002 | [SPC 步骤 ID 类型标准化匹配](file:///C:/Users/V0141351/.roo/skills/type-normalization-step-matching/SKILL.md) | 类型系统 | 1.0 | 2026-05-14 |
| S003 | [帆软报表数据爬虫](file:///C:/Users/V0141351/.roo/skills/finereport-crawler/SKILL.md) | Python 爬虫 | 1.0 | 2026-05-14 |

> **💡 提示**：上述技能已在 Roo Code 全局 Skills Store 中注册。当 AI 遇到匹配的问题描述时，会自动加载对应 SKILL.md。

---

## 本地模板

[`templates/skill-template.md`](templates/skill-template.md) — 新建 Skill 的模板文件，仍保留在项目中供参考。

---

## 如何贡献新的 Skill

1. 复制 [`templates/skill-template.md`](templates/skill-template.md) 到 `C:\Users\V0141351\.roo\skills\<skill-name>\` 目录
2. 在文件头部添加 YAML frontmatter：
   ```yaml
   ---
   name: <skill-name>
   description: <当出现此问题时，应加载该 Skill>
   ---
   ```
3. 按模板填写问题描述、根因分析、解决方案和关键代码
4. 在本索引表中添加新条目

---

> **版本**: 2.0  
> **最后更新**: 2026-05-16  
> **变更说明**: 技能内容已全部迁移至全局 Skills Store，本文件仅保留索引功能
