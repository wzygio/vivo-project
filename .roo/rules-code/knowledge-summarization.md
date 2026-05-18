# 📚 知识归纳流程（Code 模式专属）

> **最高优先级**：每次 Coding 完成后，在调用 `attempt_completion` 之前，**必须**执行本流程。

---

## 1. 触发条件

以下条件**全部满足**时触发：

| 条件 | 说明 |
|------|------|
| ✅ 代码修改完成 | 所有源文件已创建/修改完毕 |
| ✅ 单元测试 100% PASS | `uv run pytest tests/unit/ -v --tb=short` |
| ✅ 集成测试 100% PASS | `uv run pytest tests/integration/ -v --tb=short` |
| ✅ 无语法/类型错误 | `uv run pyright` 或等效检查通过 |

> **例外**：如果本次仅调整文档（`.md` 文件）或配置文件（`.yaml` / `.json`），可跳过本流程。

---

## 2. Step 1：架构变更检测

检查本次修改是否涉及以下**可记录到 `ARCHITECTURE.md` / `docs/design/`** 的变更：

| 检测项 | 判断标准 |
|--------|----------|
| 新领域 | 是否创建了新的 `src/xxx_domain/` 包？ |
| 新服务 | 是否新增了 `application/xxx_service.py`？ |
| 新数据流 | 是否新增了 DAO 层或外部数据源连接？ |
| 新缓存策略 | 是否引入了新的 `@st.cache_data` 模式？ |
| 新配置项 | 是否新增了 config 字段或 YAML 配置？ |
| 新测试模式 | 是否使用了未记录的 fixtures / factories？ |

**无变更** → 跳到 Step 2
**有变更** → 在 Step 3 生成架构提案（`ARCHITECTURE.md` / `docs/design/`）

---

## 3. Step 2：新解决方案检测

检查本次是否使用了**可记录到 `skills/`** 的新技术方案：

| 检测项 | 判断标准 |
|--------|----------|
| 新技术 | 是否使用了未在 `skills/README.md` 中列出的库或工具？ |
| 新模式 | 是否使用了未记录的设计模式 / 算法策略？ |
| 新故障处理 | 是否遇到了新的故障类型并找到了解决方案？ |
| 新集成方式 | 是否使用了新的外部系统集成方式？ |

**无变更** → 跳到 Step 4
**有变更** → 在 Step 3 生成 `skill` 提案

---

## 4. Step 3：生成提案文件

### 4.1 架构变更提案（格式）

```
文件名：docs/plans/spec_知识提案_YYYYMMDD.md
```

内容模板：
```markdown
# 知识提案：架构变更 · YYYY-MM-DD

## 变更摘要
（一句话描述本次变更）

## 涉及文件清单
- [`路径`](路径): 变更说明

## 建议更新的目标文件与插入位置

根据变更类型，选择以下目标之一：

### A. 系统级架构变更 → 更新 [ARCHITECTURE.md](ARCHITECTURE.md)
#### 新增领域 / 服务 / 数据流描述
（具体内容）

#### 建议插入位置
（指明在 `ARCHITECTURE.md` 中的位置，如"数据流架构"或"领域模块划分"章节）

### B. 领域级业务变更 → 更新 [docs/design/](docs/design/) 下对应的设计文档
#### 新增领域 / 服务 / 数据流描述
（具体内容）

#### 建议插入位置
（指明在 `docs/design/` 中哪个文件及章节，如 `docs/design/<domain_name>.md` 的"核心算法"章节）

## 回滚指南
（如果用户不采纳，如何回退）
```

### 4.2 新解决方案提案（格式）

```
文件名：docs/plans/skill_提案_XXX.md
```

内容模板（参考 `skills/templates/skill-template.md`）：
```markdown
# Skill 提案：{问题标题}

## 问题描述
## 根因分析
## 解决方案（含关键代码片段）
## 验证方法
## 建议插入 skills/README.md 的位置
```

---

## 5. Step 4：在 attempt_completion 中告知用户

在 `attempt_completion` 的 `result` 中，**必须包含以下信息**：

```markdown
## 📋 知识归纳摘要

| 类型 | 状态 | 提案文件 |
|------|------|----------|
| 架构变更 | ✅ 有变更 / ❌ 无变更 | [`docs/plans/spec_知识提案_YYYYMMDD.md`](docs/plans/spec_知识提案_YYYYMMDD.md) |
| 新解决方案 | ✅ 有变更 / ❌ 无变更 | [`docs/plans/skill_提案_XXX.md`](docs/plans/skill_提案_XXX.md) |

> 请审阅上述提案文件。如同意采纳，我将把内容合并到 `ARCHITECTURE.md` / `docs/design/` 或 `skills/` 中。
```

---

## 6. 用户决策后的操作

| 用户反馈 | 操作 |
|----------|------|
| "采纳" | 将提案内容合并到 `ARCHITECTURE.md` / `docs/design/` 对应文件，或 `skills/README.md` + 新建 skill 文件 |
| "拒绝" | 删除 `docs/plans/` 下的提案文件 |
| "修改后采纳" | 按用户要求修改后合并 |

---

## 7. 原则

1. **提案 ≠ 正式文档**：提案是临时文件，只有经用户确认后才会写入正式文档
2. **不污染正式文档**：未经用户许可，绝不直接修改 `ARCHITECTURE.md`、`docs/design/` 或 `skills/` 下的文件
3. **可追溯**：提案文件以日期命名，可随时删除
4. **零冗余**：如果检测无变更，在 attempt_completion 中明确说明"本次无架构变更/新解决方案需记录"
