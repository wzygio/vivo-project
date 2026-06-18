# 开发框架规范

> **最后更新**: 2026-05-18

---

## 1. 多 Agent 协作规范 (EPCC Flow)

此部分为硬性纪律，所有 AI Agent 必须遵守：

### 1.1 Explore（探索）
必须先阅读相关文件，读懂上下文后再行动。不得在不了解现有代码的情况下直接修改。

### 1.2 Plan（计划）
必须先输出修改计划，交由人类审核。计划通过后，方可进入编码阶段。

**入口文件**：参考 [`PLANS.md`](../plans/PLANS.md) 了解当前活跃计划。

### 1.3 Code（编码）
- 必须包含 **Type Hints** 和基础异常捕获
- **结构化输出**：明确指出修改了哪个文件的哪几行
- **🚨 熔断机制**：同一个 Bug 连续修复 3 次失败，必须立即停止并要求人类介入

### 1.4 Commit（提交）
遵循 TDD 纪律（见下文）。

---

## 2. TDD 纪律

- **必须先写测试**，再写实现代码
- **✅ 验收标准**：`uv run pytest tests/ -v --tb=short` 必须达到 **100% PASS** 才算完成

### 测试结构

```
tests/
├── conftest.py          # Pytest Fixtures
├── factories.py         # 测试数据工厂
├── unit/                # 单元测试
└── integration/         # 集成测试
    ├── test_config.py
    ├── test_equipment_parts_db.py
    ├── test_mapping_flow.py
    ├── test_new_product_flow.py
    ├── test_spc_db.py
    └── test_spc_table.py
```

---

## 3. 防御性编程

- 所有函数必须包含 **完整 Type Hints**
- 所有数据库/文件/I/O 操作必须包含 **try-except 异常捕获**
- 所有外部输入必须做 **类型校验** 和 **边界检查**
- 优先使用 `Optional` / `Union` 类型标注，避免裸 `None`

---

## 4. 红线纪律（负面清单）

以下为 **硬性约束**，任何时候**不得违反**：

### 🚫 禁止静态重构已有核心逻辑
- `_simulate_concentration` 和 `_distribute_sheet_from_lot`（`sheet_lot_processor.py`）
- Mapping 级联衰减算法（`mapping_processor.py`）

### 🚫 禁止修改数据库连接单例模式
- `DatabaseManager.__new__` 单例 + 失败重试

### 🚫 禁止消除 `@st.cache_data`
- L2 缓存移除将导致全量查询的性能灾难

### 🚫 禁止简化 Parquet 快照增量更新
- TTL 保护 + 2 天缓冲 + 三防线容灾的降级策略

### 🚨 Agent 熔断机制
- 同一个 Bug 连续修复 3 次失败，必须立即停止并要求人类介入

---

> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`yield_domain.md`](./yield_domain.md) · [`spc_domain.md`](./spc_domain.md)
