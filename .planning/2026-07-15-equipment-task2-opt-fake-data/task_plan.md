# Equipment Task2-opt Data Fabrication

## Goal

将 `feat-equipment_domain.md` 的 Task2-opt 转化为可执行、可验收的 Issue，并按测试优先方式生成一份能被设备域真实读取、通过设备烟测的仿造数据集。

## Current Phase

Complete

## Phases

### Phase 1: 需求与数据契约发现

- [x] 精确提取并解释 Task2-opt。
- [x] 阅读设备域架构、术语、数据加载流程、现有快照和测试入口。
- [x] 识别仿造数据必须满足的 schema、关联关系、时间范围与业务约束。
- **Status:** completed

### Phase 2: Issue 创建与 Triage

- [x] 按本地 Markdown tracker 规则创建初始 `needs-triage` Issue。
- [x] 补齐验收标准、边界、风险、测试与 Agent Brief。
- [x] 通过 triage 状态机推进至 `ready-for-agent`。
- **Status:** completed

### Phase 3: RED 测试与生成方案

- [x] 为数据生成契约、确定性和可加载性编写失败测试。
- [x] 明确输出目录、随机种子、规模和不覆盖真实数据的安全边界。
- **Status:** completed

### Phase 4: 数据仿造实现

- [x] 实现最小可维护的数据生成路径。
- [x] 生成目标数据集并检查表结构、取值和跨表一致性。
- [x] 必要时迭代修正生成规则。
- **Status:** completed

### Phase 5: 烟测与交付

- [x] 运行设备域聚焦测试和真实烟测入口。
- [x] 迭代直至烟测通过。
- [x] 更新 Issue、计划证据并说明数据集位置与使用方式。
- **Status:** completed

### Phase 6: 空参数扩展需求与 Issue readiness

- [x] 分析空参数规格的唯一性、共享站点-机台风险和前端展示现状。
- [x] 创建独立 Issue 并补充稳定键、兼容边界和验收条件。
- [x] Triage 至 `ready-for-agent` 并添加 Agent Brief。
- **Status:** completed

### Phase 7: 空参数合成键 RED→GREEN

- [x] RED：锁定稳定键、无模糊回退、全规格生成和前端隐藏列行为。
- [x] GREEN：实现共享规格身份键、空参数生成与精确匹配。
- [x] REFACTOR：集中前端可见列契约并保持非空 LIKE 行为不变。
- **Status:** completed

### Phase 8: 全规格数据集再生成

- [x] 显式覆盖本任务生成的同签名快照，保留其他历史快照。
- [x] 验证 1,685 条唯一当前记录和 1,781 条报表全覆盖。
- **Status:** completed

### Phase 9: 集成、设备与页面烟测

- [x] 运行聚焦单元/集成测试和设备 smoke。
- [x] 运行 Streamlit 页面烟测，验证指标、明细和参数列不可见。
- [x] 更新 Issue、计划和交付证据至待人工复核。
- **Status:** completed

## Key Questions

1. Task2-opt 指定要仿造哪些数据文件、规模和时间跨度？
2. 设备域读取器对列名、dtype、枚举值、主外键与时间字段有哪些硬约束？
3. “可用”应通过哪些现有服务或页面路径证明？
4. 数据集应写入现有快照目录还是独立输出目录，如何避免破坏用户数据？

## Decisions Made

| Decision | Rationale |
|---|---|
| 使用独立计划 ID | 保留已完成的性能优化计划，避免历史证据被覆盖。 |
| 先锁定数据契约再生成 | 仿造数据只有被生产读取路径接受并支撑计算，才算“可用”。 |
| 采用 RED→GREEN→REFACTOR | 用户要求持续迭代至烟测通过，测试先行可定位每次修正。 |
| 只生成唯一可监控底层键 | 1,519 条空参数规格无法被现有匹配器安全查询；262 条可监控规格可无冲突折叠为 166 个键。 |
| 写入当前配置快照目录并默认拒绝覆盖 | 生产加载器可直接消费，同时保护已有真实快照。 |
| 空参数使用稳定业务身份合成键 | 448 个站点-机台组包含多条空参数规格，只有规格级键能避免错配。 |
| 合成键仅用于仿造快照内部匹配 | 保持规格参数为空，不伪造真实数据库参数，也不暴露给前端。 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| None | 1 | — |

## Verification Evidence

- `python -m pytest` 生成器及设备集成范围：11 passed。
- `python tools/smoke.py equipment`：21 passed。
- Streamlit AppTest：0 exception、0 error、1 dataframe，页面指标与生成时间一致。
- `compileall`、定向 `git diff --check`、新增文件尾随空白检查：通过。
- 空参数扩展最终验证：集成 6 passed，设备 smoke 29 passed。
- 数据断言：1,685 条唯一当前记录、1,519 条合成键、1,781/1,781 报表行有值。
- 页面断言：0 exception/error，1 个明细表；两种表格参数列不可见。

## Guardrails

- 不覆盖或删除用户现有数据。
- 保留工作树中与本任务无关的修改。
- 固定随机种子，保证数据可复现。
- 生成结果必须经生产读取路径及设备烟测验证。
