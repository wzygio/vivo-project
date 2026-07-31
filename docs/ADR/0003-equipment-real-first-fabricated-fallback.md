# adr-0003：关键备件采用真实快照优先、独立仿造快照补缺

- Status: Accepted
- Date: 2026-07-21
- Scope: `equipment_domain` 当前值快照的生成、更新、加载与关键备件报表匹配

## Context

关键备件报表需要覆盖规格基线中的全部可监控项。数据库快照只能命中其中一部分，空参数规格则依赖内部合成键才能形成稳定匹配。此前数据库查询与仿造器共同写入 `part_life_snapshot_<signature>.parquet`，真实刷新会覆盖仿造结果，报表也只能消费单一来源，无法同时保证真实值优先和缺口补齐。

首次仿造与后续按日推进具有不同的前置条件和失败语义。将二者合并会导致“更新文件不存在时静默重新生成”等不可审计行为。页面侧还受 adr-0001 约束：`st.cache_data` 只能跨越 DataFrame、原生容器和标量。

## Decision

1. 数据库快照继续使用 `part_life_snapshot_<signature>.parquet`，独立仿造快照使用 `part_life_fabricated_<signature>.parquet`。两者使用相同规格签名定位，但互不覆盖。
2. 首次生成由 `generate_fabricated_snapshot()` 和 `tools/fabricate_equipment_data.py` 负责：
   - 每个唯一可监控键生成一条当前值；
   - 测量值均匀落在规格值的 0%–100%；
   - 测量时间独立随机落在基准时刻前两天内；
   - 固定随机种子和基准时刻时结果可复现；
   - 已有目标文件默认拒绝覆盖，只有显式覆盖才替换。
3. 后续更新由 `update_fabricated_snapshot()` 和 `tools/update_fabricated_equipment_data.py` 负责：
   - 必须存在结构有效、能够映射回规格的仿造快照；
   - 文件年龄小于 24 小时时跳过，`--force` 仅绕过新鲜度检查；
   - 每条测量时间在原值上增加一天；
   - 每条测量值增加规格值的 30%；若新值超过 100%，则按固定种子重置到 0%–30%；
   - 键集合与行数保持不变。
4. 报表按每条规格执行两阶段匹配：先查询真实快照；只有真实快照无匹配时才查询仿造快照。不得先拼接两个来源再按时间取最新，因为较新的仿造记录也不能覆盖真实记录。
5. 空参数规格继续使用内部合成键精确匹配，非空参数继续使用既有 SQL LIKE 语义。合成键、匹配来源与参数名不在前端展示。
6. 报表加载是只读操作，不隐式生成或更新仿造数据。24 小时 TTL 由独立更新命令执行，以保持报表访问和数据变更解耦。
7. 页面缓存继续只返回原生 payload，在缓存外构造 ViewModel，遵循 adr-0001。

## Consequences

### Positive

- 真实数据库刷新不会删除仿造补缺数据，仿造更新也不会污染真实缓存。
- 真实值优先级由逐规格控制流保证，与两个来源的时间先后无关。
- 首次生成和更新的失败边界、TTL、强制行为可分别测试和审计。
- 1,781 条规格的完整报表可得到 248 条真实匹配和 1,533 条仿造补缺，当前审计无未匹配规格。
- 空参数规格无需显示参数名称，也能通过稳定内部键得到当前值。

### Negative

- 运行维护需要显式调用两个命令：首次创建使用生成命令，超过 24 小时后的推进使用更新命令。
- 文件 mtime 决定更新资格；复制或恢复文件可能改变其年龄，必要时应由操作者明确使用 `--force`。
- 报表不会自动修复缺失或损坏的仿造文件；此类问题由更新命令明确报错并通过运维流程处理。

## Alternatives considered

### 合并真实和仿造快照后统一取最新记录

Rejected。较新的仿造时间会覆盖仍然有效的真实值，违反真实数据优先的业务要求。

### 继续复用数据库快照文件名

Rejected。真实刷新和仿造生成会相互覆盖，无法形成稳定的补缺来源。

### 报表访问时自动生成或更新仿造数据

Rejected。读取页面将产生文件写入，并把 TTL、随机更新失败与 Streamlit 缓存生命周期耦合，难以审计和测试。

### 更新输入缺失时自动退化为首次生成

Rejected。它掩盖丢失或路径错误，破坏首次生成与更新的独立职责和失败边界。

## Verification

- 生成、更新、命令边界、匹配器和服务 focused 回归：`32 passed`。
- 更新器边界补充回归：`3 passed`。
- 真实规格集成测试：`1 passed`。
- Equipment 领域 smoke：`34 passed`。
- Python 编译和范围内 `git diff --check`：通过。
- 数据审计：真实快照 2,501,155 行；独立仿造快照 1,685 行；报表 1,781 行，其中真实匹配 248、仿造补缺 1,533、未匹配 0；真实值与时间优先级逐项保持。
- 浏览器烟测：默认 Array 1,351 条，TP 筛选 430 条；指标和表格正常，桌面/窄屏无页面级横向溢出，参数列未显示，无可见 Streamlit 执行异常。

## References

- `.scratch/equipment-snapshot-fallback/issues/01-use-real-snapshot-with-fabricated-fallback.md`
- `.planning/2026-07-21-equipment-real-fabricated-fallback/task_plan.md`
- `.planning/2026-07-21-equipment-real-fabricated-fallback/progress.md`
- `config/equipment_config.yaml`
- `src/equipment_domain/infrastructure/fake_data.py`
- `src/equipment_domain/infrastructure/fake_data_updater.py`
- `src/equipment_domain/core/parts_matcher.py`
- `src/equipment_domain/application/parts_service.py`
- `tests/unit/test_equipment_data_fabricator.py`
- `tests/unit/test_equipment_data_updater.py`
- `tests/unit/test_equipment_data_commands.py`
- `tests/integration/test_equipment_fake_dataset.py`
- `.planning/2026-07-21-equipment-real-fabricated-fallback/equipment-desktop.png`
- `.planning/2026-07-21-equipment-real-fabricated-fallback/equipment-table-narrow.png`
