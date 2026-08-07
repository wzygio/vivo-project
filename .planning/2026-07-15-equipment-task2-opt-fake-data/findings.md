# Findings & Decisions

## Requirements

- 分析 `feat-equipment_domain.md` 中的 `Task2-opt：数据仿造`。
- 创建本地 Markdown Issue，并 triage 至 `ready-for-agent`。
- 使用文件式计划执行。
- 生成一份可用的仿造数据集，持续迭代直至烟测通过。

## Research Findings

- Task2-opt 要求仿照当前数据集生成底层数据，不生成历史数据；完成后依次通过集成测试与设备烟测，趋势图不在本次验收范围。
- 页面主表依次经过规格加载、签名快照读取、三键 LIKE 匹配、原始超规审计、展示修饰、进度/状态计算。
- 规格基线有 1,781 行、9 列、无重复整行；寿命规格为 13.5–370,000，单位分布为 sheet 1,074、KWH 422、pcs 270、≤Kw 15。
- 参数名称仅有三种非空模式：Target 131 行、Mask 123 行、PRE_SPRT 8 行；共 262 条可监控规格，折叠为 166 个唯一底层键，且无同键规格冲突。
- 其余 1,519 行参数为空；现有匹配器明确拒绝空参数，因此不得为它们伪造底层记录。
- 当前规格签名为 `e1f06d78da21`，现有目录无对应快照。已有四份快照含 124–293 万行和约 90 天历史，仅适合作为 schema/命名分布参考。
- 生产快照 schema 为 `step_id/object`, `sub_equip_id/object`, `param_name/object`, `value/float64`, `glass_start_time/datetime64[ns]`，关键字段均非空。
- 现有设备烟测入口为 `uv run --no-sync python tools/smoke.py equipment`，覆盖设备部件单元测试与缓存边界测试。
- `.out-of-scope/` 不存在；ADR-0001/0002 不阻塞独立数据生成，但必须保留 Streamlit 缓存和既有业务计算。
- 用户补充要求覆盖全部空参数规格，并明确保留关键备件页面、前端不显示参数名称。
- 1,519 条空参数规格具有 1,519 个唯一完整业务身份，无重复；稳定 SHA-256 摘要探针无碰撞。
- 这些规格仅覆盖 463 个站点-机台组合，其中 448 组含多条规格、单组最多 8 条，因此站点-机台模糊回退必然产生错配风险。
- 前端两种表格均用显式列顺序渲染，当前未包含“参数名称”或“匹配参数名”；需要测试固化，而不是删除页面。
- 空参数规格逐条生成、非空参数规格按现有键去重后，预期快照规模为 1,519 + 166 = 1,685 条。

## Technical Decisions

| Decision | Rationale |
|---|---|
| 固定输入确定性生成 | 便于测试、复现和审计；交付数据使用显式 `as_of`。 |
| 每个唯一底层键只生成一条当前记录 | 满足“暂不仿造历史数据”，也避免百万级无意义数据。 |
| 数值以规格线比例生成 | 自动适配 KWH、sheet、pcs、Kw 的不同数量级，无需错误地统一单位。 |
| 默认拒绝覆盖 | 当前签名无文件，可直接新增；未来重复执行需显式确认覆盖。 |
| 合成键由完整业务身份生成 | 不依赖行号/排序，规格重排后仍可复现；1,519 条当前输入无碰撞。 |
| 匹配器只对空参数执行合成键精确匹配 | 保护非空 LIKE 语义，并确保真实快照没有合成记录时不会误匹配。 |

## Issues Encountered

| Issue | Resolution |
|---|---|
| None | — |

## Resources

- `docs/dev_docs/dev_prompt/feat-equipment_domain.md`
- `src/equipment_domain/`
- `tests/`
