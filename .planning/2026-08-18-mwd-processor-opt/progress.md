# Progress: 入库良率修饰逻辑简化

## 2026-08-18 — Session 1

- 完成需求分析：读取需求文档、现链路代码、修饰表（COM 实测结构）。
- 输出 PRD：`docs/PRD/PRD-2026-08-18-入库良率修饰逻辑简化.md`（方案评估：可行；D1-D3 设计决策）。
- development-flow 模块 1 完成：issue `.scratch/mwd-processor-opt/issues/01-simplify-yield-modifier-pipeline.md`
  创建并 triage 至 `ready-for-agent`（Agent Brief 已填）。
- 模块 2 进行中：D4 已核实（警戒线 loader 保留）；计划已建，待用户批准。

## 2026-08-18 — Session 1（续）

- 用户批准计划（调整 D1：Group 由 Code 汇总）+ 强调 C1（sheet_lot 不动）/C2（新分支开发）。
- 已建分支 `feat/mwd-processor-opt`；pytest 基线 436 passed / 5 failed（既有）。
- Phase 1.1 完成：基线记录于 findings.md。
- Phase 2 完成（TDD 6 轮 RED→GREEN）：`modifier_table.py` 全量实现，
  `test_modifier_table.py` 27 passed。关键修正：pandas map 后 None→NaN 需用
  `pd.notna` 判断；缩放倍数口径与目标回退链对齐（上月指定/当月原始），
  保证趋势与 Mapping 一致。

## 2026-08-18 — Session 2（worktree 迁移）

- 用户恢复 `resources/入库良率修饰表.xlsx` 并要求改用 worktree 开发。
- 用户已将全部工作区改动提交：master=`1fdbe6b v3.3: chore`，
  feat/mwd-processor-opt=`90c4ae7 v3.3: chore`（含本任务全部产物）。
- 主目录切回 master（运行项目不受影响）；新建 worktree
  `D:/wzy/Python/vivo-project-mwd`（feat/mwd-processor-opt），后续开发全部在
  worktree 进行，测试用主目录 `.venv` 运行（worktree 验证 27 passed）。

## 2026-08-18 — Session 2（续，Phase 3-6）

- Phase 3 完成：`daily_generator.py`（月中锚点插值 + blake2b 白噪声 + 月内整数分配），9 项单测通过。
- Phase 4 完成：facade 重写（Code 指定良损驱动、Group 由 Code 汇总）、旧链路 5 模块
  + 4 旧测试文件删除；`test_defect_panel_count_alignment.py` 重写 5 项通过。
- Phase 5 完成：service 接线（`resolve_modifier_table_path`/`_build_modifier_context`，
  缓存 key 增加 `modifier_signature`）、5 个产品 yaml 增加 `yield_modifier_config`、
  页面改调 `inject_mapping_config_to_config`、CLI `tools/update_yield_modifier_table.py`、
  smoke.py yield 清单更新。接线测试 5 项通过。
- Phase 6 完成：`prepare_mapping_data` 新增 `monthly_factors`（步骤 2.5，级联之前），
  级联段 diff 全为新增行（红线零改动）；4 项单测通过。
- 新决策 D5：当月良损用与趋势一致的修饰后 panel 明细计算（保证 Mapping 数学口径），
  已记录于 PRD。
- 签名存储从 `data/<prod>/` 改为修饰表旁 `<表名>.sig.json`（测试隔离 + 多产品共享表
  时按 `<product>:<level>` 键区分），空表不写回。

## 2026-08-18 — Session 2（续，Phase 7 收官）

- 全量回归两轮：461 passed / 5 failed = 既有基线，无新失败。
- 数值 E2E（真实 M678 快照，`output/tmp/verify_modifier_e2e.py`）7/7：
  当月行写回 / 指定解析 / 倍数 1.5 两位小数 / Code 月趋势 == 指定水准 /
  Mapping 上调 34→34（级联天花板绑定，设计内）与下调 34→29 / 清空回落原始。
  注意：Mapping 只展示最新 5 批次（实测最新批次月为 2026-07，当前月 2026-08
  仅 171 片），断言须按 Mapping 实际覆盖月份选取。
- 浏览器 E2E（playwright-cli，worktree :8510）：经首页 sys.path 引导后看板
  无异常渲染完成；截图 `output/test-results/yield_modifier_dashboard.png`。
  经验：yield 页面无自举 sys.path，直连页面前须先跑 Home.py；页面文本断言
  避免可见性等待（隐藏的预警摘要文本会命中）。
- CLI M678 实跑写回正确。E2E 造成的 resources/ 二进制 churn 已全部还原。
- 文档：算法文档重写 + ARCHITECTURE.md 更新；ADR-0016 已建。
- 最终命令：`.venv python -m pytest tests/unit -q` → 461 passed / 5 failed（基线）。
- 有意排除：级联衰减、defect_multipliers 语义、趋势图人工修正.xlsx 迁移、
  codebaseline.xlsx 文件清理、周/日粒度指定（详见 issue Out of scope）。
