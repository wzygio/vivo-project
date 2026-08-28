# 发现与决策

## 需求
- 按评估文档“建议实施顺序与验收”从高到低实施剩余优化。
- 遇到无法从代码、测试和领域文档判断的业务逻辑时向用户确认。

## 研究发现
- 待阶段 1 补充。

## 技术决策
| 决策 | 理由 |
|------|------|
| 使用 pytest TDD 保护行为 | 优化跨 Application/Core/Infrastructure，回归风险高 |
| 先测调用次数与结果等价，再测耗时 | 避免只依赖易波动的墙钟时间 |

## 遇到的问题
| 问题 | 解决方案 |
|------|---------|

## 资源
- `docs/dev_docs/generated/yield_domian/mwd-processor-opt-assessment.md`
- `docs/dev_docs/generated/yield_domian/mwd-processor-opt-algorithm.md`
- `src/yield_domain/application/yield_service.py`
- `src/yield_domain/core/mwd_trend/`
# 2026-08-28 当前代码核对

- 仓库不存在 `.codegraph/`，本任务按项目约定改用 `rg` 和定向文件读取。
- `YieldAnalysisService.get_code_level_trend_data()`、`get_mwd_trend_data()`、`get_mapping_data()` 在各自 cache miss 路径中分别调用 `_build_modifier_context()`；Group 路径还会先调用 Code 路径，因此整页冷加载可能重复同步修饰表和读取工作簿。
- `sync_modifier_table()` 对 `group`、`code` 循环，分别调用 `compute_current_month_loss()`；后者每次都会复制 Panel、解析整列日期、生成整列月份字符串、过滤当月并计算当月总投入，因此同一上下文扫描两遍。
- `read_workbook_sheet()` 当前捕获所有 `ValueError` 后直接返回空表；只有“目标 Sheet 确实不存在”才应返回空表，文件格式/引擎类 `ValueError` 应进入 COM 回退。
- `prepare_code_raw_data()` 已不再聚合 Code 原始日度不良数，评估文档中该部分确实已完成；后续只处理仍未完成的共享基础事实。
- P0/P1 定向基线（modifier + service wiring + Excel）为 `56 passed`，耗时 `15.74s`；现有 14 条 warning 均来自 `_apply_current_month_loss()` 的 pandas concat FutureWarning。
- P0 实施后同组测试为 `59 passed`、`2.57s`。该时间包含缓存与工作簿测试，不能单独视为算法加速比，但确认新增共享缓存、单次解析和 Excel 分类契约均已覆盖。
- 新入口 `compute_current_month_losses()` 只解析/过滤一次日期，并同时返回 Group/Code；兼容入口 `compute_current_month_loss()` 保留。
- `get_modifier_context()` 以 config、product_dir、Panel 快照签名和修饰表签名为缓存键；三个消费者不再直接构建上下文。
- Excel 的非 Sheet 缺失 `ValueError` 已进入 COM 回退；真实企业加密文件仍需最终人工环境验收，本地用模拟 COM 边界覆盖。
- Group 日度现直接从 Code `daily_full` 复用日期、每日投入并按 Group 汇总；Panel 只读取 Group 清单，不再解析日期、计算原始 Group 日度不良或二次补齐日历。
- Code 生成现对整张长表只解析一次日期、只生成一次月份键，按 Code/月在 NumPy 数组中分配，循环结束后一次写回 `defect_panel_count`；保留原分配器与稳定哈希，因此业务公式未变。
- P1 定向回归为 `19 passed`、`0.50s`；新增“Panel 日期不可解析但 Code 日度有效时 Group 仍可生成”和“两 Code 只解析一次日期”契约。
- P2 将月/周/日的日期解析和良损列计算提升为每张完整表一次；`daily_full`/`daily`、`weekly_full`/`weekly` 共享准备结果，并在 melt/排序前先按真实日期截取近期窗口。
- P2 相关回归为 `21 passed`、`0.51s`；新增 Code、Group 两条“完整与近期共用一次日期解析”契约。
- P3 候选快速路径的 20,000 次隔离基准：无饱和 1.143s vs 原算法 1.258s（仅 1.10x）；有饱和包装器 2.520s vs 原算法 1.631s（慢 1.55x）。候选代码已撤回。
- 最终 MWD/Mapping/Excel 定向集合 89 passed（2.91s）；完整 unit 为 703 passed、5 个与本改动无关的既有失败（41.00s）。
- 真实企业加密工作簿 COM 冷启动/写回未在自动环境执行，保留为唯一人工验收项。
