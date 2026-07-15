# 项目性能优化后风险清单

- Date: `2026-07-14`
- Scope: Issue 01 领域烟测入口；Issue 02 CPM 周期能力聚合
- Outcome: 未发现由本次优化新增的业务/功能风险；两个 Issue 均保留

## Issue 01：领域烟测入口

| ID | 风险 | 排查证据 | 结果 |
|---|---|---|---|
| S-01 | 快速领域遗漏跨域依赖 | 默认 `all` 合同测试；文档明确共享/不确定变更及发布前使用完整回归 | 受控残余风险；不撤回 |
| S-02 | 测试重命名导致少运行或零运行 | `test_spc_smoke_resolves_only_existing_spc_and_cpm_tests`、`test_empty_domain_target_cannot_pass_silently` | 未发现 |
| S-03 | 默认快速模式造成虚假信心 | `test_smoke_defaults_to_complete_unit_suite` | 未发现；默认完整 |
| S-04 | 既有失败被隐藏 | SPC/Yield 烟测均返回非零并输出既有故障；宽回归仍为同一 7 个失败 | 未隐藏 |
| S-05 | 新入口破坏原 pytest 命令 | 原命令仍直接运行；入口只新增文件和文档 | 未发现 |
| S-06 | 领域映射串线 | SPC、Yield、Equipment 路由单测；Equipment 实跑 13/13 | 未发现 |
| S-07 | 旧 Yield 测试依赖手工 `PYTHONPATH` | `test_smoke_configures_repo_and_src_import_paths`；无环境变量实跑 Yield 后仅剩既有过期符号错误 | 已消除手工准备要求 |

## Issue 02：CPM 周期能力聚合

| ID | 风险 | 排查证据 | 结果 |
|---|---|---|---|
| C-01 | 聚合过滤顺序改变 | `test_build_period_capability_report_filters_before_taking_first_limits` | 未发现 |
| C-02 | 首个有效规格/控制限/target 改变 | 同上，覆盖前置无效行、NaN 和后续不同值 | 未发现 |
| C-03 | NaN 分组键或排序改变 | `test_build_period_capability_report_keeps_nan_group_keys`；真实结果逐位比较 | 未发现 |
| C-04 | `std(ddof=1)`、单样本或零方差改变 | 单 Sheet Point sigma、旧有日窗口测试；近常数 CPK 精确审计 | 中间实现曾命中，已修正；最终未发现 |
| C-05 | Point sigma 完整键错配 | `test_build_period_capability_report_isolates_point_sigma_by_factory` | 未发现 |
| C-06 | Point sigma fallback 改变 | `test_build_period_capability_report_falls_back_per_group_when_point_stats_missing` | 未发现 |
| C-07 | 月/周/日窗口、ISO 周或非连续日期改变 | `build_period_axis`、available/all-available axis、older-days 回归 | 未发现 |
| C-08 | sample/point count 或 dtype 改变 | 全 Point 与混合 fallback 的 `int64`/`float64` 断言；真实逐位比较 | 未发现 |
| C-09 | 向量化增加内存导致大数据失败 | 104 万点位真实形状两条路径稳定完成；日期轴不再复制整张 DataFrame，只处理时间 Series 与唯一日期 | 未发现 |
| C-10 | 浮点归约顺序改变 CPM/CPK | HEAD 旧实现与工作区新实现 `assert_frame_equal(check_exact=True)`，两条 sigma 路径均通过 | 未发现 |
| C-11 | 缓存、数据库、页面接口改变 | diff 范围仅 core 聚合、测试工具、测试与文档；SPC service/dashboard 回归包含在 67 passed 中 | 未发现 |

## 既有但非本次引入的故障

- `test_override_logic.py` 导入已不存在的 `create_mwd_trend_data`，导致完整/Yield 收集失败。
- `test_code_selector_filter.py` 两项缺少 `count_threshold`。
- CPM 页面 alerts 顺序断言 1 项失败。
- Shadow EMA 2 项断言失败。
- Yield 全局策略 2 项仍期望未包含 TP 的旧配置。
- `resources/critical_parts_baseline.csv` 与当前 UTF-8-only loader 编码不兼容。

这些故障在优化前已存在。本次任务没有修改其业务逻辑，也没有把它们计为优化回归。

## 撤回判定

- Issue 01：没有命中需撤回的新增风险；保留。
- Issue 02：原生 groupby 浮点归约曾命中 C-04/C-10，相关写法已在开发过程中撤回；最终实现恢复逐位等价并达到 46.0% Point Value 加速，因此保留。
