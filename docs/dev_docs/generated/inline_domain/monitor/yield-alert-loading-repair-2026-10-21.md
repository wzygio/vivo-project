# 自动预警良率加载修复与历史补齐

日期：2026-10-21。用户授权修复良率两行“无法加载”，生成 8 月 1 日至今天的结果，并确认趋势文件保留期间全部月、周报警。

## 复现与原因

只读复现命令 `output/tmp/yield-alert-repair-20261021/check_results.py` 首次执行失败：两份结果工作簿均不存在，7 个产品的 14 个单元格全部为 `missing → error`。此前 P0 只完成代码与隔离验证，未实际运行生产补齐。

补齐过程中进一步确认两处真实问题：

1. 源健康时间由 `datetime.now().isoformat()` 生成、不带时区，原读取器按 UTC 解释，导致北京时间有效结果被判为未来时间。已统一无时区值为 `Asia/Shanghai` 本地时间，保留实际 TTL、参考日和覆盖校验。
2. 警戒线读取先报 `BadZipFile`；增加只读 COM 后仍无法打开。正式规格文件实际是测试无效内容：17 字节的 `not-an-excel-file` 被企业加密包装后显示为 529 字节。`test_warning_workbook_failure_is_not_cached` 使用全局注册表注入的绝对路径，`tmp_path / absolute_path` 仍指向正式文件；本次复测也再次触发了该缺陷。测试已在写入前把路径改绑到临时目录并断言路径归属。

规格恢复使用保留的 `output/tmp/harness-resource-backup/yield_domain/shared/入库不良率规格.xlsx`。恢复前与提交 `c141bcdb` 的原规格逐单元格比较：7 个产品 sheet 的值全部一致。恢复后各产品均成功解析出 51 条警戒线。无效文件保留于诊断目录，未自行调整业务限值。

## 修复与发布契约

- 警戒线读取经应用拥有的 `YieldDataPort` 装配到基础设施读适配器；普通 XLSX 原样解析，`BadZipFile` 复用既有只读 COM，保留首行矩阵语义。其他读取异常继续上抛，不以空规格代替失败。
- Lot 文件保留所选期间原计算输出中的全部超规，页面继续展示最近 30 天；矩阵继续筛上一完整 ISO 周。
- 趋势文件逐月/逐周复用原判断规则，保留全部历史报警。程序生成的 `is_current` 标记保留原“每条序列最新比较”的当前状态，矩阵只消费当前记录，页面继续使用原最新预警。
- 原月标签带“月”字，窗口解析现已兼容；原展示接口仅保留最近 3 个周期，补充 `monthly_full` / `weekly_full` 聚合输出供历史发布，图表周期不变。最终趋势重发只执行趋势分支，保留已成功的 Lot 文件。
- 批量入口新增成对的 `--start-date`、`--end-date`。为判断 8 月相对 7 月的变化，计算读取 7 月 1 日起的基线，但结果覆盖为 8 月 1 日至 10 月 21 日。跨起日的周按周期区间相交保留，不虚构缺失基线。
- 发布后检查文件实际可读状态；过期、失败、缺失或覆盖不足不能在 CLI 被称为成功。矩阵仍无数据库/重计算回退。

现行规则见 [Yield 结果发布规则](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)，对应决策为 [ADR-0035](../../../../ADR/0035-published-alert-results-for-matrix.md)。

## 实际执行与验证

生产补齐使用原页面同一生产用例，等价命令：

```powershell
.venv/Scripts/python.exe tools/publish_yield_alert_results.py --start-date 2026-08-01 --end-date 2026-10-21
```

实际操作包装器位于 `output/tmp/yield-alert-repair-20261021/publish_all.py`，只增加安全进度、顺序执行、进程内存释放和本进程数据库连接的 10 分钟语句超时。未改变正式 Web 进程连接池或监听。Panel 复用今天上午更新的 7 份快照，元数据约 426 万行；源日期前移和当天截止沿用原策略。阵列时间继续按原 Lot ID 范围查询；预先检查了视图依赖、底层分区统计及只做规划的 EXPLAIN，未运行无界业务查询或 EXPLAIN ANALYZE。

最终扩展回归：**246 passed，72.74 秒**。另对 15 个 Python 文件做语法检查、验证 5 个本地文档链接，并通过限定文件的 `git diff --check`。

```powershell
.venv/Scripts/python.exe -m pytest -q tests/unit/yield_domain tests/unit/app/sections/monitor tests/unit/app/sections/yield_domain tests/unit/app/sections/shared/test_alert_center.py tests/unit/app/pages/test_yield_alert_publication.py tests/unit/app/pages/test_yield_dashboard_cache_wiring.py tests/unit/app/pages/test_auto_warning_shared_cache.py tests/integration/test_yield_alert_result_files.py tests/architecture/test_backend_dependencies.py
```

新增回归覆盖本地无时区时间、完整 Lot 历史、月周历史与当前状态隔离、前期基线、明确窗口，以及加密警戒线首行恢复和异常传播。测试已不再向正式规格文件写入夹具。限定代码 `git diff --check` 通过。

最终生产结果已实际落盘；以下是完整期间记录数，当前看板不会把所有历史记录都计为红灯：

| 产品 | Lot 超规 | 月周历史预警 | 当前 Lot 单元格 | 当前趋势单元格 |
|---|---:|---:|---|---|
| M626 | 1 | 31 | 正常 | 正常 |
| M678 | 0 | 12 | 正常 | 正常 |
| Z571 | 0 | 26 | 正常 | 正常 |
| M673 | 0 | 2 | 正常 | 正常 |
| Z517 | 3 | 53 | 正常 | 正常 |
| Z576 | 15 | 10 | 预警（上一完整周 1 条） | 正常 |
| Z553 | 0 | 3 | 正常 | 正常 |
| 合计 | 19 | 137 | 7 格均加载 | 7 格均加载 |

两份文件的产品元数据覆盖均为 `2026-08-01 → 2026-10-21`。最终冷进程审计设置 `pd.read_sql` 和矩阵 `DatabaseManager` 为禁止调用，14 个单元格全部可读，SQL 调用为 0。原复现命令再次执行已通过。生成没有绕过参考日、覆盖和 TTL 校验；保留合法零报警。

全部 Lot 成功生成后，最终趋势专门重发耗时 116.30 秒；这不是完整页面打开耗时。此后仅做文件读取审计，没有启动临时 Web 服务或重启正式服务；操作进程均已完成，读取 helper 释放其创建的 Excel 实例。

本次生成会沿用原报表维护表同步流程。未修改源 L1 刷新、报警阈值、详情图像或下方良率图像查询门控；未新增定时任务或常驻服务，也未把历史报警直接计入今天的红灯。

## 证据位置与后续刷新

诊断及生成证据集中于 `output/tmp/yield-alert-repair-20261021/`：首次状态、源快照元数据、数据库低成本规模探查、规格副本一致性、各产品发布结果及最终读灯核验。历史故障内容和生成结果属于本地业务证据，不随意删除。

后续由相应产品入库报表或同一批量入口更新结果；参考日和 TTL 校验继续生效。只运行矩阵预计算会读取已发布文件，不会生成过期产品的新分析结果。
