# 自动预警结果文件：三项 P0 交付记录

日期：2026-10-20。依据：[性能分析与修订方案](alert-matrix-performance-analysis-2026-10-20.md)第 8、10 节。三项 P0 已实现；本记录不将隔离测试换算为生产耗时收益。

## 实现范围

| P0 | 实现与结果 |
|---|---|
| Yield 结果生产 | 原入库分析页调用同一 application 用例，分别发布 Lot 超规、Group/Code 月周趋势报警工作簿；发布先于 Sheet/Mapping 和图像门控。保留原计算、指标缓存参数和资源手动刷新语义。 |
| 矩阵与预计算消费 | 默认 Yield 两行只读取已发布文件，缺失、过期、失败或覆盖不足均不触发数据库/完整分析。现有 `warm_alert_matrix.py` 经相同矩阵入口消费，无需另设计算机制。 |
| Inline 文件读取 | 四类 OOS 默认使用已有 `ExcelAlarmReader` / `ExcelAlarmStore`，按工作簿版本缓存整本解析，再按产品筛选。 |

两份结果默认位置：

- `resources/yield_domain/sheet_lot/yield_lot_oos_results.xlsx`
- `resources/yield_domain/mwd_trend/yield_trend_alert_results.xlsx`

位置在 `config/global.yaml` 注册。文件首次成功生成时创建；每产品结果和元数据原子发布，跨进程锁避免不同产品并发写入相互覆盖。失败保留旧记录并更新不可用状态；写入失败向调用方报告。无变化不重写文件，不延长来源有效期。

准确字段、有效性、失败语义、解析缓存及运维命令以 [Yield 结果规则](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)为准，架构决策见 [ADR-0035](../../../../ADR/0035-published-alert-results-for-matrix.md)。原 [ADR-0032](../../../../ADR/0032-all-product-yield-readonly-cache.md)已注明：矩阵状态不再为下方良率图像预热 Group 条目，良率图像自身的只读应用缓存及独立查询门控继续保留。

## 验证证据

以下命令通过：**276 passed，1 deselected，52.90 秒**。该时间为测试套件运行时间。

```powershell
.venv/Scripts/python.exe -m pytest -q tests/unit/app/sections/monitor tests/unit/app/sections/yield_domain tests/unit/app/sections/shared/test_alert_center.py tests/unit/app/pages/test_yield_dashboard_cache_wiring.py tests/unit/app/pages/test_yield_alert_publication.py tests/unit/app/pages/test_auto_warning_shared_cache.py tests/unit/yield_domain tests/unit/inline_domain/application/monitor/test_excel_alarm_reader.py tests/architecture/test_backend_dependencies.py tests/integration/test_alert_matrix_integration.py tests/integration/test_yield_alert_result_files.py tests/unit/test_global_resource_registry.py -k 'not test_all_existing_resource_files_have_a_global_registry_entry'
```

关键断言：

- 实际入库页面 AppTest：Sheet 加载失败前，两类结果均已发布；普通 rerun 不重复改写相同结果。
- 默认矩阵 Yield 两行：禁止调用原分析服务、SQL 和数据库初始化；缺文件显示不可用，合法零报警显示正常，有报警显示预警。
- Inline：4 个工作簿 × 7 个产品只解析 4 次；同版本再读取不解析，修改 SPC 文件后总计增加到 5 次。
- 两个独立进程同时发布不同产品，均保留；第三个冷进程只凭两份文件恢复四个 Yield 单元格。
- 预计算入口读取结果文件并发布临时 JSON；失败、空来源、覆盖/日期/TTL、损坏恢复及原子写失败均有测试。
- 下方全产品 Group 趋势缓存保持命中、产品定向失效、资源和日期变化失效，始终采用只读模式。

CLI 配置检查已通过，不读取/写入业务数据：

```powershell
.venv/Scripts/python.exe tools/publish_yield_alert_results.py --check
```

限定文件的 `git diff --check` 通过。另检查 20 个 Python 文件语法和 31 个交付文档本地链接，全部通过；确认两份生产结果工作簿尚未创建。静态检查记录位于 `output/test-results/alert-result-files-p0/static-verification.json`。

## 已知检查限制

资源全集注册检查单独执行仍失败：`test_all_existing_resource_files_have_a_global_registry_entry` 发现四个既有未注册文件，分别为 `resources/yield_domain/mwd_trend/66F1A010` 和 `resources/inline_domain/monitor/` 下三个 `北极星报警率与CPK汇总.xlsx.before-…backfill-20261009-….bak`。它们不是本次生成文件；未删除或擅自登记。本次回归只排除这一项，未将其算作通过。

未连接生产数据库，未运行生产结果补齐/预热，未改写人工维护工作簿，未测试真实企业加密文件的耗时。本轮页面验证使用 AppTest 和临时文件，未启动浏览器测试服务。

## 启用与后续

当前产品下次成功执行入库报表时会发布结果。首次需要全产品补齐时，可执行下面的同源生产入口；该命令会执行原报表的数据读取/计算，**本轮尚未执行**：

```powershell
.venv/Scripts/python.exe tools/publish_yield_alert_results.py
```

之后现有预计算和 Web 进程均可直接消费。未补齐、过期或失败的产品显示不可用，不由矩阵补算。未新增定时任务，L1 更新及点击详情渲染保持原有机制。

生产前后秒数需在真实结果发布后分别测量；配置重复解析、工序字典延后及按筛选范围求值等 P1 尚未实施。
