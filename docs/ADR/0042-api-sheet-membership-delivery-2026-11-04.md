# API 1.3.0：边界默认值与 SPC 周度 Sheet ID 验收

日期：2026-11-04。状态：实现、相关回归、真实数据与正式服务验证完成。

## 交付行为

- LOWER／lower、NULL／空值／缺省统一为 `命中边界=spec`；明确的规格／控制边界保留。
- CPK／CPM 的对象类型仍为 capability，每行一个周期项目；对象编号为该产品、厂别、站点、参数、完整 ISO 周的全部 Sheet ID 数组，去重、文本升序、保留前导零。片／Lot 仍使用字符串。
- `entity_id` 按单个片号是否在名单中筛选，返回项目的完整名单；分页及计数按项目行，名单变化使旧游标失效。
- JSON 保留数组；CSV 以标准 JSON 单元格保留全名单，无静默条数截断。
- SPC 正常报表自动从 By Sheet 制备点位发布原子周度快照；独立同步命令复用同一流程。HTTP 仅调用 SPC 应用层只读查询，不启动报表、数据库或维护写入。

## 数据完整性

真实源覆盖在原始快照事务锁内与文件代际一同捕获，转换显示时间后随点位、制备和原生缓存传递；当前文件不能扩大旧缓存的覆盖证明。完整周资料不足拒绝发布；完整覆盖的空周写入空代，防止继续读取旧名单。成员读取还验证策略、业务键、真实跨度及指标值锚点，失败沿用来源不可用语义。

首次真实同步前备份了三份 SPC 维护工作簿。M626 正常报表返回 633,061 个制备点位、14,900 个不同 Sheet（整个查询窗口）；相关不达项目周内只有其中对应业务键的 3 个片号。未把整月或整个产品的全部片号混入该项目。

## 自动化验证

相关回归 **536 passed**，其中包含 **5 项真实网络 E2E**；所测 API、成员应用／存储及调用程序共 1,271 条语句，覆盖 1,172 条，覆盖率 **92.21%**。Office 禁止哨兵未触发。

```powershell
uv run --no-sync python -m pytest -p output.tmp.no_live_office_pytest tests/unit/api tests/integration/test_anomaly_api_capability.py tests/integration/test_anomaly_api_files.py tests/unit/test_fetch_sheet_anomalies.py tests/unit/test_sync_spc_sheet_membership.py tests/unit/inline_domain/application/spc tests/unit/inline_domain/application/shared/test_decorated_features.py tests/unit/inline_domain/infrastructure/measurement/test_measurement_snapshot_repository.py tests/unit/inline_domain/application/monitor/test_capability_alert_query.py tests/unit/inline_domain/infrastructure/monitor/test_cpk_latest_excel_store.py tests/unit/inline_domain/core/monitor/test_cpk_latest.py tests/e2e/test_anomaly_api_sheet_membership_e2e.py tests/architecture -q
```

本机执行使用 `--cache-dir output/tmp/uv-cache`；覆盖数据写入 `output/test-results/api-sheet-membership-gate/`，避免改写根目录的已跟踪覆盖文件。上面的 Office 插件是本次输出目录中的验证辅助文件，不属于发布依赖。持续使用时可以直接运行所列测试，测试自身的文件夹具和网络 E2E 也禁止数据库／Office访问。

测试覆盖真实临时工作簿与成员文件 → Uvicorn → 调用程序 → JSON／CSV；CPK／CPM 各两个项目、607／503 个片号完整往返。涵盖去重、前导零、跨业务键／周隔离、成员筛选、分页、部分失败、普通 OOS 兼容，以及成功／异常／KeyboardInterrupt 的最小无数据库服务清理。

新增源覆盖回归证明：partial fallback 保留真实旧覆盖；刷新 manifest 不改变旧点位的证明；当前 provider 不能扩大 native 覆盖；不匹配代际在能力写入前拒绝；完整空覆盖使旧名单失效。修复了既有 SPC reload 测试未恢复父 package 属性的测试污染，业务断言不变。

这是本次影响范围回归，未声称整个仓库测试通过，也未删除或修复无关历史基线问题。

## 真实 HTTP 与正式服务

| 检查 | 结果 |
|---|---|
| 本地独立制备点位见证 | 目标业务键／周的片号与 API 返回逐集合相等 |
| M626／2026-W44 CPK | 1 项，3 个 Sheet ID，1.14695639973149，complete=true，边界 spec |
| M626／2026-W44 CPM | 1 项，3 个 Sheet ID，1.151287233428474，complete=true，边界 spec |
| 片号成员筛选 | 返回原项目的完整 3 个片号 |
| M626／2026-W41 | CPK／CPM 各 2 项、每页 1 项共 2 页，与不分页完全一致 |
| 所有授权产品 | 各指标返回可用的 M626 项，明确标记 M673 上周资料不可用 |
| 原 SPC OOS | 成功空结果，complete=true，字符串编号契约保留 |
| 健康与认证 | live／ready 200；无密钥业务与 OpenAPI 均 401 |
| OpenAPI | 1.3.0；能力编号 array、片／Lot 编号 string |
| 正式更新 | 原 host manager 停止其旧进程族并验证监听释放，启动新进程就绪；配置原字节、凭证及授权保持 |
| 临时服务 | 正常／失败／中断及真实数据服务均记录清理成功，监听释放；正式 8890 保留 |

正式入口仍为 `http://10.72.26.31:8890`。本次正式取数在当前主机执行；未把它当作另一台设备的取数验证。原始密钥按已有授权仅从 `.env` 的 `API_KEY` 在内存读取，未输出或保存到测试证据。

## 文件与证据

- [调用说明](../../../../../references/domain/inline_domain/api/api-caller-guide-2026-11-03.md)、[部署说明](../../../../../references/domain/inline_domain/api/guide-sheet-anomaly-api.md)、[程序介绍](../../../../../references/domain/inline_domain/api/architecture-sheet-anomaly-api.md)。
- [规格与任务](../../../../../.scratch/anomaly-api-sheet-membership/PRD.md)、[ADR-0037](../../../../ADR/0037-published-anomaly-api.md)。
- `output/test-results/api-sheet-membership-gate/{junit.xml,coverage.json}`：536 项整合验证及覆盖。
- `output/test-results/api-sheet-membership-e2e/`：网络 E2E、完整／部分导出与生命周期记录。
- `output/test-results/api-sheet-membership-real/{source-publication,http-verification,deployment,formal-verification,temporary-service}.json`：真实生产、独立见证、HTTP、部署及清理证据。
- `output/reports/anomaly-api/spc-{cpk,cpm}-sheets-20261104-123020.{json,csv}` 及 CSV 元数据：真实完整导出。
- `output/test-results/api-sheet-membership-real/before-source-20261104-122601/`：同步前的三份维护工作簿备份。

维护入口为 `tools/sync_spc_sheet_membership.py`；片号快照位于 `data/inline_domain/spc/sheet_membership/`，属于可重建数据。正常报表会自动更新；资料缺失时按部署说明同步，不在 HTTP 请求中重算。
