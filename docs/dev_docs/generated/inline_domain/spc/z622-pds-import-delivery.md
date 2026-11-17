# Z622 SPC 数据导入与验证记录

日期：2026-11-16。范围：[新品数据导入需求](../../../dev_spec/inline_domain/spc/task-新品数据导入.md)。状态：监控发现、真实 PDS 导入、共享 raw 快照发布、目标回归、完整管线集成及浏览器 E2E 均已通过。交付为本地工作区改动，未执行 Git 发布或生产服务重启。

## 已确认范围

- 新品 Z622 使用 `eda.array_pds_result_t` 的 ARRAY 测量，批次固定为 `L3ZB6BE03`，即 Glass ID 前九位。
- 用户确认仅导入测量数据，暂不计算能力指标。M626 仅用于确定监控站点和参数，不提供 Z622 阈值。
- 用户允许在 SPC infrastructure 之外，对组装层增加新品分发，以保持首次加载、强刷和快照来源一致；应用、核心计算和前端交互沿用现有 SPC 链路。
- 没有数据库写入、临时表、DDL 或无界事实扫描。数据库凭证未进入生成物。

## 监控配置发现

先检查源表字段、主键／索引和分区目录，再使用有限时间范围提取参考组合。数据库为 Greenplum 6；PDS 为多级分区源，根关系自身的零行估算不能代表底层数据量。递归目录统计估算约 240.6 亿行、分区目录 1,232 项；这些是规划统计，不是实际扫描得到的精确行数。

参考产品为需求示例中的 M626。观测窗口为源时间 `2026-09-01 <= time < 2026-11-17`，产品字典／白名单读取也按已核验的时间字段设置上界；已有 `15690` 站点排除继续生效。

| 厂别 | 实际观测组合 | 与 SPC 白名单匹配及排除后 |
|---|---:|---:|
| ARRAY | 107 | 102 |
| OLED | 41 | 31 |
| TP | 22 | 22 |
| 合计 | 170 | 155 |

维护产物为 [`Z622监控站点参数.xlsx`](../../../../../resources/inline_domain/spc/Z622监控站点参数.xlsx)，位置通过 `config/global.yaml → resources.inline_domain.files.spc_z622_monitoring` 解析。

| Sheet | 内容与用途 |
|---|---|
| `monitoring` | 155 个有效参考组合，列为 `factory, step_id, param_name, data_type`；PDS 只使用其中 102 个 ARRAY 组合 |
| `observed_reference` | 170 个原始观测组合，保留参考来源 |
| `provenance` | 参考产品、时间窗、指定批次、仅测量决定与去重边界 |

只读发现证据位于 `output/tmp/z622/`：`schema.json`、`partitions.json`、三厂 `reference-*.parquet` 及计划文本、`catalog.parquet`、`monitored.parquet`。这些文件是可重建任务证据，维护工作簿才是运行输入。

## 运行契约

PDS 查询按日限定分区时间，从源时间 2026-10-01 起读取到请求截止日末。每次 SQL 绑定批次前缀和精确的站点参数组合，连接为只读、超时 30 秒，以 50,000 行分块接收；SQL 不增加排序、不用 `LIMIT` 截断完整点位。

字段映射为 `glass_id → sheet_id`、`glass_start_time → start_time`、`sub_equip_id → unit_id`、`value → param_value`，产品／厂别固定为 Z622／ARRAY，Lot 为 Glass 前九位。共享制备在点位修饰及最终 `raw_measurements_df` 之前按完整点位键保留最新记录；原始快照保留源事实和源时间。显示前推、报表当日截止、日期排除、异常过滤和主制程处理仍由原有管线负责。

Z622 元数据只有监控身份和未知规格数值；规格为空值不会转成 0，也不引用 M626 规格。现有能力准入据此排除 CPK／CPM 数值计算。源签名绑定 PDS 来源、批次和实际生效的逻辑监控组合，不绑定企业加密容器字节；监控组合变更后不能命中旧来源快照或把其作为断库回退。签名作为共享发布器的可选附加元数据，随 Parquet／policy／manifest 在原有事务内一起暂存、提交及失败回滚，附加字段不能覆盖原生覆盖和文件身份字段。

企业保护工作簿使用现有解密组件的 `excel-values` 策略：Office 只读打开，禁用宏及链接更新，仅提取配置值，不执行 SaveAs；临时标准表归 `output/decrypted_files/` 并在读取后清理。已用当前受保护的维护工作簿实证读出 102 个 ARRAY 组合。该策略避免为取得表格值触发 Office 重写来源文件，配置签名仍依据逻辑组合。

## 真实导入与快照

实际执行了源时间 2026-10-01 至 2026-11-16 的 47 个日窗查询，完整结果保存为 `output/tmp/z622/pds-import.parquet`，计数见同目录 `import-stats.json`。

| 项目 | 实测结果 |
|---|---:|
| 原始记录 | 14,714 |
| 完整点位键数量 | 14,707 |
| 同点位键的额外复测记录 | 7 |
| Glass 数 | 30 |
| 站点参数组合数 | 33 |

所有记录的 Lot 均为 `L3ZB6BE03`；实际最早源时间为 `2026-11-06 05:21:31`，最晚为 `2026-11-16 15:45:53`。102 个 ARRAY 配置组合是允许读取范围，其中实际有数据的为 33 个，不能将两者混为“导入了 102 个指标”。

已发布共享原始快照 `data/inline_domain/shared/inline_measurements_Z622.parquet`，保留 14,714 条源记录。按现行四天前推和报表当日截止规则投影后返回 8,434 条显示记录；之后共享制备才进行最新点位选择。快照发布证据为 `output/tmp/z622/snapshot-publication.json`。

发布后以 `engine=None` 再次读取，缓存命中仍返回 8,434 条显示记录，验证读取既有新源快照不依赖数据库连接。

该覆盖证明的显示边界为 `2026-08-05` 至 `2026-11-20`，来自既有源时间 8 月 1 日至 11 月 16 日的滚动覆盖与四天前推。它表示来源策略已处理该窗口，不表示实际观测跨越整个窗口，也不表示未来日期被当前页面展示；PDS 业务来源下界仍为 10 月 1 日，实际观测跨度及报表截止分别按上述规则判断。

## 验收清单与当前证据

| 要求 | 证明材料 | 当前核验状态 |
|---|---|---|
| 先分析现有数据源并输出监控配置 | 三厂发现结果、监控工作簿、provenance | 已核对 170／155／102 组合计数 |
| 只读取新品指定批次及组合，全部查询筛选分区时间 | PDS adapter、查询参数契约测试、真实导入计数 | 已通过；47 日窗取得 14,714 条记录 |
| 复测保留最新且不跨指标误删 | raw 保留复测、共享五键去重、捕获事实完整管线测试 | 单元及集成通过；实际 raw 包含 7 条额外复测 |
| 加载、刷新、缓存、回退都使用一致来源 | 组装分发、来源签名、覆盖元数据与原子发布测试 | 单元测试已覆盖；包括 manifest 故障回滚及配置写前变化 |
| Z622 可用且无借用规格／能力值 | 全局启用配置、未知规格元数据、完整报告测试 | 完整管线集成和浏览器正常／空／失败场景通过 |
| 原产品行为不变 | 共享测量／SPC／图表／导出／依赖边界目标回归 | 275 项通过，0 失败、0 错误、0 跳过 |
| E2E 与临时进程清理 | 捕获 PDS 事实的真实管线、浏览器、无 DB 生命周期检查 | 主流程及无 DB 正常／失败／中断清理均通过；所建进程结束、端口释放 |

最终目标回归为 275 项，证据 `output/test-results/z622-spc/regression.xml`；独立完整管线集成为 1 项，证据 `integration.xml`，合计 276 个不同测试通过。源码契约子集另跑 50 项通过，证据 `source-contracts.xml`；这 50 项包含于目标回归，不重复相加。`coverage.json` 中 `spc_repository.py` 为 155／162 条语句覆盖，95.68%，显示四舍五入为 96%。该覆盖率是此适配器文件的语句覆盖率，不代表整个项目覆盖率。

最终逐项完成审计为 `output/test-results/z622-spc/completion-audit.json`：重新读取 JUnit、浏览器结果、受保护工作簿和真实 raw 快照，并现场核实记录中的全部临时进程身份均已结束、监听端口均已释放。工作簿三 Sheet 的内容及 155 个监控组合／170 个观测组合／102 个 ARRAY 组合再次经解密核对，raw 的 14,714 行／14,707 个完整键／30 片 Glass 与导入计数一致。

新增 `tests/unit/inline_domain/infrastructure/spc/repositories/test_z622_repository.py` 的 27 项使用独立工作簿和 fake SQL 传输，不连接数据库。覆盖只读连接、每日上下界、批次、精确组合、分块、不排序不截断、非法及加密工作簿、raw 复测、完整键最新点位、未知规格、缓存／强刷／回退、配置代次失效、来源属性保留、组装刷新分发及发布故障回滚。独立集成测试核验捕获事实经完整管线仍保留原值、选择最新点位、无有效 CPK／CPM，并构建真实三图。

临时服务生命周期证据为 `output/test-results/z622-spc/lifecycle-result.json`：无数据库的最小页面分别完成正常、预期失败和预期中断场景，每场景均记录创建的进程身份及实际监听端口，结束时 `cleaned=true`、存活进程和监听端口清单均为空。该结果证明 runner 的清理边界，不替代新品报表浏览器验收。

浏览器结果为 `output/test-results/z622-spc/result.json`，`passed=true`，覆盖正常、空结果和失败三场景。正常报告返回 8,434 个最终点位、19 个指标；站点 `10140` 下默认选中的 `SE_L1T` 和 `SE_L1T_UNI` 生成 6 幅含真实 trace 的图表，全部 `scrollZoom=false`，浏览器错误清单为空。`Z622-points.csv` 下载包含 8,434 行，与本次最终点位输出一致。`normal.png` 和 `failure.png` 保存页面证据。

最终浏览器重验通过，主流程 `processes.json` 记录所建 Streamlit／Node／Chrome 进程身份及端口 `62001` 的实际监听，结束时 `cleaned=true`、`alive=[]`、`listening_ports=[]`。验证保留了用户生产服务，未执行其 HTTP 验收或重启。正常场景 `pipeline-normal.json` 中的 47 次 SQL 传输是对已捕获 PDS 事实按生产参数重放；缺失的主制程履历使用空 stub，不把此结果扩大为对生产履历源可用性的证明。

可重复验证命令：

```powershell
.venv/Scripts/python.exe -m pytest `
  tests/unit/inline_domain/infrastructure/spc/repositories/test_spc_repository_shared_raw.py `
  tests/unit/inline_domain/infrastructure/spc/repositories/test_z622_repository.py `
  tests/unit/inline_domain/infrastructure/measurement/test_measurement_snapshot_repository.py `
  tests/unit/inline_domain/infrastructure/measurement/test_measurement_retention.py `
  tests/unit/inline_domain/application/spc/test_spc_service.py `
  tests/unit/inline_domain/application/spc/test_spc_recent_window.py `
  tests/unit/inline_domain/application/spc/test_spc_point_policy_cache.py `
  tests/unit/inline_domain/application/spc/test_spc_capability_ledger.py `
  tests/unit/inline_domain/application/spc/test_sheet_membership.py `
  tests/unit/inline_domain/application/spc/test_membership_coverage_composition.py `
  tests/unit/inline_domain/application/shared/test_decorated_features.py `
  tests/unit/inline_domain/test_composition_refresh.py `
  tests/unit/app/sections/spc/test_spc_dashboard.py `
  tests/unit/app/sections/spc/test_spc_export.py `
  tests/architecture/test_backend_dependencies.py `
  -q --junitxml=output/test-results/z622-spc/regression.xml

.venv/Scripts/python.exe -m pytest tests/integration/test_z622_spc_pipeline.py `
  -q --junitxml=output/test-results/z622-spc/integration.xml

.venv/Scripts/python.exe -m pytest `
  tests/unit/inline_domain/infrastructure/spc/repositories/test_spc_repository_shared_raw.py `
  tests/unit/inline_domain/infrastructure/spc/repositories/test_z622_repository.py `
  -q --junitxml=output/test-results/z622-spc/source-contracts.xml `
  --cov=src.inline_domain.infrastructure.spc.spc_repository `
  --cov-report=json:output/test-results/z622-spc/coverage.json

.venv/Scripts/python.exe tests/e2e/run_z622_spc_browser.py --lifecycle-check
.venv/Scripts/python.exe tests/e2e/run_z622_spc_browser.py
```

捕获式完整管线仅替换 SQL 传输和不可用履历读取，真实执行新源 adapter、共享快照／制备、SPC 报告与图表。真实数据库导入与捕获式浏览器验证分别证明查询接入和下游消费，不能用浏览器 fake 传输证明生产数据库当前可用。验证资源、快照、工作簿及临时服务记录均放在 `output/test-results/z622-spc/`，不写入用户维护台账。

设计理由和维护边界由 [ADR-0045](../../../../ADR/0045-z622-spc-pds-source.md) 记录；运行链路由 [SPC 数据流](../../../../../references/domain/inline_domain/spc/data-flow-spc.md) 维护。
