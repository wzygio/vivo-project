# API 空结果排查与完整下载验证

核实时间：2026-11-03 19:22（上海时区）。查询范围：2026-07-01～2026-11-01，全部授权产品／分类，OOS。

## 1. 排查结论

`failed_sources` 仅列出不可用来源，不能将列表中的 `result_unavailable` 解释成所有分类均读取失败，也不能将其解释成没有超规片。正式接口本次返回 HTTP 200、0 行、`complete=false`，18 个来源组合不可用；SPC 的 7 个产品分别查询均返回 HTTP 200、`complete=true`、0 行。

本次未发现导致 SPC 错误返回空的 API 缺陷。真实工作簿确实存在超规片，但均不满足当前公开查询规则。未调整业务释放决定或 PPA 排除规则。

## 2. SPC 逐层核对

使用实际 `read_api_workbook → ExcelAlarmStore → ExcelAlarmReader(strict_empty=True) → SheetAlertQuery` 链路读取和筛选。源时间均为原生时间类型，7 个产品的看板公开开关均允许公开。

| 产品 | 工作表总行数 | 查询日期内 | 日期内 Flag=False | 命中 PPA 排除 | 最终公开行数 |
|---|---:|---:|---:|---:|---:|
| M626 | 89 | 77 | 0 | 0 | 0 |
| M678 | 200 | 106 | 0 | 0 | 0 |
| Z571 | 0 | 0 | 0 | 0 | 0 |
| M673 | 1389 | 456 | 456 | 456 | 0 |
| Z517 | 136 | 123 | 0 | 0 | 0 |
| Z576 | 203 | 203 | 0 | 0 | 0 |
| Z553 | 28 | 7 | 0 | 0 | 0 |

M673 的 456 条 False 记录全部包含当前独立预警排除项 `PPA`，所以共同查询不输出；其余非空产品的记录全部为 True，按现有规则不释放。上周默认窗口为 2026-10-26～2026-11-01，M673 在该窗口本身没有记录，其余产品仍受 True 决定限制。

## 3. 不可用来源的实际原因

| 来源 | 本次核实情况 | 对请求的影响 |
|---|---|---|
| Yield，7 个产品 | 已发布范围均为 2026-08-01～2026-11-03，发布时间及源数据仍在有效期内；应用服务状态为 `uncovered` | 请求从 7 月开始，超出已发布覆盖范围，7 个组合不可用 |
| Q-Time，7 个产品 | ARRAY 只覆盖 2026-10-01～2026-11-03；OLED／TP 缺少发布元数据 | 请求超出覆盖，且全部厂别查询所需发布资料不齐，7 个组合不可用 |
| M673 的 AOI_TT／AOI_RS、Z576／Z553 的 CTQ | 已核实对应产品工作表均不存在 | 4 个组合沿用既有的来源不可用处理 |

将开始日期改到覆盖范围内可以避免对应的超覆盖条件，但不能补齐缺少的发布资料，也不能改变 Flag 释放结果。API 查询不会补算历史或自动创建来源工作表。

## 4. 下载方式与边界

[查询程序](../../../../../tools/fetch_sheet_anomalies.py) 新增“下载全部明细(CSV)”和“保存完整响应(JSON)”。查询完成后选择保存位置即可。

- 窗口只预览 20 行；CSV 与 JSON 保存本次取得的全部记录，不按 20／500 行截断。
- 可选分页会自动取完所有页，未完成分页时不提供导出；仍受 API 原有容量、超时和查询范围约束。
- 部分成功显示“下载可用部分(CSV)”，默认文件名含 `partial`。CSV 旁的 `.csv.meta.json` 保存完整性、失败来源及实际条数，应一并保留。
- CSV 使用 UTF-8 BOM、固定 11 个公开字段；潜在公式文本加单引号保护。Excel 导入时将站点和对象编号设为文本，保留前导零；原始公开值可读取 JSON。
- 在其它 Python 程序中可调用 `save_records_csv(result, Path(...))`，示例见[调用说明](../../../../../references/domain/inline_domain/api/api-caller-guide-2026-11-03.md#appendix-f-保存结果及自动运行)。

同时修复了客户端的新查询参数错误或请求失败后，仍可能导出上一次结果的问题：现在新查询开始即清空旧结果并禁用下载。

## 5. 验证与证据

- 客户端、API 单元及真实临时工作簿 HTTP 集成：**237 项通过**。命令：`.venv/Scripts/python.exe -m pytest tests/unit/test_fetch_sheet_anomalies.py tests/integration/test_anomaly_api_client.py tests/integration/test_anomaly_api_files.py tests/unit/api -q --tb=short`。
- 客户端专项 **74 项通过，覆盖率 89%**。合成的 1205 行结果在完整／部分成功两条路径均完整保存为 CSV 和 JSON，超过窗口预览和每页上限。
- 正式 API 只读复测及 CSV 导出成功，本次真实数据为 0 行、18 个不可用来源；没有将夹具记录作为真实生产取数证据。
- 未启动临时 HTTP 服务；正式 API 未停止、重启或改变监听。工作簿均通过只读路径访问，未执行决定维护或重新发布。

诊断证据位于 `output/test-results/api-spc-investigation-20261103/`：

| 文件 | 内容 |
|---|---|
| `reader-aggregate.json` | SPC 各层聚合计数 |
| `policy-and-live-spc-counts.json` | 公开开关及 7 次正式 SPC 查询摘要 |
| `failed-source-diagnosis.json` | Yield／Q-Time 发布范围及正式接口复测 |
| `missing-inline-sheets.json` | 4 个 Inline 失败组合的工作表存在性 |
| `targeted-junit.xml` | 237 项验证结果 |

诊断脚本没有写入源工作簿。一次只读前后源字节摘要不同，原因未归因；这份记录证明当次读取快照，不将其表述为源文件字节完全不变。后续生产更新会改变实际行数、发布覆盖及完整性状态。
