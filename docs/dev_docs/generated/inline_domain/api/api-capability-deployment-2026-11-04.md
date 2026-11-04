# SPC CPK／CPM API 正式部署记录

日期：2026-11-04。正式服务已加载 API 1.2.0，并完成本机真实取数及完整导出验证。

## 1. 授权与配置迁移

用户明确批准先备份，仅新增 CPM 公开政策行、复制 CPK 当前七产品设置，并保留访问地址、凭证和授权范围更新正式服务。

配置为 `resources/inline_domain/monitor/compliance_config.xlsx`；备份为 `compliance_config.xlsx.before-api-cpm-policy-20261104.bak`。第10行新增 `SPC 趋势波动（CPM）`，M626、M678、M673、Z517、Z576、Z553 为 False（允许按业务规则提供），Z571 为 True（隐藏）。两指标此后独立维护。

迁移核对原单元格保留，七产品设置重新加载后一致。证据：[policy-migration.json](../../../../../output/test-results/api-cpk-cpm-20261104/policy-migration.json)。

## 2. 正式服务更新

北京时间09:33，沿用现有托管机制加载新版。正式地址为 `http://10.72.26.31:8890`，监听 `0.0.0.0:8890`，版本1.2.0。旧进程已退出、旧监听已释放，新服务就绪；部署配置原文件字节保持一致，凭证及授权保留。

证据：[deployment.json](../../../../../output/test-results/api-cpk-cpm-20261104/deployment.json)。

## 3. 上周真实结果

通过正式8890接口验证，默认上一完整周为 **2026-W44：2026-10-26～2026-11-01**。

| 查询范围 | 事件 | 明细行数 | complete | 指标值 |
|---|---|---:|---|---:|
| M626 | CPK | 1 | true | 1.14695639973149 |
| M626 | CPM | 1 | true | 1.151287233428474 |

两条均为 ARRAY／15450／M3／OVL1_Y，下限1.33，对象类型capability，发生时间为统计截止日期2026-11-01。必要六字段均完整，与当前本地台账核对一致。

全授权产品查询时，CPK／CPM各返回1条可用明细，`complete=false`；两事件均列出M673来源 `result_unavailable`。该来源缺少对应上周资料，不能将可用部分视为七产品完整统计。

## 4. 真实分页和完整导出

另查询 **2026-W41**，CPK／CPM各2条、实际各请求2页；分页合并结果与不分页完全一致。该历史周测试与上周各1条分开记录。

六组查询均保存CSV／JSON，并核对全部导出行数：

| 文件名前缀 | 每种格式行数 |
|---|---:|
| `spc-cpk-last-week-20261104-093456` | 1 |
| `spc-cpm-last-week-20261104-093456` | 1 |
| `spc-cpk-all-products-last-week-20261104-093456` | 1，可用部分 |
| `spc-cpm-all-products-last-week-20261104-093456` | 1，可用部分 |
| `spc-cpk-real-pagination-20261104-093456` | 2 |
| `spc-cpm-real-pagination-20261104-093456` | 2 |

文件位于 `output/reports/anomaly-api/`。CSV固定17列，旁附 `.csv.meta.json` 保留完整性和失败来源；JSON保留完整响应。文件保存本次实际取得的全部记录，不按20行预览或500行每页限制截断。

证据：[verification.json](../../../../../output/test-results/api-cpk-cpm-20261104/verification.json)。

## 5. 兼容性和验证边界

- 存活、就绪均200；不带密钥访问OpenAPI及业务接口均401。
- 旧默认OOS查询仍为完整空结果：0条、complete=true。
- 同机使用内网地址查询CPM得到相同1条完整结果；另一设备应在该设备运行调用程序验证。
- 本次HTTP验证未新增Office实例；配置迁移创建的Excel实例另核实退出。原始密钥不写入报告或导出。
- 11-03专项367项通过；当日全量3267通过、10项既有失败、3跳过。本次配置部署未改业务代码，不重复执行这些检查，也不将历史全量表述为全绿。

调用方仍需检查 `meta.complete`、失败来源和分页状态；部署成功不代表所有来源当前都可用。
