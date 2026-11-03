# 北极星异常ID API 调用说明

适用 Python 3.11+，无需安装第三方库。更新日期：2026-11-03。

## 1. 复制到 Python 程序中调用

1. 将 [fetch_sheet_anomalies.py](../../../../tools/fetch_sheet_anomalies.py) 放在你的调用程序**同一文件夹**。
2. 将下面的完整示例保存为 `call_api.py`，在 Python IDE 中运行这个文件。按提示输入维护者提供的原始 API Key；也可在程序运行环境中设置 `API_KEY`。
3. 修改 `params` 选择查询范围，参数见下一节。返回的 `records` 就是供你程序处理的明细列表。

```python
import os
from getpass import getpass
from fetch_sheet_anomalies import ApiCallError, fetch_anomalies

base_url = "http://10.72.26.31:8890"
params = {}  # 全部已授权产品和分类；OOS；上一完整周；默认不分页
api_key = os.environ.get("API_KEY") or getpass("请输入原始 API Key：")

try:
    result = fetch_anomalies(base_url, api_key, params=params)
except ApiCallError as exc:
    print(f"调用失败：{exc}")
    raise SystemExit(1)
finally:
    del api_key

records = result["data"]
print(f"收到 {len(records)} 条明细")

if not result["meta"]["complete"]:
    print("部分来源不可用，当前收到的只是可用部分：")
    print(result["meta"]["failed_sources"])

for record in records[:3]:  # 仅打印前 3 条预览；records 中保留全部返回明细
    print(record)
```

在 API 所在主机上调用时，`base_url` 也可填 `http://127.0.0.1:8890`。密钥必须用原始值，不是摘要；不要写进共享代码或日志。若你的系统已有密钥配置方式，直接将配置值传给 `fetch_anomalies` 即可。

要查询**上周 CPK 不达项**，只需将上面示例的 `params` 改为以下内容；查询 CPM 时把 `CPK` 换成 `CPM`，其余程序不变：

```python
params = {"sub_domain": "SPC", "event_type": "CPK"}
```

## 2. 查询参数

**所有筛选都可省略。** 在示例的 `params` 字典里填写需要的条件；多个条件同时满足才会返回。不要用空字符串表示全选，直接不填该字段。

| 参数 | 用途／示例 | 不填时 |
|---|---|---|
| `product` | 产品，例如 `"M626"` | 全部已启用且获授权产品 |
| `sub_domain` | 分类：`SPC`、`CTQ`、`AOI_TT`、`AOI_RS`、`Yield`、`Q-Time`，注意大小写 | 全部授权分类 |
| `event_type` | `OOS` 超规格；`OOC` 超控制限；`CPK`／`CPM` 能力不达项（仅 SPC） | `OOS` |
| `start_date` | 开始日期，例如 `"2026-10-26"`，包含当天 | 上周一 |
| `end_date` | 截止日期，例如 `"2026-11-01"`，包含当天 | 上周日 |
| `factory` | 厂别，例如 `"ARRAY"`、`"OLED"`、`"TP"` | 全部授权厂别 |
| `step_id` | 发生站点，例如 `"00100"`，保留前导零 | 全部站点 |
| `layer` | 层别，例如 `"PI2"` | 全部层别 |
| `parameter_name` | 监控参数名称 | 全部参数 |
| `entity_type` | `"sheet"`、`"lot"` 或 `"capability"`（能力周期项目） | 全部对象类型 |
| `entity_id` | 对象编号，按原字符串填写 | 全部对象 |

例如，只查 M626 的 SPC，并指定日期：把示例中的 `params = {}` 替换为以下内容，其余代码不变。示例日期可按需修改。

```python
params = {
    "product": "M626",
    "sub_domain": "SPC",
    "start_date": "2026-10-26",
    "end_date": "2026-11-01",
}
```

- 日期按上海时区计算，包含首尾两天；建议开始、截止一起填。只填一项时，另一项仍使用对应默认日期。
- CPK／CPM 只返回日期范围内**完整包含且已经结束的 ISO 周**，不将整周统计值拆成几天。日期留空即上一完整周；只选周中几天时没有可返回的完整周。
- 文本条件精确匹配。Yield、Q-Time 只支持 OOS；Yield 的站点和层别为 `null`，不匹配站点／层别筛选。
- 不必手动处理分页。确需分批获取时，给函数增加 `page_size=100`，它会自动取完；不要把 `limit` 或 `cursor` 放进 `params`。详见 Appendix C。

以下为按需查阅的补充内容，首次接入只需上面两节。

## Appendix A 返回字段与完整性

业务列是固定白名单：

| 字段 | 含义 |
|---|---|
| 分类 | SPC／CTQ／AOI_TT／AOI_RS／Yield／Q-Time |
| 发生站点 | 原站点代码；Q-Time 取起点；仅 Yield 可为 `null` |
| 部门 | 真实厂别；Yield使用已发布factory，不能为空 |
| 层别 | Yield固定为`null`；其他来源取工序描述首个下划线之前的前缀，不能为空 |
| 监控参数 | 参数名、TT名称、RS Code、Yield异常Code或Q-Time路径名 |
| 异常事件 | OOS／OOC／CPK／CPM，独立于命中边界 |
| 对象类型／对象编号 | sheet／lot 与原始编号；capability 与该能力周期项目的稳定编号 |
| 产品型号 | 每条记录所属产品原代码 |
| 发生时间 | OOS／OOC 的 Inline／Q-Time 为带时区时间；Yield 为入库日期；CPK／CPM 为统计截止日期 `YYYY-MM-DD` |
| 命中边界 | 已发布的 USL／LSL／UCL／LCL 等，可空 |

Yield 只有日期精度，返回日期而不是虚构零点事件时间。一片在多个站点／参数上可以多条，`returned_count` 是本页明细行数，不是去重异常片数。

CPK／CPM 每行代表一个产品、厂别、站点、参数、指标和统计周的能力项目，`对象类型=capability`，不是一片或一个 Lot；`发生时间` 表示统计截止日期。能力记录另有以下六列，原有片／Lot 记录不新增这些 JSON 字段：

| 字段 | 含义 |
|---|---|
| 指标值 | 已维护且公开的 CPK 或 CPM 数值 |
| 达标下限 | 当前为 1.33；指标值严格低于它才是不达项 |
| 统计周期类型 | 当前为 `week` |
| 统计周期 | ISO 周标签，例如 `2026-W44` |
| 统计开始日期／统计截止日期 | 该周周一／周日，例如 `2026-10-26`／`2026-11-01` |

`capability:` 开头的对象编号由公开业务键稳定生成，供精确查询／去重；不代表 Sheet ID。只提供有明确释放决定且符合既有跨度、日期及公开规则的项目，查询不会重新计算能力值。缺少周期台账资料仍可能导致来源不可用。

响应外层为 `data` 和 `meta`。`data` 是明细列表，`meta` 说明本次查询：

| meta 字段 | 含义 |
|---|---|
| `complete` | 所选来源是否全部读取成功；每次先检查它 |
| `failed_sources` | 不可用的产品、分类和事件清单 |
| `status` | `ok` 表示公开结果为空，`alert` 表示存在异常明细 |
| `returned_count` | 本页明细行数 |
| `has_more`／`next_cursor` | 是否还有下一页，以及下一页凭条 |
| `period_start`／`period_end_exclusive` | 实际查询的开始时刻及不包含的结束时刻 |
| `timezone` | 时区，当前为 `Asia/Shanghai` |
| `read_at` | 此次取数时间，不是源数据更新时间 |
| `limitations` | 接口提供的数据限制说明列表 |

**每次先检查meta.complete。** complete=true表示选中来源均读取成功，failed_sources为空；complete=false表示部分来源不可用，data只含可用来源。status=ok／alert描述已返回记录是否为空，不能替代完整性判断。失败来源逐项列出product、sub_domain、event_type和固定code=result_unavailable，不包含路径或内部诊断。例如：

```json
{"complete": false, "failed_sources": [{"product": "PRODUCT_DEMO", "sub_domain": "Yield", "event_type": "OOS", "code": "result_unavailable"}]}
```

上面展示的是meta中的部分字段。即使data为空，complete=false也不能解释为全部来源均无异常。全部选中来源均失败时返回503，error.failed_sources列出失败来源。请求级配置错误、超时、容量或限流失败仍返回相应错误，不能保证逐来源清单。

以下是虚构的完整响应，仅用于认识格式，不代表生产数据：

```json
{
  "data": [
    {
      "分类": "SPC",
      "发生站点": "00100",
      "部门": "ARRAY",
      "层别": "PI2",
      "监控参数": "示例参数",
      "异常事件": "OOS",
      "对象类型": "sheet",
      "对象编号": "DEMO_SHEET_001",
      "产品型号": "M626",
      "发生时间": "2026-10-28T10:00:00+08:00",
      "命中边界": "USL"
    }
  ],
  "meta": {
    "status": "alert",
    "returned_count": 1,
    "has_more": false,
    "next_cursor": null,
    "period_start": "2026-10-26T00:00:00+08:00",
    "period_end_exclusive": "2026-11-02T00:00:00+08:00",
    "timezone": "Asia/Shanghai",
    "read_at": "2026-11-03T09:00:00+08:00",
    "limitations": [],
    "complete": true,
    "failed_sources": []
  }
}
```

`period_end_exclusive` 是不包含的结束时刻；上例表示包含 11 月 1 日全天。`null` 是 JSON 空值，不是字符串 `"NULL"`。`complete` 与 `has_more` 要分别检查：取到最后一页仍可能只是部分来源的结果。

## Appendix B 接口与支持范围

| 项目 | 约定 |
|---|---|
| 业务地址 | `http://10.72.26.31:8890/api/v1/sheet-anomalies`，地址变化时向维护者确认 |
| 方法 | `GET`，不提交请求体 |
| 凭证 | 请求头 `X-API-Key`，填写分配给本调用方的原始密钥 |
| 返回格式 | JSON，包含 `data` 明细及 `meta` 查询说明 |
| 接口结构 | `GET /openapi.json`，需要同样的密钥；没有 `/docs` 或 `/redoc` 页面 |

开始前取得维护者提供的地址、原始密钥及授权范围。密钥通过企业批准的凭据渠道交付，不放进 URL、截图、共享文档或日志；不要用服务端摘要代替原始密钥。

本接口只读取已发布的异常结果，不主动刷新报表或重建历史。授权包含某来源，并不保证它此刻可用。支持范围如下：

| `sub_domain` |`event_type` | 对象类型 |
|---|---|---|
| `SPC` | `OOS`／`OOC` | `sheet` |
| `SPC` | `CPK`／`CPM` | `capability`，周统计项目 |
| `CTQ` | `OOS`／`OOC` | `sheet` |
| `AOI_TT` | `OOS`／`OOC` | `sheet` |
| `AOI_RS` | `OOS`／`OOC` | `sheet`／`lot`，保留原 By Sheet／By Lot |
| `Yield` | `OOS` | `lot` |
| `Q-Time` | `OOS` | `lot` |

不包含其它趋势、IJP 比例汇总、备件、IQC、图像和原始量测点。Lot 报警不会展开成该 Lot 所有片都报警。默认事件仍为 OOS，CPK／CPM 需显式指定，两个指标分别查询。

## Appendix C 可选分页

分页是把同一查询的明细分成多次返回，便于分批传输和处理。**本接口不强制分页**：不传 `limit` 就尝试一次返回完整结果，也不需要 `cursor`。初次调用和普通取数可以不分页；本节保留可选协议，供有分批处理需求的系统使用。

分页不是后台读取 Excel 的分批开关，也不能绕过公开结果总容量及超时限制。新查询程序默认不分页；填写“每页行数”后会自动取完并合并结果，无需手动复制游标。

如果一次查询多个产品、多个分类，服务会先合并它们的异常明细，统一按发生时间等字段倒序排列，再按 `limit` 分页。

例如合并后共有 23 行，`limit=10`：第一页最多 10 行，第二页最多 10 行，第三页剩余 3 行。**每页 10 行是全部所选来源合计 10 行。** 第一页可能同时包含 M626 的 SPC 和 M678 的 Yield。

翻页步骤如下：

1. 第一次请求带 `limit`，不带 `cursor`。
2. 如果 `has_more=true`，取出 `next_cursor`。
3. 保持原查询条件和 `limit`，把这个值作为下一次的 `cursor`。构造网址时对它做 URL 编码。
4. 重复到 `has_more=false`，合并每页 `data`，并检查完整性。

游标是临时凭条，不是页码，默认有效期 300 秒。不要编辑它或跨查询使用；收到 410 时丢弃尚未完成的整组分页数据，从第一页重新取。服务重启后旧游标也可能报 422，需确认条件后重新开始。

游标绑定调用方、授权、实际查询范围、公开结果和失败来源状态。结果或范围改变、快照淘汰、过期等情况可能需要重新查询。429 时按响应头 `Retry-After` 等待，不要快速无限重试。

本文调用函数的分页写法如下。它会自动传递原始接口的 `limit` 和 `cursor`，返回合并后的完整明细：

```python
result = fetch_anomalies(base_url, api_key, params=params, page_size=100)
```

这段是第 1 节函数调用行的替换写法，应放在该示例的 `try` 内；不要在示例最后追加，因为那里已经清除了密钥变量。函数遇到分页失效、重复游标或查询范围变化时会抛出 `ApiCallError`，不会返回未完成的半组分页。

## Appendix D 状态码与问题反馈

| 状态 | 含义及处理 |
|---|---|
| 200，complete=true，status=alert | 全部选中来源可用，公开范围存在异常明细 |
| 200，complete=true，status=ok，空数组 | 有效查询没有可提供的异常；不等于原始源数据刚完成更新 |
| 200，complete=false | 部分来源不可用；核对failed_sources，不能将data视为完整结果 |
| 401 | 检查是否提交有效 `X-API-Key` |
| 403 | 检查产品、厂别、分类授权和启用产品 |
| 410 | 分页快照失效，从第一页重查 |
| 422 | 检查参数名、值、OOC适用分类及游标 |
| 429 | 请求过频或读取槽位忙，按 `Retry-After` 等待 |
| 503 | 全部选中来源不可用，或请求级超时／容量／配置问题；检查安全错误码及可用的failed_sources |
| 500 | 未预期故障，记录 `X-Request-ID`，交由维护者排查 |

提供测试时间、调用地址及条件、HTTP 状态码、失败来源和请求编号；隐藏密钥和完整请求头。程序导出的 JSON 包含 `data`、`meta` 及客户端说明 `client`：`client.pages_fetched` 为实际请求页数，`client.request_ids` 用于排查。合并分页后，`meta.returned_count` 表示已取得的总明细行数。

```text
测试时间（北京时间）：
使用本机地址还是其它设备：
API 地址、产品、分类、事件及日期：
HTTP 状态码／程序提示：
complete、returned_count、failed_sources：
请求编号：
预期对象编号与实际结果：
```

部分来源失败和空结果的处理见本附录上方表格。客户端只读服务，不会补算数据、修改工作簿或恢复不可用来源；服务维护见部署说明。

## Appendix E 本机与其它设备测试

| 执行位置 | base_url | 能验证什么 |
|---|---|---|
| API 所在的当前主机 | `http://127.0.0.1:8890` | 本机真实 HTTP、鉴权及查询，不需要模拟服务 |
| 当前主机使用内网地址 | `http://10.72.26.31:8890` | 本机到内网监听入口 |
| 另一台内网设备 | `http://10.72.26.31:8890` | 额外验证跨设备网络可达 |

只需修改第 1 节的 `base_url`，运行同一份 Python 程序即可。当前主机访问内网地址成功，不能代替另一台设备的网络验证。

如需人工查看结果，可运行 [fetch_sheet_anomalies.py](../../../../tools/fetch_sheet_anomalies.py) 的图形窗口；项目电脑也可双击 [查询窗口入口](../../../../query_anomaly_api.cmd)。填地址和密钥，选择条件后点击“查询完整结果”，再点击“下载全部明细(CSV)”选择保存位置；也可用“保存完整响应(JSON)”保留查询说明。高级筛选请使用第 1 节的函数调用。

窗口只预览前 20 行，下载保存本次查询已取得的**全部明细**，不按 20 行或每页 500 行截断。“每页行数”留空为一次取全；填写后程序自动取完所有页再允许下载。部分来源失败时，按钮显示“下载可用部分(CSV)”，文件名默认含 `partial`；CSV 旁的同名 `.csv.meta.json` 记录实际条数、完整性和失败来源。不能把可用部分当成完整统计。

### HTTP 200，但返回 0 条

`complete=true` 且 `data=[]` 表示本次完整查询没有可提供的明细，不是调用失败，也不需要继续翻页；`complete=false` 表示部分来源不可用，即使数组为空也不能解释为全部来源均无异常。

旧示例固定查询 M626／SPC／OOS／上一完整周。2026-11-03 的默认日期为 2026-10-26 至 2026-11-01，不能据此判断其它产品和分类。已有正式测试复现了这一空结果，详细范围及限制见 [调用测试记录](../../../../docs/dev_docs/generated/inline_domain/api/api-client-delivery-2026-11-03.md)。

若要核对一条预期异常，请维护者提供当前应可查询的产品、分类、日期和对象编号，再填入 `params`。接口只读已发布结果，不会因查询而刷新报表或补算历史。

## Appendix F 保存结果及自动运行

取得 `result` 后，可在你的 Python 程序里保存 JSON：

```python
from fetch_sheet_anomalies import default_output, save_result

output_path = save_result(result, default_output())
print(f"已保存：{output_path}")
```

默认输出至程序对应目录的 `output/reports/anomaly-api/`，文件名带时间；也可将自己的 `pathlib.Path` 传给 `save_result`，不会覆盖已有文件。结果文件不含请求密钥，仍应按企业数据管理要求保管。

需要可用 Excel 打开的明细文件时，在取得 `result` 后保存 CSV：

```python
from pathlib import Path
from fetch_sheet_anomalies import save_records_csv

output_path = save_records_csv(result, Path("异常明细.csv"))
print(f"已保存：{output_path}")
```

CSV 包含原有 11 个业务列及 6 个能力字段、全部返回行；普通片／Lot 行的能力列为空。另生成 `异常明细.csv.meta.json` 说明文件，目标文件已存在时请改名，不会覆盖。导出不另设行数上限，仍受 API 原有容量及超时限制。CSV 使用 UTF-8 BOM；用 Excel 的“数据 → 从文本/CSV”导入时，将“发生站点”和“对象编号”设为文本，避免 Excel 自动去掉前导零或改成科学计数法。可能被表格软件识别为公式的文本会加单引号保护，原始公开值可从 JSON 取得。

自动任务不需要交互输入时，在程序运行环境中设置 `API_KEY`，第 1 节会优先读取它。`.env` 文件和进程环境变量不是同一回事，示例不会自动加载 `.env`；若你的系统已经从 `.env` 或其它配置中心读取了密钥，将其值直接传给函数即可。

导出结构为 `data`、`meta` 和 `client`。`client.pages_fetched` 为实际请求页数，`client.request_ids` 为排查用请求编号；使用自动分页时，`meta.returned_count` 是合并后的总明细行数。

## Appendix G 部署、架构与验证记录

- [API 服务部署说明](guide-sheet-anomaly-api.md)：服务安装、启动和来源准备。
- [API 程序介绍](architecture-sheet-anomaly-api.md)：架构、缓存及运行边界。
- [调用测试记录](../../../../docs/dev_docs/generated/inline_domain/api/api-client-delivery-2026-11-03.md)：已验证内容与正式数据限制。
