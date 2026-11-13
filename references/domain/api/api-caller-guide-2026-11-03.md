# 北极星异常ID API 调用说明

适用 Python 3.11+，无需安装第三方库。更新日期：2026-11-13。

**测试脚本：** 在测试脚本“fetch_sheet_anomalies.py”所在路径下，打开 PowerShell，执行以下命令即可看到可视化调试窗口：

```powershell
uv run --no-sync python fetch_sheet_anomalies.py
```

不传参数即打开查询窗口，可填写条件、查询和下载结果。密钥优先从进程环境变量 `API_KEY`／`VIVO_API_KEY` 取得，也可在窗口手动输入；此命令不会自动加载项目 `.env`。`--no-sync` 使用已准备好的项目环境，不在启动时同步依赖。需要在其它 Python 程序中调用时，按下面的示例导入函数即可。

## 1. 复制到 Python 程序中调用

1. 将 [fetch_sheet_anomalies.py](../../../tools/fetch_sheet_anomalies.py) 放在你的调用程序**同一文件夹**。
2. 将下面的完整示例保存为 `call_api.py`，在 Python IDE 中运行这个文件。按提示输入维护者提供的原始 API Key；也可在程序运行环境中设置 `API_KEY`。
3. 修改 `params` 选择查询范围，参数见下一节。返回的 `records` 就是供你程序处理的明细列表。

```python
import os
from getpass import getpass
from fetch_sheet_anomalies import ApiCallError, fetch_anomalies

base_url = "http://10.72.26.31:8890"
params = {}
api_key = os.environ.get("API_KEY") or getpass("API Key: ")

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

本文适用 API 1.4.0。CPK／CPM 的 `对象编号` 返回 Sheet ID 列表；普通片／Lot 仍返回一个编号字符串。取得结果后可直接处理列表：

```python
for record in records:
    if record["对象类型"] == "capability":
        sheet_ids = record["对象编号"]
        print(record["统计周期"], len(sheet_ids), sheet_ids)
```

## 2. 查询参数

**所有筛选都可省略。** 在示例的 `params` 字典里填写需要的条件；多个条件同时满足才会返回。

直接发起 HTTP 请求时，所有已定义的查询参数都将空字符串和纯空白值视为“未填写”。例如，调试器中勾选 `parameter_name` 但值留空，与取消勾选相同；空日期沿用各自的上周默认日期，空 `limit` 表示不分页，空 `cursor` 表示从首次查询开始。有值的游标仍须同时提供有效 `limit`。此规则不适用于鉴权请求头，`X-API-Key` 仍须有效。

本文使用的 `fetch_sheet_anomalies.py` 封装函数保留客户端的非空字符串校验；在其 `params` 字典中，继续直接省略不需要的字段。原始 HTTP 请求及最简版 `call_anomaly_api.py` 可以直接发送空白查询值。

| 参数 | 用途／示例 | 不填时 |
|---|---|---|
| `product` | 产品，例如 `"M626"` | 全部已启用且获授权产品 |
| `sub_domain` | 分类：`SPC`、`CTQ`、`AOI_TT`、`AOI_RS`、`Yield`、`Q-Time`，注意大小写 | 全部授权分类 |
| `event_type` | `OOS` 超规格；`OOC` 超控制限；`CPK`／`CPM` 能力不达项（仅 SPC） | 全部适用事件：SPC 四类；CTQ／AOI 两类；Yield／Q-Time 仅 OOS |
| `start_date` | 开始日期，例如 `"2026-10-26"`，包含当天 | 上周一 |
| `end_date` | 截止日期，例如 `"2026-11-01"`，包含当天 | 上周日 |
| `factory` | 厂别，例如 `"ARRAY"`、`"OLED"`、`"TP"` | 全部授权厂别 |
| `step_id` | 发生站点，例如 `"00100"`，保留前导零 | 全部站点 |
| `layer` | 层别，例如 `"PI2"` | 全部层别 |
| `parameter_name` | 监控参数名称 | 全部参数 |
| `entity_type` | `"sheet"`、`"lot"` 或 `"capability"`（能力周期项目） | 全部对象类型 |
| `entity_id` | 单个 Sheet ID／Lot ID；CPK／CPM 按该 Sheet 是否在项目名单中筛选，返回完整名单 | 全部对象 |

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
- 省略 `event_type` 即全选；直接 HTTP 请求传空字符串或纯空白也等同省略，不要传 `ALL` 或列表。只需要超规明细时填写 `"event_type": "OOS"`；只需要能力不达项时分别填写 CPK 或 CPM。混合结果用每行的 `异常事件` 区分；分页、返回行数和失败来源都包含全部选中事件。
- 不必手动处理分页。确需分批获取时，给函数增加 `page_size=100`，它会自动取完；不要把 `limit` 或 `cursor` 放进 `params`。详见 Appendix C。

以下为按需查阅的补充内容，首次接入只需上面两节。脚本各函数的用途、参数和使用方式见 [Appendix G 函数参考](#appendix-g-查询脚本函数参考)。

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
| 对象类型 | `sheet`：片；`lot`：批次；`capability`：一个统计周的能力项目 |
| 对象编号 | 片／Lot 为原始编号字符串；CPK／CPM 为该项目统计周的全部 Sheet ID 数组，例如 `["00001", "00002"]` |
| 产品型号 | 每条记录所属产品原代码 |
| 发生时间 | OOS／OOC 的 Inline／Q-Time 为带时区时间；Yield 为入库日期；CPK／CPM 为统计截止日期 `YYYY-MM-DD` |
| 命中边界 | 明确的 USL／LSL／USL/LSL／UCL／LCL／UCL/LCL 保留；LOWER、NULL、空值或缺省统一为 `spec` |

Yield 只有日期精度，返回日期而不是虚构零点事件时间。一片在多个站点／参数上可以多条，`returned_count` 是本页明细行数，不是去重异常片数。

CPK／CPM 每行代表一个产品、厂别、站点、参数、指标和统计周的能力项目，`对象类型=capability`，不是一片或一个 Lot；`发生时间` 表示统计截止日期。能力记录另有以下六列，原有片／Lot 记录不新增这些 JSON 字段：

| 字段 | 含义 |
|---|---|
| 指标值 | 已维护且公开的 CPK 或 CPM 数值 |
| 达标下限 | 当前为 1.33；指标值严格低于它才是不达项 |
| 统计周期类型 | 当前为 `week` |
| 统计周期 | ISO 周标签，例如 `2026-W44` |
| 统计开始日期／统计截止日期 | 该周周一／周日，例如 `2026-10-26`／`2026-11-01` |

CPK／CPM 的片号取自 SPC 报表 By Sheet 绘图使用的制备点位，按同产品、厂别、站点、参数和统计周筛选，去重并按文本升序排列，保留前导零。名单包含该周的全部有效片号，不限于单片 OOS；名单中的每片不因此被判为单片异常。按 `entity_id` 查询时仍返回项目的完整名单。缺少台账或匹配的完整成员资料时，该来源不可用。

`spec` 表示未细分边界方向的规格／达标要求不满足，CPK／CPM 低于达标下限也使用它。它不是数值；具体能力下限看 `达标下限`。USL／LSL 表示规格上限／下限，UCL／LCL 表示控制上限／下限，带 `/` 的值表示来源同时标记两种边界。

**从 1.2 升级：** CPK／CPM 不再返回 `capability:…` 字符串；调用方需要把它的 `对象编号` 当作列表处理。JSON 导出保留数组；CSV 每个能力项目仍占一行，片号单元格是标准 JSON 文本，Python 可用 `json.loads(cell)` 还原全部片号。

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

不包含其它趋势、IJP 比例汇总、备件、IQC、图像和原始量测点。Lot 报警不会展开成该 Lot 所有片都报警。默认查询全部适用事件，也可以显式选择单一事件。来自不同事件的同一片可能有多行，不要只用对象编号去重。

## Appendix C 可选分页

分页是把同一查询的明细分成多次返回，便于分批传输和处理。**本接口不强制分页**：不传 `limit` 就尝试一次返回完整结果，也不需要 `cursor`。初次调用和普通取数可以不分页；本节保留可选协议，供有分批处理需求的系统使用。

分页不是后台读取 Excel 的分批开关，也不能绕过公开结果总容量及超时限制。新查询程序默认不分页；填写“每页行数”后会自动取完并合并结果，无需手动复制游标。CPK／CPM 按能力项目行分页，`returned_count` 也是项目行数，不是名单内的片数；一个项目的片号列表不会拆页或静默截短。

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
| 503 | `read_timeout` 表示已连接但结果读取超时，可缩小产品／分类后重试；其它情况包括全部来源不可用、容量或配置问题，检查错误码及可用的 `failed_sources` |
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

如需人工查看结果，按文档开头的 `uv run` 指令运行 [fetch_sheet_anomalies.py](../../../tools/fetch_sheet_anomalies.py)。填地址和密钥，选择条件后点击“查询完整结果”，再点击“下载全部明细(CSV)”选择保存位置；也可用“保存完整响应(JSON)”保留查询说明。高级筛选请使用第 1 节的函数调用。

窗口只预览前 20 行，下载保存本次查询已取得的**全部明细**，不按 20 行或每页 500 行截断。“每页行数”留空为一次取全；填写后程序自动取完所有页再允许下载。部分来源失败时，按钮显示“下载可用部分(CSV)”，文件名默认含 `partial`；CSV 旁的同名 `.csv.meta.json` 记录实际条数、完整性和失败来源。不能把可用部分当成完整统计。

### HTTP 200，但返回 0 条

`complete=true` 且 `data=[]` 表示本次完整查询没有可提供的明细，不是调用失败，也不需要继续翻页；`complete=false` 表示部分来源不可用，即使数组为空也不能解释为全部来源均无异常。

旧示例固定查询 M626／SPC／OOS／上一完整周。2026-11-03 的默认日期为 2026-10-26 至 2026-11-01，不能据此判断其它产品和分类。此前正式测试复现了这一空结果；它仅说明当时的筛选范围，不能代替当前查询。

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

自动任务不需要交互输入时，在程序运行环境中设置 `API_KEY`，第 1 节会优先读取它。可以在 Python IDE 的运行配置中设置一次，以后不必手动输入。配套查询窗口和命令行也优先读取 `API_KEY`，兼容旧名称 `VIVO_API_KEY`；窗口自动填入时用星号显示，查询后清空输入框，后续查询仍可从环境变量自动取得。密钥不会保存到导出或程序配置文件。

`.env` 文件和进程环境变量不是同一回事，示例、独立调用程序和上述 `uv run` 指令不会自动加载 `.env`；若你的系统已经从自己的 `.env` 或其它配置中心读取了密钥，将其值直接传给函数即可。不要把项目 `.env` 或密钥发给同事，独立脚本使用对方自己的凭证。

自动填入发生在调用程序，HTTP 每次仍须携带 `X-API-Key`；服务端不会给未提供密钥的请求代填默认值。

导出结构为 `data`、`meta` 和 `client`。`client.pages_fetched` 为实际请求页数，`client.request_ids` 为排查用请求编号；使用自动分页时，`meta.returned_count` 是合并后的总明细行数。

## Appendix G 查询脚本函数参考

本节对应 [fetch_sheet_anomalies.py](../../../tools/fetch_sheet_anomalies.py) 的全部函数和方法。导入模块不会打开窗口或发送请求。业务程序通常只需 `fetch_anomalies`，需要保存时再调用两个导出函数；函数前的 `_` 表示内部辅助功能。

### G.1 常用函数速查

| 函数 | 功能及使用方式 | 返回值 |
|---|---|---|
| `fetch_anomalies(base_url, api_key, params=None, *, page_size=None, timeout=120, opener=None, max_pages=10000)` | 发出 HTTP GET，校验响应；指定分页时自动取完并合并。详细参数见下一节 | 包含 `data`、`meta`、`client` 的字典 |
| `configured_api_key()` | 从当前进程环境中优先读 `API_KEY`，其次读 `VIVO_API_KEY`；不读取 `.env`，不弹窗 | 密钥字符串；未配置则为 `""`，不要打印它 |
| `result_summary(result)` | 对已取得的结果生成一句中文说明，区分完整、有缺失来源和完整空结果 | 说明字符串；本身不打印 |
| `default_output()` | 生成默认 JSON 保存路径；本身不创建目录或文件 | `Path`，形如脚本所在项目根目录下的 `output/reports/anomaly-api/anomalies-时间戳.json` |
| `save_result(result, path)` | 将全部已取得结果保存为 UTF-8 JSON，包含完整性及客户端信息；详见 Appendix F | 实际保存的 `Path` |
| `save_records_csv(result, path)` | 保存全部已取得明细及同名 `.csv.meta.json` 说明；固定 17 列，数组写成 JSON 文本 | CSV 的 `Path` |
| `validate_base_url(value)` | 校验 HTTP／HTTPS 服务基础地址，去掉首尾空白及末尾 `/`；地址中不能带账号密码、查询条件或片段 | 规范后的地址字符串；无效则抛出 `ApiCallError` |
| `direct_opener()` | 构造默认 HTTP 连接器：直连、不使用环境代理、不跟随重定向，HTTPS 使用正常证书校验；通常无需手动调用 | 供 `fetch_anomalies(opener=...)` 使用的连接器 |

`save_result` 和 `save_records_csv` 不会再次请求 API，不受窗口 20 行预览限制。它们拒绝尚有后续页的结果，但允许保存 `complete=false` 的可用部分，并保留失败来源说明。父目录会自动创建，已有文件不会覆盖；路径没有强制后缀，应自行使用 `.json`／`.csv`。只调用导出函数时不会自动给文件名加 `partial`，请先检查完整性。

### G.2 查询函数参数与行为

前三个参数可以按位置传入；`*` 后的参数必须写名称，例如 `timeout=60`。

| 参数 | 填写方式 | 默认值及约束 |
|---|---|---|
| `base_url` | 服务基础地址，例如 `"http://10.72.26.31:8890"` | 必填；不要附加 `/api/v1/sheet-anomalies`，函数会自动拼接 |
| `api_key` | 原始密钥字符串 | 必填；非空，最长 1024 个字符，仅允许无空格的可打印 ASCII 字符 |
| `params` | 第 2 节的筛选字典，例如 `{"sub_domain": "SPC"}` | `None` 或 `{}` 为默认查询；键只能是第 2 节列出的筛选字段，值须为非空字符串；不要传 `limit` 或 `cursor` |
| `page_size` | 单次请求的最多明细行数，例如 `100` | `None` 表示不分页；设置时须为整数 1～500。函数仍会自动取得所有页 |
| `timeout` | 每次 HTTP 请求的超时秒数 | `45`；可填 1～120 的整数或小数，是单次请求限制，不是整个分页过程的总时长 |
| `opener` | 自定义连接器，提供 `open(Request, timeout=...)` 方法 | `None` 使用 `direct_opener()`；主要用于测试或明确需要自定义网络访问的场景 |
| `max_pages` | 最多请求多少页 | `10000`；须为正整数。达到上限仍有后续页时抛错，不返回前几页冒充完整结果 |

函数不弹出密钥输入框、不打印结果，也不保存文件。日期默认值和业务筛选由服务端处理；一次调用返回的 `meta.returned_count` 是合并后的总行数。`client` 记录请求页数、是否启用分页和有效的请求编号。

来源缺失但服务仍返回可用结果时，函数正常返回且 `meta.complete=false`；网络错误、非 200 响应、响应格式错误或分页状态变化时抛出 `ApiCallError`，不会返回未完成的分页前缀。默认单次响应读取上限为 32 MiB，服务端自身容量限制仍然适用。函数不自动重试，请按 Appendix D 判断是否重查。

下面的完整程序展示“读取凭证 → 查询 → 查看摘要 → 保存”。将它与脚本放在同一文件夹，在 Python IDE 中运行；只有查询一次，两个文件都使用该次结果。

```python
from getpass import getpass
from fetch_sheet_anomalies import (
    ApiCallError, configured_api_key, default_output, fetch_anomalies,
    result_summary, save_records_csv, save_result,
)

api_key = configured_api_key() or getpass("请输入原始 API Key：")
try:
    result = fetch_anomalies(
        "http://10.72.26.31:8890",
        api_key,
        params={"product": "M626", "sub_domain": "SPC"},
        page_size=100,  # 每页 100 行，自动取完；省略则一次取全
        timeout=120,
    )
except ApiCallError as exc:
    print(f"调用失败：{exc}")
    raise SystemExit(1)
finally:
    del api_key

print(result_summary(result))
if not result["meta"]["complete"]:
    print("不可用来源：", result["meta"]["failed_sources"])

path = default_output()
if not result["meta"]["complete"]:
    path = path.with_name(path.stem + "-partial" + path.suffix)
try:
    print("JSON：", save_result(result, path))
    print("CSV：", save_records_csv(result, path.with_suffix(".csv")))
except ApiCallError as exc:
    print(f"保存失败：{exc}")
    raise SystemExit(2)
```

两个导出调用各自独立：如果 JSON 已保存而 CSV 保存失败，JSON 仍会保留。只需要内存中的结果时，处理 `result["data"]` 即可，不必调用任何导出函数。

### G.3 窗口与程序入口

| 函数 | 功能及使用方式 |
|---|---|
| `launch_gui(defaults=None)` | 打开 Tkinter 查询窗口并等待关闭，正常关闭返回 `0`。在 Python 中用 `launch_gui()` 即可；默认地址为本机 8890。可传 `argparse.Namespace` 预填 `base_url`、`product`、`sub_domain`、`event_type`、`start_date`、`end_date`、`page_size`，并设置请求 `timeout`。密钥从环境变量或窗口取得；保存位置和格式在窗口选择，高级筛选需改用 `fetch_anomalies` |
| `main(argv=None)` | 解析程序参数，调用窗口或执行查询并保存。`None` 读取实际命令行；`main([])` 打开窗口；`main(["--gui", "--base-url", "http://10.72.26.31:8890"])` 打开预填内网地址的窗口。提供参数但未写 `--gui` 时走命令行查询，可用 `--cli` 明确指定；密钥从环境或隐藏输入取得，没有密钥命令行参数 |

可在自己的 Python 文件中这样打开窗口；不要在服务器后台或没有图形桌面的环境调用：

```python
from argparse import Namespace
from fetch_sheet_anomalies import launch_gui

launch_gui(Namespace(base_url="http://10.72.26.31:8890", sub_domain="SPC"))
```

`main` 的正常业务返回码：`0` 表示查询完整且保存成功，或窗口正常关闭；`3` 表示已保存部分来源结果；`2` 表示查询、输入、窗口或保存失败。命令行帮助和参数解析还可能直接抛出 `SystemExit`。多数集成程序直接调用 `fetch_anomalies`，无需调用 `main`。

窗口内还有以下局部回调，只由按钮、定时器及后台线程调用，不能从模块直接导入：

| 函数及所在位置 | 功能／触发方式 |
|---|---|
| `launch_gui.show_text(text)` | 替换只读结果预览文本，由查询及结果处理调用 |
| `launch_gui.run_query()` | “查询完整结果”按钮触发；读取条件、清除旧结果与密钥输入，启动工作线程并暂时禁用查询和下载 |
| `launch_gui.run_query.work()` | 工作线程入口，执行 `fetch_anomalies`，将结果或安全错误放入消息队列 |
| `launch_gui.poll()` | 主窗口每 100 毫秒检查队列，更新状态、最多 20 行预览和下载按钮；完整结果保存在内存 |
| `launch_gui.export(format='json')` | 保存按钮触发；弹出文件选择框并调用 JSON 或 CSV 导出。部分来源结果的建议文件名带 `partial` |

### G.4 异常类型、内部函数和类方法

下面用于理解脚本或排查问题。普通调用者通过前面的公共函数完成操作，不需要拼装这些内部调用。

| 函数／方法 | 功能及内部使用方式 |
|---|---|
| `ApiCallError.__init__(message, *, status=None, request_id='')` | 构造客户端异常；读取 `exc.status` 获取 HTTP 状态或 `None`，`exc.request_id` 获取有效请求编号或空字符串，`str(exc)` 获取提示。用 `except ApiCallError as exc` 捕获；传入自定义消息时也不要包含密钥 |
| `SafeArgumentParser.error(message)` | 参数解析失败时显示用法并以 `SystemExit(2)` 退出，使用固定提示，避免回显误填的密钥；由解析器调用 |
| `NoCredentialRedirect.redirect_request(req, fp, code, msg, headers, newurl)` | 返回 `None`，拒绝自动跟随重定向，避免将密钥转发到其它地址；由 HTTP 连接器调用 |
| `_request_id(headers)` | 从响应头取 `X-Request-ID`，仅保留 32 位十六进制文本；不合法或缺失返回空字符串 |
| `_read_page(opener, base, api_key, params, timeout)` | 拼接接口路径和参数，发送带 `X-API-Key` 的 GET，限制响应大小、解析 JSON、转换网络及 HTTP 错误；返回 `(payload, request_id)`，供查询循环使用 |
| `_validate_page(payload)` | 校验明细、行数、完整性、失败来源和分页状态，返回 `(rows, meta)`；错误时抛出 `ApiCallError`，由查询及导出调用 |
| `_completed_export(result)` | 在上述校验后确认没有后续页，返回 `(rows, meta)`；它检查的是分页完成，不要求全部来源都可用 |
| `_write_new_text(path, text, *, encoding='utf-8')` | 创建父目录并以独占新建方式写文本，返回 `Path`；遇到已有文件报错，写入失败时清理本次新建文件 |
| `_csv_value(value)` | 转换 CSV 单元格：列表转为紧凑 JSON 文本，`None` 转为空白，可能被识别为公式的标量文本加单引号保护；返回字符串，由 CSV 导出调用 |

## Appendix H 部署、架构与验证记录

- [API 服务部署说明](guide-sheet-anomaly-api.md)：服务安装、启动和来源准备。
- [API 程序介绍](architecture-sheet-anomaly-api.md)：架构、缓存及运行边界。
- [CPK／CPM正式验收](../../../docs/ADR/0040-api-capability-deployment-2026-11-04.md)：当时的真实取数、完整导出与来源状态。
