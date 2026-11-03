# 异常片／Lot 明细 API 使用说明

更新日期：2026-11-02。本文对应已合并到master的实现，实际部署地址和交付验证以项目交付记录为准；仓库包含代码不代表服务已经启动。

## 1. 能查询什么

接口从已发布到本地的结果工作簿中读取异常，不会因为一次请求而查询生产数据库、重新分析、补齐结果或写入工作簿。只有明确释放的记录才进入公开结果，并遵守当前产品范围和公开可见性规则。

| `sub_domain` | `event_type` | 对象类型 |
|---|---|---|
| `SPC` | `OOS`／`OOC` | `sheet` |
| `CTQ` | `OOS`／`OOC` | `sheet` |
| `AOI_TT` | `OOS`／`OOC` | `sheet` |
| `AOI_RS` | `OOS`／`OOC` | `sheet`／`lot`，保留原 By Sheet／By Lot |
| `Yield` | `OOS` | `lot` |
| `Q-Time` | `OOS` | `lot` |

不包含趋势、CPK／CPM、IJP 比例汇总、备件、IQC、图像和原始量测点。Lot 报警不会展开成该 Lot 所有片都报警。

查询范围默认服务器上海时区的上一完整 ISO 周：上周一 00:00 起，本周一 00:00 止，右端不包含。例如参考日2026-11-02默认日期为2026-10-26至2026-11-01。可通过start_date／end_date指定包含首尾两天的日期范围；只传一项时，另一项独立采用对应默认日期。开始晚于截止返回422。已发布数据使用现有报表显示时间，不再次前推日期。历史查询受本地已发布内容及原有覆盖、有效性限制，API不会补算历史或延长有效期。

## 2. 启动前准备

在仓库根目录使用项目虚拟环境。依赖包含 FastAPI、Uvicorn；按项目既有依赖管理方式同步环境。确认下面四项已经就绪：

1. 请求产品在 `config/global.yaml` 的启用产品范围内，并给调用系统分配了对应授权。
2. 对应原报表已成功发布本地结果，且满足该来源原有有效性／覆盖规则。
3. 公开可见性配置包含该分类和产品；缺少政策或无法读取时关闭数据输出。
4. 站点类来源已维护本地站点描述；Yield已按全局分组映射发布厂别。各来源均需有效释放决定。

资源路径由 [全局资源配置](../../../../config/global.yaml) 解析，文件名可独立配置。API 不接受客户端提交资源路径或工作表名。

### 2.1 初始化本地元数据

```powershell
.venv/Scripts/python.exe tools/sync_api_alert_metadata.py --initialize
```

这个命令不连接数据库；文件已存在时保留原文件。配置键为 `resources.shared_kernel.files.api_alert_metadata`，默认位置为 `resources/shared_kernel/shared/api_alert_metadata.xlsx`。这是需要保留的维护资料，不是可清理缓存。

新建工作簿只要求站点字典；Yield不使用这份元数据文件：

| 工作表 | 列名 | 维护内容 |
|---|---|---|
| `站点字典` | `step_id`、`description` | 真实站点编号与工序描述，编号以文本保存 |

层别取描述的第一个 `_` 之前的内容，去掉两端空白；`PI2_CURE_THK` 对应 `PI2`。没有下划线时使用完整描述。描述缺失或前缀为空时，不能形成完整结果。

### 2.2 同步指定站点描述

这是操作人员显式执行的独立命令，只有这个同步步骤会按指定站点访问数据库，API 请求不会执行它：

```powershell
.venv/Scripts/python.exe tools/sync_api_alert_metadata.py --step "00100" --step "00200" --timeout-ms 10000 --max-rows 10000
```

先查询 `mdw.dwr_mes_processoperationspec_v`，SQL 执行失败才回退 `mdw.dwr_mes_processoperationspec`，按 `oper_code` 与 `description` 形成映射。查询使用明确站点集合、参数绑定、返回条数上限和事务内超时；每次最多指定 1,000 个站点，返回上限最多 10,000 行，超时范围 1～60,000 毫秒。

未查齐全部请求站点、同站点存在冲突描述或超过行数上限时，同步失败，不把部分结果当成完整字典发布。成功时只替换请求站点，保留其他站点和已有工作表。字典同站点应只有一个有效描述，不能用空白行内容或冲突记录代替真实说明。

Q-Time 的“发生站点”和层别均采用**起点 `f_step`**，所以必须维护起点的字典记录。

### 2.3 Yield 自动厂别与空层别

Yield Lot超规在原有计算与发布流程中保存`defect_group`和`factory`，API直接读取发布的`factory`，不再维护或查询`Yield归属`。映射在 [global.yaml](../../../../config/global.yaml) 中配置：

```yaml
data_source:
  defect_group_factory_mapping:
    Array_Line: ARRAY
    Array_Pixel: ARRAY
    Array_Mura: ARRAY
    OLED_Mura: OLED
    "TP_Short NG": TP
    "TP 容值NG": TP
```

配置不能为空，分组和厂别均为非空文本；厂别规范为大写。发布者在计算时读取当前配置，纯计算不读YAML。映射属于现有Yield策略签名的一部分，新增或修改映射后旧Yield发布可能变为stale，需按正常报表／发布流程重新生成；编辑映射本身不会改写结果，也不会延长数据有效期。

入库不良无法直接定位发生工序，因此Yield的`发生站点`和`层别`一律返回JSON `null`，不填写默认层别，不查询站点字典。厂别仍必须完整且满足调用方授权；未知分组的超规记录会保留在发布结果中，但厂别为空，明确释放后API返回503。旧发布文件缺厂别时同样需正常重发，GET不会回退到人工归属或自行补算。其它来源仍要求真实站点及有效层别。

已有`Yield归属`工作表保留原内容，初始化／同步命令不再要求、创建或验证该表，也不会删除它。

### 2.4 维护人工释放决定

Inline OOS／OOC 沿用各自现有维护工作簿的 Flag。Q-Time 沿用原有人工决策台账；新的发布结果保留明确的 False 证据。旧 Q-Time 结果没有该证据时，需要正常 Q-Time 报表生产重新发布，API 不会在读取时补 False。

Yield 的持久决定位于 Lot 结果工作簿的 `<产品>__flags` 工作表。对已有、尚未初始化控制的产品显式执行：

```powershell
.venv/Scripts/python.exe tools/initialize_yield_alert_controls.py --product PRODUCT_DEMO
```

`PRODUCT_DEMO` 是虚构示例，执行时替换为已启用且已有发布结果的产品。命令不查询数据库、不运行 Yield 分析，也不刷新原发布时间；不存在发布结果时不能凭空初始化成功。

`<产品>__flags` 包含“超规 Lot ID”“异常 Code”“入库时间”“flag”。稳定键是前面三列，日期规范为 `YYYY/MM/DD`；产品由工作表归属确定。新异常默认 `True`，只有操作人员确认需要释放的键才改为 `False`。建议仅填写明确的 `True`／`False`，缺失、Delete 或不可识别值不会释放。

维护权威决定表，而不是只修改生成结果表中的同名列。正常发布会在文件锁内合并决定，保留人工历史和其他产品；本期不再出现的旧决定不会因此被删除。保存决定后读取会应用有效决定；若原发布已过期或失败，仍需原报表正常更新，编辑 Flag 不会延长数据有效期。

## 3. 配置调用方凭证

给每个调用系统分配独立、足够随机的 API Key，通过请求头 `X-API-Key` 传递。服务端配置的是密钥的 SHA-256 小写十六进制摘要，原始密钥通过企业批准的凭据渠道交付给调用方，不写入 Git、业务 YAML、请求 URL 或日志。

将如下结构的完整 JSON 注入启动 API 的同一进程环境变量 `VIVO_API_CLIENTS`：

```json
[
  {
    "client_id": "report-demo",
    "key_sha256": "<替换为该调用方密钥的64位小写SHA-256摘要>",
    "products": ["PRODUCT_DEMO"],
    "factories": ["ARRAY"],
    "sub_domains": ["SPC", "CTQ", "AOI_TT", "AOI_RS", "Yield", "Q-Time"]
  }
]
```

这段 JSON 只展示结构，必须替换摘要和虚构产品；摘要占位文字不是有效配置。`client_id`、密钥摘要不得重复。产品、厂别、分类授权均应是非空列表。当前数据范围取调用方授权与项目启用产品的交集。

可复制的结构模板另见 [api-clients.example.json](../../../../config/api-clients.example.json)；其中全零摘要和虚构产品也必须替换。首次在PowerShell中配置一个最小SPC调用方，可使用下面的示例，先将产品替换为实际启用值：

```powershell
$apiSecretKey = & .venv/Scripts/python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
$apiHashAlgorithm = [System.Security.Cryptography.SHA256]::Create()
try {
    $apiKeyHash = [BitConverter]::ToString($apiHashAlgorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($apiSecretKey))).Replace('-', '').ToLowerInvariant()
} finally {
    $apiHashAlgorithm.Dispose()
}
$apiClients = @(@{
    client_id = 'quality-reader'
    key_sha256 = $apiKeyHash
    products = @('PRODUCT_DEMO')
    factories = @('ARRAY')
    sub_domains = @('SPC')
})
$env:VIVO_API_CLIENTS = ConvertTo-Json -InputObject $apiClients -Depth 4 -Compress
```

在同一终端启动下节API。原始随机密钥保存在 `$apiSecretKey`，应通过批准的安全渠道交给该调用方，供其填写请求头；服务配置只保存摘要。不要把它打印到共享日志、写入仓库或与其他调用方共用。

程序每次鉴权读取进程环境配置。修改外部终端的环境变量不会自动改变已运行的 API 进程；由正式托管环境注入更新并按运维方式重启。不要把长期共享密钥放进浏览器前端源码。

## 4. 运行与接口说明

从仓库根目录启动单 worker 示例：

```powershell
.venv/Scripts/python.exe -m uvicorn api.main:create_app --factory --host 127.0.0.1 --port 8890 --workers 1 --no-access-log
```

这个示例只监听本机，端口需确认未被占用；前台运行时可用 Ctrl+C 结束该进程。供其他系统访问时，可以在本机监听内网地址并由调用方直接访问，也可以使用运维提供的HTTPS入口。HTTP不加密请求头中的调用密钥；2026-11-02用户明确选择当前电脑内网直连。原Streamlit服务独立运行。

### 4.0 Windows当前用户后台托管

仓库提供 [后台托管工具](../../../../tools/anomaly_api_service.py) 和 [首次密钥设置窗口](../../../../tools/setup_anomaly_api_credentials.ps1)。本次部署范围和实际验证状态见 [部署记录](../../../../docs/dev_docs/generated/inline_domain/monitor/api-deployment-2026-11-02.md)。

首次设置窗口由当前用户输入并自行保存至少32字符的随机密钥；原始密钥只用于该窗口内的鉴权检查，不写文件、日志或命令行。摘要与批准的授权保存在当前用户的 `%LOCALAPPDATA%\Tianzhu\AnomalyApi\deployment.json`，启动工具将其中调用方JSON注入worker的`VIVO_API_CLIENTS`环境。该文件不是业务YAML，不包含原始密钥，不加入Git。现有profile不会被首次设置工具覆盖。

```powershell
powershell.exe -NoProfile -STA -File tools/setup_anomaly_api_credentials.ps1
.venv/Scripts/python.exe tools/anomaly_api_service.py status
```

后台工具以单worker运行，另有一个本机监督进程负责异常退出后退避重启。日志按天保留14份，位于`output/logs/anomaly-api/server.log`；进程身份、启动意图及维护状态也归该目录。不会启动报表生产或补齐业务资料。

首次启动成功后，在当前用户的Windows启动目录创建`Tianzhu Anomaly API.lnk`，指向项目虚拟环境的`pythonw.exe`。这是**登录后自动启动**，不是开机即运行的Windows系统服务；未登录、注销、休眠或关机时不保证可调用。无人值守开机运行需运维另行配置适合企业文件读取的服务账号。

以下命令仅管理该工具记录并验证身份的API进程，不停止原Streamlit或其他端口上的服务。更新授权profile后需停止并重新启动，使新环境生效；进程重启会使原分页游标失效。

```powershell
.venv/Scripts/python.exe tools/anomaly_api_service.py stop
.venv/Scripts/python.exe tools/anomaly_api_service.py start
```

仅停止进程不会删除登录启动项；如需永久取消，应由该用户删除其启动目录中的上述专用快捷方式。初始监听地址／端口由用户profile的`host`／`port`提供。修改前确认端口归属与网络范围，不重用报表端口。真实来源仍受发布有效性、字典、Yield发布厂别及人工释放决定限制；`/health/ready`成功不代表所有业务来源均可提供明细。

| 路径 | 说明 |
|---|---|
| `GET /health/live` | 进程存活，不访问数据库 |
| `GET /health/ready` | 检查凭证及启用产品配置，不保证每一产品结果都可用 |
| `GET /api/v1/sheet-anomalies` | 需要 `X-API-Key` 的业务查询 |
| `GET /openapi.json` | 需要同样凭证的接口 schema |

当前未开放 `/docs` 或 `/redoc` 交互页面；可把获授权读取的 OpenAPI 导入调用工具。关闭访问日志避免把请求 URL 写入默认 access log。当前启动命令的服务日志输出到终端，正式托管应将标准输出／错误归档到 `output/logs/`，不要额外记录密钥或完整请求头。

### 4.1 查询参数（前六个字段与下方返回数据中的前六个必要字段一一对应）

| 参数 | 要求 |
|---|---|
| `sub_domain` | 可选，省略表示全部授权分类；显式值严格使用第1节大小写 |
| `step_id` | 可选，发生站点精确匹配 |
| `factory` | 可选，显式请求必须在授权范围；省略表示所有已授权厂别 |
| `layer` | 可选，层别精确匹配 |
| `parameter_name` | 可选，监控参数精确匹配 |
| `event_type` | 默认 `OOS`；四类 Inline 可指定 `OOC`；Yield／Q-Time 指定 OOC 返回422 |
| `entity_type` | 可选，`sheet` 或 `lot` |
| `product` | 可选，省略表示启用且获授权的全部产品；显式填写产品原代码 |
| `start_date` | 可选，YYYY-MM-DD，含当天；默认上个ISO周周一 |
| `end_date` | 可选，YYYY-MM-DD，含当天；默认上个ISO周周日 |
| `entity_id` | 可选，对象编号精确匹配 |
| `limit` | 可选，省略不按条数截断；显式1～500时启用分页 |
| `cursor` | 下一页令牌，首个请求不填写；使用时必须同时提供显式limit |

所有查询参数都可省略；认证请求头X-API-Key仍为必需。省略参数表示授权范围内全选，空字符串不表示全选。参数按“且”组合，文本精确匹配，保留站点／对象编号前导零。不接受表路径、工作表名、强制刷新或绕过公开政策等额外参数。无效或未知参数返回422。

未指定分类而event_type=OOC时，仅查询获授权且支持OOC的四类Inline。Yield的站点／层别为null，带站点或层别筛选时不匹配。

请求示例（仍需X-API-Key请求头）：

```text
GET /api/v1/sheet-anomalies
GET /api/v1/sheet-anomalies?sub_domain=SPC&step_id=00100&layer=PI2
GET /api/v1/sheet-anomalies?start_date=2026-10-25&end_date=2026-11-01
GET /api/v1/sheet-anomalies?limit=200
```

下面是调用方 Python 示例，凭证由其运行环境注入，不在代码中写死或打印：

```python
import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

base_url = os.environ["VIVO_API_BASE_URL"].rstrip("/")
params = {"product": "PRODUCT_DEMO", "sub_domain": "SPC", "event_type": "OOS", "limit": 100}
request = Request(
    f"{base_url}/api/v1/sheet-anomalies?{urlencode(params)}",
    headers={"X-API-Key": os.environ["VIVO_API_KEY"]},
)
with urlopen(request, timeout=40) as response:
    result = json.load(response)
print(json.dumps(result, ensure_ascii=False, indent=2))
```

`VIVO_API_BASE_URL` 填运维提供的地址；本机示例为 `http://127.0.0.1:8890`。虚构产品必须替换为真实启用且获授权的产品。调用失败时按下一节处理状态码。

### 4.2 返回数据（前六个字段为业务方规定的必要字段）

业务列是固定白名单：

| 字段 | 含义 |
|---|---|
| 分类 | SPC／CTQ／AOI_TT／AOI_RS／Yield／Q-Time |
| 发生站点 | 原站点代码；Q-Time 取起点；仅 Yield 可为 `null` |
| 部门 | 真实厂别；Yield使用已发布factory，不能为空 |
| 层别 | Yield固定为`null`；其他来源取工序描述首个下划线之前的前缀，不能为空 |
| 监控参数 | 参数名、TT名称、RS Code、Yield异常Code或Q-Time路径名 |
| 异常事件 | OOS／OOC，独立于命中边界 |
| 对象类型／对象编号 | sheet／lot 与对应原始字符串编号 |
| 产品型号 | 每条记录所属产品原代码 |
| 发生时间 | Inline／Q-Time 为带时区时间；Yield 为入库日期 `YYYY-MM-DD` |
| 命中边界 | 已发布的 USL／LSL／UCL／LCL 等，可空 |

Yield 只有日期精度，返回日期而不是虚构零点事件时间。一片在多个站点／参数上可以多条，`returned_count` 是本页明细行数，不是去重异常片数。

响应外层为data和meta。meta包含status、returned_count、has_more、next_cursor、实际查询范围period_start／period_end_exclusive、timezone、read_at、limitations，以及complete和failed_sources。read_at是此次取数时间，不是源数据更新时间。

**每次先检查meta.complete。** complete=true表示选中来源均读取成功，failed_sources为空；complete=false表示部分来源不可用，data只含可用来源。status=ok／alert描述已返回记录是否为空，不能替代完整性判断。失败来源逐项列出product、sub_domain、event_type和固定code=result_unavailable，不包含路径或内部诊断。例如：

```json
{"complete": false, "failed_sources": [{"product": "PRODUCT_DEMO", "sub_domain": "Yield", "event_type": "OOS", "code": "result_unavailable"}]}
```

上面展示的是meta中的部分字段。即使data为空，complete=false也不能解释为全部来源均无异常。全部选中来源均失败时返回503，error.failed_sources列出失败来源。请求级配置错误、超时、容量或限流失败仍返回相应错误，不能保证逐来源清单。

cursor就是服务发出的“下一页取数凭条”。默认完整取数不需要它；数据较多时，先带limit请求第一页，再保留相同筛选条件和limit，把meta.next_cursor原样作为下一次的cursor，直到has_more=false。调用方无需理解或拼接令牌内容。

先合并各产品／分类的公开结果，按发生时间及公开身份字段统一倒序，再整体分页。游标绑定调用方、授权、实际启用产品／分类／厂别范围、日期、筛选、公开结果及失败来源状态，不能跨查询复用。默认有效期300秒，且数量／字节有上限；进程重启、淘汰、权限／范围／公开结果／失败状态变化可能要求重查。

收到410时，从不带 cursor 的第一页重新查询，并丢弃上次未完成的分页集合，避免拼接两个版本。不要修改或解析游标内容；篡改返回422。进程重启会更换签名密钥，因此旧游标也可能返回422；确认请求参数未变且服务刚重启时，同样从第一页重查。只读接口可以重试，但429应等待 `Retry-After` 指定时间，不能快速无限重试。

## 5. 常见结果与故障处理

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

排查503时，依次核对正式发布及其日期／覆盖、原有人工决定、站点类来源的本地字典、Yield发布factory和全局分组映射、启用产品及当前公开政策。不要通过删除元数据、填写假站点或把全部Flag改为False绕过问题。

Inline 当前没有统一“当日发布＋TTL”的数据时效协议；Q-Time、Yield 继续检查各自已有发布和源健康有效性。缓存TTL不代表数据实时，也不会由一次GET延长发布时间。

## 6. 运行边界与维护

正式架构决定及取舍见 [ADR-0037](../../../../docs/ADR/0037-published-anomaly-api.md)。

默认每调用系统每分钟60次，来源地址还有有限总频率限制；同步读取并发默认2、请求等待上限30秒。工作簿缓存默认16项、64MiB缓存与单文件上限、总行数20万；公开分页快照默认64份、总字节16MiB。默认不限返回行数仍受16MiB公开结果容量限制，超限返回503，不会静默截断；显式分页也需要完整快照，不能绕过容量。限制由应用设置控制，超限明确返回忙或不可用。

Excel解析是同步操作；首次冷读需要解析工作簿，分页并不把整本Excel读取变为按行读取。企业保护文件可能使用已有Office COM回退，运行账号与交互环境必须单独验证。HTTP超时不会强制终止Office调用；仍运行的读取占用有限槽位，服务不会无限新增替代线程。

API缓存与分页快照位于本进程内存，不与Streamlit或其他worker共享。因此当前按单worker运行，水平扩展前需设计游标路由或共享存储。正式运行前还需确认主机可用性、凭证注入、文件可读性和运维托管；本功能没有自动创建这些外部环境。

维护字典和人工决定应保留备份及既有资源生命周期；历史Yield归属表保留但不参与读取。API GET 不维护这些文件。临时测试产物归 `output/test-results/` 或 `output/tmp/`；实际正式日志按项目规则归 `output/logs/`，不得把维护工作簿当缓存删除。

实现依据：[API入口](../../../../api/main.py)、[公开投影](../../../../api/service.py)、[元数据读取及同步](../../../../src/shared_kernel/infrastructure/api_alert_metadata.py)、[看板架构](architecture-alert-dashboard.md)。开发过程与最终验证由[开发方案](../../../../docs/dev_docs/generated/inline_domain/monitor/sheet-anomaly-api-development-plan.md)及交付记录追踪。

## 7. 后续返回字段扩展方向

本次删除起始站点，Q-Time发生站点继续使用起点。除部分结果所需的meta完整性标记外，不新增业务明细列。后续可考虑：

| 信息 | 用途与前提 |
|---|---|
| 实测值、最小／最大值 | 判断异常程度；各来源量测口径不同，Lot未必有单一数值 |
| 规格／控制上下限、单位、超出幅度 | 解释超限方向与程度；需区分OOS／OOC并规范单位 |
| Yield缺陷分组 | 按Array_Line等分组汇总；现有发布已包含defect_group |
| 发布时间、数据覆盖范围 | 判断新鲜度；需统一各来源元数据，不能混同发生时间 |
| 稳定异常事件ID | 支持去重／增量同步；需定义跨来源稳定键与重新发布规则 |
| Lot／Sheet关联或Q-Time终点 | 支持追踪；需确认本地发布内容及业务含义 |

这些是未来建议，当前接口不返回以上新增信息。
