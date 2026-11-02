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

查询范围固定为服务器上海时区的上一完整 ISO 周：上周一 00:00 起，本周一 00:00 止，右端不包含。例如参考日 2026-11-02 的窗口为 `[2026-10-26, 2026-11-02)`。不接受任意历史日期参数。已发布数据使用现有报表显示时间，不再次前推日期。

## 2. 启动前准备

在仓库根目录使用项目虚拟环境。依赖包含 FastAPI、Uvicorn；按项目既有依赖管理方式同步环境。确认下面四项已经就绪：

1. 请求产品在 `config/global.yaml` 的启用产品范围内，并给调用系统分配了对应授权。
2. 对应原报表已成功发布本地结果，且满足该来源原有有效性／覆盖规则。
3. 公开可见性配置包含该分类和产品；缺少政策或无法读取时关闭数据输出。
4. 已初始化 API 元数据，并维护所需站点描述、Yield 归属与释放决定。

资源路径由 [全局资源配置](../../../../config/global.yaml) 解析，文件名可独立配置。API 不接受客户端提交资源路径或工作表名。

### 2.1 初始化本地元数据

```powershell
.venv/Scripts/python.exe tools/sync_api_alert_metadata.py --initialize
```

这个命令不连接数据库；文件已存在时保留原文件。配置键为 `resources.shared_kernel.files.api_alert_metadata`，默认位置为 `resources/shared_kernel/shared/api_alert_metadata.xlsx`。这是需要保留的维护资料，不是可清理缓存。

工作簿有两张表：

| 工作表 | 列名 | 维护内容 |
|---|---|---|
| `站点字典` | `step_id`、`description` | 真实站点编号与工序描述，编号以文本保存 |
| `Yield归属` | `product`、`lot_id`、`parameter_name`、`factory`、`step_id`、`description` | Yield 的业务归属；下面单独说明 |

层别取描述的第一个 `_` 之前的内容，去掉两端空白；`PI2_CURE_THK` 对应 `PI2`。没有下划线时使用完整描述。描述缺失或前缀为空时，不能形成完整结果。

### 2.2 同步指定站点描述

这是操作人员显式执行的独立命令，只有这个同步步骤会按指定站点访问数据库，API 请求不会执行它：

```powershell
.venv/Scripts/python.exe tools/sync_api_alert_metadata.py --step "00100" --step "00200" --timeout-ms 10000 --max-rows 10000
```

先查询 `mdw.dwr_mes_processoperationspec_v`，SQL 执行失败才回退 `mdw.dwr_mes_processoperationspec`，按 `oper_code` 与 `description` 形成映射。查询使用明确站点集合、参数绑定、返回条数上限和事务内超时；每次最多指定 1,000 个站点，返回上限最多 10,000 行，超时范围 1～60,000 毫秒。

未查齐全部请求站点、同站点存在冲突描述或超过行数上限时，同步失败，不把部分结果当成完整字典发布。成功时只替换请求站点，保留其他站点与 `Yield归属`。字典同站点应只有一个有效描述，不能用空白行内容或冲突记录代替真实说明。

Q-Time 的“发生站点”和层别均采用**起点 `f_step`**，所以必须维护起点的字典记录。

### 2.3 维护 Yield 厂别与层别

Yield 已发布 Lot 结果本身没有可靠厂别／发生站点，因此通过 `Yield归属` 显式维护，不推断固定厂别或站点：

| 列 | 规则 |
|---|---|
| `product` | 已启用产品代码，与请求产品精确匹配 |
| `lot_id` | 填写 Lot 号时为该 Lot 专用；留空时为产品＋异常参数的默认归属 |
| `parameter_name` | 与发布结果的“异常 Code”精确匹配 |
| `factory` | **必须填写**真实维护厂别，例如 `ARRAY`／`OLED`／`TP` |
| `step_id` | 可留空，这是唯一允许发生站点为 `NULL` 的来源 |
| `description` | 站点留空时必须填写维护描述，用其前缀形成层别 |

先匹配产品＋异常 Code＋指定 Lot；没有 Lot 专用记录时，才使用同产品＋异常 Code 且 `lot_id` 留空的默认行。每一层匹配结果必须唯一。

填写了 `step_id` 时，层别必须来自 `站点字典` 中该站点的描述；不会拿归属表中的描述掩盖缺失字典。站点留空时，`发生站点` 返回 `null`，但 `factory` 与有效描述仍必须存在。厂别、层别或其他必需资料不足时，本次结果返回 `503`，不会只删除缺资料的记录后声称查询完整。

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

仅停止进程不会删除登录启动项；如需永久取消，应由该用户删除其启动目录中的上述专用快捷方式。初始监听地址／端口由用户profile的`host`／`port`提供。修改前确认端口归属与网络范围，不重用报表端口。真实来源仍受发布有效性、字典、Yield归属及人工释放决定限制；`/health/ready`成功不代表所有业务来源均可提供明细。

| 路径 | 说明 |
|---|---|
| `GET /health/live` | 进程存活，不访问数据库 |
| `GET /health/ready` | 检查凭证及启用产品配置，不保证每一产品结果都可用 |
| `GET /api/v1/sheet-anomalies` | 需要 `X-API-Key` 的业务查询 |
| `GET /openapi.json` | 需要同样凭证的接口 schema |

当前未开放 `/docs` 或 `/redoc` 交互页面；可把获授权读取的 OpenAPI 导入调用工具。关闭访问日志避免把请求 URL 写入默认 access log。当前启动命令的服务日志输出到终端，正式托管应将标准输出／错误归档到 `output/logs/`，不要额外记录密钥或完整请求头。

### 4.1 查询参数

| 参数 | 要求 |
|---|---|
| `product` | 必填，产品原代码，区分实际配置值，不拼接界面注释 |
| `sub_domain` | 必填，严格使用第1节中的六个值与大小写 |
| `event_type` | 默认 `OOS`；四类 Inline 可指定 `OOC`；Yield／Q-Time 指定 OOC 返回422 |
| `factory` | 可选，显式请求必须在授权范围；省略表示所有已授权厂别 |
| `entity_type` | 可选，`sheet` 或 `lot` |
| `entity_id` | 可选，对象编号精确匹配 |
| `limit` | 默认100，范围1～500 |
| `cursor` | 下一页令牌，首个请求不填写 |

不接受表路径、工作表名、任意日期、强制刷新或绕过公开政策等额外参数。无效或未知参数返回422。

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

### 4.2 返回数据与分页

业务列是固定白名单：

| 字段 | 含义 |
|---|---|
| 分类 | SPC／CTQ／AOI_TT／AOI_RS／Yield／Q-Time |
| 发生站点 | 原站点代码；Q-Time 取起点；仅 Yield 可为 `null` |
| 部门 | 真实厂别；Yield 使用维护归属，不能为空 |
| 层别 | 工序／维护描述首个下划线之前的前缀，不能为空 |
| 监控参数 | 参数名、TT名称、RS Code、Yield异常Code或Q-Time路径名 |
| 异常事件 | OOS／OOC，独立于命中边界 |
| 对象类型／对象编号 | sheet／lot 与对应原始字符串编号 |
| 产品型号 | 请求产品原代码 |
| 发生时间 | Inline／Q-Time 为带时区时间；Yield 为入库日期 `YYYY-MM-DD` |
| 起始站点 | Q-Time 的起点，与该来源发生站点一致；其余来源可空 |
| 命中边界 | 已发布的 USL／LSL／UCL／LCL 等，可空 |

Yield 只有日期精度，返回日期而不是虚构零点事件时间。一片在多个站点／参数上可以多条，`returned_count` 是本页明细行数，不是去重异常片数。

响应外层为 `data` 和 `meta`。`meta` 含 `status`、`returned_count`、`has_more`、`next_cursor`、查询周起止、`timezone`、`read_at` 和 `limitations`。`read_at` 是此次取数时间，不是源数据更新时间。

读取下一页时保留首个请求的全部过滤条件及 `limit`，把 `next_cursor` 原样放进 `cursor`。游标绑定调用方、授权范围、查询条件、参考周和公开结果，不能跨查询复用。默认有效期300秒，且数量／字节有上限；进程重启、淘汰、权限／公开结果变化可能要求重查。

收到410时，从不带 cursor 的第一页重新查询，并丢弃上次未完成的分页集合，避免拼接两个版本。不要修改或解析游标内容；篡改返回422。进程重启会更换签名密钥，因此旧游标也可能返回422；确认请求参数未变且服务刚重启时，同样从第一页重查。只读接口可以重试，但429应等待 `Retry-After` 指定时间，不能快速无限重试。

## 5. 常见结果与故障处理

| 状态 | 含义及处理 |
|---|---|
| 200，`status=alert` | 当前公开范围存在异常明细 |
| 200，`status=ok`，空数组 | 有效查询没有可提供的异常；不等于原始源数据刚完成更新 |
| 401 | 检查是否提交有效 `X-API-Key` |
| 403 | 检查产品、厂别、分类授权和启用产品 |
| 410 | 分页快照失效，从第一页重查 |
| 422 | 检查参数名、值、OOC适用分类及游标 |
| 429 | 请求过频或读取槽位忙，按 `Retry-After` 等待 |
| 503 | 本地结果、公开政策、释放证据、元数据完整性或来源有效性暂不可用；联系维护者 |
| 500 | 未预期故障，记录 `X-Request-ID`，交由维护者排查 |

排查503时，依次核对正式发布及其日期／覆盖、原有人工决定、所需本地字典、Yield归属、启用产品及当前公开政策。不要通过删除元数据、填写假站点或把全部Flag改为False绕过问题。

Inline 当前没有统一“当日发布＋TTL”的数据时效协议；Q-Time、Yield 继续检查各自已有发布和源健康有效性。缓存TTL不代表数据实时，也不会由一次GET延长发布时间。

## 6. 运行边界与维护

正式架构决定及取舍见 [ADR-0037](../../../../docs/ADR/0037-published-anomaly-api.md)。

默认每调用系统每分钟60次，来源地址还有有限总频率限制；同步读取并发默认2、请求等待上限30秒。工作簿缓存默认16项、64MiB缓存与单文件上限、总行数20万；公开分页快照默认64份、总字节16MiB。限制由应用设置控制，超限明确返回忙或不可用。

Excel解析是同步操作；首次冷读需要解析工作簿，分页并不把整本Excel读取变为按行读取。企业保护文件可能使用已有Office COM回退，运行账号与交互环境必须单独验证。HTTP超时不会强制终止Office调用；仍运行的读取占用有限槽位，服务不会无限新增替代线程。

API缓存与分页快照位于本进程内存，不与Streamlit或其他worker共享。因此当前按单worker运行，水平扩展前需设计游标路由或共享存储。正式运行前还需确认主机可用性、凭证注入、文件可读性和运维托管；本功能没有自动创建这些外部环境。

维护字典、Yield归属和人工决定应保留备份及既有资源生命周期。API GET 不维护这些文件。临时测试产物归 `output/test-results/` 或 `output/tmp/`；实际正式日志按项目规则归 `output/logs/`，不得把维护工作簿当缓存删除。

实现依据：[API入口](../../../../api/main.py)、[公开投影](../../../../api/service.py)、[元数据读取及同步](../../../../src/shared_kernel/infrastructure/api_alert_metadata.py)、[看板架构](architecture-alert-dashboard.md)。开发过程与最终验证由[开发方案](../../../../docs/dev_docs/generated/inline_domain/monitor/sheet-anomaly-api-development-plan.md)及交付记录追踪。
