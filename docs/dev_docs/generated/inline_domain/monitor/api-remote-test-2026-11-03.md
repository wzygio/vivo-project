# 在其它设备测试异常片／Lot API：步骤与案例

日期：2026-11-03。面向第一次调用 API 的 Windows 用户；配套阅读 [调用入门](api-caller-guide-2026-11-03.md)。

本文用 Windows 自带的 PowerShell 测试，不需要安装 Python 或复制整个项目。所有业务查询均为 GET，不修改服务端业务数据。

## 1. 准备与通过标准

准备另一台能访问相应内网的 Windows 电脑、维护者交付的原始 API Key，以及当前调用地址：

```text
http://10.72.26.31:8890
```

请在**另一台电脑**执行后续步骤；在服务电脑本机执行不能证明跨设备可访问。密钥通过企业批准的凭据渠道取得，不填进本文。服务电脑需保持开机、唤醒、用户登录，且 API 正常运行。

分三层判断测试是否通过：

| 层次 | 通过标准 |
|---|---|
| 网络与服务 | 另一台电脑打开 `/health/live`，返回 `{"status":"ok"}` |
| 业务接口 | 带正确密钥请求业务地址，收到 HTTP 200 和包含 `data`、`meta` 的 JSON；同时确认完整性 |
| 实际取数 | 返回至少一条事先确认可查询的记录，且产品、分类、对象编号及发生时间相符；需要时取完所有页 |

空数组可以证明请求成功，但不能证明已经获取到预期异常记录。部分来源失败时可以验证可用来源的取数，但不能验收全范围完整取数。

## 2. 第一步：浏览器检查连通性，不需要密钥

在另一台电脑的 Edge 或 Chrome 地址栏打开：

```text
http://10.72.26.31:8890/health/live
```

预期页面显示：

```json
{"status":"ok"}
```

打不开时，先检查是否连接同一可路由内网、地址是否改变，以及维护者是否确认服务正在运行。可在 PowerShell 中执行下面的端口检查，将结果发给维护者：

```powershell
Test-NetConnection 10.72.26.31 -Port 8890
```

`TcpTestSucceeded=True` 只表示 TCP 端口可连接，还需浏览器健康检查和业务查询。不要自行关闭防火墙。电脑使用企业代理时，可请维护者确认内网地址是否应绕过代理。

## 3. 第二步：复制一段 PowerShell，带密钥取第一页

按 Windows 键，输入 `PowerShell` 并打开“Windows PowerShell”。无需以管理员身份运行。

先复制下面两行执行。它们定义本次查询条件：查 M626、SPC、默认 OOS、上一完整周，每页最多 10 行。可按第 5 节案例替换条件。

```powershell
$apiQueryUrl = 'http://10.72.26.31:8890/api/v1/sheet-anomalies?product=M626&sub_domain=SPC&limit=10'
$apiUrl = $apiQueryUrl
```

再完整复制执行以下代码。提示输入 API Key 时粘贴维护者交付的原始密钥并按回车；输入内容不会以明文回显。代码在调用结束后清除本次保存的请求头和密钥变量，仍应在个人可信电脑上操作。

```powershell
$result = $null
$apiResponse = $null
$apiHeaders = @{}
$apiKeyPointer = [IntPtr]::Zero
$apiKeyInput = Read-Host '请输入 API Key（输入内容隐藏）' -AsSecureString
try {
    $apiKeyPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($apiKeyInput)
    $apiHeaders['X-API-Key'] = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($apiKeyPointer)
    $apiResponse = Invoke-WebRequest -UseBasicParsing -Uri $apiUrl -Method Get -Headers $apiHeaders -TimeoutSec 60 -ErrorAction Stop
    $result = $apiResponse.Content | ConvertFrom-Json
    Write-Host ('HTTP 状态：' + [int]$apiResponse.StatusCode)
    Write-Host ('请求编号：' + $apiResponse.Headers['X-Request-ID'])
    Write-Host '查询说明：'
    $result.meta | ConvertTo-Json -Depth 6
    Write-Host '本页明细：'
    $result.data | Format-Table -AutoSize
} catch {
    if ($_.Exception.Response) {
        Write-Host ('HTTP 状态：' + [int]$_.Exception.Response.StatusCode)
        Write-Host ('请求编号：' + $_.Exception.Response.Headers['X-Request-ID'])
    } else {
        Write-Host '未收到 HTTP 响应，请检查地址、网络、代理、服务状态或超时。'
    }
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message
    }
} finally {
    $apiHeaders.Clear()
    if ($apiKeyPointer -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($apiKeyPointer)
    }
    $apiKeyInput.Dispose()
    Remove-Variable apiKeyInput, apiKeyPointer, apiHeaders -ErrorAction SilentlyContinue
}
```

操作时不要开启会记录完整请求头的调试日志。若输入密钥时终端没有显示明文，这是正常现象。不要输入密钥摘要或占位文字。

预期看到 `HTTP 状态：200`，随后是 `meta` 说明及本页明细。终端宽度不足时表格可能省略部分字段，可单独执行：

```powershell
$result.data | ConvertTo-Json -Depth 6
```

若 `complete=false`，查看 `failed_sources`；若返回 503，说明该请求暂时无法提供有效结果，并不一定是网络失败。不要改写来源数据来制造测试通过。

## 4. 第三步：尝试下一页

仅在本页 `meta.has_more=true` 时执行：

```powershell
$apiNextCursor = [Uri]::EscapeDataString([string]$result.meta.next_cursor)
$apiUrl = $apiQueryUrl + '&cursor=' + $apiNextCursor
```

然后**只重新执行第 3 节的长代码块**，按提示再次输入密钥，即可取下一页。不要重新执行设置 `$apiQueryUrl`、`$apiUrl` 的两行代码，否则会回到第一页。

继续翻页时，使用最新响应中的 `next_cursor`，重复本节操作，直到 `has_more=false`。每次长代码块会用最新页覆盖 `$result`，不会自动累积历史页。若需要保留，每页成功后先执行下一节的导出命令，给每一页取不同文件名。

始终保留相同的 `$apiQueryUrl` 和 `limit`。不要改日期、产品等条件后继续用旧游标。410 表示需要重新开始；确认服务重启或旧游标失效导致 422 时，也从第一页重试。重新开始时执行 `$apiUrl = $apiQueryUrl`，丢弃上一组未完成数据。

可选：把当前成功响应保存到个人测试电脑的桌面，以下文件名表示第一页；保存第二页前将 `page-1` 改成 `page-2`，以免覆盖。文件包含返回数据、不含请求密钥，应按企业数据管理要求保管。

```powershell
if ($null -ne $result) {
    $apiOutputFile = Join-Path ([Environment]::GetFolderPath('Desktop')) 'anomaly-api-page-1.json'
    $result | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $apiOutputFile -Encoding UTF8
    Write-Host ('已保存到：' + $apiOutputFile)
}
```

## 5. 推荐执行的测试案例

更换查询时，将下表的“查询后缀”拼在 `http://10.72.26.31:8890/api/v1/sheet-anomalies` 后面，重新设置 `$apiQueryUrl` 和 `$apiUrl`，再执行第 3 节长代码块。每次更换条件从第一页开始。

| 案例 | 查询后缀／操作 | 预期及判断 |
|---|---|---|
| A：最小业务查询 | `?product=M626&sub_domain=SPC&limit=10` | 正常来源应 200；检查 `complete`、明细和分页。来源不可用时可能 503，不能据此断言密钥错误 |
| B：指定日期 | `?product=M626&sub_domain=SPC&start_date=2026-10-26&end_date=2026-11-01&limit=10` | 示例日期含首尾；正常来源 200，`period_end_exclusive` 为 2026-11-02 零点。有无记录需按已发布数据判断 |
| C：跨产品、跨分类 | `?limit=10` | 查询全部启用且授权产品／分类，默认 OOS；可能 200 部分成功，须检查 `failed_sources`；全部失败则 503 |
| D：Yield Lot | `?product=M626&sub_domain=Yield&limit=10` | 可用且有记录时，对象类型为 lot，站点和层别为 null，部门非空；无有效来源则 503 |
| E：SPC OOC | `?product=M626&sub_domain=SPC&event_type=OOC&limit=10` | 查询超控制限明细，与 OOS 独立；有无记录和来源可用性需分别判断 |
| F：日期写反 | `?product=M626&sub_domain=SPC&start_date=2026-11-01&end_date=2026-10-26` | 使用正确密钥时应 422，表示参数校验生效 |
| G：不支持的事件组合 | `?product=M626&sub_domain=Yield&event_type=OOC` | 使用正确密钥时应 422 |
| H：翻页 | A／B／C 中将 `limit=10` 换为 `limit=1`，然后按第 4 节继续 | 总明细超过 1 行时，第一页应 `has_more=true`；若仅 0／1 行，没有下一页是正常结果 |

还可以验证“不带密钥的请求被拒绝”：浏览器直接打开下面的网址，预期返回 401；如服务过载或配置异常，先恢复服务再核对鉴权结果。

```text
http://10.72.26.31:8890/api/v1/sheet-anomalies?product=M626&sub_domain=SPC
```

### 必做：核对一条真实记录

请维护者事先提供一条**当前接口应可查询**的记录信息：产品、分类、OOS／OOC、对象类型、对象编号、日期。它应属于已授权范围，并满足当前结果可用条件。

把这些条件填入查询。以下编号为虚构占位，必须替换；例中日期也需按实际记录替换：

```text
?product=M626&sub_domain=SPC&event_type=OOS&entity_type=sheet&entity_id=REPLACE_WITH_REAL_ID&start_date=2026-10-26&end_date=2026-11-01&limit=10
```

收到 200 后，确认 `complete=true`，并核对返回记录的产品型号、分类、异常事件、对象编号、发生时间；如还有下一页，继续取完。同一对象不同参数可以多条，不能强求只返回一行。

若未返回预期记录，先核对日期和原编号，再请维护者确认当前发布内容和可查询状态。指定很早的日期不会让 API 自动重建历史数据。不要仅凭健康检查或空数组宣布“真实数据取数通过”。

## 6. 出问题时发什么信息

| 现象 | 后续操作 |
|---|---|
| 网页打不开／连接超时 | 提供连通检查结果，维护者核对主机、端口和网络 |
| 401 | 核对原始密钥和请求头；不要把密钥发到聊天或工单 |
| 403 | 维护者核对授权产品、厂别、分类和当前启用产品 |
| 422 | 核对参数名、大小写、日期、事件组合和游标 |
| 429 | 等待响应头 `Retry-After` 指定时间，再重试 |
| 503 | 提供 `error.code`、存在时的 `error.failed_sources` 和请求编号，维护者检查服务及来源 |
| 200 但 `complete=false` | 记录 `failed_sources`，本次只有部分可用数据 |
| 200 且完整，但无明细 | 核对日期、条件、当前发布及预期记录是否可查；空数组本身不是接口故障 |

反馈模板，不填写密钥：

```text
测试时间（北京时间）：
调用设备／所在内网：
调用网址与条件：
health/live 是否显示 ok：
HTTP 状态码：
X-Request-ID：
complete／returned_count／has_more：
失败来源或错误码：
预期对象编号与实际结果：
```

## 7. 本文编写时的核对范围

2026-11-03 已按仓库当前请求模型、响应模型和 HTTP 处理逻辑核对示例；文中 JSON 和示例对象编号均为虚构数据。

离线检查已通过：文档链接、JSON 响应模型、全部 6 段 PowerShell 语法；模拟成功、HTTP 错误和连接错误时的密钥变量清理；游标编码；内存测试客户端的查询、部分成功、503、日期／事件校验、无密钥拒绝和翻页。检查不启动监听服务、不连接数据库、不读取业务工作簿。

本次在服务电脑尝试本机及内网地址的健康检查均未连通；托管工具报告 `supervisor_alive=false`、`listener_owned=false`、`ready=false`。因此**当前没有新增的正式服务取数成功证据**，需维护者先确认服务恢复，再在另一台设备执行本文步骤。此前 2026-11-02 的远端健康检查成功记录不能代替今天的测试。

本次未读取或取得原始密钥，未对正式业务接口执行带密钥查询，未停止或重启正式服务。离线示例检查不能代替跨设备实测。
