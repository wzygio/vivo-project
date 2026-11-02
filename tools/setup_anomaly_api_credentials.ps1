param([string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot))
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class AnomalyApiSetupWindow {
    [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr handle, int command);
}
'@
$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path
$profilePath = Join-Path $env:LOCALAPPDATA 'Tianzhu\AnomalyApi\deployment.json'
if (Test-Path -LiteralPath $profilePath) { throw 'A deployment profile already exists. It will not be overwritten.' }
$form = New-Object Windows.Forms.Form
$form.Text = '天枢 API — 设置调用密钥'
$form.Size = New-Object Drawing.Size(640, 365)
$form.StartPosition = 'CenterScreen'
$form.TopMost = $true
$form.Font = New-Object Drawing.Font('Microsoft YaHei UI', 10)
$form.Add_Shown({ $null = [AnomalyApiSetupWindow]::ShowWindow($form.Handle, 5); $form.Activate() })
$instructions = New-Object Windows.Forms.Label
$instructions.Location = New-Object Drawing.Point(20, 15)
$instructions.Size = New-Object Drawing.Size(590, 95)
$instructions.Text = "请先在自己的密码管理器中生成并保存至少 32 字符的随机密钥。`r`n在下方输入两遍。以后调用 API 时需要这个密钥，请自行保管。`r`n服务只保存 SHA-256 摘要，原始密钥不会写入文件或日志。"
$form.Controls.Add($instructions)
$first = New-Object Windows.Forms.TextBox
$first.Location = New-Object Drawing.Point(20, 120)
$first.Size = New-Object Drawing.Size(590, 30)
$first.UseSystemPasswordChar = $true
$form.Controls.Add($first)
$second = New-Object Windows.Forms.TextBox
$second.Location = New-Object Drawing.Point(20, 165)
$second.Size = New-Object Drawing.Size(590, 30)
$second.UseSystemPasswordChar = $true
$form.Controls.Add($second)
$status = New-Object Windows.Forms.Label
$status.Location = New-Object Drawing.Point(20, 210)
$status.Size = New-Object Drawing.Size(590, 40)
$status.Text = '授权范围：已确认的 7 个产品，ARRAY／OLED／TP，全部 6 类来源。'
$form.Controls.Add($status)
$button = New-Object Windows.Forms.Button
$button.Text = '保存摘要并启动 API'
$button.Location = New-Object Drawing.Point(155, 265)
$button.Size = New-Object Drawing.Size(310, 35)
$form.Controls.Add($button)
$button.Add_Click({
    if ($first.Text -cnotmatch '^[\x21-\x7E]{32,256}$' -or $first.Text -cne $second.Text) {
        $status.Text = '两次输入必须相同，并使用 32～256 个无空格的 ASCII 字符。'
        return
    }
    $button.Enabled = $false
    try {
        $hash = [Security.Cryptography.SHA256]::Create()
        try {
            $digest = [BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($first.Text))).Replace('-', '').ToLowerInvariant()
        } finally { $hash.Dispose() }
        $profile = @{
            version = 1; project_root = $ProjectRoot; host = '0.0.0.0'; port = 8890
            clients = @(@{
                client_id = 'quality-reader'; key_sha256 = $digest
                products = @('M626', 'M678', 'Z571', 'M673', 'Z517', 'Z576', 'Z553')
                factories = @('ARRAY', 'OLED', 'TP')
                sub_domains = @('SPC', 'CTQ', 'AOI_TT', 'AOI_RS', 'Yield', 'Q-Time')
            })
        }
        $null = New-Item -ItemType Directory -Path (Split-Path -Parent $profilePath) -Force
        $json = ConvertTo-Json -InputObject $profile -Depth 6
        $stream = [IO.File]::Open($profilePath, 'CreateNew', 'Write', 'None')
        try {
            $bytes = (New-Object Text.UTF8Encoding($false)).GetBytes($json)
            $stream.Write($bytes, 0, $bytes.Length)
        } finally { $stream.Dispose() }
        $status.Text = '正在启动后台 API，请稍候……'
        $form.Refresh()
        & (Join-Path $ProjectRoot '.venv\Scripts\python.exe') (Join-Path $ProjectRoot 'tools\anomaly_api_service.py') start --install-startup
        if ($LASTEXITCODE -ne 0) { throw 'API startup failed; the digest profile is retained for recovery.' }
        $request = [Net.HttpWebRequest]::Create('http://127.0.0.1:8890/openapi.json')
        $request.Proxy = $null
        $request.Timeout = 10000
        $request.Headers.Add('X-API-Key', $first.Text)
        $response = $request.GetResponse()
        try {
            if ([int]$response.StatusCode -ne 200) { throw 'Authenticated probe failed.' }
        } finally { $response.Close() }
        $probe = @{ authenticated_openapi_status = 200; checked_at = (Get-Date).ToString('o') }
        $probe | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $ProjectRoot 'output\logs\anomaly-api\credential-probe.json') -Encoding UTF8
        $form.DialogResult = [Windows.Forms.DialogResult]::OK
        $form.Close()
    } catch {
        $status.Text = '设置失败。已保存的摘要会保留，请检查 API 日志。未记录原始密钥。'
        $button.Enabled = $false
    } finally {
        $first.Clear()
        $second.Clear()
        $digest = $null
    }
})
try { $null = $form.ShowDialog() }
finally { $first.Clear(); $second.Clear(); $form.Dispose() }
