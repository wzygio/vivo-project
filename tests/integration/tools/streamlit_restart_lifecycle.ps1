param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path,
    [string]$EvidencePath,
    [switch]$CleanupOnly
)
$ErrorActionPreference = 'Stop'
. (Join-Path $ProjectRoot 'tools\restart_streamlit.ps1')
if (-not $EvidencePath) { $EvidencePath = Join-Path $ProjectRoot 'output\test-results\streamlit-startup-fixture-lifecycle.json' }
if ($CleanupOnly) {
    if (-not (Test-Path -LiteralPath $EvidencePath)) { exit 0 }
    $saved = Get-Content -LiteralPath $EvidencePath -Raw -Encoding UTF8 | ConvertFrom-Json
    $recoveryRoot = [System.IO.Path]::GetFullPath($saved.fixture)
    if ((Split-Path $recoveryRoot -Parent) -ne (Join-Path $ProjectRoot 'output\tmp') -or (Split-Path $recoveryRoot -Leaf) -notmatch '^startup-lifecycle-[a-f0-9]{32}$') {
        throw 'Recovery fixture path is outside the test workspace'
    }
    if ($saved.controller) {
        $current = Get-CimInstance Win32_Process -Filter "ProcessId=$($saved.controller.pid)"
        if ($null -ne $current -and $current.CreationDate.ToString('o') -eq $saved.controller.created_at -and $current.CommandLine.Contains((Join-Path $recoveryRoot 'interrupted-controller.ps1'))) {
            $handle = Get-Process -Id $current.ProcessId
            try {
                $null = $handle.Handle
                if ([math]::Abs(($handle.StartTime - $current.CreationDate).TotalMilliseconds) -gt 1) { throw 'Recovery controller identity changed' }
                Stop-Process -InputObject $handle -Force
                if (-not $handle.WaitForExit(10000)) { throw 'Recovery controller remains' }
            }
            finally { $handle.Dispose() }
        }
    }
    foreach ($port in $saved.ports) {
        $owned = @(Get-ProjectStreamlit $recoveryRoot $port)
        Assert-StreamlitPortOwner @(Get-StreamlitListeners $port) $owned
        Stop-VerifiedStreamlit $owned $recoveryRoot $port 20
        if (@(Get-ProjectStreamlit $recoveryRoot $port).Count -gt 0 -or @(Get-StreamlitListeners $port).Count -gt 0) { throw "Recovery cleanup incomplete on port $port" }
    }
    Write-Output 'Recovery verified: fixture controller, servers and both listeners cleared'
    exit 0
}
$fixtureRoot = Join-Path $ProjectRoot ('output\tmp\startup-lifecycle-' + [guid]::NewGuid().ToString('N'))
$ports = @(18503, 18504)
$controller = $null
$evidence = $EvidencePath
$report = [ordered]@{fixture=$fixtureRoot; ports=$ports; scenarios=@(); cleanup='pending'}
function Save-Lifecycle { $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $evidence -Encoding UTF8 }
function Assert-FixtureEmpty {
    foreach ($port in $ports) {
        if (@(Get-ProjectStreamlit $fixtureRoot $port).Count -gt 0 -or @(Get-StreamlitListeners $port).Count -gt 0) { throw "Fixture port $port or processes remain" }
    }
}
foreach ($port in $ports) {
    if (@(Get-StreamlitListeners $port).Count -gt 0) { throw "Test port $port already occupied; nothing stopped" }
}
New-Item -ItemType Directory -Path (Join-Path $fixtureRoot 'app') -Force | Out-Null
New-Item -ItemType Directory -Path (Split-Path $evidence) -Force | Out-Null
New-Item -ItemType Junction -Path (Join-Path $fixtureRoot '.venv') -Target (Join-Path $ProjectRoot '.venv') | Out-Null
Set-Content -LiteralPath (Join-Path $fixtureRoot 'app\Home.py') -Value 'import streamlit as st; st.write("Startup fixture; no database")' -Encoding UTF8
Save-Lifecycle
try {
    if ((Invoke-StreamlitRestart $fixtureRoot 18503 20 30) -ne 0) { throw 'Normal startup failed' }
    $old = @(Get-ProjectStreamlit $fixtureRoot 18503)
    if ((Invoke-StreamlitRestart $fixtureRoot 18503 20 30) -ne 0) { throw 'Normal restart failed' }
    $new = @(Get-ProjectStreamlit $fixtureRoot 18503)
    if (@($new | Where-Object ProcessId -in $old.ProcessId).Count -gt 0 -or -not (Test-StreamlitHealth 18503)) { throw 'Restart did not replace the old service' }
    $report.scenarios += @{scenario='normal restart'; old_pids=@($old.ProcessId); new_pids=@($new.ProcessId); health_http=200}
    Save-Lifecycle
    Stop-VerifiedStreamlit $new $fixtureRoot 18503 20
    Assert-FixtureEmpty

    # A real listening process must be removed when startup verification fails.
    $healthFunction = (Get-Item Function:Test-StreamlitHealth).ScriptBlock
    try {
        function Test-StreamlitHealth { param([int]$ServerPort) return $false }
        if ((Invoke-StreamlitRestart $fixtureRoot 18503 20 2) -ne 1) { throw 'Failure was reported as success' }
    }
    finally { Set-Item Function:Test-StreamlitHealth $healthFunction }
    Assert-FixtureEmpty
    $failureFile = Get-ChildItem (Join-Path $fixtureRoot 'output\logs\streamlit-startup') -Filter '*.json' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $failure = Get-Content $failureFile.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($failure.status -ne 'failed' -or $null -eq $failure.launched_pid -or $failure.cleanup_error) { throw 'Failed-start cleanup evidence is incomplete' }
    $report.scenarios += @{scenario='startup failure'; process_started=$failure.launched_pid; error=$failure.error; processes_exited=$true; ports_released=$true}
    Save-Lifecycle

    # Terminate a restart controller while its real server is awaiting health.
    # Keep a second fixture server alive to verify cleanup covers every port.
    if ((Invoke-StreamlitRestart $fixtureRoot 18504 20 30) -ne 0) { throw 'Second fixture startup failed' }
    $shim = Join-Path $fixtureRoot 'interrupted-controller.ps1'
    $source = ". '" + (Join-Path $ProjectRoot 'tools\restart_streamlit.ps1').Replace("'", "''") + "'`r`n" +
        'function Wait-StreamlitReady { param($StartedProcess, $Root, $ServerPort, $TimeoutSeconds) Start-Sleep -Seconds 120 }' + "`r`n" +
        "exit (Invoke-StreamlitRestart '" + $fixtureRoot.Replace("'", "''") + "' 18503 20 120)"
    Set-Content -LiteralPath $shim -Value $source -Encoding UTF8
    $controller = Start-Process -FilePath "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -WindowStyle Hidden -PassThru -ArgumentList @('-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', "`"$shim`"")
    $controllerIdentity = Get-CimInstance Win32_Process -Filter "ProcessId=$($controller.Id)"
    $report.controller = @{pid=$controller.Id; created_at=$controllerIdentity.CreationDate.ToString('o')}
    Save-Lifecycle
    $deadline = (Get-Date).AddSeconds(30)
    while (@(Get-StreamlitListeners 18503).Count -eq 0 -and (Get-Date) -lt $deadline -and -not $controller.HasExited) { Start-Sleep -Milliseconds 500 }
    if (@(Get-StreamlitListeners 18503).Count -eq 0) { throw 'Interrupted startup never reached a listening process' }
    $owned = @($ports | ForEach-Object { Get-ProjectStreamlit $fixtureRoot $_ })
    $report.scenarios += @{scenario='forced controller interruption'; controller_pid=$controller.Id; controller_created_at=$controllerIdentity.CreationDate.ToString('o'); server_pids=@($owned.ProcessId); parent_pids=@($owned.ParentProcessId); ports=$ports}
    Save-Lifecycle
    Stop-Process -InputObject $controller -Force
    if (-not $controller.WaitForExit(10000)) { throw 'Interrupted controller did not exit' }
    foreach ($port in $ports) { Stop-VerifiedStreamlit @(Get-ProjectStreamlit $fixtureRoot $port) $fixtureRoot $port 20 }
    Assert-FixtureEmpty
    $report.scenarios[-1].processes_exited = $true
    $report.scenarios[-1].ports_released = $true
    $report.status = 'passed'
    Write-Output 'PASS: normal restart, failed startup, forced interruption; both fixture ports released'
}
catch { $report.status='failed'; $report.error=$_.Exception.Message; throw }
finally {
    if ($null -ne $controller) {
        if (-not $controller.HasExited) {
            $current = Get-CimInstance Win32_Process -Filter "ProcessId=$($controller.Id)"
            if ($null -ne $current -and $current.CreationDate -eq $controllerIdentity.CreationDate) { Stop-Process -InputObject $controller -Force }
        }
        if (-not $controller.WaitForExit(10000)) { throw 'Controller cleanup incomplete' }
        $controller.Dispose()
    }
    foreach ($port in $ports) { Stop-VerifiedStreamlit @(Get-ProjectStreamlit $fixtureRoot $port) $fixtureRoot $port 20 }
    Assert-FixtureEmpty
    $report.cleanup='verified: all fixture processes exited and both ports released'
    $report.finished_at=(Get-Date).ToString('o')
    Save-Lifecycle
}
