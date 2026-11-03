param(
    [string]$TaskName = "vivo-project Yield IJP Alert Publication",
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$PythonPath = Join-Path $RepoRoot ".venv\Scripts\pythonw.exe"
$JobScript = Join-Path $RepoRoot "tools\publish_dashboard_alert_results.py"
foreach ($Target in @($PythonPath, $JobScript)) {
    if (-not (Test-Path -LiteralPath $Target -PathType Leaf)) {
        throw "Required file not found: $Target"
    }
}
$Existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($Existing -and -not $Force) {
    throw "Task already exists. Use -Force to update this task: $TaskName"
}
$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $Identity -LogonType Interactive -RunLevel Limited
$Action = New-ScheduledTaskAction -Execute $PythonPath `
    -Argument "`"$JobScript`"" -WorkingDirectory $RepoRoot
# The readers require today's publication, so check both after logon and
# throughout the day. Valid publications are skipped without database access.
$Trigger = New-ScheduledTaskTrigger -Once -At ([datetime]::Today) `
    -RepetitionInterval (New-TimeSpan -Minutes 30)
$LogonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $Identity
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger @($Trigger, $LogonTrigger) `
    -Settings $Settings -Principal $Principal -Force:$Force `
    -Description "Publish expired Yield/IJP report results every 30 minutes while logged on; matrix remains file-only." | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
Get-ScheduledTaskInfo -TaskName $TaskName | Select-Object NextRunTime, LastTaskResult
