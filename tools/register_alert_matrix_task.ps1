param(
    [string]$TaskName = "vivo-project Alert Matrix Warmup",
    [string]$At = "07:30",
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$PythonPath = Join-Path $RepoRoot ".venv\Scripts\pythonw.exe"
$JobScript = Join-Path $RepoRoot "tools\warm_alert_matrix.py"
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
$Trigger = New-ScheduledTaskTrigger -Daily -At $At
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 15)
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Settings $Settings -Principal $Principal -Force:$Force `
    -Description "Daily 07:30 alert matrix status snapshot for vivo-project. Requires the user to be logged on for Excel COM." | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
Get-ScheduledTaskInfo -TaskName $TaskName | Select-Object NextRunTime, LastTaskResult
