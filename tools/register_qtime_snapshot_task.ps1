param(
    [string]$TaskName = "vivo-project Q-Time snapshot refresh",
    [string]$At = "07:00",
    [string]$PythonPath,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RefreshScript = Join-Path $RepoRoot "tools\refresh_qtime_snapshots.py"

if (-not (Test-Path -LiteralPath $RefreshScript -PathType Leaf)) {
    throw "Q-Time refresh script not found: $RefreshScript"
}

if (-not $PythonPath) {
    $VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $VenvPython -PathType Leaf) {
        $PythonPath = $VenvPython
    }
    else {
        $PythonPath = (Get-Command python -ErrorAction Stop).Source
    }
}
$PythonPath = (Resolve-Path -LiteralPath $PythonPath).Path

$ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($ExistingTask -and -not $Force) {
    throw "Scheduled task already exists. Re-run with -Force to replace: $TaskName"
}
if ($ExistingTask) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

$Action = New-ScheduledTaskAction `
    -Execute $PythonPath `
    -Argument "`"$RefreshScript`"" `
    -WorkingDirectory $RepoRoot
$Trigger = New-ScheduledTaskTrigger -Daily -At $At
$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Refresh vivo-project shared Q-Time snapshots every day." `
    -User $env:USERNAME | Out-Null

Write-Output "Registered '$TaskName' to run daily at $At using $PythonPath"
