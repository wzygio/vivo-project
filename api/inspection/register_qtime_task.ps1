param(
    [string]$TaskName = 'vivo-project Q-Time snapshot refresh',
    [string]$At = '07:00',
    [string]$PythonPath,
    [switch]$Force,
    [switch]$Plan
)

# Optional dedicated Q-Time maintenance task; unified inspection includes Q-Time.
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot 'api\inspection\qtime_publication.py') -PathType Leaf)) {
    throw 'Q-Time publication module is missing.'
}
if (-not $PythonPath) { $PythonPath = Join-Path $RepoRoot '.venv\Scripts\pythonw.exe' }
$PythonPath = (Resolve-Path -LiteralPath $PythonPath).Path
try { $Time = [timespan]::ParseExact($At, 'hh\:mm', [cultureinfo]::InvariantCulture) }
catch { throw 'Use a valid HH:mm time for the Q-Time task.' }
$Boundary = [datetime]::Today.AddDays(1).Add($Time)
$Arguments = '-m api.inspection.qtime_publication'
if ($Plan) {
    [PSCustomObject]@{Status='plan';Name=$TaskName;Execute=$PythonPath;Arguments=$Arguments;
        WorkingDirectory=$RepoRoot;StartBoundary=$Boundary.ToString('s');Time=$At} | ConvertTo-Json
    return
}
$ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($ExistingTask -and -not $Force) {
    throw 'Scheduled task already exists. Re-run with -Force to replace it.'
}
if ($ExistingTask) {
    $Actions = @($ExistingTask.Actions)
    if ($Actions.Count -ne 1 -or $Actions[0].WorkingDirectory.TrimEnd('\') -ne $RepoRoot.TrimEnd('\') -or
        ($Actions[0].Arguments -notmatch 'refresh_qtime_snapshots\.py' -and
         $Actions[0].Arguments -ne '-m api.inspection.qtime_publication')) {
        throw 'Existing task identity is not a Q-Time task owned by this project.'
    }
}
$Action = New-ScheduledTaskAction -Execute $PythonPath -Argument $Arguments -WorkingDirectory $RepoRoot
$Trigger = New-ScheduledTaskTrigger -Daily -DaysInterval 1 -At $Boundary
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 2)
$Identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $Identity -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings `
    -Principal $Principal -Force:$Force -Description 'Optional dedicated Q-Time source refresh and alert publication.' | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
