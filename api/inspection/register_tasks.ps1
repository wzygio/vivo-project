param(
    [string]$ProjectRoot,
    [string]$EnvFile,
    [string]$MailConfigPath,
    [switch]$InspectionOnly,
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'
if (-not $ProjectRoot) {
    $ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
}
$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path
$Python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$PythonWindowless = Join-Path $ProjectRoot '.venv\Scripts\pythonw.exe'
if (-not $EnvFile) { $EnvFile = Join-Path $ProjectRoot '.env' }
foreach ($Target in @($Python, $PythonWindowless, $EnvFile)) {
    if (-not (Test-Path -LiteralPath $Target -PathType Leaf)) { throw 'Required runtime or environment file is missing.' }
}
$EnvFile = (Resolve-Path -LiteralPath $EnvFile).Path
$SpecArgs = @('-m', 'api.inspection.scheduling', '--root', $ProjectRoot, '--env-file', $EnvFile)
if ($MailConfigPath) {
    if (-not (Test-Path -LiteralPath $MailConfigPath -PathType Leaf)) { throw 'Explicit mail credential file is missing.' }
    $MailConfigPath = (Resolve-Path -LiteralPath $MailConfigPath).Path
    $SpecArgs += @('--mail-config', $MailConfigPath)
}
Push-Location $ProjectRoot
try {
    $SpecJson = & $Python @SpecArgs
    if ($LASTEXITCODE -ne 0) { throw 'Cannot build scheduled task definitions.' }
    $Specs = ConvertFrom-Json -InputObject ($SpecJson -join "`n")
}
finally { Pop-Location }
if ($Specs.Count -ne 2) { throw 'Expected exactly the inspection and notification task definitions.' }
if ($InspectionOnly) { $Specs = @($Specs | Where-Object { $_.module -eq 'api.inspection' }) }
$WarmupTaskName = 'vivo-project Alert Matrix Warmup'

function Get-FutureBoundary($Trigger) {
    $Now = [datetime]::Now
    $Time = [timespan]::ParseExact($Trigger.time, 'hh\:mm', [cultureinfo]::InvariantCulture)
    if ($Trigger.kind -eq 'daily') {
        # Never back-date a new trigger: StartWhenAvailable could otherwise run
        # production immediately while the deployer is still checking the job.
        return [datetime]::Today.AddDays(1).Add($Time)
    }
    $Weekday = [System.DayOfWeek]::$($Trigger.weekday)
    $Delta = (([int]$Weekday - [int]$Now.DayOfWeek) + 7) % 7
    $Boundary = [datetime]::Today.AddDays($Delta).Add($Time)
    if ($Boundary -le $Now) { $Boundary = $Boundary.AddDays(7) }
    return $Boundary
}

$Plan = @($Specs | ForEach-Object {
    $Spec = $_
    [PSCustomObject]@{
        Name = $Spec.name; Module = $Spec.module; Execute = $Spec.execute
        Arguments = $Spec.arguments; WorkingDirectory = $Spec.working_directory
        Triggers = @($Spec.triggers | ForEach-Object {
            [PSCustomObject]@{Kind=$_.kind;Weekday=$_.weekday;Time=$_.time;StartBoundary=(Get-FutureBoundary $_).ToString('s')}
        })
    }
})
if (-not $Apply) {
    [PSCustomObject]@{Status='plan';Tasks=$Plan;RetireTasks=@($WarmupTaskName)} | ConvertTo-Json -Depth 6
    return
}

$BackupDirectory = Join-Path $ProjectRoot ('output\logs\alert_inspection\task-backups\' + [datetime]::Now.ToString('yyyyMMdd-HHmmss-ffff'))
New-Item -ItemType Directory -Path $BackupDirectory -Force | Out-Null
$Backups = @{}
$WarmupTask = Get-ScheduledTask -TaskName $WarmupTaskName -ErrorAction SilentlyContinue
if ($WarmupTask) {
    $Actions = @($WarmupTask.Actions)
    $LegacyScript = Join-Path $ProjectRoot 'tools\warm_alert_matrix.py'
    if ($WarmupTask.State -eq 'Running' -or $Actions.Count -ne 1 -or
        $Actions[0].WorkingDirectory.TrimEnd('\') -ne $ProjectRoot.TrimEnd('\') -or
        $Actions[0].Execute -ne $PythonWindowless -or
        $Actions[0].Arguments -ne ('"' + $LegacyScript + '"')) {
        throw 'Warmup task is running or its identity does not match; nothing was registered.'
    }
    $Xml = Export-ScheduledTask -TaskName $WarmupTaskName
    $Backups[$WarmupTaskName] = $Xml
    [IO.File]::WriteAllText((Join-Path $BackupDirectory ($WarmupTaskName + '.xml')), $Xml)
}
foreach ($Spec in $Specs) {
    $Existing = Get-ScheduledTask -TaskName $Spec.name -ErrorAction SilentlyContinue
    if ($Existing) {
        # Only replace the requested existing producer or a prior version of the
        # exact notification task. A same-name unrelated task is not ours.
        $Actions = @($Existing.Actions)
        if ($Actions.Count -ne 1 -or $Actions[0].WorkingDirectory.TrimEnd('\') -ne $ProjectRoot.TrimEnd('\') -or
            $Actions[0].Execute -ne $PythonWindowless -or
            ($Actions[0].Arguments -notmatch 'publish_dashboard_alert_results\.py' -and
             $Actions[0].Arguments -notmatch '^-m api\.(inspection|notification\.scheduled)(?:\s|$)')) {
            throw 'Existing task identity does not match this project; nothing was registered.'
        }
        $Xml = Export-ScheduledTask -TaskName $Spec.name
        $Backups[$Spec.name] = $Xml
        $SafeName = $Spec.name -replace '[^A-Za-z0-9 _-]', '_'
        [IO.File]::WriteAllText((Join-Path $BackupDirectory ($SafeName + '.xml')), $Xml)
    }
    else { $Backups[$Spec.name] = $null }
}

$Identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $Identity -LogonType Interactive -RunLevel Limited
$Changed = [Collections.Generic.List[string]]::new()
try {
    foreach ($Spec in $Specs) {
        $Action = New-ScheduledTaskAction -Execute $Spec.execute -Argument $Spec.arguments -WorkingDirectory $Spec.working_directory
        $Triggers = @($Spec.triggers | ForEach-Object {
            $Boundary = Get-FutureBoundary $_
            if ($_.kind -eq 'daily') { New-ScheduledTaskTrigger -Daily -DaysInterval 1 -At $Boundary }
            else { New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $_.weekday -At $Boundary }
        })
        $Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -WakeToRun `
            -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew `
            -ExecutionTimeLimit (New-TimeSpan -Hours 2)
        $Description = if ($Spec.module -eq 'api.inspection') {
            'All dashboard alert producers followed by alert matrix warmup daily at 07:15; requires logged-on user.'
        } else {
            'Weekly alert email Monday 07:20 and Tuesday 11:00, only after complete current-day inspection.'
        }
        Register-ScheduledTask -TaskName $Spec.name -Action $Action -Trigger $Triggers -Settings $Settings `
            -Principal $Principal -Force -Description $Description | Out-Null
        $Changed.Add($Spec.name)
        $Actual = Get-ScheduledTask -TaskName $Spec.name
        if (@($Actual.Actions).Count -ne 1 -or $Actual.Actions[0].Execute -ne $Spec.execute -or
            $Actual.Actions[0].Arguments -ne $Spec.arguments -or $Actual.Actions[0].WorkingDirectory -ne $Spec.working_directory -or
            @($Actual.Triggers).Count -ne $Spec.triggers.Count) {
            throw 'Registered task action or trigger inventory failed verification.'
        }
        for ($Index = 0; $Index -lt $Spec.triggers.Count; $Index++) {
            $Want = $Spec.triggers[$Index]
            $Have = $Actual.Triggers[$Index]
            if (-not $Have.Enabled -or $Have.Repetition.Interval -or
                ([datetime]$Have.StartBoundary).ToString('HH:mm') -ne $Want.time) {
                throw 'Registered task trigger has an unexpected time or repetition.'
            }
            if ($Want.kind -eq 'daily') {
                if ($Have.CimClass.CimClassName -ne 'MSFT_TaskDailyTrigger' -or $Have.DaysInterval -ne 1) {
                    throw 'Daily inspection trigger failed verification.'
                }
            }
            else {
                $DayFlag = if ($Want.weekday -eq 'Monday') { 2 } else { 4 }
                if ($Have.CimClass.CimClassName -ne 'MSFT_TaskWeeklyTrigger' -or
                    $Have.WeeksInterval -ne 1 -or $Have.DaysOfWeek -ne $DayFlag) {
                    throw 'Weekly notification trigger failed verification.'
                }
            }
        }
    }
    if ($WarmupTask) {
        $Changed.Add($WarmupTaskName)
        Unregister-ScheduledTask -TaskName $WarmupTaskName -Confirm:$false
        if (Get-ScheduledTask -TaskName $WarmupTaskName -ErrorAction SilentlyContinue) {
            throw 'Retired warmup task is still registered.'
        }
    }
}
catch {
    foreach ($Name in $Changed) {
        if ($Backups[$Name]) { Register-ScheduledTask -TaskName $Name -Xml $Backups[$Name] -Force | Out-Null }
        else { Unregister-ScheduledTask -TaskName $Name -Confirm:$false -ErrorAction SilentlyContinue }
    }
    throw 'Task registration or readback failed; targeted tasks were restored from their prior definitions.'
}
[PSCustomObject]@{Status='registered';Backups=$BackupDirectory;Tasks=@($Specs | ForEach-Object {
    $Info=Get-ScheduledTaskInfo -TaskName $_.name
    [PSCustomObject]@{Name=$_.name;Module=$_.module;NextRun=$Info.NextRunTime.ToString('s')}
})} | ConvertTo-Json -Depth 6
