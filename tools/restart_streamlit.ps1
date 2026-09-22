param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [ValidateRange(1, 65535)][int]$Port = 8503,
    [ValidateRange(1, 300)][int]$StopTimeoutSeconds = 20,
    [ValidateRange(1, 300)][int]$StartupTimeoutSeconds = 60
)

# Dot-source this file to test the functions without restarting a server.
function Test-ProjectStreamlit {
    param($Process, [string]$Root, [int]$ServerPort)
    if ($Process.Name -notin @('python.exe', 'pythonw.exe', 'streamlit.exe')) { return $false }
    $tokens = @([regex]::Matches([string]$Process.CommandLine, '"[^"]*"|\S+') |
        ForEach-Object { $_.Value.Trim('"').Replace('/', '\') })
    $runIndex = [array]::IndexOf($tokens, 'run')
    if ($runIndex -lt 1 -or $runIndex + 1 -ge $tokens.Count) { return $false }
    $launchers = @(
        (Join-Path $Root '.venv\Scripts\streamlit.exe'),
        (Join-Path $Root 'Vivo_project\Scripts\streamlit.exe')
    )
    $moduleRun = $runIndex -ge 3 -and $tokens[$runIndex - 1] -eq 'streamlit' -and $tokens[$runIndex - 2] -eq '-m'
    $localLauncher = $tokens[$runIndex - 1] -in $launchers
    if (-not ($moduleRun -or $localLauncher)) { return $false }
    $entrypoint = $tokens[$runIndex + 1]
    $localPython = [string]$Process.ExecutablePath -in @(
        (Join-Path $Root '.venv\Scripts\python.exe'),
        (Join-Path $Root '.venv\Scripts\pythonw.exe'),
        (Join-Path $Root 'Vivo_project\Scripts\python.exe')
    )
    $absoluteEntry = $entrypoint -eq (Join-Path $Root 'app\Home.py')
    $relativeEntry = $entrypoint -in @('app\Home.py', '.\app\Home.py')
    if (-not ($absoluteEntry -or ($relativeEntry -and ($localPython -or $localLauncher)))) { return $false }
    $ports = @()
    for ($index = $runIndex + 2; $index -lt $tokens.Count; $index++) {
        if ($tokens[$index] -eq '--') { break }
        if ($tokens[$index] -match '^--server\.port=(\d+)$') { $ports += $Matches[1] }
        elseif ($tokens[$index] -eq '--server.port' -and $index + 1 -lt $tokens.Count) {
            $ports += $tokens[$index + 1]
        }
    }
    return $ports.Count -eq 1 -and $ports[0] -eq [string]$ServerPort
}

function Get-ProjectStreamlit {
    param([string]$Root, [int]$ServerPort)
    Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
        Test-ProjectStreamlit $_ $Root $ServerPort
    }
}

function Get-StreamlitListeners {
    param([int]$ServerPort)
    # Query all listeners so an empty target port is not confused with a CIM error.
    Get-NetTCPConnection -State Listen -ErrorAction Stop | Where-Object LocalPort -eq $ServerPort
}

function Assert-StreamlitPortOwner {
    param($Listeners, $Processes)
    $knownIds = @($Processes | ForEach-Object { $_.ProcessId })
    foreach ($listener in $Listeners) {
        if ($listener.OwningProcess -notin $knownIds) {
            throw "Port is occupied by an unverified process (PID $($listener.OwningProcess)); nothing was stopped."
        }
    }
}

function Stop-VerifiedStreamlit {
    param($Processes, [string]$Root, [int]$ServerPort, [int]$TimeoutSeconds)
    foreach ($record in $Processes) {
        $current = Get-CimInstance Win32_Process -Filter "ProcessId=$($record.ProcessId)" -ErrorAction Stop
        if ($null -eq $current) { continue }
        if ($current.CreationDate -ne $record.CreationDate -or -not (Test-ProjectStreamlit $current $Root $ServerPort)) {
            throw "Process identity changed before shutdown (PID $($record.ProcessId))."
        }
        $handle = Get-Process -Id $record.ProcessId -ErrorAction SilentlyContinue
        if ($null -eq $handle) { continue }
        try {
            # Capture a handle before stopping; WaitForExit verifies this instance.
            $null = $handle.Handle
            if ([math]::Abs(($handle.StartTime - $current.CreationDate).TotalMilliseconds) -gt 1) {
                throw "Process identity changed while opening PID $($record.ProcessId)."
            }
            if (-not $handle.HasExited) { Stop-Process -InputObject $handle -Force -ErrorAction Stop }
            if (-not $handle.WaitForExit($TimeoutSeconds * 1000)) {
                throw "Process did not exit within the shutdown deadline (PID $($record.ProcessId))."
            }
        }
        catch {
            # A launcher can exit naturally when another process in its family stops.
            if (-not $handle.HasExited) { throw }
        }
        finally { $handle.Dispose() }
    }
}

function Test-StreamlitDescendant {
    param([int]$CandidateId, [int]$RootId, $Processes)
    $seen = @{}
    while ($CandidateId -gt 0 -and -not $seen.ContainsKey($CandidateId)) {
        if ($CandidateId -eq $RootId) { return $true }
        $seen[$CandidateId] = $true
        $record = $Processes | Where-Object ProcessId -eq $CandidateId | Select-Object -First 1
        if ($null -eq $record) { return $false }
        $CandidateId = [int]$record.ParentProcessId
    }
    return $false
}

function Test-StreamlitHealth {
    param([int]$ServerPort)
    $response = $null
    try {
        $request = [System.Net.HttpWebRequest]::Create("http://127.0.0.1:$ServerPort/_stcore/health")
        $request.Proxy = $null
        $request.Timeout = 2000
        $response = $request.GetResponse()
        return [int]$response.StatusCode -eq 200
    }
    catch [System.Net.WebException] { return $false }
    finally { if ($null -ne $response) { $response.Close() } }
}

function Wait-StreamlitReady {
    param($StartedProcess, [string]$Root, [int]$ServerPort, [int]$TimeoutSeconds)
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    while ($clock.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        if ($StartedProcess.HasExited) { throw 'The new Streamlit process exited before becoming healthy.' }
        $processes = @(Get-ProjectStreamlit $Root $ServerPort)
        $listeners = @(Get-StreamlitListeners $ServerPort)
        Assert-StreamlitPortOwner $listeners $processes
        foreach ($listener in $listeners) {
            if (-not (Test-StreamlitDescendant $listener.OwningProcess $StartedProcess.Id $processes)) {
                throw 'The listening process does not belong to this startup.'
            }
        }
        if ($listeners.Count -gt 0 -and (Test-StreamlitHealth $ServerPort)) {
            return @($listeners | Select-Object -ExpandProperty OwningProcess -Unique)
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'Streamlit health check timed out.'
}

function Assert-StreamlitRuntime {
    param([string]$Python, [string]$App)
    foreach ($required in @($Python, $App)) {
        if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Required startup file is missing: $required" }
    }
    & $Python -c 'import streamlit' 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'The project virtual environment cannot import Streamlit.' }
}

function Invoke-StreamlitRestart {
    param([string]$Root, [int]$ServerPort, [int]$StopSeconds, [int]$StartupSeconds)
    $ErrorActionPreference = 'Stop'
    $lock = $null
    $started = $null
    $logPath = $null
    $previousPythonPath = $env:PYTHONPATH
    $status = [ordered]@{
        status = 'starting'; started_at = (Get-Date).ToString('o'); port = $ServerPort
        stopped_pids = @(); launched_pid = $null; listener_pids = @()
    }
    try {
        $Root = (Resolve-Path -LiteralPath $Root).Path.TrimEnd('\')
        $logDir = Join-Path $Root 'output\logs\streamlit-startup'
        New-Item -ItemType Directory -Path $logDir -Force | Out-Null
        $runId = (Get-Date -Format 'yyyyMMdd-HHmmss-fff') + '-' + [guid]::NewGuid().ToString('N')
        $logPath = Join-Path $logDir "$runId.json"
        $status | ConvertTo-Json | Set-Content -LiteralPath $logPath -Encoding UTF8
        # Exclusive file lock works across interactive and scheduled sessions.
        $lock = [System.IO.File]::Open((Join-Path $logDir "restart-$ServerPort.lock"), 'OpenOrCreate', 'ReadWrite', 'None')
        $python = Join-Path $Root '.venv\Scripts\python.exe'
        $app = Join-Path $Root 'app\Home.py'
        # Check the runtime before interrupting a healthy existing service.
        Assert-StreamlitRuntime $python $app
        $old = @(Get-ProjectStreamlit $Root $ServerPort)
        Assert-StreamlitPortOwner @(Get-StreamlitListeners $ServerPort) $old
        Stop-VerifiedStreamlit $old $Root $ServerPort $StopSeconds
        $status.stopped_pids = @($old | ForEach-Object { $_.ProcessId })
        if (@(Get-ProjectStreamlit $Root $ServerPort).Count -gt 0 -or @(Get-StreamlitListeners $ServerPort).Count -gt 0) {
            throw 'Old Streamlit processes or port listeners remain after shutdown.'
        }
        $env:PYTHONPATH = (Join-Path $Root 'src') + ';' + $previousPythonPath
        $stdout = Join-Path $logDir "$runId.stdout.log"
        $stderr = Join-Path $logDir "$runId.stderr.log"
        $started = Start-Process -FilePath $python -WorkingDirectory $Root -WindowStyle Hidden -PassThru `
            -ArgumentList @('-m', 'streamlit', 'run', "`"$app`"", '--server.headless=true', "--server.port=$ServerPort") `
            -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        $status.launched_pid = $started.Id
        $status.listener_pids = @(Wait-StreamlitReady $started $Root $ServerPort $StartupSeconds)
        $status.status = 'success'
        return 0
    }
    catch {
        $status.status = 'failed'
        $status.error = $_.Exception.Message
        if ($null -ne $started) {
            try {
                $processes = @(Get-ProjectStreamlit $Root $ServerPort)
                $owned = @($processes | Where-Object { Test-StreamlitDescendant $_.ProcessId $started.Id $processes })
                Stop-VerifiedStreamlit $owned $Root $ServerPort $StopSeconds
            }
            catch { $status.cleanup_error = $_.Exception.Message }
        }
        [Console]::Error.WriteLine("Streamlit restart failed. See startup log: $logPath")
        return 1
    }
    finally {
        $env:PYTHONPATH = $previousPythonPath
        $status.finished_at = (Get-Date).ToString('o')
        if ($null -ne $logPath) { $status | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $logPath -Encoding UTF8 }
        if ($null -ne $started) { $started.Dispose() }
        if ($null -ne $lock) { $lock.Dispose() }
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    exit (Invoke-StreamlitRestart $ProjectRoot $Port $StopTimeoutSeconds $StartupTimeoutSeconds)
}
