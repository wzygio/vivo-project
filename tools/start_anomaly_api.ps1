# Restart this project's per-user API host, preserving deployment credentials.
[CmdletBinding()]
param([string]$ProjectRoot = '')

$ErrorActionPreference = 'Stop'
try {
    if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
        $ProjectRoot = Split-Path -Parent $PSScriptRoot
    }
    $apiRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path
    $apiPython = Join-Path $apiRoot '.venv\Scripts\python.exe'
    $apiHostTool = Join-Path $apiRoot 'tools\anomaly_api_service.py'
    $apiProfile = Join-Path $env:LOCALAPPDATA 'Tianzhu\AnomalyApi\deployment.json'
    foreach ($apiRequiredFile in @($apiPython, $apiHostTool)) {
        if (-not (Test-Path -LiteralPath $apiRequiredFile -PathType Leaf)) {
            throw "Required project file not found: $apiRequiredFile"
        }
    }
    if (-not (Test-Path -LiteralPath $apiProfile -PathType Leaf)) {
        throw 'Deployment profile missing. First run tools\setup_anomaly_api_credentials.ps1 as the deployment user.'
    }

    Write-Host 'Restarting the project API; existing requests and pagination may be interrupted.'
    $apiStartOutput = & $apiPython $apiHostTool restart
    if ($LASTEXITCODE -ne 0) {
        throw 'API host restart failed. Check output\logs\anomaly-api; the old instance may already be stopped.'
    }
    $apiStart = ($apiStartOutput -join [Environment]::NewLine) | ConvertFrom-Json
    if ($apiStart.status -notin @('ready', 'already_running')) {
        throw 'API host returned an unexpected startup result.'
    }
    $apiStatusOutput = & $apiPython $apiHostTool status
    if ($LASTEXITCODE -ne 0) {
        throw 'Could not verify the API host status. Check output\logs\anomaly-api.'
    }
    $apiStatus = ($apiStatusOutput -join [Environment]::NewLine) | ConvertFrom-Json
    if ($apiStatus.ready -ne $true -or $apiStatus.supervisor_alive -ne $true -or $apiStatus.listener_owned -ne $true) {
        throw 'API is not ready after restart. Check the host logs.'
    }
    Write-Host ("API is ready (port {0}; {1})." -f $apiStatus.port, $apiStart.status)
    Write-Host ("Local health: http://127.0.0.1:{0}/health/live" -f $apiStatus.port)
    Write-Host 'The API runs in the background. Closing this window does not stop it.'
    exit 0
} catch {
    Write-Error -Message $_.Exception.Message -ErrorAction Continue
    exit 2
}
