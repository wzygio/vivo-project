# Compatibility entry for existing Windows scheduled tasks.
[CmdletBinding()]
param([string]$ProjectRoot = '')

$Launcher = Join-Path $PSScriptRoot '..\api\anomaly\start_anomaly_api.ps1'
& $Launcher -ProjectRoot $ProjectRoot
exit $LASTEXITCODE
