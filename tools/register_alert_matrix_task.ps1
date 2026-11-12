param(
    [string]$TaskName = 'vivo-project Alert Matrix Warmup',
    [string]$At = '09:02',
    [switch]$Force
)
$ErrorActionPreference = 'Stop'
throw 'This registrar is retired. Matrix warmup now follows api.inspection; use api/inspection/register_tasks.ps1 -InspectionOnly.'
