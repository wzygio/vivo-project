param(
    [string]$TaskName = 'vivo-project Q-Time snapshot refresh',
    [string]$At = '07:00',
    [string]$PythonPath,
    [switch]$Force,
    [switch]$Plan
)

$ErrorActionPreference = 'Stop'
$CanonicalRegistrar = Join-Path $PSScriptRoot '..\api\inspection\register_qtime_task.ps1'
& $CanonicalRegistrar @PSBoundParameters
