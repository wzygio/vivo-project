@echo off
setlocal
REM Restart this project's managed API and verify readiness.
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0api\anomaly\start_anomaly_api.ps1"
set "apiExitCode=%ERRORLEVEL%"
if /I "%~1"=="--no-pause" exit /b %apiExitCode%
echo.
pause
exit /b %apiExitCode%
