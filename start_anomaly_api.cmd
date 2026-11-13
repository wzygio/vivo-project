@echo off
setlocal
REM Restart this project's managed API and verify readiness.
powershell.exe -NoProfile -File "%~dp0tools\start_anomaly_api.ps1"
set "apiExitCode=%ERRORLEVEL%"
echo.
pause
exit /b %apiExitCode%
