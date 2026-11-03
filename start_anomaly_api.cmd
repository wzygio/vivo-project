@echo off
setlocal
powershell.exe -NoProfile -File "%~dp0tools\start_anomaly_api.ps1"
set "apiExitCode=%ERRORLEVEL%"
echo.
pause
exit /b %apiExitCode%
