@echo off
setlocal
REM Restart both project services and return a failing exit code if either fails.
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0tools\restart_streamlit.ps1"
set "streamlitExitCode=%ERRORLEVEL%"
call "%~dp0start_anomaly_api.cmd" --no-pause
set "apiExitCode=%ERRORLEVEL%"
if not "%streamlitExitCode%"=="0" exit /b %streamlitExitCode%
exit /b %apiExitCode%
