@echo off
setlocal
REM Restart the project server and return the verified startup result.
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0tools\restart_streamlit.ps1"
exit /b %errorlevel%
