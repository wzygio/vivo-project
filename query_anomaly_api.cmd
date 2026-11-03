@echo off
setlocal
set "apiClientScript=%~dp0tools\fetch_sheet_anomalies.py"
if not exist "%apiClientScript%" (
    echo API client program not found: tools\fetch_sheet_anomalies.py
    pause
    exit /b 2
)
if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" "%apiClientScript%" %*
) else (
    py -3 "%apiClientScript%" %*
)
set "apiClientExitCode=%ERRORLEVEL%"
if not "%apiClientExitCode%"=="0" pause
exit /b %apiClientExitCode%
