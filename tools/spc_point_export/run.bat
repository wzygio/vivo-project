@echo off
setlocal
set "PROJECT_ROOT=%~dp0..\.."
pushd "%PROJECT_ROOT%" || exit /b 2
if not exist ".venv\Scripts\python.exe" (
  echo Project .venv is missing. Set up the existing project environment first.
  popd
  exit /b 2
)
".venv\Scripts\python.exe" -X utf8 -m tools.spc_point_export %*
set "EXPORT_EXIT=%ERRORLEVEL%"
popd
exit /b %EXPORT_EXIT%
