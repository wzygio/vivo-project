@echo off
TITLE Visionox AI System Launcher
ECHO ========================================================
ECHO       Starting Visionox M3 Ultimate System...
ECHO ========================================================

REM 1. 切换环境
D:
cd D:\wzy\Python\vivo-project
ECHO [INFO] Current directory: %cd%

REM 2. [后台] 启动门户网站 (Port 8000)
REM 使用 start 命令开启一个新窗口运行 Flask，互不干扰
ECHO [INFO] Starting Portal Service on Port 8000...
start "Visionox Portal" Vivo_project\Scripts\python.exe src\run_portal.py

REM 3. [后台] 启动 Streamlit 服务 (Port 8503)
ECHO [INFO] Starting Streamlit Core on Port 8503...
start "Streamlit Core" Vivo_project\Scripts\python.exe -m streamlit run vivo_project\app\home.py --server.port 8503 --server.headless true

REM 4. 等待几秒让服务就绪，然后自动打开浏览器访问门户入口
timeout /t 3 >nul
explorer "http://localhost:8000"

ECHO [SUCCESS] All systems are running. Close the popup windows to stop services.
PAUSE
