@echo off
TITLE Visionox AI System Launcher (Unified)
ECHO ========================================================
ECHO       Starting Visionox M3 Ultimate System...
ECHO       (Portal + Yield System + PPT Viewer)
ECHO ========================================================

REM 1. 切换环境
D:
cd D:\wzy\Python\vivo-project
call Vivo_project\Scripts\activate
ECHO [INFO] Current directory: %cd%

REM 2. [核心] 启动 Streamlit 门户服务 (Port 8503)
ECHO [INFO] Starting Integrated Portal on Port 8503...
pushd src
start "Visionox Unified System" ..\Vivo_project\Scripts\python.exe -m streamlit run vivo_project\app\Home.py --server.port 8503 --server.headless true

REM 3. 等待服务就绪
REM    因为现在要加载 HTML/JS 资源，建议多等两秒
ECHO [INFO] Waiting for services to initialize...
timeout /t 5 >nul

REM 4. 自动打开浏览器
REM    现在直接访问 8503 端口，看到的就会是那个炫酷的 HTML 门户
ECHO [INFO] Opening Browser...
explorer "http://localhost:8503"

ECHO ========================================================
ECHO [SUCCESS] System is running at http://localhost:8503
ECHO           Close the popup window to stop the service.
ECHO ========================================================