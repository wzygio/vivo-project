@echo off
TITLE Visionox AI System Launcher (Unified)
ECHO ========================================================
ECHO       Starting Visionox M3 Ultimate System...
ECHO       (Portal + Yield System + PPT Viewer)
ECHO ========================================================

REM 1. 切换到项目根目录
D:
cd D:\wzy\Python\vivo-project
ECHO [INFO] Current directory: %cd%

REM 2. 激活虚拟环境
call Vivo_project\Scripts\activate

REM 3. [关键修改] 设置 PYTHONPATH
REM 将 src 目录显式加入 Python 搜索路径
REM 这样 Python 就能直接找到 vivo_project 模块，无需进入子目录
set PYTHONPATH=%cd%\src;%PYTHONPATH%
ECHO [INFO] PYTHONPATH set to include src directory.

REM 4. [核心] 启动 Streamlit 门户服务 (Port 8503)
REM 直接从根目录启动，指定完整路径 src\vivo_project\app\Home.py
ECHO [INFO] Starting Integrated Portal on Port 8503...
start "Visionox Unified System" python -m streamlit run src\vivo_project\app\Home.py --server.port 8503 --server.headless true

REM 5. 等待服务就绪
REM    加载 HTML/JS 资源可能需要一点时间，保持 5 秒等待
ECHO [INFO] Waiting for services to initialize...
timeout /t 5 >nul

REM 6. 自动打开浏览器
ECHO [INFO] Opening Browser...
explorer "http://localhost:8503"

ECHO ========================================================
ECHO [SUCCESS] System is running at http://localhost:8503
ECHO           Close the popup window to stop the service.
ECHO ========================================================
PAUSE