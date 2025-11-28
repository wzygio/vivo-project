@echo off
TITLE Visionox AI System Launcher (Unified)
ECHO ========================================================
ECHO       Starting Visionox M3 Ultimate System...
ECHO       (Portal + Yield System + PPT Viewer)
ECHO ========================================================

REM 1. 切换到项目根目录
D:
REM 建议加上引号，防止路径中有空格
cd "D:\wzy\Python\vivo-project"
ECHO [INFO] Current directory: %cd%

REM 2. 激活虚拟环境
call Vivo_project\Scripts\activate

REM 3. 设置 PYTHONPATH
REM 将 src 目录显式加入 Python 搜索路径，确保 import vivo_project 正常工作
set PYTHONPATH=%cd%\src;%PYTHONPATH%
ECHO [INFO] PYTHONPATH set to include src directory.

REM ========================================================
REM [第一步] 强制更新数据快照 (阻塞执行)
REM ========================================================
ECHO [INFO] Step 1: Updating Data Snapshot (This may take a while)...
REM 调用 snapshot 工具，强制从数据库拉取最新数据并保存
REM 这一步会阻塞（等待），直到数据准备好，确保 UI 启动时显示的是最新数据
python src\vivo_project\utils\create_snapshot.py

REM ========================================================
REM [第二步] 启动 Streamlit 服务 (新窗口异步运行)
REM ========================================================
ECHO [INFO] Step 2: Starting Integrated Portal on Port 8503...
REM 使用 start 开启新窗口，这样 Streamlit 在后台运行，不会卡住当前的脚本窗口
start "Visionox Unified System" python -m streamlit run src\vivo_project\app\Home.py --server.port 8503 --server.headless true

REM ========================================================
REM [第三步] 等待服务预热并打开浏览器
REM ========================================================
ECHO [INFO] Step 3: Waiting for services to initialize...
REM 加载 HTML/JS 资源可能需要一点时间，保持 5 秒等待
timeout /t 5 >nul

ECHO [INFO] Opening Browser...
explorer "http://localhost:8503"

ECHO ========================================================
ECHO [SUCCESS] System is running at http://localhost:8503
ECHO           Close the popup window to stop the service.
ECHO ========================================================
REM 保持窗口开启，方便查看快照更新的日志 (Success/Error)
PAUSE