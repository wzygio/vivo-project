REM ========================================================
REM 0. 自动查杀旧进程。防止进程堆积，确保每次启动都是最新的单一实例
REM ========================================================
ECHO [INFO] Checking for existing process on port 8503...
for /f "tokens=5" %%a in ('netstat -aon ^| find ":8503" ^| find "LISTENING"') do (
    ECHO [INFO] Killing old process PID: %%a
    taskkill /f /pid %%a >nul 2>&1
)

REM ========================================================
REM 1. 设置环境变量
REM ========================================================
D:
cd "D:\wzy\Python\vivo-project"
set PYTHONPATH=%cd%\src;%PYTHONPATH%

REM ========================================================
REM 2. 激活虚拟环境
REM ========================================================
IF EXIST "Vivo_project\Scripts\activate.bat" (
    call "Vivo_project\Scripts\activate.bat"
)

REM ========================================================
REM 3. 启动 Streamlit (使用 pythonw)
REM ========================================================
ECHO [INFO] Step 2: Starting Integrated Portal on Port 8503...
uv run streamlit run src/vivo_project/app/Home.py --server.headless true --server.port 8503

ECHO [INFO] Step 3: Waiting for services to initialize...
REM 加载 HTML/JS 资源可能需要一点时间，保持 5 秒等待
timeout /t 5 >nul

REM ========================================================
REM 4. 退出
REM ========================================================
exit