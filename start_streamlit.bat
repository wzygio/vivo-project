@echo off
ECHO Starting Streamlit Report App...

REM 1. 切换到项目所在的盘符
D:

REM 2. 进入项目根目录的绝对路径
cd D:\wzy\Python\vivo-project
ECHO [INFO] Current directory is: %cd%

REM 3. [核心修改] 进入 'src' 目录：这是为了让 Python 解释器能正确找到 'vivo_project' 这个包
ECHO [INFO] Changing working directory to 'src'...
pushd src

REM 4. 启动 Streamlit 服务：
REM    [核心修改] 因为我们已在 'src' 目录中，所以需要用 '..\' 来返回上一级才能找到 'Vivo_project' 虚拟环境。
REM    同时，运行的脚本路径也变为 'vivo_project\app\home.py'
ECHO [INFO] Starting Streamlit server on port 8503...
..\Vivo_project\Scripts\python.exe -m streamlit run vivo_project\app\home.py --server.port 8504

REM 5. [推荐] 当服务停止后 (例如你按了 Ctrl+C)，退出 'src' 目录
popd