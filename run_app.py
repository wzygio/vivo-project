# run_app.py (放在项目根目录)
import sys
import os

# 获取项目根目录（即当前文件所在目录的上级目录）
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 现在可以正确导入
from src.vivo_project.app import Home

# 启动Streamlit应用
if __name__ == "__main__":
    Home.main()