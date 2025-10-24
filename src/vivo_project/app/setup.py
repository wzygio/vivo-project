# src/vivo_project/app/setup.py
import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np
import logging
from vivo_project.utils.utils import Utils

class AppSetup:
    """
    一个静态工具类，用于初始化Streamlit应用环境
    并提供可复用的UI组件。
    """
    
    @staticmethod
    def initialize_app():
        """
        初始化应用的日志系统。
        应在每个Streamlit页面（Home.py, pages/*.py）的顶部被调用。
        """
        try:
            # 1. 获取当前文件(setup.py)的绝对路径
            #    e.g., D:\wzy\Python\vivo-project\src\vivo_project\app\setup.py
            current_file_path = Path(__file__).resolve()
            # 2. 找到 'src' 目录 (setup.py -> app -> vivo_project -> src)
            src_root = current_file_path.parent.parent.parent
            # 3. 将 'src' 目录添加到 Python 搜索路径的【最前面】
            if str(src_root) not in sys.path:
                sys.path.insert(0, str(src_root))
        except NameError:
            # Fallback for environments where __file__ is not defined
            src_root = Path.cwd() / "src"
            if str(src_root) not in sys.path:
                sys.path.insert(0, str(src_root))
        
        # 初始化日志
        Utils.setup_logging("app.log")
        logging.info("Application environment initialized.")

    
    