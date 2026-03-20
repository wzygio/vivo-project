# src/vivo_project/utils/utils.py
import pandas as pd
import logging
import sys
from pathlib import Path
import streamlit as st  # [新增] 引入 streamlit

from src.shared_kernel.config import ConfigLoader

# [双保险 1] 使用 @st.cache_resource 确保 Handler 永驻内存，且全生命周期只初始化一次
@st.cache_resource
def setup_logging(log_filename: str = "app.log"):
    """
    初始化日志系统 (单例模式)。
    """
    # 1. 路径计算
    project_root = ConfigLoader.get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_filepath = log_dir / log_filename

    log_format = '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 2. 清理逻辑：虽然有 cache_resource，但为了健壮性，防止异常情况下的 Handler 堆积
    # 注意：在 cache_resource 保护下，这段代码通常只会在服务器启动时运行一次
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)

    # 3. [双保险 2] 强制使用 mode='a' (追加模式)
    try:
        file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
        root_logger.addHandler(file_handler)
    except Exception as e:
        # 如果无法写入文件，至少保证控制台能看到
        print(f"❌ 严重错误：无法初始化日志文件 Handler: {e}")

    # 4. 控制台 Handler
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
        root_logger.addHandler(console_handler)

    # 这一行日志非常关键，证明初始化成功
    logging.info(f"✅ 日志系统已启动 (单例模式 | 追加写入): {log_filepath}")
    
    # 返回 logger 实例，虽然这里不需要接收，但符合 cache_resource 规范
    return root_logger