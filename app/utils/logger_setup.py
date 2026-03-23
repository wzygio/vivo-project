import logging
from logging.handlers import TimedRotatingFileHandler # 引入企业级的时间轮转 Handler
import sys
from pathlib import Path
import streamlit as st

from src.shared_kernel.config import ConfigLoader

@st.cache_resource
def setup_logging(base_filename: str = "app"):
    """
    [企业级日志架构] 初始化日志系统 (单例模式)。
    支持按天自动切分、过期日志自动清理、按级别物理隔离。
    """
    project_root = ConfigLoader.get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, datefmt=log_date_format) # 提前实例化 Formatter 以复用

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # 根 Logger 拦截 INFO 及以上的所有级别

    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)

    try:
        # =========================================================
        #  通道 1：全量流水日志 (按天轮转)
        # =========================================================
        info_log_path = log_dir / f"{base_filename}_info.log"
        info_handler = TimedRotatingFileHandler(
            filename=info_log_path,
            when="midnight",    # 每天午夜零点自动触发切分
            interval=1,         # 间隔 1 天
            backupCount=30,     # 自动清理：最多保留最近 30 天的日志文件
            encoding='utf-8'
        )
        info_handler.setLevel(logging.INFO) # 拦截 INFO 级别
        info_handler.setFormatter(formatter)
        root_logger.addHandler(info_handler)

        # =========================================================
        #  通道 2：高优报警日志 (按天轮转，专供快速排查)
        # =========================================================
        error_log_path = log_dir / f"{base_filename}_error.log"
        error_handler = TimedRotatingFileHandler(
            filename=error_log_path,
            when="midnight",    # 每天午夜零点自动触发切分
            interval=1,         # 间隔 1 天
            backupCount=90,     # 错误日志往往需要长期追溯，保留 90 天
            encoding='utf-8'
        )
        error_handler.setLevel(logging.WARNING) # 核心隔离：只放行 WARNING, ERROR, CRITICAL
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)

    except Exception as e:
        print(f"❌ 严重错误：无法初始化企业级日志 Handler: {e}")

    # 控制台 Handler (供开发者本地实时观测)
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    logging.info("✅ SOTA 企业级日志系统已启动 (双通道隔离 | 午夜自动轮转)")
    
    return root_logger