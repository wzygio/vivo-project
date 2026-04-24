# src/vivo_project/app/setup.py
import logging
from pathlib import Path
from dotenv import load_dotenv
from app.utils.logger_setup import setup_logging

class AppSetup:
    @staticmethod
    def initialize_app(log_name="app.log"):
        """
        初始化应用的日志与环境变量系统。
        """
        setup_logging(log_name)
        
        # [修复] 应用启动时统一加载环境变量，确保后续所 有模块都能读取
        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
            logging.info("环境变量已从 .env 文件加载。")
        else:
            logging.warning(f"未找到 .env 文件: {env_path}")
        
        logging.info("Application setup complete (logging & env initialized).")

    
    