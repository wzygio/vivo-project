# src/vivo_project/app/setup.py
import logging
from app.utils.logger_setup import setup_logging

class AppSetup:
    @staticmethod
    def initialize_app(log_name="app.log"):
        """
        初始化应用的日志系统。
        """
        setup_logging(log_name)
        logging.info("Application setup complete (logging initialized).")

    
    