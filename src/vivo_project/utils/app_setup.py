# src/vivo_project/app/setup.py
import logging
from vivo_project.utils.utils import setup_logging
from vivo_project.config import CONFIG

class AppSetup:
    @staticmethod
    def initialize_app():
        """
        初始化应用的日志系统。
        """
        setup_logging("app.log")
        logging.info("Application setup complete (logging initialized).")

    
    