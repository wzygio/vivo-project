# src/handler/database_handler.py

import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Optional

# 导入我们项目统一的配置
# (请根据您的项目结构，确认此导入路径是正确的)
from vivo_project.config import CONFIG 

class DatabaseHandler:
    """
    负责管理数据库连接 (SQLAlchemy Engine)。
    这是一个单例模式实现，确保整个应用共享一个连接池。
    """
    
    # 类变量，用于存储唯一的引擎实例
    _engine: Optional[Engine] = None 

    def __init__(self):
        """
        初始化处理器。如果引擎不存在，则创建它。
        """
        if DatabaseHandler._engine is None:
            logging.info("数据库引擎尚未初始化，正在创建新实例...")
            DatabaseHandler._engine = self._create_engine()
        
        # 将类变量赋给实例变量，以便通过 db_manager.engine 访问
        self.engine = DatabaseHandler._engine

    def _create_engine(self) -> Optional[Engine]:
        """
        私有方法：读取配置并创建 SQLAlchemy 引擎。
        """
        try:
            db_config = CONFIG.get('database')
            if not db_config:
                logging.error("数据库配置 'database' 节未在config中找到。")
                return None

            # 从配置中获取连接参数
            username = db_config.get('user')
            host = db_config.get('host')
            port = db_config.get('port')
            dbname = db_config.get('dbname') # 在Oracle中，这通常是 TNS Service Name
            
            # 从环境变量中安全地获取密码
            password_env_var = db_config.get('password_env_var', 'DB_PASSWORD')
            password = os.environ.get(password_env_var)
            
            if not all([username, host, port, dbname]):
                logging.error("数据库配置不完整 (user, host, port, dbname 必须提供)。")
                return None
            
            if not password:
                logging.error(f"数据库密码未在环境变量 {password_env_var} 中设置。")
                return None
            
            # --- [请根据您的数据库类型确认] ---
            # 假设为 Oracle (因为DWT/DWS和SUBSTR语法)
            # 如果您的Oracle TNS配置正确，可能只需要 service_name
            # db_url = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={dbname}"
            
            # 如果是 PostgreSQL:
            db_url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
            
            # 如果是 SQL Server:
            # driver = db_config.get('driver', 'ODBC Driver 17 for SQL Server').replace(' ', '+')
            # db_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{dbname}?driver={driver}"
            
            logging.info(f"正在创建数据库引擎，连接至: oracle://{username}:****@{host}:{port}/{dbname}")

            engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                echo=False # 设置为True可以在日志中打印所有执行的SQL
            )

            # 测试连接
            with engine.connect() as conn:
                logging.info("数据库连接引擎创建成功，测试连接已建立。")
            
            return engine

        except ImportError:
            logging.error("创建数据库引擎失败：缺少必要的驱动 (例如 cx_Oracle 或 pyodbc)。")
            return None
        except Exception as e:
            logging.error(f"创建数据库引擎时发生未知错误: {e}", exc_info=True)
            return None

    def close(self):
        """
        (可选) 主动关闭和清理连接池。
        """
        if self.engine:
            self.engine.dispose()
            DatabaseHandler._engine = None
            logging.info("数据库连接池已主动关闭。")