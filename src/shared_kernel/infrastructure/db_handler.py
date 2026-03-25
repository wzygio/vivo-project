# src/handler/db_handler.py
# (或者你最终确定的路径，例如 src/database_handler.py)
import pandas as pd
import os, logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine
# 注意：load_dotenv 不再需要在这里导入

class DatabaseManager:
    """
    数据库连接管理器 (采用单例模式)。
    负责创建并维护唯一的 SQLAlchemy Engine 实例。
    """
    _instance = None  # 用于存储唯一实例的类变量
    engine = None     # 将engine提升为类/实例属性

    def __new__(cls):
        # __new__ 是在 __init__ 之前被调用的
        if cls._instance is None:
            logging.info("首次创建DatabaseManager实例...")
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            # 将初始化逻辑移到这里，确保只执行一次
            # 注意: load_dotenv() 已移至 app_setup.py
            cls._instance.engine = cls._instance._create_engine()
        else:
            logging.debug("返回已存在的DatabaseManager实例。")
        return cls._instance
    
    def _create_engine(self):
        """
        根据环境变量创建并返回 SQLAlchemy Engine 对象。
        """
        try:
            logging.info("正在使用SQLAlchemy尝试连接到 PostgreSQL 数据库...")
            
            # 从环境变量中获取原始密码
            password = os.getenv('DB_PASSWORD')
            db_user = os.getenv('DB_USER')
            db_host = os.getenv('DB_HOST')
            db_port = os.getenv('DB_PORT')
            db_database = os.getenv('DB_DATABASE')

            # 检查必要的环境变量是否存在
            if not all([password, db_user, db_host, db_port, db_database]):
                missing = [var for var in ['DB_PASSWORD', 'DB_USER', 'DB_HOST', 'DB_PORT', 'DB_DATABASE'] if not os.getenv(var)]
                raise ValueError(f"错误：未能从环境变量中读取到必要的数据库配置: {', '.join(missing)}")
                
            # 对密码进行URL编码
            encoded_password = quote_plus(str(password))

            # 构建数据库连接URI
            db_uri = (
                f"postgresql+psycopg2://{db_user}:{encoded_password}"
                f"@{db_host}:{db_port}/{db_database}"
            )
            
            # 创建引擎
            engine = create_engine(
                db_uri,
                pool_pre_ping=True,  # 每次执行 SQL 前，先发个 PING 探测连接是否还活着。死了就自动重连。
                pool_recycle=3600    # 每小时强制回收重建一次连接池，防止数据库端掐断长时间休眠的连接。
            )
            
            # 测试连接 (可选，但在初始化时做一次检查是好习惯)
            with engine.connect() as connection:
                logging.info("数据库连接成功！ (SQLAlchemy Engine)")
            
            return engine
        
        except Exception as e:
            logging.error(f"数据库引擎创建失败：{e}")
            return None