import pandas as pd
import logging
from typing import List, Optional
from datetime import datetime

# 引入原本底层的工具函数和类
from vivo_project.infrastructure.db_handler import DatabaseManager
from vivo_project.infrastructure.data_loader import load_panel_details, load_array_input_times

class PanelRepository:
    """
    [仓储层] PanelRepository
    职责：它是 Service 层与数据库之间的唯一接口。
    Service 层只管找它要数据，不需要知道底层是用 SQL 查的还是读 CSV 的。
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        # 依赖注入：允许传入一个现有的 db_manager，方便测试或复用连接
        if db_manager:
            self.db = db_manager
        else:
            self.db = DatabaseManager()

    def get_panel_details(self, 
                          start_date: str, 
                          end_date: str, 
                          product_code: str, 
                          work_order_types: List[str]) -> pd.DataFrame:
        """
        获取 Panel 级详细明细数据。
        """
        if self.db.engine is None:
            logging.error("数据库连接未初始化，无法查询 Panel 数据。")
            return pd.DataFrame()

        logging.info(f"[Repo] 正在从数据库查询 Panel 数据: {start_date} 至 {end_date}")
        
        # 调用底层的 data_loader (或者你可以把 SQL 逻辑直接搬到这里)
        return load_panel_details(
            db_manager=self.db,
            start_date=start_date,
            end_date=end_date,
            prod_code=product_code,
            work_order_types=work_order_types
        )

    def get_array_input_times(self, lot_ids: List[str], custom_times: Optional[dict] = None) -> pd.DataFrame:
        """
        获取 Lot 的阵列投入时间。
        """
        if not lot_ids:
            return pd.DataFrame()
            
        if self.db.engine is None:
            logging.error("数据库连接未初始化，无法查询 Array Input Time。")
            return pd.DataFrame()

        # 调用底层的 data_loader
        return load_array_input_times(
            db_manager=self.db,
            lot_ids=lot_ids,
            enable_custom_times=True if custom_times else False,
            custom_times=custom_times
        )