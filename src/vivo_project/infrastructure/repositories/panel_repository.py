import pandas as pd
import logging
import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta

# 引入配置
from vivo_project.config import CONFIG

# 引入原本底层的工具函数和类
from vivo_project.infrastructure.db_handler import DatabaseManager
from vivo_project.infrastructure.data_loader import load_panel_details, load_array_input_times

class PanelRepository:
    """
    [仓储层] PanelRepository
    职责：它是 Service 层与数据库之间的唯一接口。
    [能力]: 智能缓存模式 (TTL=12h) —— 优先读取未过期的本地快照，过期或无快照时连接数据库。
    """
    
    # 定义快照有效期 (小时)
    SNAPSHOT_TTL_HOURS = 12

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        # 依赖注入
        if db_manager:
            self.db = db_manager
        else:
            self.db = DatabaseManager()
            
        # --- 配置加载 ---
        processing_conf = CONFIG.get('processing', {})
        self.use_snapshot = processing_conf.get('use_local_snapshot', True) # 默认开启
        self.snapshot_path = Path(processing_conf.get('snapshot_path', 'data/panel_details_snapshot.parquet'))

    def get_panel_details(self, 
                          start_date: str, 
                          end_date: str, 
                          product_code: str, 
                          work_order_types: List[str],
                          force_refresh: bool = False) -> pd.DataFrame:
        """
        获取 Panel 级详细明细数据。
        :param force_refresh: True = 强制忽略快照，查库并更新。
        """
        
        # 标记快照是否有效 (初始为 False)
        is_snapshot_valid = False

        # --- A. 快照健康度检查 (TTL 逻辑) ---
        if self.use_snapshot and not force_refresh:
            if self.snapshot_path.exists():
                try:
                    # 1. 获取文件最后修改时间
                    mtime = self.snapshot_path.stat().st_mtime
                    file_time = datetime.fromtimestamp(mtime)
                    
                    # 2. 计算年龄
                    age_delta = datetime.now() - file_time
                    age_hours = age_delta.total_seconds() / 3600
                    
                    # 3. 判断是否过期
                    if age_hours < self.SNAPSHOT_TTL_HOURS:
                        logging.info(f"⏱️ [Repo] 快照年龄: {age_hours:.2f} 小时 (有效期 {self.SNAPSHOT_TTL_HOURS}h 内)，准备加载。")
                        is_snapshot_valid = True
                    else:
                        logging.warning(f"⏰ [Repo] 快照已过期 (年龄 {age_hours:.2f} 小时 > {self.SNAPSHOT_TTL_HOURS}h)，将执行数据库刷新。")
                        is_snapshot_valid = False
                        
                except Exception as e:
                    logging.error(f"⚠️ 检查快照时间时发生错误: {e}，将视为无效。")
                    is_snapshot_valid = False
            else:
                logging.info(f"ℹ️ [Repo] 本地快照不存在 ({self.snapshot_path})，准备连接数据库...")

        # --- B. 尝试读取本地快照 ---
        # 只有在 (快照被判定为有效) 时才真正去读文件
        if is_snapshot_valid:
            try:
                logging.info(f"🚀 [Repo] 正在加载本地快照: {self.snapshot_path}")
                df = pd.read_parquet(self.snapshot_path)
                if not df.empty:
                    return df
            except Exception as e:
                logging.warning(f"⚠️ 本地快照读取失败: {e}，将转为数据库查询。")
                # 读取失败 fallback 到查库，无需额外操作

        # --- C. 执行数据库查询 ---
        # 代码走到这里，说明要么 force_refresh=True，要么快照过期，要么快照不存在
        if self.db.engine is None:
            logging.error("数据库连接未初始化，无法查询 Panel 数据。")
            return pd.DataFrame()

        logging.info(f"📡 [Repo] 正在从数据库查询 Panel 数据: {start_date} 至 {end_date}")
        
        df_result = load_panel_details(
            db_manager=self.db,
            start_date=start_date,
            end_date=end_date,
            prod_code=product_code,
            work_order_types=work_order_types
        )

        # --- D. 自动保存/更新快照 ---
        if not df_result.empty and self.use_snapshot:
            try:
                logging.info(f"💾 [Repo] 获取到新数据，正在更新本地快照: {self.snapshot_path}")
                self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                df_result.to_parquet(self.snapshot_path, index=False)
                logging.info("✅ 快照更新成功")
            except Exception as e:
                logging.error(f"❌ 快照保存失败: {e}")

        return df_result

    def get_array_input_times(self, lot_ids: List[str], custom_times: Optional[dict] = None) -> pd.DataFrame:
        """
        获取 Lot 的阵列投入时间。
        保持直连数据库逻辑不变。
        """
        if not lot_ids:
            return pd.DataFrame()
            
        if self.db.engine is None:
            logging.error("数据库连接未初始化，无法查询 Array Input Time。")
            return pd.DataFrame()

        return load_array_input_times(
            db_manager=self.db,
            lot_ids=lot_ids,
            enable_custom_times=True if custom_times else False,
            custom_times=custom_times
        )