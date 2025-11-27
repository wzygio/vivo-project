import pandas as pd
import logging
import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime

# 引入配置 (假设配置字典中有相应的 Key，如果没有则使用默认值)
from vivo_project.config import CONFIG

# 引入原本底层的工具函数和类
from vivo_project.infrastructure.db_handler import DatabaseManager
from vivo_project.infrastructure.data_loader import load_panel_details, load_array_input_times

class PanelRepository:
    """
    [仓储层] PanelRepository
    职责：它是 Service 层与数据库之间的唯一接口。
    [新增能力]: 实现了“智能缓存”模式——优先读取本地快照，无快照或强制刷新时才连接数据库。
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        # 依赖注入：允许传入一个现有的 db_manager，方便测试或复用连接
        if db_manager:
            self.db = db_manager
        else:
            self.db = DatabaseManager()
            
        # --- 配置加载 ---
        # 尝试从 CONFIG 获取配置，如果不存在则使用默认值
        processing_conf = CONFIG.get('processing', {})
        self.use_snapshot = processing_conf.get('use_local_snapshot', True) # 默认开启
        # 默认路径：data/panel_details_snapshot.parquet
        self.snapshot_path = Path(processing_conf.get('snapshot_path', 'data/panel_details_snapshot.parquet'))

    def get_panel_details(self, 
                          start_date: str, 
                          end_date: str, 
                          product_code: str, 
                          work_order_types: List[str],
                          force_refresh: bool = False) -> pd.DataFrame:
        """
        获取 Panel 级详细明细数据。
        
        :param force_refresh: 如果为 True，则忽略本地快照，强制查询数据库并更新快照。
        """
        
        # --- A. 尝试读取本地快照 ---
        # 条件: 开启了快照模式 + (没有强制刷新) + 文件存在
        if self.use_snapshot and not force_refresh:
            if self.snapshot_path.exists():
                logging.info(f"🚀 [Repo] 发现本地快照，正在加载: {self.snapshot_path}")
                try:
                    df = pd.read_parquet(self.snapshot_path)
                    # [可选] 这里可以加简单的校验，例如检查快照的时间范围是否覆盖了请求的 start_date/end_date
                    # 但为了保持简单，暂且假设快照就是我们需要的数据集
                    if not df.empty:
                        return df
                except Exception as e:
                    logging.warning(f"⚠️ 本地快照读取失败: {e}，将转为数据库查询。")
            else:
                logging.info(f"ℹ️ 本地快照不存在 ({self.snapshot_path})，准备连接数据库...")

        # --- B. 执行数据库查询 ---
        if self.db.engine is None:
            logging.error("数据库连接未初始化，无法查询 Panel 数据。")
            return pd.DataFrame()

        logging.info(f"[Repo] 正在从数据库查询 Panel 数据: {start_date} 至 {end_date}")
        
        # 调用底层的 data_loader
        df_result = load_panel_details(
            db_manager=self.db,
            start_date=start_date,
            end_date=end_date,
            prod_code=product_code,
            work_order_types=work_order_types
        )

        # --- C. 自动保存快照 ---
        # 条件: 查询到了数据 + 开启了快照模式
        if not df_result.empty and self.use_snapshot:
            try:
                logging.info(f"💾 [Repo] 正在更新本地快照: {self.snapshot_path}")
                # 确保父目录存在
                self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                # 保存
                df_result.to_parquet(self.snapshot_path, index=False)
                logging.info("✅ 快照保存成功")
            except Exception as e:
                logging.error(f"❌ 快照保存失败: {e}")

        return df_result

    def get_array_input_times(self, lot_ids: List[str], custom_times: Optional[dict] = None) -> pd.DataFrame:
        """
        获取 Lot 的阵列投入时间。
        注：这个数据量通常较小且变动频繁，通常不建议做全量快照，或者使用独立的缓存策略。
        目前保持直连数据库。
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