# src/vivo_project/infrastructure/repositories/panel_repository.py
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta

# [Refactor] 移除了 vivo_project.config 引用

from vivo_project.infrastructure.db_handler import DatabaseManager
from vivo_project.infrastructure.data_loader import load_panel_details, load_array_input_times

class PanelRepository:
    """
    [仓储层] PanelRepository
    职责：它是 Service 层与数据库之间的唯一接口。
    [能力]: 智能缓存模式 (TTL=12h) —— 优先读取未过期的本地快照。
    
    Refactoring Update:
    不再依赖全局配置，所有路径和开关通过 __init__ 注入。
    """
    
    SNAPSHOT_TTL_HOURS = 12

    def __init__(
        self, 
        snapshot_path: Path,
        use_snapshot: bool = True,
        db_manager: Optional[DatabaseManager] = None
    ):
        """
        初始化 Repository。
        
        Args:
            snapshot_path (Path): 快照文件的完整路径 (必需)。
            use_snapshot (bool): 是否启用快照功能 (默认为 True)。
            db_manager (DatabaseManager, optional): 数据库管理器实例。
        """
        if db_manager:
            self.db = db_manager
        else:
            self.db = DatabaseManager()
            
        # 显式赋值，不从 global CONFIG 读取
        self.snapshot_path = snapshot_path
        self.use_snapshot = use_snapshot

    def get_panel_details(
        self, 
        start_date: str, 
        end_date: str, 
        product_code: str,
        work_order_types: List[str],
        target_defect_groups: List[str],
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取 Panel 级详细明细数据。
        
        Args:
            start_date, end_date: 查询时间范围。
            product_code: 产品代码 (必需，无默认值)。
            work_order_types: 工单类型列表 (必需，无默认值)。
            target_defect_groups: 目标不良组列表 (必需，无默认值)。
            force_refresh: 是否强制刷新。
        """
        
        # 标记快照是否有效 (初始为 False)
        is_snapshot_valid = False

        # --- A. 快照健康度检查 (TTL 逻辑) ---
        if self.use_snapshot and not force_refresh:
            if self.snapshot_path.exists():
                try:
                    mtime = self.snapshot_path.stat().st_mtime
                    file_time = datetime.fromtimestamp(mtime)
                    age_delta = datetime.now() - file_time
                    age_hours = age_delta.total_seconds() / 3600
                    
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
        if is_snapshot_valid:
            try:
                logging.info(f"🚀 [Repo] 正在加载本地快照: {self.snapshot_path}")
                df = pd.read_parquet(self.snapshot_path)
                if not df.empty:
                    return df
            except Exception as e:
                logging.warning(f"⚠️ 本地快照读取失败: {e}，将转为数据库查询。")

        # --- C. 执行数据库查询 (升级为分片模式) ---
        if self.db.engine is None:
            logging.error("数据库连接未初始化，无法查询 Panel 数据。")
            return pd.DataFrame()

        logging.info(f"📡 [Repo] 准备查询 Panel 数据: {start_date} 至 {end_date}")
        
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            chunk_days = 30
            current_start = start_dt
            all_chunks = []
            
            logging.info(f"🔄 [Repo] 启动分片查询策略 (步长: {chunk_days}天)...")

            while current_start <= end_dt:
                current_end = current_start + timedelta(days=chunk_days)
                if current_end > end_dt:
                    current_end = end_dt
                
                s_str = current_start.strftime("%Y-%m-%d")
                e_str = current_end.strftime("%Y-%m-%d")
                
                logging.info(f"   >> Fetching chunk: {s_str} ~ {e_str} ...")
                
                # 调用底层 Loader (原子查询)
                # [Refactor] 参数直接透传，不再依赖 CONFIG
                df_chunk = load_panel_details(
                    db_manager=self.db,
                    start_date=s_str,
                    end_date=e_str,
                    prod_code=product_code,
                    work_order_types=work_order_types,
                    target_defect_groups=target_defect_groups
                )
                
                if not df_chunk.empty:
                    all_chunks.append(df_chunk)
                    logging.info(f"      Got {len(df_chunk)} rows.")
                    
                current_start = current_end + timedelta(days=1)

            if all_chunks:
                df_result = pd.concat(all_chunks, ignore_index=True)
                df_result.drop_duplicates(subset=['panel_id'], inplace=True)
                logging.info(f"🎉 [Repo] 分片查询完成，总数据量: {len(df_result)} 行。")
            else:
                logging.warning("⚠️ [Repo] 所有分片查询均未返回数据。")
                df_result = pd.DataFrame()

        except Exception as query_err:
            logging.error(f"❌ [Repo] 数据库分片查询过程中发生严重错误: {query_err}", exc_info=True)
            return pd.DataFrame()

        # --- D. 自动保存/更新快照 ---
        if not df_result.empty and self.use_snapshot:
            try:
                logging.info(f"💾 [Repo] 获取到新数据，正在更新本地快照: {self.snapshot_path}")
                # 确保目录存在
                self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                df_result.to_parquet(self.snapshot_path, index=False)
                logging.info("✅ 快照更新成功")
            except Exception as e:
                logging.error(f"❌ 快照保存失败: {e}")

        return df_result

    def get_array_input_times(self, lot_ids: List[str], custom_times: Optional[dict] = None) -> pd.DataFrame:
        """
        获取 Lot 的阵列投入时间。
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