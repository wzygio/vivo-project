# src/data_loader.py
import pandas as pd
from sqlalchemy import text
import logging
from typing import TYPE_CHECKING, Dict, List, Optional
from pathlib import Path

# 从您的配置模块导入CONFIG
from vivo_project.config import CONFIG, RESOURCE_DIR

if TYPE_CHECKING:
    from vivo_project.infrastructure.db_handler import DatabaseManager

# --- [新增] 用于按倍率调整不良Panel数量的辅助函数 ---
def load_panel_details(
    db_manager: 'DatabaseManager', 
    start_date: str, 
    end_date: str, 
    prod_code: str, 
    work_order_types: list
) -> pd.DataFrame:
    """
    (V3.1 - 纯净版)
    从数据库中提取【原始的】Panel级明细数据，不进行任何业务逻辑修改。
    """
    logging.info("开始从数据库提取原始数据 (V3.1)...")
    
    start_date_fmt = start_date.replace('-', '')
    end_date_fmt = end_date.replace('-', '')
    work_orders_str = "','".join(work_order_types)

    sql_query = f"""
    WITH PanelDefects AS (
    SELECT D.PANEL_ID, D.DEFECT_CODE
    FROM DWS_DFT_WAREHOUSING_D D
    WHERE D.DATE_TIMEKEY BETWEEN '{start_date_fmt}' AND '{end_date_fmt}'
    AND D.PROD_CODE = '{prod_code}'
    )
    SELECT 
        SUBSTR(R.DESCRIPTION, 1, 10) AS batch_no,
        SUBSTR(D.PANEL_ID, 1, 9) AS lot_id, 
        SUBSTR(D.PANEL_ID, 1, 11) AS sheet_id, 
        D.PANEL_ID AS panel_id, 
        P.PRODUCTCODE AS prod_code, 
        D.FIRST_SHIP_DATE AS warehousing_time, 
        PD.DEFECT_CODE AS defect_code, 
        G.DEFECT_DESC AS defect_desc, 
        G.DEFECT_GROUP AS defect_group
    FROM DWT_WAREHOUSING_PNL D
    LEFT JOIN DWR_MES_PRODUCTSPEC P ON D.PROD_ID = P.PRODUCTSPECNAME
    LEFT JOIN DWR_MES_PRODUCTREQUEST R ON D.SUB_PROD_ID = R.PRODUCTREQUESTNAME
    LEFT JOIN PanelDefects PD ON D.PANEL_ID = PD.PANEL_ID
    LEFT JOIN IMP_CT_DFT_GROUP G ON PD.DEFECT_CODE = G.DEFECT_CODE
    WHERE D.LAST_FLAG = 'Y'
    AND D.FIRST_SHIP_DATE BETWEEN '{start_date_fmt}' AND '{end_date_fmt}'
    AND P.PRODUCTCODE = '{prod_code}'
    AND R.SUBPRODUCTIONTYPE IN ('{work_orders_str}')
    ORDER BY batch_no, lot_id, sheet_id, panel_id;
    """
    
    try:
        if db_manager.engine is None:
            raise Exception("数据库引擎未初始化。")
        
        panel_df = pd.read_sql(text(sql_query), db_manager.engine)
        panel_df.columns = panel_df.columns.str.lower()
        logging.info(f"成功从数据库提取 {len(panel_df)} 行原始数据。")
        
        return panel_df
        
    except Exception as e:
        logging.error(f"提取Panel明细数据时发生错误: {e}")
        return pd.DataFrame()
    
def update_sheet_array_times(
    times_df: pd.DataFrame,
    custom_times: Optional[Dict[str, str]] = None  # 修改类型注解
) -> pd.DataFrame:
    """
    更新指定sheet的array_input_time
    
    Args:
        times_df: 原始时间DataFrame
        custom_times: sheet_id到新时间的映射字典，格式为 {'sheet_id': 'YYYYMMDD'}
        
    Returns:
        更新后的DataFrame
    """
    if not custom_times:
        return times_df
        
    logging.info(f"开始更新 {len(custom_times)} 个Sheet的自定义阵列投入时间...")
    result_df = times_df.copy()
    failed_updates = []

    for sheet_id, new_time in custom_times.items():
        mask = result_df['sheet_id'] == sheet_id
        if mask.any():
            result_df.loc[mask, 'array_input_time'] = new_time
            logging.info(f"已更新Sheet {sheet_id} 的阵列投入时间为 {new_time}")
        else:
            # 如果sheet_id不存在，创建新行
            new_row = pd.DataFrame({
                'sheet_id': [sheet_id],
                'array_input_time': [new_time]
            })
            result_df = pd.concat([result_df, new_row], ignore_index=True)
            logging.info(f"已为Sheet {sheet_id} 创建新的阵列投入时间记录: {new_time}")

    # 验证所有自定义时间是否都已应用
    applied_times = result_df.set_index('sheet_id')['array_input_time'].to_dict()
    missing_updates = [sid for sid in custom_times if sid not in applied_times]
    
    if missing_updates:
        error_msg = f"以下Sheet的自定义时间未能成功应用: {missing_updates}"
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    return result_df

def load_array_input_times(
    db_manager: 'DatabaseManager', 
    lot_ids: List[str],
    enable_custom_times: bool = False,
    custom_times: Optional[Dict[str, str]] = None  # 修改类型注解
) -> pd.DataFrame:
    """
    根据给定的Lot ID列表，查询相关Sheet在10000站点的最早投入时间。
    可选地接受自定义时间来覆盖特定sheet的时间。
    
    Args:
        db_manager: 数据库管理器实例
        lot_ids: Lot ID列表
        enable_custom_times: 是否启用自定义时间覆盖
        custom_times: 可选，sheet_id到新时间的映射字典，格式为 {'sheet_id': 'YYYYMMDD'}
    """
    logging.info(f"开始为 {len(lot_ids)} 个Lot提取阵列投入时间...")
    
    # 如果传入的lot_id列表为空，直接返回空DataFrame
    if not lot_ids:
        logging.warning("传入的Lot ID列表为空，跳过阵列投入时间查询。")
        return pd.DataFrame()

    lot_ids_str = "','".join(lot_ids)

    sql_query = f"""
    SELECT
        sheet_id,
        MIN(sheet_start_time) AS array_input_time
    FROM
        eda.spot_eda_array_hst_v
    WHERE
        step_id = '10000'
        AND SUBSTR(sheet_id, 1, 9) IN ('{lot_ids_str}')
    GROUP BY
        sheet_id;
    """

    try:
        if db_manager.engine is None:
            raise Exception("数据库引擎未初始化。")

        times_df = pd.read_sql(text(sql_query), db_manager.engine)
        times_df.columns = times_df.columns.str.lower()
        
        # 根据开关决定是否应用自定义时间
        if enable_custom_times and custom_times:
            times_df = update_sheet_array_times(times_df, custom_times)
        
        logging.info(f"成功提取 {len(times_df)} 条Sheet的阵列投入时间记录。")
        return times_df

    except Exception as e:
        logging.error(f"提取阵列投入时间时发生错误: {e}")
        return pd.DataFrame()

def load_raw_report(file_name: str, sheet_name: str) -> pd.DataFrame | None:
    """
    读取无表头的原始 Excel 报表，用于处理复杂表头结构。
    返回的 DataFrame 列名为整数索引 (0, 1, 2...)。
    """
    file_path = RESOURCE_DIR / file_name
    if not file_path.exists():
        logging.warning(f"外部基准报表不存在: {file_path}")
        return None
        
    try:
        # header=None 是关键，防止 Pandas 乱猜表头
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        return df
    except Exception as e:
        logging.error(f"读取外部报表失败 ({file_name}): {e}")
        return None