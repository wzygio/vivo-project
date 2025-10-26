# src/data_loader.py
import pandas as pd
from sqlalchemy import text
import logging
from typing import TYPE_CHECKING, Dict, List
from pathlib import Path

# 从您的配置模块导入CONFIG
from vivo_project.config import CONFIG

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
    
def load_array_input_times(
    db_manager: 'DatabaseManager', 
    lot_ids: List[str]
) -> pd.DataFrame:
    """
    根据给定的Lot ID列表，查询相关Sheet在10000站点的最早投入时间。
    """
    logging.info(f"开始为 {len(lot_ids)} 个Lot提取阵列投入时间...")
    
    # 如果传入的lot_id列表为空，直接返回空DataFrame，避免执行无效查询
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
        logging.info(f"成功提取 {len(times_df)} 条Sheet的阵列投入时间记录。")

        return times_df

    except Exception as e:
        logging.error(f"提取阵列投入时间时发生错误: {e}")
        return pd.DataFrame()