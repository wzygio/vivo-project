# src/vivo_project/infrastructure/data_loader.py
import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from yield_domain.infrastructure.db_handler import DatabaseManager

def load_panel_details(
    db_manager: 'DatabaseManager', 
    start_date: str, 
    end_date: str, 
    prod_code: str, 
    work_order_types: List[str],
    target_defect_groups: List[str]
) -> pd.DataFrame:
    """
    (V3.1 - 纯净版)
    从数据库中提取【原始的】Panel级明细数据，不进行任何业务逻辑修改。
    参数完全由调用方注入，不依赖全局配置。
    """
    logging.info("开始从数据库提取原始数据 (V3.1)...")
    
    start_date_fmt = start_date.replace('-', '')
    end_date_fmt = end_date.replace('-', '')
    # 确保 list 不为空再 join，防止 SQL 语法错误（虽然业务上通常不为空）
    work_orders_str = "','".join(work_order_types) if work_order_types else ""

    # dws_dft_warehousing_d： 获取defect_code
    # spot_glass_batch_info： 获取批次号
    # dwr_mes_productspec： 获取产品号
    # imp_ct_dft_group： 获取defect_group及defect_desc
    sql_query = f"""
    select 
        sgbi.lot as batch_no,
        substr(dwp.panel_id, 1, 9) as lot_id, 
        substr(dwp.panel_id, 1, 11) as sheet_id, 
        dwp.panel_id as panel_id, 
        dwp.first_ship_date as warehousing_time, 
        dmp.productcode as prod_code, 
        ddwd.defect_code as defect_code,
        icdg.defect_desc as defect_desc, 
        icdg.defect_group as defect_group
    from dwt_warehousing_pnl dwp
    left join dws_dft_warehousing_d ddwd on dwp.panel_id = ddwd.panel_id 
    left join spot_glass_batch_info sgbi on substr(dwp.panel_id, 1, 11) = substr(sgbi.glass_id, 1, 11)
    left join dwr_mes_productspec dmp on dwp.prod_id = dmp.productspecname
    left join imp_ct_dft_group icdg on icdg.defect_code = ddwd.defect_code
    where dwp.last_flag = 'Y'
        and dwp.first_ship_date between '{start_date_fmt}' and '{end_date_fmt}'
        and dmp.productcode = '{prod_code}'
        and dwp.sub_prod_type in ('{work_orders_str}')
    """
    # 注意：上面的 SQL 中去掉了 order by batch_no, lot_id, sheet_id, panel_id; 数据库只负责“拿数据”，不要让它负责“排数据”，尤其是跨表大查询时

    try:
        if db_manager.engine is None:
            raise Exception("数据库引擎未初始化。")
        
        logging.info("正在执行SQL查询 (已移除DB端排序)...")
        panel_df = pd.read_sql(text(sql_query), db_manager.engine)
        panel_df.columns = panel_df.columns.str.lower()
        
        # 1. 在 Pandas 中进行排序：此时数据已经到了本地内存，Pandas 排序非常快且不会导致数据库连接超时
        if not panel_df.empty:
            panel_df.sort_values(
                by=['batch_no', 'lot_id', 'sheet_id', 'panel_id'], 
                ascending=[True, True, True, True], 
                inplace=True
            )
        
        # 2. 统一清洗层 (Sanitization Layer)
        if target_defect_groups:
            # 1. 找到所有“非目标组”且“非良品”的行
            mask_non_target = (
                ~panel_df['defect_group'].isin(target_defect_groups) & 
                panel_df['defect_group'].notna()
            )
            
            cleaned_count = mask_non_target.sum()
            
            if cleaned_count > 0:
                # 2. 强制抹除这些行的不良信息 (将其变为良品)
                cols_to_clean = ['defect_code', 'defect_desc', 'defect_group']
                panel_df.loc[mask_non_target, cols_to_clean] = np.nan
                
                logging.info(
                    f"🛡️ [数据清洗] 已抹除 {cleaned_count} 条非目标 Defect 记录 "
                    f"(Target: {target_defect_groups})。"
                )
            else:
                logging.info("🛡️ [数据清洗] 数据纯净，无需抹除。")
                
        logging.info(f"成功提取并清洗 {len(panel_df)} 行数据。")
        return panel_df
        
    except Exception as e:
        logging.error(f"提取Panel明细数据时发生错误: {e}")
        return pd.DataFrame()


def load_array_input_times(
    db_manager: 'DatabaseManager', 
    lot_ids: List[str],
    enable_custom_times: bool = False,
    custom_times: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    根据给定的Lot ID列表，查询相关Sheet在10000站点的最早投入时间。
    """
    logging.info(f"开始为 {len(lot_ids)} 个Lot提取阵列投入时间...")
    
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
        
        if enable_custom_times and custom_times:
            times_df = _update_sheet_array_times(times_df, custom_times)
        
        logging.info(f"成功提取 {len(times_df)} 条Sheet的阵列投入时间记录。")
        return times_df

    except Exception as e:
        logging.error(f"提取阵列投入时间时发生错误: {e}")
        return pd.DataFrame()


def _update_sheet_array_times(
    times_df: pd.DataFrame,
    custom_times: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    内部辅助函数：更新指定sheet的array_input_time
    """
    if not custom_times:
        return times_df
        
    logging.info(f"开始更新 {len(custom_times)} 个Sheet的自定义阵列投入时间...")
    result_df = times_df.copy()

    for sheet_id, new_time in custom_times.items():
        mask = result_df['sheet_id'] == sheet_id
        if mask.any():
            result_df.loc[mask, 'array_input_time'] = new_time
            logging.info(f"已更新Sheet {sheet_id} 的阵列投入时间为 {new_time}")
        else:
            new_row = pd.DataFrame({
                'sheet_id': [sheet_id],
                'array_input_time': [new_time]
            })
            result_df = pd.concat([result_df, new_row], ignore_index=True)
            logging.info(f"已为Sheet {sheet_id} 创建新的阵列投入时间记录: {new_time}")

    applied_times = result_df.set_index('sheet_id')['array_input_time'].to_dict()
    missing_updates = [sid for sid in custom_times if sid not in applied_times]
    
    if missing_updates:
        error_msg = f"以下Sheet的自定义时间未能成功应用: {missing_updates}"
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    return result_df


def load_excel_report(file_path: Path, sheet_name: str) -> Optional[pd.DataFrame]:
    """
    读取无表头的原始 Excel 报表。
    
    [Refactor Note]: 
    不再接收 file_name 字符串并在内部拼接 RESOURCE_DIR。
    调用者必须负责构建并传入完整的 file_path (Path对象)。
    """
    if not file_path.exists():
        logging.warning(f"外部基准报表不存在: {file_path}")
        return None
        
    try:
        # header=None 是关键，防止 Pandas 乱猜表头
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        return df
    except Exception as e:
        logging.error(f"读取外部报表失败 ({file_path.name}): {e}")
        return None