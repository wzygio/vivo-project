import logging, os
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

# --- Config & Infra ---
from vivo_project.config import CONFIG, RESOURCE_DIR
from vivo_project.infrastructure.repositories.panel_repository import PanelRepository

# --- Core (Processors) ---
from vivo_project.core.mwd_trend_processor import (
    create_mwd_trend_data, 
    create_code_level_mwd_trend_data, 
    create_current_month_trend_data
)
from vivo_project.core.sheet_lot_processor import (
    calculate_lot_defect_rates, 
    calculate_sheet_defect_rates
)
from vivo_project.core.mapping_processor import prepare_mapping_data
from vivo_project.core.defect_modifier import (
    apply_defect_multipliers, 
    apply_defect_dispersion
)

class YieldAnalysisService:
    """
    [应用服务层] YieldAnalysisService (V4.0 极速静态版)
    
    设计理念：
    为了适应 Streamlit 的重运行机制，所有方法均为静态并进行缓存。
    这确保了在下拉框切换等交互操作中，数据获取是瞬间完成的。
    """


    # ==========================================================================
    #  1. 基础数据源 (L1 & L2 Cache)
    # ==========================================================================

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_raw_panel_details() -> pd.DataFrame:
        """[L1 Cache] 从数据库加载原始数据 (单一真相来源)"""
        logging.info("--- [L1 Cache Miss] 加载原始 Panel 数据... ---")
        
        # 在静态方法内部实例化 Repo
        repo = PanelRepository()
        
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=4)
        
        return repo.get_panel_details(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            product_code=CONFIG['data_source']['product_code'],
            work_order_types=CONFIG['data_source']['work_order_types']
        )

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_modified_panel_details() -> pd.DataFrame:
        """[L2 Cache] 获取经过修饰(分散/衰减)后的 Panel 数据"""
        logging.info("--- [L2 Cache Miss] 计算修饰后数据... ---")
        
        # 1. 获取 L1 数据
        raw_df = YieldAnalysisService.get_raw_panel_details()
        if raw_df.empty: return pd.DataFrame()
        
        # 2. 应用修饰
        config = CONFIG.get('processing', {})
        processed_df = raw_df.copy()

        # # 缺陷分散
        # dispersion_config = config.get('dispersion_config', {})
        # if dispersion_config.get('enable', False):
        #     logging.info("应用缺陷分散...")
        #     processed_df = apply_defect_dispersion(processed_df, dispersion_config)

        # 缺陷衰减
        multipliers_config = config.get('defect_multipliers', {})
        if multipliers_config:
            logging.info("应用缺陷衰减...")
            processed_df = apply_defect_multipliers(processed_df, multipliers_config)
            
        return processed_df

    # ==========================================================================
    #  2. 趋势图业务 (Trend Analysis)
    # ==========================================================================

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_mwd_trend_data() -> Dict[str, pd.DataFrame] | None:
        """获取月/周/天趋势数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details()
        if panel_df.empty: return None
        
        target_defects = CONFIG['processing']['target_defect_groups']
        return create_mwd_trend_data(panel_details_df=panel_df, target_defects=target_defects)

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_current_month_trend_data() -> pd.DataFrame | None:
        """获取当月日度趋势"""
        panel_df = YieldAnalysisService.get_modified_panel_details()
        if panel_df.empty: return None
        
        target_defects = CONFIG['processing']['target_defect_groups']
        return create_current_month_trend_data(panel_details_df=panel_df, target_defects=target_defects)

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_code_level_trend_data() -> Dict[str, pd.DataFrame] | None:
        """获取 Code 级趋势数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details()
        if panel_df.empty: 
            logging.error("获取基础Panel级数据失败，无法生成Code级趋势图。")
            return None
        return create_code_level_mwd_trend_data(panel_details_df=panel_df)

    # ==========================================================================
    #  3. Sheet & Lot 级计算 (Heavy Calculation)
    # ==========================================================================

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_sheet_defect_rates() -> Dict[str, Any] | None:
        """计算 Sheet 级良率 (直接缓存结果)"""
        logging.info("--- [Cache Miss] 计算 Sheet 级良率... ---")
        
        # 1. 主数据
        panel_df = YieldAnalysisService.get_modified_panel_details()
        if panel_df.empty: return None

        # 2. 依赖数据
        lot_ids = panel_df['lot_id'].unique().tolist()
        array_times_df = YieldAnalysisService._get_array_times(tuple(lot_ids)) # 调用下面的静态辅助方法
        mwd_code_data = YieldAnalysisService.get_code_level_trend_data()
        target_defects = CONFIG['processing']['target_defect_groups']

        # 3. 核心计算
        return calculate_sheet_defect_rates(
            panel_details_df=panel_df,
            target_defects=target_defects,
            array_input_times_df=array_times_df,
            mwd_code_data=mwd_code_data
        )

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_lot_defect_rates() -> Dict[str, Any] | None:
        """计算 Lot 级良率 (直接缓存结果)"""
        logging.info("--- [Cache Miss] 计算 Lot 级良率... ---")

        # 1. 主数据
        panel_df = YieldAnalysisService.get_modified_panel_details()
        if panel_df.empty: return None

        # 2. 依赖 Sheet 结果
        sheet_results = YieldAnalysisService.get_sheet_defect_rates()
        if not sheet_results: return None

        # 3. 依赖 MWD 数据
        mwd_code_data = YieldAnalysisService.get_code_level_trend_data()
        target_defects = CONFIG['processing']['target_defect_groups']

        # 4. 核心计算
        return calculate_lot_defect_rates(
            panel_details_df=panel_df,
            sheet_results=sheet_results,
            mwd_code_data=mwd_code_data,
            target_defects=target_defects
        )

    # ==========================================================================
    #  4. Mapping 业务
    # ==========================================================================

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_mapping_data() -> pd.DataFrame:
        """准备 Mapping 数据"""
        panel_df = YieldAnalysisService.get_modified_panel_details()
        if panel_df.empty: return pd.DataFrame()
        return prepare_mapping_data(panel_details_df=panel_df)

    # ==========================================================================
    #  内部辅助方法 (依然需要缓存)
    # ==========================================================================

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def _get_array_times(lot_ids: Tuple[str, ...]) -> pd.DataFrame:
        """独立的 Array Time 查询缓存"""
        if not lot_ids: return pd.DataFrame()
        repo = PanelRepository()
        custom_times = CONFIG['processing']['array_input_time']['custom_times']
        return repo.get_array_input_times(list(lot_ids), custom_times)
    
    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def load_static_warning_lines(config_file: str = CONFIG['path']['static_warning_lines_file']):
        """
        [新功能] 读取静态 Excel 配置文件，获取每个 Code 的固定警戒线。
        读取 B列(Code) 和 F列(预警线)。
        """
        logging.info("--- 开始加载静态警戒线配置 ---")
        config_file_path = os.path.join(RESOURCE_DIR, config_file)
        logging.info(f"配置文件路径: {config_file_path}")
        
        if not os.path.exists(config_file_path):
            st.error(f"⚠️ 未找到警戒线配置文件: {config_file_path}")
            return {}

        try:
            # 1. 读取 Excel
            logging.info("步骤1: 读取Excel文件...")
            df = pd.read_excel(config_file_path, header=0, engine='openpyxl')
            logging.info(f"成功读取Excel，共 {len(df)} 行数据")
            
            # 2. 检查必要的列是否存在
            logging.info("步骤2: 检查列名...")
            df.columns = df.columns.str.strip()
            
            required_cols = ['Code', '预警线']
            if not all(col in df.columns for col in required_cols):
                logging.warning(f"未找到标准列名，尝试使用列索引读取...")
                if df.shape[1] > 5:
                    df = df.iloc[:, [1, 5]]
                    df.columns = ['Code', '预警线']
                    logging.info("成功使用列索引读取数据")
                else:
                    logging.error("Excel文件列数不足，无法读取数据")
                    return {}

            # 3. 数据清洗
            logging.info("步骤3: 开始数据清洗...")
            warning_lines = {}
            valid_count = 0
            invalid_count = 0
            
            for idx, row in df.iterrows():
                code = row['Code']
                val = row['预警线']
                
                if pd.isna(code):
                    invalid_count += 1
                    continue
                    
                if pd.isna(val) or val == '' or val == '报表无数据':
                    invalid_count += 1
                    continue
                    
                try:
                    if isinstance(val, str):
                        if '%' in val:
                            val = float(val.strip('%')) / 100.0
                        else:
                            val = float(val)
                    else:
                        val = float(val)
                    
                    warning_lines[str(code).strip()] = val
                    valid_count += 1
                    logging.debug(f"成功处理Code: {code}, 预警线: {val}")
                    
                except ValueError as e:
                    invalid_count += 1
                    logging.warning(f"跳过无效数据行 {idx}: {code} - {val}, 错误: {e}")
                    continue

            logging.info(f"数据处理完成，有效记录: {valid_count}, 无效记录: {invalid_count}")
            return warning_lines

        except Exception as e:
            logging.error(f"读取警戒线配置失败: {e}", exc_info=True)
            st.error(f"读取警戒线配置失败: {e}")
            return {}
