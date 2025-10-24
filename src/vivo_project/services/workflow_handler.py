# src/workflow_handler.py
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Tuple
import streamlit as st

from vivo_project.config import CONFIG

from vivo_project.infrastructure.db_handler import DatabaseHandler
from vivo_project.infrastructure.data_loader import load_panel_details, load_array_input_times
from vivo_project.core.mwd_trend_processor import create_mwd_trend_data, create_code_level_mwd_trend_data, create_current_month_trend_data
from vivo_project.core.sheet_lot_processor import calculate_lot_defect_rates, calculate_sheet_defect_rates
from vivo_project.core.mapping_processor import prepare_mapping_data

class WorkflowHandler:

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_raw_panel_details() -> pd.DataFrame:
        """
        [原始数据源] 仅负责从数据库加载【未经修改的】全量数据，并缓存结果。
        这是所有数据的“单一真相来源”。
        """
        logging.info("--- [L1 Cache Miss] 原始数据缓存未命中，开始执行数据库查询... ---")
        db_manager = DatabaseHandler()
        if db_manager.engine is None: return pd.DataFrame()
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=4)
        return load_panel_details(
            db_manager=db_manager,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            prod_code=CONFIG['data_source']['product_code'],
            work_order_types=CONFIG['data_source']['work_order_types']
        )
    
    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def get_modified_panel_details() -> pd.DataFrame:
        """
        [默认数据源] (已重构)
        应用所有已配置的数据修饰步骤，并缓存最终结果。
        """
        logging.info("--- [L2 Cache Miss] 修饰后数据缓存未命中，开始处理... ---")
        
        # 1. 获取原始数据
        raw_df = WorkflowHandler.get_raw_panel_details()
        if raw_df.empty:
            return pd.DataFrame()
        
        config = CONFIG.get('processing', {})
        
        # --- [核心修改] 步骤 1: (新功能) 应用缺陷分散 (修改Panel ID) ---
        dispersion_config = config.get('dispersion_config', {})
        if dispersion_config.get('enable', False):
            logging.info("应用缺陷分散 (Panel ID 重映射)...")
            # raw_df = WorkflowHandler.apply_defect_dispersion(raw_df, dispersion_config)
        
        # --- [核心修改] 步骤 2: (旧功能) 应用缺陷衰减 (随机抽样) ---
        multipliers_config = config.get('defect_multipliers', {})
        if multipliers_config:
            logging.info("应用缺陷衰减 (随机抽样)...")
            raw_df = WorkflowHandler.apply_defect_multipliers(raw_df, multipliers_config)
        
        return raw_df
    
    # --- [新增] 缺陷分散 (Panel ID 重映射) 的完整逻辑 ---
    @staticmethod
    def apply_defect_dispersion(panel_df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """
        [新功能 - V2.0] 缺陷分散引擎。
        通过确定性的加权随机映射，修改不良Panel的Lot/Sheet归属。
        """
        logging.info("开始执行缺陷分散 (Panel ID 重映射)...")
        try:
            # 1. 分离良品与不良品
            df_defective = panel_df[panel_df['defect_desc'].notna()].copy()
            df_unaffected = panel_df[panel_df['defect_desc'].isna()]

            if df_defective.empty:
                return panel_df # 没有不良品，无需处理

            # 2. 构建“宇宙”
            all_lot_ids = panel_df['lot_id'].unique().tolist()
            lot_to_sheets_map = panel_df.groupby('lot_id')['sheet_id'].unique().to_dict()
            safe_lot_map = {lot: all_lot_ids for lot in all_lot_ids}

            # 3. 构建“概率转盘”
            weight_maps = WorkflowHandler._build_weight_maps(
                all_lot_ids, 
                config.get('code_specific_lot_weights', {}), 
                config.get('default_lot_weight', 1)
            )

            # 4. [核心修正] 执行重映射，并【同时覆盖】三个关键列
            new_cols = ['panel_id', 'lot_id', 'sheet_id']
            df_defective[new_cols] = df_defective.apply(
                WorkflowHandler._get_dispersion_target,
                axis=1,
                context={
                    'lot_to_sheets_map': lot_to_sheets_map,
                    'safe_lot_map': safe_lot_map,
                    'weight_maps': weight_maps,
                    'salt': config.get('random_seed_salt', "")
                }
            )
            
            # 5. [核心修正] 直接合并，无需重命名
            return pd.concat([df_unaffected, df_defective], ignore_index=True)

        except Exception as e:
            logging.error(f"缺陷分散(apply_defect_dispersion)时发生错误: {e}", exc_info=True)
            return panel_df
    
    @staticmethod
    def _build_weight_maps(all_lots: list, specific_configs: dict, default_weight: int) -> dict:
        """[内部工具] 为每个Code创建Lot权重字典"""
        weight_maps = {}
        for code, lot_weights in specific_configs.items():
            weights = [lot_weights.get(lot, default_weight) for lot in all_lots]
            weight_maps[code] = (all_lots, weights)
        
        # 为所有未配置的Code创建默认权重
        default_weights = [default_weight] * len(all_lots)
        weight_maps['default'] = (all_lots, default_weights)
        return weight_maps
    
    @staticmethod
    def _get_dispersion_target(row: pd.Series, context: dict) -> pd.Series:
        """
        [内部工具 - V2.0] 
        对单行Panel应用确定性哈希，进行加权抽样和重构。
        返回一个包含【三个】新ID的Series，以便覆盖原始列。
        """
        try:
            original_panel_id = row['panel_id']
            defect_desc = row['defect_desc']
            
            # 1. 获取此Code对应的“概率转盘”
            lot_list, weights = context['weight_maps'].get(defect_desc, context['weight_maps']['default'])
            
            # 2. 创建确定性种子
            seed_str = f"{original_panel_id}-{row['batch_no']}-{context['salt']}"
            seed = hash(seed_str)
            
            # 3. 执行确定性加权抽样 (选择Lot)
            import random
            rng = random.Random(seed)
            new_lot_id = rng.choices(lot_list, weights=weights, k=1)[0]
            
            # 4. 从新Lot的Sheet“宇宙”中随机选一个Sheet
            available_sheets = context['lot_to_sheets_map'].get(new_lot_id)
            if not available_sheets or len(available_sheets) == 0:
                # 安全保护：如果新Lot没有Sheet，则不修改
                return pd.Series([original_panel_id, row['lot_id'], row['sheet_id']])
            
            new_sheet_id = rng.choice(available_sheets)
            
            # 5. 重构Panel ID (保留后4位)
            new_panel_id = f"{new_sheet_id}{original_panel_id[11:]}"
            
            # 6. [核心修正] 返回一个包含所有新ID的Series
            return pd.Series([new_panel_id, new_lot_id, new_sheet_id])

        except Exception:
            return pd.Series([row['panel_id'], row['lot_id'], row['sheet_id']])
        
    @staticmethod
    def apply_defect_multipliers(panel_df: pd.DataFrame, multipliers: Dict[str, float]) -> pd.DataFrame:
        """
        [新增工具函数] 根据给定的倍率字典，通过随机抽样减少特定defect_desc的不良Panel数量。
        """
        if not multipliers:
            return panel_df

        logging.info(f"开始应用不良倍率调整: {multipliers}")
        
        codes_to_process = list(multipliers.keys())
        df_to_process = panel_df[panel_df['defect_desc'].isin(codes_to_process)]
        df_unaffected = panel_df[~panel_df['defect_desc'].isin(codes_to_process)]
        
        processed_dfs = []
        for code, factor in multipliers.items():
            df_code = df_to_process[df_to_process['defect_desc'] == code]
            original_count = len(df_code)
            target_count = int(original_count * factor)
            
            if target_count < original_count:
                df_sampled = df_code.sample(n=target_count, random_state=42)
                processed_dfs.append(df_sampled)
                logging.info(f"Code '{code}': 不良数从 {original_count} 下调至 {target_count} (倍率: {factor})。")
            else:
                processed_dfs.append(df_code)

        final_df = pd.concat([df_unaffected] + processed_dfs, ignore_index=True)
        logging.info(f"不良倍率调整完成，数据从 {len(panel_df)} 行变为 {len(final_df)} 行。")
        return final_df

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_mwd_trend_workflow() -> Dict[str, pd.DataFrame] | None:
        """
        (已重构)
        执行“计算月/周/天不良率趋势”的完整工作流。
        """
        logging.info("--- 开始执行月/周/天趋势图工作流 ---")
        
        panel_details_df = WorkflowHandler.get_modified_panel_details()
        if panel_details_df is None or panel_details_df.empty:
            logging.error("获取基础Panel级数据失败，无法生成趋势图。")
            return None
        
        target_defects_from_config = CONFIG.get('processing', {}).get('target_defect_groups', [])
        
        logging.info("基础Panel数据获取成功，正在传递给月/周/天聚合处理器...")
        mwd_plot_df = create_mwd_trend_data(
            panel_details_df=panel_details_df, # <-- 传入的是最原始的panel_details_df
            target_defects=target_defects_from_config
        )

        if mwd_plot_df is None: # mwd_plot_df 现在是一个字典
            logging.error("月/周/天数据聚合失败。")
            return None
        
        logging.info("工作流成功生成月/周/天趋势图数据。")
        return mwd_plot_df

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_current_month_trend_workflow() -> pd.DataFrame | None:
        """
        执行“计算‘10月至今’日度不良率趋势”的完整工作流。
        """
        panel_details_df = WorkflowHandler.get_modified_panel_details()
        if panel_details_df is None or panel_details_df.empty:
            return None

        return create_current_month_trend_data(
            panel_details_df=panel_details_df,
            target_defects=CONFIG['processing']['target_defect_groups']
        )
 
 
    # --- [新增] 用于调用Code级趋势数据的新方法 ---
    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_code_level_mwd_trend_workflow() -> Dict[str, pd.DataFrame] | None:
        """
        执行“计算【Code级】月/周/天不良率趋势”的完整工作流。
        """
        # 同样从L1缓存中获取最原始的数据，非常高效
        panel_details_df = WorkflowHandler.get_modified_panel_details()
        if panel_details_df is None or panel_details_df.empty:
            logging.error("获取基础Panel级数据失败，无法生成Code级趋势图。")
            return None

        # 调用我们刚刚在第一步中创建的新方法
        return create_code_level_mwd_trend_data(
            panel_details_df=panel_details_df
        )

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_sheet_defect_rate_workflow() -> Dict[str, Any] | None:
        """
        [V1.2 - 修正] 执行Sheet级不良率的完整计算。
        现在负责加载并传入【所有】所需数据。
        """
        logging.info("--- [Cache Miss] Sheet级数据缓存未命中，开始完整计算... ---")
        
        # 1. 获取主数据
        panel_details_df = WorkflowHandler.get_modified_panel_details()
        if panel_details_df.empty: return None

        # 2. 获取阵列投入时间
        lot_ids = panel_details_df['lot_id'].unique().tolist()
        array_times_df = WorkflowHandler.run_array_input_time_workflow(lot_ids=tuple(lot_ids))

        # 3. [核心修改] 获取Code级月周天数据，用于计算月度均值
        mwd_code_data = WorkflowHandler.run_code_level_mwd_trend_workflow()

        sheet_results = calculate_sheet_defect_rates(
            panel_details_df=panel_details_df,
            target_defects=CONFIG['processing']['target_defect_groups'],
            array_input_times_df=array_times_df,
            mwd_code_data=mwd_code_data # <--- 传入新数据
        )

        return sheet_results

    

    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_lot_defect_rate_workflow() -> Dict[str, Any] | None:
        """
        [V4.1 - 重构以复用模拟逻辑]
        执行 Lot 级不良率的完整计算，现在直接在 Lot 级别应用模拟。
        """
        logging.info("--- [Cache Miss] Lot级数据缓存未命中，开始完整计算 (V4.1)... ---")

        # --- [核心修改 1] 获取原始 Panel 数据 ---
        # 注意：这里获取的是经过 multipliers 衰减的数据源
        panel_details_df = WorkflowHandler.get_modified_panel_details()
        if panel_details_df.empty:
            logging.error("Lot 聚合失败，因为基础 Panel 数据为空。")
            return None

        # --- [核心修改 2] 获取 Sheet 级结果 (仍然需要其基础信息用于 Lot 基础信息计算) ---
        # Sheet 级结果会被独立计算和缓存
        sheet_results = WorkflowHandler.run_sheet_defect_rate_workflow()
        if sheet_results is None:
            logging.error("Lot 聚合失败，因为依赖的 Sheet 级计算失败或为空。")
            return None

        # --- [核心修改 3] 获取 MWD Code 数据，用于模拟基准 ---
        mwd_code_data = WorkflowHandler.run_code_level_mwd_trend_workflow()
        # (获取逻辑与 Sheet 级相同)

        # --- [核心修改 4] 将 panel_details_df 和 mwd_code_data 传递给 Lot 计算函数 ---
        # 假设 calculate_lot_defect_rates 是 SheetLotProcessor 类的一个静态方法
        return calculate_lot_defect_rates(
            panel_details_df=panel_details_df,       # <--- 传递 Panel 数据
            sheet_results=sheet_results,           # <--- 传递 Sheet 结果 (用于获取 sheet base info)
            mwd_code_data=mwd_code_data,           # <--- 传递 MWD 数据
            target_defects=CONFIG['processing']['target_defect_groups']
        )
    
    # --- [新增] 用于获取和缓存阵列投入时间的新工作流 ---
    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_array_input_time_workflow(lot_ids: Tuple[str, ...]) -> pd.DataFrame:
        """
        根据Lot ID列表，获取并缓存相关的阵列投入时间。
        """
        logging.info("--- [Cache Miss] 阵列投入时间缓存未命中，开始查询数据库... ---")
        db_manager = DatabaseHandler()
        if db_manager.engine is None: return pd.DataFrame()
        
        # 将元组转换回列表以供函数使用
        return load_array_input_times(db_manager=db_manager, lot_ids=list(lot_ids))
    

    
    @staticmethod
    @st.cache_data(ttl=f"{CONFIG['application']['cache_ttl_hours']}h")
    def run_mapping_data_workflow() -> pd.DataFrame:
        """
        执行“为Mapping图准备数据”的完整工作流。
        结果会被缓存。
        """
        # 1. 从L1缓存中获取最原始的、包含批次号的Panel数据
        panel_details_df = WorkflowHandler.get_modified_panel_details()
        if panel_details_df is None or panel_details_df.empty:
            logging.error("获取基础Panel级数据失败，无法准备Mapping数据。")
            return pd.DataFrame()

        # 2. 调用我们在DataProcessor中创建的新方法
        return prepare_mapping_data(
            panel_details_df=panel_details_df
        )