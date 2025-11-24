import logging
import pandas as pd
import streamlit as st
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random

# --- Config & Infra ---
from vivo_project.config import CONFIG
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
from vivo_project.core.defect_modifier import apply_defect_multipliers, apply_defect_dispersion

class YieldAnalysisService:
    """
    [应用服务层] YieldAnalysisService (V3.0 完整还原版)
    完全复刻 WorkflowHandler 的所有业务逻辑，采用标准 Service 架构。
    """

    def __init__(self):
        # 依赖注入：初始化仓储层
        self.repo = PanelRepository()
        self.cache_ttl = f"{CONFIG['application']['cache_ttl_hours']}h"
        # 预加载常用配置
        self.target_defects = CONFIG['processing']['target_defect_groups']
        self.custom_array_times = CONFIG['processing']['array_input_time']['custom_times']

    # ==========================================================================
    #  1. 基础数据获取 (Orchestration & Modifiers)
    # ==========================================================================

    def get_prepared_panel_data(self) -> pd.DataFrame:
        """
        [入口] 获取经修饰后的 Panel 数据。
        对应原 get_modified_panel_details
        """
        # 1. 获取原始数据 (L1 Cache)
        raw_df = self._load_raw_data_cached()
        if raw_df.empty: return pd.DataFrame()

        # 2. 应用修饰 (逻辑较轻，通常不缓存，或者单独缓存)
        return self._apply_modifiers(raw_df)

    def _apply_modifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        [内部逻辑] 执行数据修饰 (分散 & 衰减)
        """
        config = CONFIG.get('processing', {})
        processed_df = df.copy()

        # # 1. 缺陷分散 (Panel ID 重映射)
        # dispersion_config = config.get('dispersion_config', {})
        # if dispersion_config.get('enable', False):
        #     logging.info("应用缺陷分散 (Panel ID 重映射)...")
        #     processed_df = apply_defect_dispersion(processed_df, dispersion_config)

        # 2. 缺陷衰减 (随机抽样)
        multipliers_config = config.get('defect_multipliers', {})
        if multipliers_config:
            logging.info("应用缺陷衰减 (随机抽样)...")
            processed_df = apply_defect_multipliers(processed_df, multipliers_config)
            
        return processed_df

    # ==========================================================================
    #  2. 趋势图业务 (Trend Analysis)
    # ==========================================================================

    def get_mwd_trend_data(self) -> Dict[str, pd.DataFrame] | None:
        """对应原 run_mwd_trend_workflow"""
        panel_df = self.get_prepared_panel_data()
        if panel_df.empty: return None
        return self._compute_mwd_trend_cached(panel_df, self.target_defects)

    def get_current_month_trend_data(self) -> pd.DataFrame | None:
        """对应原 run_current_month_trend_workflow"""
        panel_df = self.get_prepared_panel_data()
        if panel_df.empty: return None
        return self._compute_current_month_trend_cached(panel_df, self.target_defects)

    def get_code_level_trend_data(self) -> Dict[str, pd.DataFrame] | None:
        """
        [您指出的遗漏项] 对应原 run_code_level_mwd_trend_workflow
        """
        panel_df = self.get_prepared_panel_data()
        if panel_df.empty: 
            logging.error("获取基础Panel级数据失败，无法生成Code级趋势图。")
            return None
        return self._compute_code_level_trend_cached(panel_df)

    # ==========================================================================
    #  3. Sheet & Lot 级计算 (Heavy Calculation)
    # ==========================================================================

    def get_sheet_defect_rates(self) -> Dict[str, Any] | None:
        """对应原 run_sheet_defect_rate_workflow"""
        # 1. 获取主数据
        panel_df = self.get_prepared_panel_data()
        if panel_df.empty: return None

        # 2. 获取依赖数据：Array Input Times
        lot_ids = panel_df['lot_id'].unique().tolist()
        # 注意：这里需要转tuple以配合静态缓存方法的签名
        array_times_df = self._load_array_times_cached(tuple(lot_ids))

        # 3. 获取依赖数据：Code Level Trend (复用上面的方法)
        mwd_code_data = self.get_code_level_trend_data()

        # 4. 执行核心计算 (Cached)
        return self._calculate_sheet_rates_cached(
            panel_df, 
            array_times_df, 
            mwd_code_data, 
            self.target_defects
        )

    def get_lot_defect_rates(self) -> Dict[str, Any] | None:
        """对应原 run_lot_defect_rate_workflow"""
        # 1. 获取主数据
        panel_df = self.get_prepared_panel_data()
        if panel_df.empty: return None

        # 2. 获取依赖数据：Sheet 结果
        sheet_results = self.get_sheet_defect_rates()
        if not sheet_results:
            logging.error("Lot 聚合失败，因为依赖的 Sheet 级计算失败或为空。")
            return None

        # 3. 获取依赖数据：Code Level Trend
        mwd_code_data = self.get_code_level_trend_data()

        # 4. 执行核心计算 (Cached)
        return self._calculate_lot_rates_cached(
            panel_df, 
            sheet_results, 
            mwd_code_data, 
            self.target_defects
        )

    # ==========================================================================
    #  4. Mapping 业务
    # ==========================================================================

    def get_mapping_data(self) -> pd.DataFrame:
        """对应原 run_mapping_data_workflow"""
        panel_df = self.get_prepared_panel_data()
        if panel_df.empty: return pd.DataFrame()
        return self._prepare_mapping_cached(panel_df)

    # ==========================================================================
    #  5. 静态缓存层 (Static Cached Implementation)
    #  完全对应原 WorkflowHandler 中的 @st.cache_data 方法
    # ==========================================================================

    @staticmethod
    @st.cache_data(ttl="4h")
    def _load_raw_data_cached() -> pd.DataFrame:
        """[L1 Cache] 加载原始数据"""
        repo = PanelRepository() # 临时实例化
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=4)
        return repo.get_panel_details(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            product_code=CONFIG['data_source']['product_code'],
            work_order_types=CONFIG['data_source']['work_order_types']
        )

    @staticmethod
    @st.cache_data(ttl="4h")
    def _load_array_times_cached(lot_ids: Tuple[str, ...]) -> pd.DataFrame:
        """加载阵列投入时间"""
        if not lot_ids: return pd.DataFrame()
        repo = PanelRepository()
        custom_times = CONFIG['processing']['array_input_time']['custom_times']
        return repo.get_array_input_times(list(lot_ids), custom_times)

    @staticmethod
    @st.cache_data(ttl="4h")
    def _compute_code_level_trend_cached(panel_df: pd.DataFrame) -> Dict[str, pd.DataFrame] | None:
        """Code 级趋势计算"""
        return create_code_level_mwd_trend_data(panel_details_df=panel_df)

    @staticmethod
    @st.cache_data(ttl="4h")
    def _compute_mwd_trend_cached(panel_df, target_defects):
        """MWD 趋势计算"""
        return create_mwd_trend_data(panel_details_df=panel_df, target_defects=target_defects)
    
    @staticmethod
    @st.cache_data(ttl="4h")
    def _compute_current_month_trend_cached(panel_df, target_defects):
        """当月趋势计算"""
        return create_current_month_trend_data(panel_details_df=panel_df, target_defects=target_defects)

    @staticmethod
    @st.cache_data(ttl="4h")
    def _prepare_mapping_cached(panel_df):
        """Mapping 数据准备"""
        return prepare_mapping_data(panel_details_df=panel_df)

    @staticmethod
    @st.cache_data(ttl="2h")
    def _calculate_sheet_rates_cached(panel_df, array_times_df, mwd_code_data, target_defects):
        """Sheet 良率核心计算"""
        return calculate_sheet_defect_rates(
            panel_details_df=panel_df,
            target_defects=target_defects,
            array_input_times_df=array_times_df,
            mwd_code_data=mwd_code_data
        )

    @staticmethod
    @st.cache_data(ttl="2h")
    def _calculate_lot_rates_cached(panel_df, sheet_results, mwd_code_data, target_defects):
        """Lot 良率核心计算"""
        return calculate_lot_defect_rates(
            panel_details_df=panel_df,
            sheet_results=sheet_results,
            mwd_code_data=mwd_code_data,
            target_defects=target_defects
        )