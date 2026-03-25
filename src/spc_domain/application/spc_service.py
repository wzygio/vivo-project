# 🎯 Target File: src/spc_domain/application/spc_service.py
# 🛠️ Action: 全文件替换 (修复 Pydantic 赋值 Bug 与 ALL 扫描逻辑)

import logging
import pandas as pd
import numpy as np
import streamlit as st
from typing import TYPE_CHECKING, Tuple, Optional, List
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, ConfigDict

# 引入底层配置与仓储层
from spc_domain.infrastructure.data_loader import SpcQueryConfig
from spc_domain.infrastructure.repositories.spc_repository import SpcRepository

# 引入核心计算引擎
from spc_domain.core.spc_calculator import (
    preprocess_sheet_features, 
    apply_spc_rules, 
    aggregate_spc_metrics
)

if TYPE_CHECKING:
    from yield_domain.infrastructure.db_handler import DatabaseManager

class SpcDashboardViewModel(BaseModel):
    """SPC 看板视图模型"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    global_summary_df: pd.DataFrame
    detail_df: pd.DataFrame

class SpcAnalysisService:
    _custom_end_date: Optional[datetime] = None

    @classmethod
    def set_analysis_end_date(cls, end_date: Optional[datetime] = None):
        cls._custom_end_date = end_date

    @classmethod
    def get_time_window(cls) -> Tuple[datetime, datetime]:
        current_end = cls._custom_end_date or datetime.now()
        current_start = current_end - relativedelta(months=3)
        return current_start, current_end

    @staticmethod
    def _apply_time_bucket_mapping(df: pd.DataFrame, time_type: str, end_dt: datetime) -> pd.DataFrame:
        """
        [内部辅助] 极速时间桶映射引擎
        V4.4 架构级重构：支持“重叠数据魔方 (Overlapping Data Cubes)”。
        """
        if df.empty or 'sheet_start_time' not in df.columns:
            return df
            
        df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce') 
        
        # 预计算各维度的基础格式
        day_str = df['sheet_start_time'].dt.strftime('%Y%m%d')
        month_str = df['sheet_start_time'].dt.strftime('%Y') + 'M' + df['sheet_start_time'].dt.strftime('%m')
        iso_cal = df['sheet_start_time'].dt.isocalendar()
        week_str = iso_cal.year.astype(str) + 'W' + iso_cal.week.astype(str).str.zfill(2)

        # 构建防呆的绝对排序基准 (利用 1_, 2_, 3_ 强制保证 月->周->天 的 X 轴顺序)
        day_sort = "3_" + day_str
        week_sort = "2_" + week_str
        month_sort = "1_" + month_str
        
        if time_type == 'DAY':
            df['time_group'], df['sort_index'] = day_str, day_sort
        elif time_type == 'WEEK':
            df['time_group'], df['sort_index'] = week_str, week_sort
        elif time_type == 'MONTH':
            df['time_group'], df['sort_index'] = month_str, month_sort
        elif time_type == 'MIXED':
            end_dt_ts = pd.to_datetime(end_dt).normalize()
            
            # 1. 天级数据桶 (最近 7 天)
            day_bound = end_dt_ts - pd.Timedelta(days=6)
            mask_day = df['sheet_start_time'] >= day_bound
            df_day = df[mask_day].copy()
            df_day['time_group'] = day_str[mask_day]
            df_day['sort_index'] = day_sort[mask_day]
            
            # 2. 周级数据桶 (最近 3 个完整自然周，包含天级数据，打破互斥)
            w0_iso = end_dt_ts.isocalendar()
            w1_iso = (end_dt_ts - pd.Timedelta(days=7)).isocalendar()
            w2_iso = (end_dt_ts - pd.Timedelta(days=14)).isocalendar()
            # 稳健提取年份与周数 (索引 [0] 是年, [1] 是周)
            target_weeks = [
                f"{w0_iso[0]}W{str(w0_iso[1]).zfill(2)}",
                f"{w1_iso[0]}W{str(w1_iso[1]).zfill(2)}",
                f"{w2_iso[0]}W{str(w2_iso[1]).zfill(2)}"
            ]
            mask_week = week_str.isin(target_weeks)
            df_week = df[mask_week].copy()
            df_week['time_group'] = week_str[mask_week]
            df_week['sort_index'] = week_sort[mask_week]
            
            # 3. 月级数据桶 (最近 3 个完整自然月，强制抛弃如 12月 等多余数据)
            m0 = end_dt_ts
            m1 = m0 - pd.DateOffset(months=1)
            m2 = m0 - pd.DateOffset(months=2)
            target_months = [
                m0.strftime('%YM%m'),
                m1.strftime('%YM%m'),
                m2.strftime('%YM%m')
            ]
            mask_month = month_str.isin(target_months)
            df_month = df[mask_month].copy()
            df_month['time_group'] = month_str[mask_month]
            df_month['sort_index'] = month_sort[mask_month]
            
            # 物理堆叠重叠的数据桶
            df = pd.concat([df_month, df_week, df_day], ignore_index=True)

        else:
            logging.warning(f"未知的时间颗粒度: {time_type}，降级为按日(DAY)聚合。")
            df['time_group'], df['sort_index'] = day_str, day_sort

        return df

    @staticmethod
    @st.cache_data(show_spinner=False, ttl=3600)
    def get_spc_dashboard_data(
        _db_manager: 'DatabaseManager', 
        query_config_json: str, 
        time_type: str = 'MIXED'
    ) -> SpcDashboardViewModel:
        """
        [企业级 V4.2] 修复 Pydantic 赋值异常与目录自动扫描逻辑
        """
        # 1. 严格实例化配置对象，防止类/实例混淆
        try:
            config_instance = SpcQueryConfig.model_validate_json(query_config_json)
        except Exception as e:
            logging.error(f"Config 解析失败: {e}")
            return SpcDashboardViewModel(global_summary_df=pd.DataFrame(), detail_df=pd.DataFrame())

        target_prod = config_instance.prod_code
        start_dt, end_dt = SpcAnalysisService.get_time_window()
        
        # 2. 智能探测产品目录
        search_prods: List[str] = []
        data_root = Path("data")
        
        if target_prod.upper() == "ALL":
            if data_root.exists():
                # [核心修复]: 探测 data/{prod}/ 目录下直接存放了 spc_snapshot_{prod}.parquet 的文件夹
                for d in data_root.iterdir():
                    if d.is_dir():
                        # 检查该目录下是否存在符合命名的 SPC 缓存文件
                        if (d / f"spc_snapshot_{d.name}.parquet").exists():
                            search_prods.append(d.name)
        else:
            search_prods = [target_prod]

        all_status_dfs = []

        for prod in search_prods:
            # [核心修复]: 移除 spc_cache 子层级，直接指向数据根目录
            prod_snapshot_dir = data_root / prod 
            prod_snapshot_dir.mkdir(parents=True, exist_ok=True)

            # 使用 dict 传参，避开 Pydantic 内部 model_copy 的版本差异风险
            repo = SpcRepository(snapshot_dir=prod_snapshot_dir, use_snapshot=True, db_manager=_db_manager)
            
            # 手动构建提取参数
            current_fetch_config = config_instance.model_copy()
            current_fetch_config.prod_code = prod
            current_fetch_config.start_date = start_dt.strftime("%Y-%m-%d")
            current_fetch_config.end_date = end_dt.strftime("%Y-%m-%d")

            m_df = repo.get_spc_measurements(current_fetch_config)
            s_df = repo.get_spc_spec_limits(prod)
            
            if not m_df.empty:
                # 立即降维判定，减少内存占用
                features = preprocess_sheet_features(measure_df=m_df, spec_df=s_df)
                status = apply_spc_rules(sheet_features=features)
                all_status_dfs.append(status)

        if not all_status_dfs:
            return SpcDashboardViewModel(global_summary_df=pd.DataFrame(), detail_df=pd.DataFrame())

        # 4. 合并并贴上时间标签
        full_status_df = pd.concat(all_status_dfs, ignore_index=True)
        full_status_df = SpcAnalysisService._apply_time_bucket_mapping(full_status_df, time_type.upper(), end_dt)

        # 5. 双轨聚合逻辑 (强制补齐必填参数)
        global_summary_df = aggregate_spc_metrics(
            spc_status_df=full_status_df, 
            group_cols=['sort_index', 'time_group'],
            time_group_col='time_group'
        ) 
        
        detail_df = aggregate_spc_metrics(
            spc_status_df=full_status_df, 
            group_cols=['sort_index', 'time_group', 'prod_code', 'factory'],
            time_group_col='time_group'
        ) 

        # 6. 最终排序
        if not global_summary_df.empty:
            global_summary_df = global_summary_df.sort_values('sort_index').drop(columns=['sort_index'])
        if not detail_df.empty:
            detail_df = detail_df.sort_values(['sort_index', 'factory']).drop(columns=['sort_index'])

        return SpcDashboardViewModel(global_summary_df=global_summary_df, detail_df=detail_df)