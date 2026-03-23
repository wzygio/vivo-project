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
        if df.empty or 'sheet_start_time' not in df.columns:
            return df
        
        # 确保时间列格式正确
        df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce')
        
        day_str = df['sheet_start_time'].dt.strftime('%Y%m%d')
        month_str = df['sheet_start_time'].dt.strftime('%Y') + 'M' + df['sheet_start_time'].dt.strftime('%m')
        iso_cal = df['sheet_start_time'].dt.isocalendar()
        week_str = iso_cal.year.astype(str) + 'W' + iso_cal.week.astype(str).str.zfill(2)

        day_sort = df['sheet_start_time'].dt.normalize()
        week_sort = df['sheet_start_time'].dt.to_period('W').dt.start_time
        month_sort = df['sheet_start_time'].dt.to_period('M').dt.start_time
        
        if time_type == 'DAY':
            df['time_group'], df['sort_index'] = day_str, day_sort
        elif time_type == 'WEEK':
            df['time_group'], df['sort_index'] = week_str, week_sort
        elif time_type == 'MONTH':
            df['time_group'], df['sort_index'] = month_str, month_sort
        elif time_type == 'MIXED':
            end_dt_normalized = pd.to_datetime(end_dt).normalize()
            day_bound = end_dt_normalized - pd.Timedelta(days=6)
            week_bound = day_bound - pd.Timedelta(days=21)

            cond_day = df['sheet_start_time'] >= day_bound
            cond_week = (df['sheet_start_time'] >= week_bound) & (df['sheet_start_time'] < day_bound)
            cond_month = df['sheet_start_time'] < week_bound

            df['time_group'] = np.select([cond_day, cond_week, cond_month], [day_str, week_str, month_str], default=day_str)
            df['sort_index'] = np.select([cond_day, cond_week, cond_month], [day_sort, week_sort, month_sort], default=day_sort)
        
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