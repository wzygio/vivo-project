import logging
import pandas as pd
import numpy as np
import streamlit as st
from typing import TYPE_CHECKING, Tuple, Optional
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

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

class SpcAnalysisService:
    """
    [应用服务层] SPC 报表服务 (V3.0 混合时间轴版)
    职责：
    1. 维护全站统一的 SPC 看板时间轴 (固定为前 3 个月)。
    2. 调度 SpcRepository 提取/缓存底层数据。
    3. 动态映射单一维度 (Day/Week/Month) 或 高管混合看板维度 (MIXED)。
    4. 驱动 Core 层计算引擎生成附带 sort_index 的多维数据魔方。
    """

    # ==========================================================================
    #  1. 全局状态控制 (看板统一时间轴)
    # ==========================================================================
    _custom_end_date: Optional[datetime] = None

    @classmethod
    def set_analysis_end_date(cls, end_date: Optional[datetime] = None):
        """允许开发人员/后台注入并锁定 SPC 看板的全局结束时间"""
        cls._custom_end_date = end_date
        if end_date:
            start_date = end_date - relativedelta(months=3)
            logging.warning(f"🕒 [SPC 全局广播] 分析时间窗口已强制锁定: {start_date.date()} -> {end_date.date()}")
        else:
            logging.info("🕒 [SPC 全局广播] 看板已恢复实时滚动模式 (截止至今日)。")

    @classmethod
    def get_time_window(cls) -> Tuple[datetime, datetime]:
        """动态获取 SPC 看板的时间窗口 (严格限制为截止时间前 3 个月)"""
        current_end = cls._custom_end_date or datetime.now()
        current_start = current_end - relativedelta(months=3)
        return current_start, current_end

    # ==========================================================================
    #  2. 核心报表生成管线
    # ==========================================================================
    @staticmethod
    def _apply_time_bucket_mapping(df: pd.DataFrame, time_type: str, end_dt: datetime) -> pd.DataFrame:
        """
        [内部辅助] 极速时间桶映射引擎
        新增支持 'MIXED' (三月三周七天) 混合模式，并注入 sort_index 保障跨维度排序正确性。
        """
        if df.empty or 'sheet_start_time' not in df.columns:
            return df
            
        df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce') 
        
        # 预计算各维度的基础格式
        day_str = df['sheet_start_time'].dt.strftime('%Y%m%d')
        month_str = df['sheet_start_time'].dt.strftime('%Y') + 'M' + df['sheet_start_time'].dt.strftime('%m')
        iso_cal = df['sheet_start_time'].dt.isocalendar()
        week_str = iso_cal.year.astype(str) + 'W' + iso_cal.week.astype(str).str.zfill(2)

        # 预计算用于绝对排序的基准时间戳
        day_sort = df['sheet_start_time'].dt.normalize()
        week_sort = df['sheet_start_time'].dt.to_period('W').dt.start_time
        month_sort = df['sheet_start_time'].dt.to_period('M').dt.start_time
        
        if time_type == 'DAY':
            df['time_group'] = day_str
            df['sort_index'] = day_sort
            
        elif time_type == 'WEEK':
            df['time_group'] = week_str
            df['sort_index'] = week_sort
            
        elif time_type == 'MONTH':
            df['time_group'] = month_str
            df['sort_index'] = month_sort
            
        elif time_type == 'MIXED':
            # === [核心算法] 混合看板：三月三周七天 边界判定 ===
            # 计算以 end_dt 为锚点的安全切割边界
            end_dt_normalized = pd.to_datetime(end_dt).normalize()
            
            # 边界 1：最近 7 天 (包含 end_dt 当天往前推 6 天)
            day_bound = end_dt_normalized - pd.Timedelta(days=6)
            # 边界 2：七天之前的 3 周 (21天)
            week_bound = day_bound - pd.Timedelta(days=21)
            # 注：早于 week_bound 的所有数据皆划归为 MONTH。由于底层 Repo 限制了总长度为 3 个月，这就完美凑成了“三月”

            # 构建布尔掩码矩阵
            cond_day = df['sheet_start_time'] >= day_bound
            cond_week = (df['sheet_start_time'] >= week_bound) & (df['sheet_start_time'] < day_bound)
            cond_month = df['sheet_start_time'] < week_bound

            # 向量化路由时间组名
            df['time_group'] = np.select(
                [cond_day, cond_week, cond_month],
                [day_str, week_str, month_str],
                default=day_str
            )
            # 向量化路由排序权重
            df['sort_index'] = np.select(
                [cond_day, cond_week, cond_month],
                [day_sort, week_sort, month_sort],
                default=day_sort
            )

        else:
            logging.warning(f"未知的时间颗粒度: {time_type}，降级为按日(DAY)聚合。")
            df['time_group'] = day_str
            df['sort_index'] = day_sort

        return df

    @staticmethod
    @st.cache_data(show_spinner=False, ttl=3600)
    def get_spc_report(
        _db_manager: 'DatabaseManager', 
        snapshot_dir_str: str, 
        query_config_json: str, 
        time_type: str = 'MIXED' # 默认调整为混合看板模式
    ) -> pd.DataFrame:
        """
        [对外核心接口] 生成一站式 SPC 特性报警率报表。
        """
        config = SpcQueryConfig.model_validate_json(query_config_json)
        snapshot_dir = Path(snapshot_dir_str)
        
        logging.info(f"==> 开始生成 SPC 报表 (产品: {config.prod_code}, 维度: {time_type}) <==")

        # 强制接管时间池边界 (前三个月)
        start_dt, end_dt = SpcAnalysisService.get_time_window()
        config.start_date = start_dt.strftime("%Y-%m-%d")
        config.end_date = end_dt.strftime("%Y-%m-%d")

        # 实例化仓储引擎获取数据
        repo = SpcRepository(snapshot_dir=snapshot_dir, use_snapshot=True, db_manager=_db_manager)
        measure_df = repo.get_spc_measurements(config)
        spec_df = repo.get_spc_spec_limits(config.prod_code)

        if measure_df.empty:
            logging.warning("SPC 底层数据提取为空，终止报表生成。")
            return pd.DataFrame()

        # 拷贝数据并执行时间桶映射 (传入 end_dt 用于混合轴切割)
        processing_df = measure_df.copy()
        processing_df = SpcAnalysisService._apply_time_bucket_mapping(processing_df, time_type.upper(), end_dt)

        # 驱动 Core 层计算流水线
        sheet_features = preprocess_sheet_features(measure_df=processing_df, spec_df=spec_df) 
        status_df = apply_spc_rules(sheet_features=sheet_features) 
        
        # [架构升级] 聚合键中强行编入 sort_index，防止折叠时丢失绝对时间属性
        aggregation_dimensions = ['sort_index', 'time_group', 'prod_code', 'factory']
        final_report_df = aggregate_spc_metrics(spc_status_df=status_df, group_cols=aggregation_dimensions) 

        if not final_report_df.empty:
            # [核心保障] 前端图表的 X 轴是否错乱，全靠这里的 sort_index 兜底！
            final_report_df = final_report_df.sort_values(by=['sort_index', 'factory'], ascending=True)
            # 为了数据传输与前端表格渲染纯净，将作为中转变量的 sort_index 抛弃 (此时数据已经按时间排好队了)
            final_report_df = final_report_df.drop(columns=['sort_index'])

        logging.info("==> SPC 报表生成管线执行完毕。 <==")
        return final_report_df