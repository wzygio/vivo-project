# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

from vivo_project.config import CONFIG


@staticmethod
def create_mwd_trend_data(
    panel_details_df: pd.DataFrame, 
    target_defects: list,
    ema_span: int = 7,           # [参数化] 统一控制 EMA 跨度，默认 7
    scaling_factor: float = 0.7  # [参数化] 统一控制衰减因子，默认 0.7
) -> Dict[str, pd.DataFrame] | None:
    """
    (V4.0 - 分离式 EMA)
    核心升级：采用"分离式 EMA" (Split EMA) 算法。
    分别对'分子(不良数)'和'分母(入库数)'进行平滑，最后相除得到良率。
    这能天然解决"小样本高不良率"拉高整体曲线的问题。
    """
    logging.info(f"开始为Group级执行'分离式 EMA'处理 (Span={ema_span}, Scale={scaling_factor})...")
    if panel_details_df.empty: return None
    
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 5000
        
        # 数据预处理
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = df['warehousing_time'].max()
        
        # 构建日度汇总 (Dense Matrix)
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)
        
        # 末日过滤
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None
        
        # --- [核心修改] 分离式 EMA 计算 ---
        # 1. 先计算分母(入库量)的 EMA
        # 使用 adjust=True 以适应 Group 级对近期数据的敏感度需求
        ema_denominator = daily_summary['total_panels'].ewm(span=ema_span, adjust=True, min_periods=1).mean()
        
        for group in target_defects:
            if group in daily_summary.columns:
                # 2. 计算分子(不良数)的 EMA
                ema_numerator = daily_summary[group].ewm(span=ema_span, adjust=True, min_periods=1).mean()
                
                # 3. 相除得到平滑后的良率 (Split EMA Rate)
                # 这种算法下，如果某天 total_panels 很小，它对 ema_denominator 的影响很小，
                # 同理 ema_numerator 的变化也小，因此计算出的 Rate 不会剧烈波动。
                smoothed_rate = ema_numerator / ema_denominator
                
                # 4. 应用衰减因子并反算用于展示的 Count
                attenuated_rate = smoothed_rate * scaling_factor
                daily_summary[group] = np.round(attenuated_rate * daily_summary['total_panels']).astype(int)
                
                logging.info(f"成功为{group}计算分离式EMA (Split EMA)")
        
        # 准备结果字典和格式化函数
        results = {}
        rate_to_group_map = {f"{group.lower()}_rate": group for group in target_defects}
        rate_cols = list(rate_to_group_map.keys())
        
        def _aggregate_and_format(agg_df, time_format_str):
            """辅助函数：计算比率并格式化时间列"""
            for group in target_defects:
                agg_df[f"{group.lower()}_rate"] = agg_df.get(group, 0) / agg_df['total_panels']
            
            if time_format_str == 'ISO':
                iso_df = agg_df.index.isocalendar()
                agg_df['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format)
            else:
                agg_df['time_period'] = agg_df.index.strftime(time_format_str)
            
            melted = agg_df.reset_index().melt(
                id_vars=['time_period', 'total_panels'],
                value_vars=rate_cols, 
                var_name='defect_group_raw', 
                value_name='defect_rate'
            )
            melted['defect_group'] = melted['defect_group_raw'].map(rate_to_group_map)
            return melted.sort_values(by='time_period')
        
        # 处理不同时间维度
        monthly_values = CONFIG['processing']['group_monthly_values']
        weekly_values = CONFIG['processing']['group_weekly_values']
        
        results['monthly'] = _aggregate_and_format(
            _process_group_monthly_data(daily_summary, target_defects, monthly_values, today),
            '%Y-%m月'
        )
        results['weekly'] = _aggregate_and_format(
            _process_group_weekly_data(daily_summary, target_defects, weekly_values, today),
            'ISO' 
        )
        
        # 处理日度数据
        seven_days_ago = today - relativedelta(days=6)
        daily_data_filtered = daily_summary[daily_summary.index >= seven_days_ago]
        results['daily'] = _aggregate_and_format(daily_data_filtered, '%m-%d')

        logging.info("成功执行Group级'分离式 EMA'处理。")
        return results
        
    except Exception as e:
        logging.error(f"在执行Group级趋势处理时发生错误: {e}", exc_info=True)
        return None

@staticmethod
def _process_group_monthly_data(daily_summary: pd.DataFrame, target_defects: list, 
                            monthly_values: dict, today: dt) -> pd.DataFrame:
    """处理月度数据的工具函数"""
    monthly_values = monthly_values or {}
    two_months_ago = today - relativedelta(months=3)
    monthly_data_raw = daily_summary[daily_summary.index.to_period('M') >= pd.Period(two_months_ago, 'M')] # type: ignore
    monthly_agg = monthly_data_raw.resample('M').sum()
    
    for group in target_defects:
        if group in monthly_agg.columns:
            for date in monthly_agg.index:
                time_period = date.strftime('%Y-%m')
                if group in monthly_values:
                    specified_value = monthly_values[group].get(time_period)
                    if specified_value is not None:
                        monthly_agg.loc[date, group] = np.round(
                            specified_value * monthly_agg.loc[date, 'total_panels']
                        ).astype(int)
    return monthly_agg


@staticmethod
def _process_group_weekly_data(daily_summary: pd.DataFrame, target_defects: list,
                        weekly_values: dict, today: dt) -> pd.DataFrame:
    """处理周度数据的工具函数"""
    weekly_values = weekly_values or {}
    three_weeks_ago = today - relativedelta(weeks=2)
    weekly_data_raw = daily_summary[daily_summary.index.to_period('W') >= pd.Period(three_weeks_ago, 'W')] # type: ignore
    weekly_agg = weekly_data_raw.resample('W').sum()
    
    for group in target_defects:
        if group in weekly_agg.columns:
            for date in weekly_agg.index:
                iso_year, iso_week, _ = date.isocalendar()
                time_period = f"{iso_year}-W{iso_week:02d}"
                if group in weekly_values:
                    specified_value = weekly_values[group].get(time_period)
                    if specified_value is not None:
                        weekly_agg.loc[date, group] = np.round(
                            specified_value * weekly_agg.loc[date, 'total_panels']
                        ).astype(int)
    return weekly_agg


    
@staticmethod
def create_code_level_mwd_trend_data(
    panel_details_df: pd.DataFrame,
    ema_span: int = 7,          
    scaling_factor: float = 0.7 
) -> Dict[str, pd.DataFrame] | None:
    """
    [V3.0 - 纯净分离式 EMA 版] 
    1. 彻底移除了"中值钳制"逻辑。
    2. 应用"分离式 EMA" (Split EMA)：分别平滑分子和分母。
    3. 保留 adjust=False，确保历史模拟值的一致性。
    """
    logging.info(f"开始聚合Code级数据 (Split EMA, Span={ema_span})...")
    if panel_details_df.empty: return None
    
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 5000
        CODE_MONTHLY_VALUES = CONFIG['processing'].get('code_monthly_values', {}) or {}
        CODE_WEEKLY_VALUES = CONFIG['processing'].get('code_weekly_values', {}) or {}

        # 数据预处理
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())

        # 构建基础日度汇总
        daily_total_panels = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels') # type: ignore
        daily_code_defects = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count') # type: ignore
        
        base_daily_df = pd.merge(daily_total_panels.reset_index(), daily_code_defects.reset_index(), on='warehousing_time', how='left')
        base_daily_df['defect_panel_count'].fillna(0, inplace=True)
        base_daily_df['warehousing_time'] = pd.to_datetime(base_daily_df['warehousing_time'])

        # 末日过滤器
        if not base_daily_df.empty:
            last_day_date = base_daily_df['warehousing_time'].max()
            last_day_panel_count = base_daily_df[base_daily_df['warehousing_time'] == last_day_date]['total_panels'].iloc[0]
            if last_day_panel_count < MIN_PANEL_COUNT_FOR_TODAY:
                base_daily_df = base_daily_df[base_daily_df['warehousing_time'] < last_day_date]

        if base_daily_df.empty: return None

        # 填充 NaN 以便 groupby 正常工作
        base_daily_df['defect_group'].fillna("NoDefect", inplace=True)
        base_daily_df['defect_desc'].fillna("NoDefect", inplace=True)

        # --- [核心升级: 分离式 EMA] ---
        # 不再计算 raw_rate，而是直接处理 Count 和 Total
        # 使用 adjust=False 保持 Code 级数据的历史稳定性
        
        # 1. 计算分子的 EMA (不良数)
        base_daily_df['ema_numerator'] = base_daily_df.groupby('defect_desc')['defect_panel_count'].transform(
            lambda x: x.ewm(span=ema_span, adjust=False, min_periods=1).mean()
        )
        
        # 2. 计算分母的 EMA (入库数)
        # 注意：虽然 daily_total_panels 每天是一样的，但对于每个 Code 来说，
        # groupby 后的时间序列可能是不连续的(如果它是稀疏的)，或者连续的。
        # 无论如何，我们都对该 Code 所在时间线上的 Total Panels 进行平滑。
        base_daily_df['ema_denominator'] = base_daily_df.groupby('defect_desc')['total_panels'].transform(
            lambda x: x.ewm(span=ema_span, adjust=False, min_periods=1).mean()
        )
        
        # 3. 相除得到平滑良率
        base_daily_df['smoothed_rate'] = base_daily_df['ema_numerator'] / base_daily_df['ema_denominator']
        
        # 4. 应用衰减
        base_daily_df['attenuated_rate'] = base_daily_df['smoothed_rate'] * scaling_factor
        
        # 更新最终值
        base_daily_df['defect_panel_count'] = np.round(base_daily_df['attenuated_rate'] * base_daily_df['total_panels']).astype(int)
        base_daily_df['defect_rate'] = base_daily_df['attenuated_rate']
        
        # 清理临时列
        base_daily_df.drop(columns=['ema_numerator', 'ema_denominator', 'smoothed_rate'], inplace=True, errors='ignore')

        results = {}
        results['monthly'] = _process_code_monthly_data(base_daily_df, CODE_MONTHLY_VALUES, today)
        results['weekly'] = _process_code_weekly_data(base_daily_df, CODE_WEEKLY_VALUES, today)

        # 输出全量日度数据
        results['daily_full'] = base_daily_df[base_daily_df['defect_group'] != 'NoDefect'].copy()

        # UI 显示用的最近7天数据
        seven_days_ago = today - relativedelta(days=6)
        daily_data_ui = results['daily_full'][results['daily_full']['warehousing_time'] >= seven_days_ago].copy()
        if not daily_data_ui.empty:
            daily_data_ui['time_period'] = daily_data_ui['warehousing_time'].dt.strftime('%m-%d') # type: ignore
            results['daily'] = daily_data_ui

        logging.info("成功聚合Code级趋势数据 (分离式 EMA)。")
        return results

    except Exception as e:
        logging.error(f"在聚合Code级趋势数据时发生错误: {e}", exc_info=True)
        return None

@staticmethod
def _process_code_monthly_data(base_daily_df: pd.DataFrame, monthly_values: dict, today: dt) -> pd.DataFrame:
    """处理Code级月度数据的工具函数"""
    monthly_values = monthly_values or {}
    two_months_ago = today - relativedelta(months=3)
    monthly_data_raw = base_daily_df[base_daily_df['warehousing_time'].dt.to_period('M') >= pd.Period(two_months_ago, 'M')].copy() # type: ignore
    
    if monthly_data_raw.empty: return pd.DataFrame()
        
    monthly_data_raw['time_period'] = monthly_data_raw['warehousing_time'].dt.strftime('%Y-%m月')
    monthly_agg = monthly_data_raw.groupby(['time_period', 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'), 
        total_panels=('total_panels', 'sum')
    ).reset_index()

    for idx, row in monthly_agg.iterrows():
        code_desc = row['defect_desc']
        time_period = row['time_period'].replace('月', '') 
        if code_desc in monthly_values:
            specified_value = monthly_values[code_desc].get(time_period)
            if specified_value is not None:
                monthly_agg.at[idx, 'defect_panel_count'] = int(specified_value * row['total_panels']) # type: ignore

    monthly_agg['defect_rate'] = monthly_agg['defect_panel_count'] / monthly_agg['total_panels']
    return monthly_agg[monthly_agg['defect_group'] != 'NoDefect']


@staticmethod
def _process_code_weekly_data(base_daily_df: pd.DataFrame, weekly_values: dict, today: dt) -> pd.DataFrame:
    """处理Code级周度数据的工具函数"""
    weekly_values = weekly_values or {}
    three_weeks_ago = today - relativedelta(weeks=2)
    weekly_data_raw = base_daily_df[
        base_daily_df['warehousing_time'].dt.to_period('W') >= pd.Period(three_weeks_ago, 'W') # type: ignore
    ].copy()
    
    if weekly_data_raw.empty: return pd.DataFrame()
    
    iso_df = weekly_data_raw['warehousing_time'].dt.isocalendar()
    weekly_data_raw['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format)
    
    weekly_agg = weekly_data_raw.groupby(['time_period', 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'),
        total_panels=('total_panels', 'sum')
    ).reset_index()
    
    for idx, row in weekly_agg.iterrows():
        code_desc = row['defect_desc']
        time_period = row['time_period']
        if code_desc in weekly_values:
            specified_value = weekly_values[code_desc].get(time_period)
            if specified_value is not None:
                weekly_agg.at[idx, 'defect_panel_count'] = int(specified_value * row['total_panels'])
    
    weekly_agg['defect_rate'] = weekly_agg['defect_panel_count'] / weekly_agg['total_panels']
    return weekly_agg[weekly_agg['defect_group'] != 'NoDefect']


@staticmethod
def create_current_month_trend_data(panel_details_df: pd.DataFrame, target_defects: list) -> pd.DataFrame | None:
    """
    [新增] 为“10月至今”的日度趋势图准备数据。
    [同步升级] 使用"分离式 EMA"逻辑。
    """
    logging.info("开始为“本月至今”日度趋势图准备数据 (Split EMA)...")
    if panel_details_df.empty: return None
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 10000
        EMA_SPAN = 7
        SCALING_FACTOR = 0.7
        
        # 1. 初始数据聚合
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)

        # 2. 动态筛选出当月1日至今的数据
        today = df['warehousing_time'].max()
        start_of_current_month = today.replace(day=1) 
        daily_summary = daily_summary[daily_summary.index >= start_of_current_month]

        # 3. 末日数据过滤器
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None

        # 4. 应用"分离式 EMA"
        ema_denominator = daily_summary['total_panels'].ewm(span=EMA_SPAN, adjust=True, min_periods=1).mean()
        
        for group in target_defects:
            if group in daily_summary.columns:
                ema_numerator = daily_summary[group].ewm(span=EMA_SPAN, adjust=True, min_periods=1).mean()
                smoothed_rate = ema_numerator / ema_denominator
                attenuated_rate = smoothed_rate * SCALING_FACTOR
                daily_summary[group] = np.round(attenuated_rate * daily_summary['total_panels']).astype(int)

        # 5. 最终格式化
        rate_to_group_map = {f"{group.lower()}_rate": group for group in target_defects}
        rate_cols = list(rate_to_group_map.keys())
        
        for group in target_defects:
            daily_summary[f"{group.lower()}_rate"] = daily_summary.get(group, 0) / daily_summary['total_panels']
        daily_summary['time_period'] = daily_summary.index.strftime('%m-%d') # type: ignore
        
        melted = daily_summary.reset_index().melt(
            id_vars='time_period', value_vars=rate_cols, 
            var_name='defect_group_raw', value_name='defect_rate'
        )
        melted['defect_group'] = melted['defect_group_raw'].map(rate_to_group_map)
        
        logging.info("成功生成“本月至今”日度趋势数据 (分离式 EMA)。")
        return melted.sort_values(by='time_period')
        
    except Exception as e:
        logging.error(f"在生成“本月至今”趋势数据时发生错误: {e}", exc_info=True)
        return None