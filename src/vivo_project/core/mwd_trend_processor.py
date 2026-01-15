# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

from vivo_project.config import CONFIG


@staticmethod
def create_mwd_trend_data(
    panel_details_df: pd.DataFrame, 
    target_defects: list,
    ema_span: int = 4,
    scaling_factor: float = 0.9
) -> Dict[str, pd.DataFrame] | None:
    """
    (V5.0 - Shadow EMA 抗噪版)
    Group 级趋势分析：引入 'Shadow EMA' (影子基准) 机制。
    """
    logging.info(f"开始为Group级执行'Shadow EMA'抗噪处理 (Span={ema_span}, Scale={scaling_factor})...")
    if panel_details_df.empty: return None
    
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 5000
        
        # 1. 数据预处理
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = df['warehousing_time'].max()
        
        # 2. 构建日度汇总 (Dense Matrix)
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)
        
        # 3. 末日过滤
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None
        
        # 4. Shadow EMA 计算
        for group in target_defects:
            if group in daily_summary.columns:
                smoothed_rates = _calculate_adaptive_shadow_ema(
                    daily_counts=daily_summary[group].to_numpy(),
                    daily_totals=daily_summary['total_panels'].to_numpy(),
                    span=ema_span
                )
                
                # 应用衰减 & 反算 Count
                attenuated_rates = np.array(smoothed_rates) * scaling_factor
                daily_summary[group] = np.round(attenuated_rates * daily_summary['total_panels']).astype(int)
                
                logging.info(f"成功为 Group '{group}' 计算 Shadow EMA")

        # 5. 格式化输出
        results = {}
        rate_to_group_map = {f"{group.lower()}_rate": group for group in target_defects}
        rate_cols = list(rate_to_group_map.keys())
        
        def _aggregate_and_format(agg_df, time_format_str):
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
        
        seven_days_ago = today - relativedelta(days=6)
        daily_data_filtered = daily_summary[daily_summary.index >= seven_days_ago]
        results['daily'] = _aggregate_and_format(daily_data_filtered, '%m-%d')

        return results
        
    except Exception as e:
        logging.error(f"在执行Group级Shadow EMA处理时发生错误: {e}", exc_info=True)
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
    ema_span: int = 4,          
    scaling_factor: float = 0.7 
) -> Dict[str, pd.DataFrame] | None:
    """
    (V5.0 - Shadow EMA 抗噪版)
    Code 级趋势分析：全面升级为使用 _calculate_adaptive_shadow_ema。
    """
    logging.info(f"开始聚合Code级数据 (Shadow EMA, Span={ema_span})...")
    if panel_details_df.empty: return None
    
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 500
        CODE_MONTHLY_VALUES = CONFIG['processing'].get('code_monthly_values', {}) or {}
        CODE_WEEKLY_VALUES = CONFIG['processing'].get('code_weekly_values', {}) or {}

        # 1. 数据预处理
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())

        # 2. 构建基础日度汇总
        # 注意：这里是 Long Format (每个 Code 每天一行)
        daily_total_panels = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels') # type: ignore
        daily_code_defects = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count') # type: ignore
        
        base_daily_df = pd.merge(daily_total_panels.reset_index(), daily_code_defects.reset_index(), on='warehousing_time', how='left')
        base_daily_df['defect_panel_count'].fillna(0, inplace=True)
        base_daily_df['warehousing_time'] = pd.to_datetime(base_daily_df['warehousing_time'])

        # 3. 末日过滤器
        if not base_daily_df.empty:
            last_day_date = base_daily_df['warehousing_time'].max()
            last_day_panel_count = base_daily_df[base_daily_df['warehousing_time'] == last_day_date]['total_panels'].iloc[0]
            if last_day_panel_count < MIN_PANEL_COUNT_FOR_TODAY:
                base_daily_df = base_daily_df[base_daily_df['warehousing_time'] < last_day_date]

        if base_daily_df.empty: return None

        # 填充 NaN 以便处理
        base_daily_df['defect_group'].fillna("NoDefect", inplace=True)
        base_daily_df['defect_desc'].fillna("NoDefect", inplace=True)

        # --- [核心升级] 迭代应用 Shadow EMA ---
        # 由于 Shadow EMA 是状态相关的，无法直接向量化，我们对每个 Code 分别计算
        base_daily_df['attenuated_rate'] = 0.0 # 初始化结果列
        
        unique_codes = base_daily_df['defect_desc'].unique()
        
        for code in unique_codes:
            if code == "NoDefect": continue
            
            # 提取该 Code 的时间序列
            mask = base_daily_df['defect_desc'] == code
            # 必须按时间排序，否则 EMA 逻辑会乱
            code_subset = base_daily_df[mask].sort_values('warehousing_time')
            
            counts = code_subset['defect_panel_count'].values
            totals = code_subset['total_panels'].values
            
            # 计算 Shadow EMA
            smoothed = _calculate_adaptive_shadow_ema(counts, totals, ema_span)
            
            # 应用衰减
            attenuated = np.array(smoothed) * scaling_factor
            
            # 回写结果 (使用索引对齐)
            base_daily_df.loc[code_subset.index, 'attenuated_rate'] = attenuated
        
        # 更新最终 Count 和 Rate
        base_daily_df['defect_panel_count'] = np.round(base_daily_df['attenuated_rate'] * base_daily_df['total_panels']).astype(int)
        base_daily_df['defect_rate'] = base_daily_df['attenuated_rate']
        
        # 结果生成
        results = {}
        results['monthly'] = _process_code_monthly_data(base_daily_df, CODE_MONTHLY_VALUES, today)
        results['weekly'] = _process_code_weekly_data(base_daily_df, CODE_WEEKLY_VALUES, today)

        results['daily_full'] = base_daily_df[base_daily_df['defect_group'] != 'NoDefect'].copy()

        seven_days_ago = today - relativedelta(days=6)
        daily_data_ui = results['daily_full'][results['daily_full']['warehousing_time'] >= seven_days_ago].copy()
        if not daily_data_ui.empty:
            daily_data_ui['time_period'] = daily_data_ui['warehousing_time'].dt.strftime('%m-%d') # type: ignore
            results['daily'] = daily_data_ui

        logging.info("成功聚合Code级趋势数据 (Shadow EMA)。")
        return results

    except Exception as e:
        logging.error(f"在聚合Code级趋势数据时发生错误: {e}", exc_info=True)
        return None


@staticmethod
def _process_code_monthly_data(base_daily_df: pd.DataFrame, monthly_values: dict, today: dt) -> pd.DataFrame:
    """处理Code级月度数据的工具函数 (保持不变)"""
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
    """处理Code级周度数据的工具函数 (保持不变)"""
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
    (V5.0 - Shadow EMA 抗噪版)
    本月至今趋势：全面升级为使用 _calculate_adaptive_shadow_ema。
    """
    logging.info("开始为“本月至今”日度趋势图准备数据 (Shadow EMA)...")
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

        # 2. 筛选
        today = df['warehousing_time'].max()
        start_of_current_month = today.replace(day=1) 
        daily_summary = daily_summary[daily_summary.index >= start_of_current_month]

        # 3. 末日过滤
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None

        # 4. Shadow EMA 计算
        for group in target_defects:
            if group in daily_summary.columns:
                smoothed_rates = _calculate_adaptive_shadow_ema(
                    daily_counts=daily_summary[group].values,
                    daily_totals=daily_summary['total_panels'].values,
                    span=EMA_SPAN
                )
                
                attenuated_rates = np.array(smoothed_rates) * SCALING_FACTOR
                daily_summary[group] = np.round(attenuated_rates * daily_summary['total_panels']).astype(int)

        # 5. 格式化
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
        
        logging.info("成功生成“本月至今”日度趋势数据 (Shadow EMA)。")
        return melted.sort_values(by='time_period')
        
    except Exception as e:
        logging.error(f"在生成“本月至今”趋势数据时发生错误: {e}", exc_info=True)
        return None


# ==============================================================================
#  核心算法实现：Shadow EMA (请保留在文件末尾或模块级)
# ==============================================================================
def _calculate_adaptive_shadow_ema(daily_counts: np.ndarray, daily_totals: np.ndarray, span: int) -> List[float]:
    """
    [算法核心] 自适应影子 EMA (Adaptive Shadow EMA)
    实现了"显示值"与"基准值"的分离，从根本上解决异常值拖尾问题。
    """
    n = len(daily_counts)
    if n == 0: return []
    
    alpha = 2 / (span + 1)
    smoothed_rates = []
    
    # 状态变量初始化
    trend_n = daily_counts[0]
    trend_d = daily_totals[0]
    
    first_rate = trend_n / trend_d if trend_d > 0 else 0.0
    smoothed_rates.append(first_rate)
    
    for i in range(1, n):
        raw_n = daily_counts[i]
        raw_d = daily_totals[i]
        
        if raw_d == 0:
            smoothed_rates.append(0.0)
            continue
            
        raw_rate = raw_n / raw_d
        prev_base_rate = trend_n / trend_d if trend_d > 0 else 0.0
        
        # 判定 Spike: 3倍暴涨 且 绝对值增加 > 2%
        is_spike = (raw_rate > prev_base_rate * 3.0) and (raw_rate - prev_base_rate > 0.02)
        
        if is_spike:
            # 分支 A: 发生异常 -> 激进显示，保守记忆
            display_n = alpha * raw_n + (1 - alpha) * trend_n
            display_d = alpha * raw_d + (1 - alpha) * trend_d
            display_rate = display_n / display_d if display_d > 0 else 0.0
            
            smoothed_rates.append(display_rate)
            
            # 基准更新：假装良率正常
            clamped_n = prev_base_rate * raw_d
            trend_n = alpha * clamped_n + (1 - alpha) * trend_n
            trend_d = alpha * raw_d     + (1 - alpha) * trend_d
            
        else:
            # 分支 B: 正常波动 -> 正常更新
            trend_n = alpha * raw_n + (1 - alpha) * trend_n
            trend_d = alpha * raw_d + (1 - alpha) * trend_d
            
            current_rate = trend_n / trend_d if trend_d > 0 else 0.0
            smoothed_rates.append(current_rate)
            
    return smoothed_rates