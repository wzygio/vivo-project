# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

from vivo_project.config import CONFIG


@staticmethod
def create_mwd_trend_data(panel_details_df: pd.DataFrame, target_defects: list) -> Dict[str, pd.DataFrame] | None:
    """
    (V3.7 - 修复周别一致性)
    使用指数移动平均(EMA)进行数据平滑。
    强制使用 ISO 8601 (周一~周日) 标准计算周别，解决 Group 与 Code 级周号不一致问题。
    """
    logging.info("开始为Group级月/周/天执行'柔化衰减'数据处理 (V3.7 - ISO Fix)...")
    if panel_details_df.empty: return None
    
    try:
        # 配置参数
        GROUP_EMA_SPANS = {
            'OLED_Mura': 4,
            'default': 7
        }
        SCALING_FACTOR = 0.7
        MIN_PANEL_COUNT_FOR_TODAY = 5000
        
        # 数据预处理
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())
        
        # 构建日度汇总
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels')
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0)
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)
        
        # 末日过滤
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None
        
        # EMA处理
        for group in target_defects:
            if group in daily_summary.columns:
                raw_rate = daily_summary[group] / daily_summary['total_panels']
                ema_span = GROUP_EMA_SPANS.get(group, GROUP_EMA_SPANS['default'])
                smoothed_rate = raw_rate.ewm(span=ema_span, adjust=True, min_periods=1).mean()
                attenuated_rate = smoothed_rate * SCALING_FACTOR
                daily_summary[group] = np.round(attenuated_rate * daily_summary['total_panels']).astype(int)
                logging.info(f"成功为{group}计算EMA柔化衰减")
        
        # 准备结果字典和格式化函数
        results = {}
        rate_to_group_map = {f"{group.lower()}_rate": group for group in target_defects}
        rate_cols = list(rate_to_group_map.keys())
        
        def _aggregate_and_format(agg_df, time_format_str):
            """
            辅助函数：计算比率并格式化时间列
            [修改] 增加对 'ISO' 特殊标记的处理
            """
            for group in target_defects:
                agg_df[f"{group.lower()}_rate"] = agg_df.get(group, 0) / agg_df['total_panels']
            
            # --- [核心修改] 统一周别格式化逻辑 ---
            if time_format_str == 'ISO':
                # 针对周度数据，使用 isocalendar 确保周一到周日为一周
                # agg_df.index 此时通常是 Resample 后的 Sunday，ISO 算法会将其归入正确的一周
                iso_df = agg_df.index.isocalendar()
                agg_df['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format)
            else:
                agg_df['time_period'] = agg_df.index.strftime(time_format_str)
            # -----------------------------------

            melted = agg_df.reset_index().melt(
                id_vars='time_period', 
                value_vars=rate_cols, 
                var_name='defect_group_raw', 
                value_name='defect_rate'
            )
            melted['defect_group'] = melted['defect_group_raw'].map(rate_to_group_map)
            return melted.sort_values(by='time_period')
        
        # 使用工具函数处理月度和周度数据
        monthly_values = CONFIG['processing']['group_monthly_values']
        weekly_values = CONFIG['processing']['group_weekly_values']
        
        # 处理不同时间维度的数据
        results['monthly'] = _aggregate_and_format(
            _process_group_monthly_data(daily_summary, target_defects, monthly_values, today),
            '%Y-%m月'
        )
        # [修改] 传入 'ISO' 标记
        results['weekly'] = _aggregate_and_format(
            _process_group_weekly_data(daily_summary, target_defects, weekly_values, today),
            'ISO' 
        )
        
        # 处理日度数据
        seven_days_ago = today - relativedelta(days=6)
        daily_data_filtered = daily_summary[daily_summary.index >= seven_days_ago]
        results['daily'] = _aggregate_and_format(daily_data_filtered, '%m-%d')
        
        logging.info("成功执行Group级EMA'柔化衰减'处理。")
        return results
        
    except Exception as e:
        logging.error(f"在执行Group级EMA柔化衰减时发生错误: {e}", exc_info=True)
        return None

@staticmethod
def _process_group_monthly_data(daily_summary: pd.DataFrame, target_defects: list, 
                            monthly_values: dict, today: dt) -> pd.DataFrame:
    """处理月度数据的工具函数"""
    two_months_ago = today - relativedelta(months=3)
    monthly_data_raw = daily_summary[daily_summary.index.to_period('M') >= pd.Period(two_months_ago, 'M')] # type: ignore
    monthly_agg = monthly_data_raw.resample('M').sum()
    
    # 应用指定值
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
                        logging.info(f"已为{group}在{time_period}设置指定值")
    
    return monthly_agg

@staticmethod
def _process_group_weekly_data(daily_summary: pd.DataFrame, target_defects: list,
                        weekly_values: dict, today: dt) -> pd.DataFrame:
    """处理周度数据的工具函数 (已适配 ISO 周)"""
    three_weeks_ago = today - relativedelta(weeks=2)
    weekly_data_raw = daily_summary[daily_summary.index.to_period('W') >= pd.Period(three_weeks_ago, 'W')] # type: ignore
    # Resample 'W' 默认以周日结束。即 Index 为该周的周日。
    weekly_agg = weekly_data_raw.resample('W').sum()
    
    # 应用指定值
    for group in target_defects:
        if group in weekly_agg.columns:
            for date in weekly_agg.index:
                # --- [核心修改] 使用 ISO 算法生成 Key，确保与配置文件和前端展示一致 ---
                iso_year, iso_week, _ = date.isocalendar()
                time_period = f"{iso_year}-W{iso_week:02d}"
                # ----------------------------------------------------------------
                
                if group in weekly_values:
                    specified_value = weekly_values[group].get(time_period)
                    if specified_value is not None:
                        weekly_agg.loc[date, group] = np.round(
                            specified_value * weekly_agg.loc[date, 'total_panels']
                        ).astype(int)
                        logging.info(f"已为{group}在{time_period}设置指定值")
    
    return weekly_agg

    
@staticmethod
def create_code_level_mwd_trend_data(panel_details_df: pd.DataFrame) -> Dict[str, pd.DataFrame] | None:
    """
    [V2.3 - ISO Fix] 
    1. 修复周别计算逻辑，统一使用 ISO 8601 (Mon-Sun)。
    2. 解决 Group 与 Code 级图表周号错位问题。
    """
    logging.info("开始聚合Code级月、周、天数据 (V2.3 - ISO Fix)...")
    if panel_details_df.empty: return None
    
    try:
        # 配置参数
        EMA_SPAN = 4
        SCALING_FACTOR = 0.7
        MIN_PANEL_COUNT_FOR_TODAY = 5000

        # 月度指定值配置
        CODE_MONTHLY_VALUES = CONFIG['processing'].get('code_monthly_values', {}) or {}

        
        # 数据预处理
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())

        # 构建日度汇总
        daily_total_panels = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels')
        daily_code_defects = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count')
        
        base_daily_df = pd.merge(daily_total_panels.reset_index(), daily_code_defects.reset_index(), on='warehousing_time', how='left')
        
        # 修复IntCastingNaNError
        base_daily_df['defect_panel_count'].fillna(0, inplace=True)
        base_daily_df['defect_rate'] = base_daily_df['defect_panel_count'] / base_daily_df['total_panels']
        base_daily_df['warehousing_time'] = pd.to_datetime(base_daily_df['warehousing_time'])

        # 末日数据过滤器
        if not base_daily_df.empty:
            last_day_date = base_daily_df['warehousing_time'].max()
            last_day_panel_count = base_daily_df[base_daily_df['warehousing_time'] == last_day_date]['total_panels'].iloc[0]
            if last_day_panel_count < MIN_PANEL_COUNT_FOR_TODAY:
                logging.warning(
                    f"最后一天的Panel入库数 ({last_day_panel_count}) 小于阈值 {MIN_PANEL_COUNT_FOR_TODAY}。"
                    f"为避免数据失真，将忽略 {last_day_date.date()} 的数据。"
                )
                base_daily_df = base_daily_df[base_daily_df['warehousing_time'] < last_day_date]

        if base_daily_df.empty: return None

        # EMA处理
        base_daily_df['defect_group'].fillna("NoDefect", inplace=True)
        base_daily_df['defect_desc'].fillna("NoDefect", inplace=True)
        base_daily_df['smoothed_rate'] = base_daily_df.groupby('defect_desc')['defect_rate'].transform(
            lambda x: x.ewm(span=EMA_SPAN, adjust=True, min_periods=1).mean()
        )
        base_daily_df['attenuated_rate'] = base_daily_df['smoothed_rate'] * SCALING_FACTOR
        base_daily_df['defect_panel_count'] = np.round(base_daily_df['attenuated_rate'] * base_daily_df['total_panels']).astype(int)
        base_daily_df['defect_rate'] = base_daily_df['attenuated_rate']
        
        results = {}

        # 使用工具函数处理月度数据
        results['monthly'] = _process_code_monthly_data(base_daily_df, CODE_MONTHLY_VALUES, today)

        # 处理周度数据
        three_weeks_ago = today - relativedelta(weeks=2)
        weekly_data_raw = base_daily_df[base_daily_df['warehousing_time'].dt.to_period('W') >= pd.Period(three_weeks_ago, 'W')].copy()
        
        if not weekly_data_raw.empty:
            # --- [核心修改] 使用 ISO Calender 生成标准的 YYYY-Wxx ---
            # 这样 Mon(周一) 到 Sun(周日) 都会被分配到同一个 Week Number
            iso_df = weekly_data_raw['warehousing_time'].dt.isocalendar()
            weekly_data_raw['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format) # type: ignore
            # -----------------------------------------------------
            
            weekly_agg = weekly_data_raw.groupby(['time_period', 'defect_group', 'defect_desc']).agg(
                defect_panel_count=('defect_panel_count', 'sum'), 
                total_panels=('total_panels', 'sum')
            ).reset_index()
            weekly_agg['defect_rate'] = weekly_agg['defect_panel_count'] / weekly_agg['total_panels']
            results['weekly'] = weekly_agg[weekly_agg['defect_group'] != 'NoDefect']

        # 处理日度数据
        seven_days_ago = today - relativedelta(days=6)
        daily_data_final = base_daily_df[base_daily_df['warehousing_time'] >= seven_days_ago].copy()
        if not daily_data_final.empty:
            daily_data_final['time_period'] = daily_data_final['warehousing_time'].dt.strftime('%m-%d')
            results['daily'] = daily_data_final[daily_data_final['defect_group'] != 'NoDefect']

        logging.info("成功聚合Code级月、周、天数据 (最终稳定版 - ISO Fix)。")
        return results

    except Exception as e:
        logging.error(f"在聚合Code级趋势数据时发生错误: {e}", exc_info=True)
        return None


@staticmethod
def _process_code_monthly_data(base_daily_df: pd.DataFrame, monthly_values: dict, today: dt) -> pd.DataFrame:
    """处理Code级月度数据的工具函数"""
    two_months_ago = today - relativedelta(months=3)
    monthly_data_raw = base_daily_df[base_daily_df['warehousing_time'].dt.to_period('M') >= pd.Period(two_months_ago, 'M')].copy()
    
    if monthly_data_raw.empty:
        return pd.DataFrame()
        
    monthly_data_raw['time_period'] = monthly_data_raw['warehousing_time'].dt.strftime('%Y-%m月')
    monthly_agg = monthly_data_raw.groupby(['time_period', 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'), 
        total_panels=('total_panels', 'sum')
    ).reset_index()

    # 应用指定值
    for idx, row in monthly_agg.iterrows():
        code_desc = row['defect_desc']
        time_period = row['time_period'].replace('月', '')  # 移除'月'字以匹配格式
        if code_desc in monthly_values:
            specified_value = monthly_values[code_desc].get(time_period)
            if specified_value is not None:
                monthly_agg.at[idx, 'defect_panel_count'] = int(specified_value * row['total_panels']) # type: ignore
                logging.info(f"已为{code_desc}在{time_period}设置指定值")

    monthly_agg['defect_rate'] = monthly_agg['defect_panel_count'] / monthly_agg['total_panels']
    return monthly_agg[monthly_agg['defect_group'] != 'NoDefect']


@staticmethod
def create_current_month_trend_data(panel_details_df: pd.DataFrame, target_defects: list) -> pd.DataFrame | None:
    """
    [新增] 为“10月至今”的日度趋势图准备数据。
    应用与Group级图表完全相同的“末日截断”和“柔化衰减”逻辑。
    """
    logging.info("开始为“10月至今”日度趋势图准备数据...")
    if panel_details_df.empty: return None
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 10000
        EMA_SPAN = 7
        SCALING_FACTOR = 0.7
        
        # 1. 初始数据聚合
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels')
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0)
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)

        # 2. [核心逻辑] 动态筛选出当月1日至今的数据
        today = pd.to_datetime(dt.now().date())
        start_of_current_month = today.replace(day=1) 
        daily_summary = daily_summary[daily_summary.index >= start_of_current_month]

        # 3. 应用“末日数据”过滤器
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None

        # 4. 应用“柔化衰减”逻辑 (EMA)
        for group in target_defects:
            if group in daily_summary.columns:
                raw_rate = daily_summary[group] / daily_summary['total_panels']
                smoothed_rate = raw_rate.ewm(span=EMA_SPAN, adjust=True, min_periods=1).mean()
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
        
        logging.info("成功生成“10月至今”日度趋势数据。")
        return melted.sort_values(by='time_period')
        
    except Exception as e:
        logging.error(f"在生成“10月至今”趋势数据时发生错误: {e}", exc_info=True)
        return None