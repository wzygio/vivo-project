# src/data_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

# --- [新增] 用于按倍率调整不良Panel数量的辅助函数 ---
@staticmethod
def create_mwd_trend_data(panel_details_df: pd.DataFrame, target_defects: list) -> Dict[str, pd.DataFrame] | None:
    """
    (V3.6 - 升级至EMA)
    使用指数移动平均(EMA)进行数据平滑，使当天数据权重更高。
    """
    logging.info("开始为Group级月/周/天执行“柔化衰减”数据处理 (V3.6 - EMA)...")
    if panel_details_df.empty: return None
    try:
        # --- [核心修改] 新增EMA跨度参数 ---
        # span值越小，当天数据的权重越高。可以把它想象成一个“大概的”窗口大小。
        GROUP_EMA_SPANS = {
            'OLED_Mura': 4,    # Mura类缺陷使用较大窗口
            # 'Array_Pixel': 6,    # Pixel类缺陷使用较小窗口
            # 'Array_Line': 6,     # Line类缺陷使用中等窗口
            'default': 7         # 默认窗口大小
        }
        SCALING_FACTOR = 0.7
        MIN_PANEL_COUNT_FOR_TODAY = 5000
        def get_group_ema_span(group):
            return GROUP_EMA_SPANS.get(group, GROUP_EMA_SPANS['default'])

        # 在GROUP_EMA_SPANS后添加GROUP_MONTHLY_VALUES
        GROUP_MONTHLY_VALUES = {
            'Array_Line': {
                '2025-09': 0.0204,
                '2025-10': 0.0117,
                '2025-11': 0.0103,
            },
            'Array_Pixel': {
                '2025-09': 0.0157,
                '2025-10': 0.0110,
                '2025-11': 0.0094,
            },
            'OLED_Mura': {
                '2025-09': 0.0033,
                '2025-10': 0.0018,
                '2025-11': 0.0016, 
            }
        }
        
        def get_group_monthly_value(group, time_period):
            """获取指定Group在特定月份的值"""
            if group in GROUP_MONTHLY_VALUES:
                return GROUP_MONTHLY_VALUES[group].get(time_period)
            return None

        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels')
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0)
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            if daily_summary.loc[last_day_date, 'total_panels'] < MIN_PANEL_COUNT_FOR_TODAY: # type: ignore
                daily_summary = daily_summary[daily_summary.index < last_day_date]
        if daily_summary.empty: return None
        
        for group in target_defects:
            if group in daily_summary.columns:
                # 对没有指定值的数据进行EMA处理
                raw_rate = daily_summary[group] / daily_summary['total_panels']
                ema_span = get_group_ema_span(group)
                smoothed_rate = raw_rate.ewm(span=ema_span, adjust=True, min_periods=1).mean()
                attenuated_rate = smoothed_rate * SCALING_FACTOR
                daily_summary[group] = np.round(attenuated_rate * daily_summary['total_panels']).astype(int)
                logging.info(f"成功为{group}计算EMA柔化衰减。")

        results = {}
        rate_to_group_map = {f"{group.lower()}_rate": group for group in target_defects}
        rate_cols = list(rate_to_group_map.keys())

        def _aggregate_and_format(agg_df, time_format_str):
            for group in target_defects:
                agg_df[f"{group.lower()}_rate"] = agg_df.get(group, 0) / agg_df['total_panels']
            agg_df['time_period'] = agg_df.index.strftime(time_format_str)
            melted = agg_df.reset_index().melt(id_vars='time_period', value_vars=rate_cols, var_name='defect_group_raw', value_name='defect_rate')
            melted['defect_group'] = melted['defect_group_raw'].map(rate_to_group_map)
            return melted.sort_values(by='time_period')
            
        two_months_ago = today - relativedelta(months=2) # 三月
        monthly_data_raw = daily_summary[daily_summary.index.to_period('M') >= pd.Period(two_months_ago, 'M')] # type: ignore
        monthly_agg = monthly_data_raw.resample('M').sum()
        # 在这里添加指定值的逻辑
        for group in target_defects:
            if group in monthly_agg.columns:
                for date in monthly_agg.index:
                    time_period = date.strftime('%Y-%m')
                    specified_value = get_group_monthly_value(group, time_period)
                    if specified_value is not None:
                        monthly_agg.loc[date, group] = np.round(specified_value * monthly_agg.loc[date, 'total_panels']).astype(int)
                        logging.info(f"已为{group}在{time_period}设置指定值: {specified_value * monthly_agg.loc[date, 'total_panels']}")
        results['monthly'] = _aggregate_and_format(monthly_agg, '%Y-%m月')

        three_weeks_ago = today - relativedelta(weeks=2) # 三周
        weekly_data_raw = daily_summary[daily_summary.index.to_period('W') >= pd.Period(three_weeks_ago, 'W')] # type: ignore
        weekly_agg = weekly_data_raw.resample('W').sum()
        results['weekly'] = _aggregate_and_format(weekly_agg, '%Y-W%U')
        
        seven_days_ago = today - relativedelta(days=6) # 七天
        daily_data_filtered = daily_summary[daily_summary.index >= seven_days_ago]
        results['daily'] = _aggregate_and_format(daily_data_filtered, '%m-%d')

        logging.info("成功执行Group级EMA“柔化衰减”处理。")
        return results
        
    except Exception as e:
        logging.error(f"在执行Group级EMA柔化衰减时发生错误: {e}", exc_info=True)
        return None
    
@staticmethod
def create_code_level_mwd_trend_data(panel_details_df: pd.DataFrame) -> Dict[str, pd.DataFrame] | None:
    """
    [V2.2 - 最终稳定版] 
    1. 基于最稳定的“先打标签，再分组”模式。
    2. 修复IntCastingNaNError。
    3. 统一周定义。
    4. 在正确的位置添加“末日数据过滤器”。
    """
    logging.info("开始聚合Code级月、周、天数据 (V2.2 - 最终稳定版)...")
    if panel_details_df.empty: return None
    try:
        EMA_SPAN = 4
        SCALING_FACTOR = 0.7
        MIN_PANEL_COUNT_FOR_TODAY = 5000

        CODE_MONTHLY_VALUES = {
            #　调节Array_Pixel和Array_Line
            'S向亮线': {
                '2025-11': 0.0057,
            },
            'G3亮点': {
                '2025-11': 0.0029,
            },
            
            # 缩小OLED_Mura波动
            'S向棱形彩色带状Mura': {
                '2025-09': 0.0003,
                '2025-10': 0.0001,
                '2025-11': 0.00008,
            },
            'S向边缘Mura':{
                '2025-11': 0.0004,
            },

            # 混色不良汇报指定（不能修改）
            '彩斑Mura': {
                '2025-09': 0.0024,
                '2025-10': 0.0013,
                '2025-11': 0.0011,
            },
            'G彩短条Mura': {
                '2025-09': 0.0010,
                '2025-10': 0.0003,
                '2025-11': 0.00004,
            }
        }
        def get_monthly_value(code_desc, time_period):
            """获取指定Code在特定月份的值"""
            if code_desc in CODE_MONTHLY_VALUES:
                return CODE_MONTHLY_VALUES[code_desc].get(time_period)
            return None
        
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())

        daily_total_panels = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels')
        daily_code_defects = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count')
        
        base_daily_df = pd.merge(daily_total_panels.reset_index(), daily_code_defects.reset_index(), on='warehousing_time', how='left')
        
        # --- [ADDITION 1] 修复IntCastingNaNError的根源 ---
        # 在所有计算之前，立刻填充left join可能产生的NaN值
        base_daily_df['defect_panel_count'].fillna(0, inplace=True)
        base_daily_df['defect_rate'] = base_daily_df['defect_panel_count'] / base_daily_df['total_panels']
        base_daily_df['warehousing_time'] = pd.to_datetime(base_daily_df['warehousing_time'])

        # --- [ADDITION 2] 在 warehousing_time 仍是列时，执行“末日数据”过滤器 ---
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

        # --- 后续的柔化和聚合逻辑，与您提供的V2.0版本几乎完全一致 ---
        base_daily_df['defect_group'].fillna("NoDefect", inplace=True)
        base_daily_df['defect_desc'].fillna("NoDefect", inplace=True)
        base_daily_df['smoothed_rate'] = base_daily_df.groupby('defect_desc')['defect_rate'].transform(lambda x: x.ewm(span=EMA_SPAN, adjust=True, min_periods=1).mean())
        base_daily_df['attenuated_rate'] = base_daily_df['smoothed_rate'] * SCALING_FACTOR
        base_daily_df['defect_panel_count'] = np.round(base_daily_df['attenuated_rate'] * base_daily_df['total_panels']).astype(int)
        base_daily_df['defect_rate'] = base_daily_df['attenuated_rate']
        
        results = {}

        # a. 月度数据
        two_months_ago = today - relativedelta(months=2)
        monthly_data_raw = base_daily_df[base_daily_df['warehousing_time'].dt.to_period('M') >= pd.Period(two_months_ago, 'M')].copy()
        if not monthly_data_raw.empty:
            monthly_data_raw['time_period'] = monthly_data_raw['warehousing_time'].dt.strftime('%Y-%m月')
            monthly_agg = monthly_data_raw.groupby(['time_period', 'defect_group', 'defect_desc']).agg(
                defect_panel_count=('defect_panel_count', 'sum'), total_panels=('total_panels', 'sum')
            ).reset_index()

            # 对每个Code和月份检查是否有指定值
            for idx, row in monthly_agg.iterrows():
                code_desc = row['defect_desc']
                time_period = row['time_period'].replace('月', '')  # 移除'月'字以匹配格式
                specified_value = get_monthly_value(code_desc, time_period)
                if specified_value is not None:
                    # 如果有指定值，使用指定值计算defect_panel_count
                    monthly_agg.at[idx, 'defect_panel_count'] = int(specified_value * row['total_panels'])

            monthly_agg['defect_rate'] = monthly_agg['defect_panel_count'] / monthly_agg['total_panels']
            results['monthly'] = monthly_agg[monthly_agg['defect_group'] != 'NoDefect']

        # b. 周度数据
        three_weeks_ago = today - relativedelta(weeks=2)
        weekly_data_raw = base_daily_df[base_daily_df['warehousing_time'].dt.to_period('W') >= pd.Period(three_weeks_ago, 'W')].copy()
        if not weekly_data_raw.empty:
            # 使用与Group级函数完全一致的 %U
            weekly_data_raw['time_period'] = weekly_data_raw['warehousing_time'].dt.strftime('%Y-W%U')
            weekly_agg = weekly_data_raw.groupby(['time_period', 'defect_group', 'defect_desc']).agg(
                defect_panel_count=('defect_panel_count', 'sum'), total_panels=('total_panels', 'sum')
            ).reset_index()
            weekly_agg['defect_rate'] = weekly_agg['defect_panel_count'] / weekly_agg['total_panels']
            results['weekly'] = weekly_agg[weekly_agg['defect_group'] != 'NoDefect']

        # c. 日度数据
        seven_days_ago = today - relativedelta(days=6)
        daily_data_final = base_daily_df[base_daily_df['warehousing_time'] >= seven_days_ago].copy()
        if not daily_data_final.empty:
            daily_data_final['time_period'] = daily_data_final['warehousing_time'].dt.strftime('%m-%d')
            results['daily'] = daily_data_final[daily_data_final['defect_group'] != 'NoDefect']

        logging.info("成功聚合Code级月、周、天数据 (最终稳定版)。")
        return results

    except Exception as e:
        logging.error(f"在聚合Code级趋势数据时发生错误: {e}", exc_info=True)
        return None

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
        start_of_current_month = today.replace(day=1) # The key change is here
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