# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

from vivo_project.config import CONFIG
from vivo_project.core.trend_regulator import TrendRegulator

@staticmethod
def create_mwd_trend_data(
    panel_details_df: pd.DataFrame, 
    ema_span: int = 14,
    scaling_factor: float = 1.2
) -> Dict[str, pd.DataFrame] | None:
    """
    (V5.1 - 最终防线版)
    流程：Shadow EMA -> 原始聚合 -> 智能调节 -> [最后]手动覆盖
    """
    logging.info(f"开始为Group级执行'Shadow EMA'抗噪处理 (Span={ema_span}, Scale={scaling_factor})...")
    if panel_details_df.empty: return None
    
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 5000
        
        # 1. 数据预处理 & 日度汇总 (Shadow EMA)
        # ------------------------------------------------------------------
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = df['warehousing_time'].max()
        
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
        
        target_defects = sorted(panel_details_df['defect_group'].dropna().unique().tolist())
        # Shadow EMA 计算
        for group in target_defects:
            if group in daily_summary.columns:
                smoothed_rates = _calculate_adaptive_shadow_ema(
                    daily_counts=daily_summary[group].to_numpy(),
                    daily_totals=daily_summary['total_panels'].to_numpy(),
                    span=ema_span
                )
                attenuated_rates = np.array(smoothed_rates) * scaling_factor
                daily_summary[group] = np.round(attenuated_rates * daily_summary['total_panels']).astype(int)
                logging.info(f"成功为 Group '{group}' 计算 Shadow EMA")

        # 2. 生成 Wide Format 的月度/周度原始数据 (纯聚合，无修饰)
        # ------------------------------------------------------------------
        monthly_agg = _aggregate_group_monthly_raw(daily_summary, today)
        weekly_agg = _aggregate_group_weekly_raw(daily_summary, today)
        
        # 3. 智能调节 (Smart Regulation)
        # ------------------------------------------------------------------
        # 这一步可能会压平月度数据，并回溯修改周度数据
        monthly_regulated, weekly_regulated = TrendRegulator.regulate_monthly_and_weekly(
            monthly_agg, 
            weekly_agg
        )
        
        # 4. [最后防线] 手动覆盖 (Manual Override)
        # ------------------------------------------------------------------
        # 无论前面算法怎么算，只要 Config 里填了数，这里就会强制覆盖
        monthly_values = CONFIG['processing'].get('group_monthly_values', {})
        weekly_values = CONFIG['processing'].get('group_weekly_values', {})
        
        monthly_final = _apply_manual_overrides(monthly_regulated, monthly_values, target_defects, 'monthly')
        weekly_final = _apply_manual_overrides(weekly_regulated, weekly_values, target_defects, 'weekly')
        
        # 5. 格式化输出 (Format)
        # ------------------------------------------------------------------
        # 定义内部格式化函数 (复用逻辑)
        def _format_df(agg_df, time_format_str):
            # 重算 Rate (因为 Count 可能在 Step 3 或 4 被修改了)
            for group in target_defects:
                if group in agg_df.columns:
                    agg_df[f"{group.lower()}_rate"] = agg_df[group] / agg_df['total_panels']
                else:
                    agg_df[f"{group.lower()}_rate"] = 0.0
            
            # 生成时间标签
            if time_format_str == 'ISO':
                iso_df = agg_df.index.isocalendar()
                agg_df['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format)
            else:
                agg_df['time_period'] = agg_df.index.strftime(time_format_str)
            
            # Melt
            melted = agg_df.reset_index().melt(
                id_vars=['time_period', 'total_panels'],
                value_vars=rate_cols, 
                var_name='defect_group_raw', 
                value_name='defect_rate'
            )
            melted['defect_group'] = melted['defect_group_raw'].map(rate_to_group_map)
            return melted.sort_values(by='time_period')

        results = {}
        rate_to_group_map = {f"{group.lower()}_rate": group for group in target_defects}
        rate_cols = list(rate_to_group_map.keys())
        results['monthly'] = _format_df(monthly_final, '%Y-%m月')
        results['weekly'] = _format_df(weekly_final, 'ISO')
        results['daily_full'] = _format_df(daily_summary, '%Y-%m-%d')
        
        # 日度数据直接格式化 (日度通常不做手动覆盖，保持真实)
        seven_days_ago = today - relativedelta(days=6)
        daily_data_filtered = daily_summary[daily_summary.index >= seven_days_ago]
        results['daily'] = _format_df(daily_data_filtered, '%m-%d')

        return results
        
    except Exception as e:
        logging.error(f"在执行Group级趋势聚合流程时发生错误: {e}", exc_info=True)
        return None

# ==============================================================================
#  Level 1: 原始聚合函数 (只聚合，不修改)
# ==============================================================================
@staticmethod
def _aggregate_group_monthly_raw(daily_summary: pd.DataFrame, today: dt) -> pd.DataFrame:
    """仅负责时间过滤和重采样聚合"""
    two_months_ago = today - relativedelta(months=3)
    monthly_data_raw = daily_summary[daily_summary.index.to_period('M') >= pd.Period(two_months_ago, 'M')] # type: ignore
    return monthly_data_raw.resample('M').sum()

@staticmethod
def _aggregate_group_weekly_raw(daily_summary: pd.DataFrame, today: dt) -> pd.DataFrame:
    """仅负责时间过滤和重采样聚合"""
    three_weeks_ago = today - relativedelta(weeks=2)
    weekly_data_raw = daily_summary[daily_summary.index.to_period('W') >= pd.Period(three_weeks_ago, 'W')] # type: ignore
    return weekly_data_raw.resample('W').sum()

# ==============================================================================
#  Level 3: 手动覆盖函数 (最后防线)
# ==============================================================================
@staticmethod
def _apply_manual_overrides(
    df: pd.DataFrame, 
    override_values: dict, 
    target_defects: list, 
    period_type: str
) -> pd.DataFrame:
    """
    应用 Config 中的手动数值。此操作优先级最高，会覆盖之前所有的计算结果。
    """
    if not override_values or df.empty:
        return df
        
    df_mod = df.copy()
    
    for group in target_defects:
        if group not in df_mod.columns: continue
        if group not in override_values: continue
        
        # 遍历时间索引，查找配置
        for date_idx in df_mod.index:
            # 根据类型生成 Key (YYYY-MM 或 YYYY-Wxx)
            if period_type == 'monthly':
                time_key = date_idx.strftime('%Y-%m')
            else: # weekly
                iso_year, iso_week, _ = date_idx.isocalendar()
                time_key = f"{iso_year}-W{iso_week:02d}"
            
            # 检查是否有配置
            specified_val = override_values[group].get(time_key)
            
            if specified_val is not None:
                # 覆盖逻辑：Count = Rate * Total
                total = df_mod.loc[date_idx, 'total_panels']
                new_count = int(np.round(specified_val * total))
                df_mod.loc[date_idx, group] = new_count
                # [修改后] 极简日志
                logging.warning(f"[手动覆盖-Group] {group} ({time_key}): {specified_val:.2%}")
                
    return df_mod

@staticmethod
def create_code_level_mwd_trend_data(
    panel_details_df: pd.DataFrame,
    ema_span: int,          
    scaling_factor: float
) -> Dict[str, pd.DataFrame] | None:
    """
    (V5.1 - 最终防线版)
    Code 级趋势分析：Shadow EMA -> 原始聚合 -> 智能调节 -> 手动覆盖
    """
    logging.info(f"开始聚合Code级数据 (Shadow EMA, Span={ema_span})...")
    if panel_details_df.empty: return None
    
    try:
        MIN_PANEL_COUNT_FOR_TODAY = 500
        
        # 1. Shadow EMA 计算 (生成 base_daily_df)
        # --------------------------------------------------------------
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        today = pd.to_datetime(dt.now().date())

        # 构建基础日度汇总 (Long Format)
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

        base_daily_df['defect_group'].fillna("NoDefect", inplace=True)
        base_daily_df['defect_desc'].fillna("NoDefect", inplace=True)

        # Shadow EMA 迭代
        base_daily_df['attenuated_rate'] = 0.0 
        unique_codes = base_daily_df['defect_desc'].unique()
        for code in unique_codes:
            if code == "NoDefect": continue
            mask = base_daily_df['defect_desc'] == code
            code_subset = base_daily_df[mask].sort_values('warehousing_time')
            counts = code_subset['defect_panel_count'].values
            totals = code_subset['total_panels'].values
            smoothed = _calculate_adaptive_shadow_ema(counts, totals, ema_span)
            attenuated = np.array(smoothed) * scaling_factor
            base_daily_df.loc[code_subset.index, 'attenuated_rate'] = attenuated
        
        base_daily_df['defect_panel_count'] = np.round(base_daily_df['attenuated_rate'] * base_daily_df['total_panels']).astype(int)

        # 2. 原始聚合 (Raw Aggregation)
        # --------------------------------------------------------------
        # 生成带时间戳的 Long Format 中间态
        monthly_agg = _aggregate_code_monthly_raw(base_daily_df, today)
        weekly_agg = _aggregate_code_weekly_raw(base_daily_df, today)


        # 3. 智能调节 (Smart Regulation)
        # --------------------------------------------------------------
        monthly_regulated, weekly_regulated = TrendRegulator.regulate_code_monthly_and_weekly(
            monthly_agg,
            weekly_agg
        )

        # 4. 手动覆盖 (Manual Override)
        # --------------------------------------------------------------
        code_monthly_values = CONFIG['processing'].get('code_monthly_values', {})
        code_weekly_values = CONFIG['processing'].get('code_weekly_values', {})

        monthly_final = _apply_code_manual_overrides(monthly_regulated, code_monthly_values, 'monthly')
        weekly_final = _apply_code_manual_overrides(weekly_regulated, code_weekly_values, 'weekly')

        # 5. 格式化输出
        # --------------------------------------------------------------
        results = {}
        results['monthly'] = _format_code_df(monthly_final, '%Y-%m月')
        results['weekly'] = _format_code_df(weekly_final, 'ISO')
        results['daily_full'] = _format_code_df(base_daily_df, '%Y-%m-%d')

        # 日度格式化 (过滤最近7天)
        seven_days_ago = today - relativedelta(days=6)
        daily_data_ui = base_daily_df[base_daily_df['warehousing_time'] >= seven_days_ago].copy()
        if not daily_data_ui.empty:
            # 剔除 NoDefect 用于展示
            daily_data_ui = daily_data_ui[daily_data_ui['defect_group'] != 'NoDefect']
            daily_data_ui['time_period'] = daily_data_ui['warehousing_time'].dt.strftime('%m-%d') # type: ignore
            # 补全 defect_rate 列
            daily_data_ui['defect_rate'] = daily_data_ui['defect_panel_count'] / daily_data_ui['total_panels']
            results['daily'] = daily_data_ui

        logging.info("成功聚合Code级趋势数据 (Shadow EMA + Smart Regulation)。")
        return results

    except Exception as e:
        logging.error(f"在聚合Code级趋势数据时发生错误: {e}", exc_info=True)
        return None

# --- Code 级 Helper 函数 ---
@staticmethod
def _aggregate_code_monthly_raw(base_daily_df: pd.DataFrame, today: dt) -> pd.DataFrame:
    """Code 级月度原始聚合 (保留 warehousing_time 为 Timestamp 用于调节)"""
    two_months_ago = today - relativedelta(months=3)
    # 筛选时间
    mask = base_daily_df['warehousing_time'].dt.to_period('M') >= pd.Period(two_months_ago, 'M') # type: ignore
    raw = base_daily_df[mask].copy()
    if raw.empty: return pd.DataFrame()
    
    # 按月 + Code 聚合
    # 为了保留时间戳以便 regulate，我们将时间归一化为该月第一天或最后一天
    # 这里使用 Grouper 按月聚合
    agg = raw.groupby([pd.Grouper(key='warehousing_time', freq='M'), 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'),
        total_panels=('total_panels', 'sum')
    ).reset_index()
    return agg

@staticmethod
def _aggregate_code_weekly_raw(base_daily_df: pd.DataFrame, today: dt) -> pd.DataFrame:
    """Code 级周度原始聚合"""
    three_weeks_ago = today - relativedelta(weeks=2)
    mask = base_daily_df['warehousing_time'].dt.to_period('W') >= pd.Period(three_weeks_ago, 'W') # type: ignore
    raw = base_daily_df[mask].copy()
    if raw.empty: return pd.DataFrame()
    
    # 按周 + Code 聚合
    agg = raw.groupby([pd.Grouper(key='warehousing_time', freq='W'), 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'),
        total_panels=('total_panels', 'sum')
    ).reset_index()
    return agg

@staticmethod
def _apply_code_manual_overrides(df: pd.DataFrame, override_values: dict, period_type: str) -> pd.DataFrame:
    """Code 级手动覆盖 (Long Format)"""
    if not override_values or df.empty: return df
    df_mod = df.copy()
    
    # 遍历每一行进行检查 (由于 Long Format 行数多，也可以优化为遍历 Config，但遍历行逻辑更简单)
    # 优化策略：只遍历 Config 中存在的 Code
    for code, time_map in override_values.items():
        mask_code = df_mod['defect_desc'] == code
        if not mask_code.any(): continue
        
        # 对该 Code 的数据应用时间映射
        for idx in df_mod[mask_code].index:
            date_val = df_mod.loc[idx, 'warehousing_time']
            
            # 生成 Time Key
            if period_type == 'monthly':
                time_key = date_val.strftime('%Y-%m') # type: ignore
            else:
                iso_year, iso_week, _ = date_val.isocalendar() # type: ignore
                time_key = f"{iso_year}-W{iso_week:02d}"
            
            if time_key in time_map:
                spec_val = time_map[time_key]
                total = df_mod.loc[idx, 'total_panels']
                df_mod.loc[idx, 'defect_panel_count'] = int(np.round(spec_val * total))
                logging.warning(f"[手动覆盖-Code] {code} ({time_key}): {spec_val:.2%}")

    return df_mod

@staticmethod
def _format_code_df(df: pd.DataFrame, time_format_str: str) -> pd.DataFrame:
    """Code 级格式化输出"""
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    
    # 生成展示用的 time_period 字符串
    if time_format_str == 'ISO':
        iso_df = df_out['warehousing_time'].dt.isocalendar() # type: ignore
        df_out['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format)
    else:
        df_out['time_period'] = df_out['warehousing_time'].dt.strftime(time_format_str) # type: ignore
        
    # 计算 Rate
    df_out['defect_rate'] = df_out['defect_panel_count'] / df_out['total_panels']
    
    # 剔除 NoDefect
    return df_out[df_out['defect_desc'] != 'NoDefect']


@staticmethod
def create_current_month_trend_data(panel_details_df: pd.DataFrame) -> pd.DataFrame | None:
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
        target_defects = sorted(panel_details_df['defect_group'].dropna().unique().tolist())
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