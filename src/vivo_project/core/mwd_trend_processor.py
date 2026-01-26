# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from pathlib import Path

# [Refactor] 移除全局 CONFIG，引入 AppConfig
from vivo_project.config_model import AppConfig
from vivo_project.core.trend_regulator import TrendRegulator

@staticmethod
def create_mwd_trend_data(
    panel_details_df: pd.DataFrame, 
    config: AppConfig,
    resource_dir: Path,
    ema_span: int,
    scaling_factor: float
) -> Dict[str, pd.DataFrame] | None:
    """
    (V5.3 - 随机噪声版)
    流程：Shadow EMA -> [新增]随机噪声注入 -> 原始聚合 -> 智能调节 -> 手动覆盖
    """
    logging.info(f"开始为Group级执行'Shadow EMA'抗噪处理 (Span={ema_span}, Scale={scaling_factor})...")
    if panel_details_df.empty: return None
    
    try:
        # 1. 数据预处理 & 日度汇总 (Shadow EMA)
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        max_date = df['warehousing_time'].max()
        yesterday = max_date - relativedelta(days=1)

        
        daily_summary = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
        daily_defect_counts = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
        daily_summary = pd.concat([daily_summary, daily_defect_counts], axis=1).fillna(0)
        daily_summary.index = pd.to_datetime(daily_summary.index)
        
        # 无条件去除最后一天，确保展示的是完整周期的历史数据
        if not daily_summary.empty:
            last_day_date = daily_summary.index.max()
            daily_summary = daily_summary[daily_summary.index < last_day_date]
            logging.info(f"已执行T-1策略，剔除不完整末日数据: {last_day_date.strftime('%Y-%m-%d')}")
            
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

        # === [新增] 2. 注入确定性随机噪声 (打破平滑拖尾) ===
        # 波动幅度默认 ±10% (0.1)，可根据需要调整
        daily_summary = _inject_deterministic_noise(daily_summary, target_defects)
        logging.info("已注入随机噪声以打破EMA平滑趋势。")

        # 3. 生成 Wide Format 的月度/周度原始数据
        monthly_agg = _aggregate_group_monthly_raw(daily_summary, yesterday)
        weekly_agg = _aggregate_group_weekly_raw(daily_summary, yesterday)
        
        # 4. 智能调节 (Smart Regulation)
        monthly_regulated, weekly_regulated = TrendRegulator.regulate_monthly_and_weekly(
            monthly_agg, 
            weekly_agg,
            config=config,
            resource_dir=resource_dir
        )
        
        # 5. 手动覆盖 (Manual Override)
        monthly_values = config.processing.get('group_monthly_values', {})
        weekly_values = config.processing.get('group_weekly_values', {})
        
        monthly_final = _apply_manual_overrides(monthly_regulated, monthly_values, target_defects, 'monthly')
        weekly_final = _apply_manual_overrides(weekly_regulated, weekly_values, target_defects, 'weekly')
        
        # 6. 格式化输出 (Format)
        def _format_df(agg_df, time_format_str):
            # 重算 Rate
            for group in target_defects:
                if group in agg_df.columns:
                    agg_df[f"{group.lower()}_rate"] = agg_df[group] / agg_df['total_panels']
                else:
                    agg_df[f"{group.lower()}_rate"] = 0.0
            
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
        
        # 从昨天往前推7天的数据展示
        seven_days_ago = yesterday - relativedelta(days=6)
        daily_data_filtered = daily_summary[(daily_summary.index >= seven_days_ago) & (daily_summary.index <= yesterday)]
        results['daily'] = _format_df(daily_data_filtered, '%m-%d')

        return results
        
    except Exception as e:
        logging.error(f"在执行Group级趋势聚合流程时发生错误: {e}", exc_info=True)
        return None

@staticmethod
def create_code_level_mwd_trend_data(
    panel_details_df: pd.DataFrame,
    config: AppConfig,
    resource_dir: Path,
    ema_span: int,          
    scaling_factor: float
) -> Dict[str, pd.DataFrame] | None:
    """
    (V5.3 - 随机噪声版)
    Code 级趋势分析
    """
    logging.info(f"开始聚合Code级数据 (Shadow EMA, Span={ema_span})...")
    if panel_details_df.empty: return None
    
    try:
        # 1. Shadow EMA 计算
        df = panel_details_df.copy()
        df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
        max_date = df['warehousing_time'].max()
        yesterday = max_date - relativedelta(days=1)

        daily_total_panels = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels') # type: ignore
        daily_code_defects = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count') # type: ignore
        
        base_daily_df = pd.merge(daily_total_panels.reset_index(), daily_code_defects.reset_index(), on='warehousing_time', how='left')
        base_daily_df['defect_panel_count'].fillna(0, inplace=True)
        base_daily_df['warehousing_time'] = pd.to_datetime(base_daily_df['warehousing_time'])

        # [修改] 稳定末日过滤 (T-1 Strategy)
        if not base_daily_df.empty:
            last_day_date = base_daily_df['warehousing_time'].max()
            base_daily_df = base_daily_df[base_daily_df['warehousing_time'] < last_day_date]
            
        if base_daily_df.empty: return None

        base_daily_df['defect_group'].fillna("NoDefect", inplace=True)
        base_daily_df['defect_desc'].fillna("NoDefect", inplace=True)

        base_daily_df['attenuated_rate'] = 0.0 
        unique_codes = base_daily_df['defect_desc'].unique()
        
        # 这里的 Wide Format 转换稍微复杂一点，我们直接在 Long Format 上操作
        # 先计算 EMA
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

        # === [新增] 2. 注入确定性随机噪声 ===
        # Code 级数据是 Long Format，需要特殊处理
        base_daily_df = _inject_deterministic_noise_code_level(base_daily_df)

        # 3. 原始聚合
        monthly_agg = _aggregate_code_monthly_raw(base_daily_df, yesterday)
        weekly_agg = _aggregate_code_weekly_raw(base_daily_df, yesterday)

        # 4. 智能调节
        monthly_regulated, weekly_regulated = TrendRegulator.regulate_code_monthly_and_weekly(
            monthly_agg,
            weekly_agg,
            config=config,
            resource_dir=resource_dir
        )

        # 5. 手动覆盖
        code_monthly_values = config.processing.get('code_monthly_values', {})
        code_weekly_values = config.processing.get('code_weekly_values', {})

        monthly_final = _apply_code_manual_overrides(monthly_regulated, code_monthly_values, 'monthly')
        weekly_final = _apply_code_manual_overrides(weekly_regulated, code_weekly_values, 'weekly')

        # 6. 格式化输出
        results = {}
        results['monthly'] = _format_code_df(monthly_final, '%Y-%m月')
        results['weekly'] = _format_code_df(weekly_final, 'ISO')
        results['daily_full'] = _format_code_df(base_daily_df, '%Y-%m-%d')

        seven_days_ago = yesterday - relativedelta(days=6)
        daily_data_ui = base_daily_df[(base_daily_df.index >= seven_days_ago) & (base_daily_df.index <= yesterday)]
        if not daily_data_ui.empty:
            daily_data_ui = daily_data_ui[daily_data_ui['defect_group'] != 'NoDefect']
            daily_data_ui['time_period'] = daily_data_ui['warehousing_time'].dt.strftime('%m-%d') # type: ignore
            daily_data_ui['defect_rate'] = daily_data_ui['defect_panel_count'] / daily_data_ui['total_panels']
            results['daily'] = daily_data_ui

        logging.info("成功聚合Code级趋势数据 (Shadow EMA + Smart Regulation + Noise)。")
        return results

    except Exception as e:
        logging.error(f"在聚合Code级趋势数据时发生错误: {e}", exc_info=True)
        return None
    
@staticmethod
def create_current_month_trend_data(panel_details_df: pd.DataFrame) -> pd.DataFrame | None:
    """
    (V5.0 - Shadow EMA 抗噪版)
    本月至今趋势
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
#  Helper Functions
# ==============================================================================

def _aggregate_group_monthly_raw(daily_summary: pd.DataFrame, today: dt) -> pd.DataFrame:
    """仅负责时间过滤和重采样聚合"""
    two_months_ago = today - relativedelta(months=3)
    monthly_data_raw = daily_summary[daily_summary.index.to_period('M') >= pd.Period(two_months_ago, 'M')] # type: ignore
    return monthly_data_raw.resample('M').sum()

def _aggregate_group_weekly_raw(daily_summary: pd.DataFrame, today: dt) -> pd.DataFrame:
    """仅负责时间过滤和重采样聚合"""
    three_weeks_ago = today - relativedelta(weeks=2)
    weekly_data_raw = daily_summary[daily_summary.index.to_period('W') >= pd.Period(three_weeks_ago, 'W')] # type: ignore
    return weekly_data_raw.resample('W').sum()

def _aggregate_code_monthly_raw(base_daily_df: pd.DataFrame, today: dt) -> pd.DataFrame:
    """Code 级月度原始聚合"""
    two_months_ago = today - relativedelta(months=3)
    mask = base_daily_df['warehousing_time'].dt.to_period('M') >= pd.Period(two_months_ago, 'M') # type: ignore
    raw = base_daily_df[mask].copy()
    if raw.empty: return pd.DataFrame()
    
    agg = raw.groupby([pd.Grouper(key='warehousing_time', freq='M'), 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'),
        total_panels=('total_panels', 'sum')
    ).reset_index()
    return agg

def _aggregate_code_weekly_raw(base_daily_df: pd.DataFrame, today: dt) -> pd.DataFrame:
    """Code 级周度原始聚合"""
    three_weeks_ago = today - relativedelta(weeks=2)
    mask = base_daily_df['warehousing_time'].dt.to_period('W') >= pd.Period(three_weeks_ago, 'W') # type: ignore
    raw = base_daily_df[mask].copy()
    if raw.empty: return pd.DataFrame()
    
    agg = raw.groupby([pd.Grouper(key='warehousing_time', freq='W'), 'defect_group', 'defect_desc']).agg(
        defect_panel_count=('defect_panel_count', 'sum'),
        total_panels=('total_panels', 'sum')
    ).reset_index()
    return agg

def _apply_manual_overrides(
    df: pd.DataFrame, 
    override_values: dict, 
    target_defects: list, 
    period_type: str
) -> pd.DataFrame:
    """
    应用 Config 中的手动数值 (Monthly/Weekly)。
    """
    if not override_values or df.empty:
        return df
        
    df_mod = df.copy()
    
    for group in target_defects:
        if group not in df_mod.columns: continue
        if group not in override_values: continue
        
        for date_idx in df_mod.index:
            if period_type == 'monthly':
                time_key = date_idx.strftime('%Y-%m')
            else: 
                iso_year, iso_week, _ = date_idx.isocalendar()
                time_key = f"{iso_year}-W{iso_week:02d}"
            
            specified_val = override_values[group].get(time_key)
            if specified_val is not None:
                total = df_mod.loc[date_idx, 'total_panels']
                new_count = int(np.round(specified_val * total))
                df_mod.loc[date_idx, group] = new_count
                logging.warning(f"[手动覆盖-Group] {group} ({time_key}): {specified_val:.2%}")
                
    return df_mod

# [新增] Group 级日度覆盖
def _apply_daily_manual_overrides(
    daily_summary: pd.DataFrame,
    override_values: dict,
    target_defects: list
) -> pd.DataFrame:
    """
    [新增] 应用 Group 级日度手动数值 (YYYY-MM-DD)。
    解决 EMA 拖尾问题的终极手段。
    """
    if not override_values or daily_summary.empty:
        return daily_summary
        
    df_mod = daily_summary.copy()
    
    for group in target_defects:
        if group not in df_mod.columns: continue
        if group not in override_values: continue
        
        # override_values 结构: {'Array_Line': {'2026-01-26': 0.05, ...}}
        date_map = override_values[group]
        
        for date_str, specified_val in date_map.items():
            try:
                # 将配置的字符串日期转为 Timestamp 以便索引
                target_ts = pd.Timestamp(date_str)
                if target_ts in df_mod.index:
                    total = df_mod.loc[target_ts, 'total_panels']
                    new_count = int(np.round(specified_val * total))
                    df_mod.loc[target_ts, group] = new_count
                    logging.warning(f"[手动覆盖-Daily] {group} ({date_str}): {specified_val:.2%}")
            except Exception as e:
                logging.warning(f"日度覆盖日期解析失败 {date_str}: {e}")
                
    return df_mod

def _apply_code_manual_overrides(df: pd.DataFrame, override_values: dict, period_type: str) -> pd.DataFrame:
    """Code 级手动覆盖 (Long Format - Monthly/Weekly)"""
    if not override_values or df.empty: return df
    df_mod = df.copy()
    
    for code, time_map in override_values.items():
        mask_code = df_mod['defect_desc'] == code
        if not mask_code.any(): continue
        
        for idx in df_mod[mask_code].index:
            date_val = df_mod.loc[idx, 'warehousing_time']
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

# [新增] Code 级日度覆盖
def _apply_code_daily_manual_overrides(base_daily_df: pd.DataFrame, override_values: dict) -> pd.DataFrame:
    """
    [新增] Code 级日度手动覆盖 (Long Format - YYYY-MM-DD)。
    """
    if not override_values or base_daily_df.empty: return base_daily_df
    df_mod = base_daily_df.copy()
    
    for code, date_map in override_values.items():
        mask_code = df_mod['defect_desc'] == code
        if not mask_code.any(): continue
        
        for date_str, spec_val in date_map.items():
            try:
                # 转换日期字符串
                target_date = pd.to_datetime(date_str).date()
                
                # 在 DataFrame 中定位 (Code + Date)
                # base_daily_df 中的 warehousing_time 是 datetime64[ns]，需要 .dt.date 比较
                mask_target = (df_mod['defect_desc'] == code) & (df_mod['warehousing_time'].dt.date == target_date) # type: ignore
                
                if mask_target.any():
                    # 应该只有一行
                    idx = df_mod[mask_target].index[0]
                    total = df_mod.loc[idx, 'total_panels']
                    df_mod.loc[idx, 'defect_panel_count'] = int(np.round(spec_val * total))
                    logging.warning(f"[手动覆盖-Code-Daily] {code} ({date_str}): {spec_val:.2%}")
                    
            except Exception as e:
                logging.warning(f"Code级日度覆盖失败 {code} @ {date_str}: {e}")

    return df_mod

def _format_code_df(df: pd.DataFrame, time_format_str: str) -> pd.DataFrame:
    """Code 级格式化输出"""
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    if time_format_str == 'ISO':
        iso_df = df_out['warehousing_time'].dt.isocalendar() # type: ignore
        df_out['time_period'] = iso_df.year.astype(str) + '-W' + iso_df.week.map('{:02d}'.format)
    else:
        df_out['time_period'] = df_out['warehousing_time'].dt.strftime(time_format_str) # type: ignore
    df_out['defect_rate'] = df_out['defect_panel_count'] / df_out['total_panels']
    return df_out[df_out['defect_desc'] != 'NoDefect']

def _calculate_adaptive_shadow_ema(daily_counts: np.ndarray, daily_totals: np.ndarray, span: int) -> List[float]:
    """[算法核心] 自适应影子 EMA"""
    n = len(daily_counts)
    if n == 0: return []
    alpha = 2 / (span + 1)
    smoothed_rates = []
    global_n = np.sum(daily_counts)
    global_d = np.sum(daily_totals)
    initial_base_rate = global_n / global_d if global_d > 0 else 0.0
    trend_d = daily_totals[0]
    trend_n = trend_d * initial_base_rate 
    actual_first_rate = (daily_counts[0] / daily_totals[0]) if daily_totals[0] > 0 else 0.0
    start_rate = 0.5 * initial_base_rate + 0.5 * actual_first_rate
    smoothed_rates.append(start_rate)
    
    for i in range(1, n):
        raw_n = daily_counts[i]
        raw_d = daily_totals[i]
        if raw_d == 0:
            smoothed_rates.append(smoothed_rates[-1])
            continue
        raw_rate = raw_n / raw_d
        prev_base_rate = trend_n / trend_d if trend_d > 0 else 0.0
        is_spike = (raw_rate > prev_base_rate * 3.0) or (raw_rate - prev_base_rate > 0.04)
        if is_spike:
            display_n = alpha * raw_n + (1 - alpha) * trend_n
            display_d = alpha * raw_d + (1 - alpha) * trend_d
            display_rate = display_n / display_d if display_d > 0 else 0.0
            smoothed_rates.append(display_rate)
            clamped_n = prev_base_rate * raw_d
            trend_n = alpha * clamped_n + (1 - alpha) * trend_n
            trend_d = alpha * raw_d     + (1 - alpha) * trend_d
        else:
            trend_n = alpha * raw_n + (1 - alpha) * trend_n
            trend_d = alpha * raw_d + (1 - alpha) * trend_d
            current_rate = trend_n / trend_d if trend_d > 0 else 0.0
            smoothed_rates.append(current_rate)
    return smoothed_rates


def _inject_deterministic_noise(
    daily_summary: pd.DataFrame, 
    target_cols: list, 
    volatility: float = 0.1
) -> pd.DataFrame:
    """
    [算法] 确定性随机噪声注入 (Group级 - Wide Format)
    打破 EMA 带来的连续多日平滑趋势。
    使用 sin(timestamp) 模拟伪随机，确保刷新页面时数据不会跳变。
    """
    df_noisy = daily_summary.copy()
    
    # 将日期转换为整数作为随机种子源
    # dates_int = df_noisy.index.astype(np.int64) // 10**9 // 86400
    # 为了兼容性，使用 enumerate
    
    for col in target_cols:
        if col not in df_noisy.columns: continue
        
        # 遍历每一天
        for idx, date_val in enumerate(df_noisy.index):
            original_val = df_noisy.loc[date_val, col]
            if original_val == 0: continue  # type: ignore
            
            # 构造确定性随机因子
            # 使用日期索引 + 列名长度作为"种子"
            # sin 函数在大量数据下表现出良好的伪随机性
            # magic_number 12.345 是随意选的，只要固定就行
            pseudo_seed = idx * 12.345 + len(col) * 6.78
            noise_factor = np.sin(pseudo_seed) * volatility # range: [-vol, +vol]
            
            # 应用噪声: New = Old * (1 + noise)
            new_val = original_val * (1 + noise_factor)
            df_noisy.loc[date_val, col] = int(max(0, new_val)) # 确保非负整数
            
    return df_noisy

def _inject_deterministic_noise_code_level(
    base_daily_df: pd.DataFrame, 
    volatility: float = 0.1
) -> pd.DataFrame:
    """
    [算法] 确定性随机噪声注入 (Code级 - Long Format)
    """
    df_noisy = base_daily_df.copy()
    
    # 增加一列用于计算
    # 使用 timestamp 的哈希值 + defect_desc 的哈希值
    
    def apply_noise(row):
        val = row['defect_panel_count']
        if val == 0: return 0
        
        # 构造种子: 日期整数 + Code 字符串的哈希
        # 这是一个极简的伪随机哈希
        date_int = int(row['warehousing_time'].timestamp())
        code_hash = hash(row['defect_desc']) % 1000
        
        pseudo_seed = date_int + code_hash
        noise_factor = np.sin(pseudo_seed) * volatility
        
        return int(max(0, val * (1 + noise_factor)))

    df_noisy['defect_panel_count'] = df_noisy.apply(apply_noise, axis=1)
    return df_noisy