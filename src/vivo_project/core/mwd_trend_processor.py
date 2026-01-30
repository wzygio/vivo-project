# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List, Callable, Tuple
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from pathlib import Path

from vivo_project.config_model import AppConfig
from vivo_project.core.trend_regulator import TrendRegulator

class MWDTrendProcessor:

    # ==========================================================================
    #  主入口 1: Group 级趋势分析
    # ==========================================================================
    @staticmethod
    def create_mwd_trend_data(
        panel_details_df: pd.DataFrame, 
        config: AppConfig,
        resource_dir: Path,
        ema_span: int,
        scaling_factor: float,
        USE_TOP_DOWN_STRATEGY
    ) -> Dict[str, pd.DataFrame] | None:
        
        logging.info(f"Group级趋势分析 (模式: {'Top-Down' if USE_TOP_DOWN_STRATEGY else 'EMA+Noise'})...")
        if panel_details_df.empty: return None
        
        try:
            # 1. 准备 Raw Data
            raw_daily, today, target_defects = _prepare_group_raw_data(panel_details_df)
            if raw_daily is None: return None

            # 定义 Group 级的月/周聚合 lambda
            agg_m = lambda d, t: _safe_trend_aggregator(d, t, 'M', is_group_level=True)
            agg_w = lambda d, t: _safe_trend_aggregator(d, t, 'W', is_group_level=True)

            # 2. 准备配置参数
            m_vals = config.processing.get('group_monthly_values', {})
            w_vals = config.processing.get('group_weekly_values', {})
            d_vals = config.processing.get('group_daily_values', {})

            # 3. 执行策略流水线
            if USE_TOP_DOWN_STRATEGY:
                monthly, weekly, daily = _execute_top_down_pipeline(
                    raw_daily_df=raw_daily,
                    today=today,
                    agg_funcs=(agg_m, agg_w),
                    reg_func=TrendRegulator.regulate_monthly_and_weekly,
                    override_funcs=(_apply_manual_overrides, _apply_manual_overrides),
                    override_vals=(m_vals, w_vals),
                    gen_func=_generate_daily_from_weekly_baseline,
                    config=config,
                    resource_dir=resource_dir,
                    # Kwargs for generator/overrides
                    target_defects=target_defects,
                    scaling_factor=scaling_factor,
                    volatility=0.1
                )
            else:
                monthly, weekly, daily = _execute_ema_pipeline(
                    raw_daily_df=raw_daily,
                    today=today,
                    calc_daily_func=lambda df: _calc_group_ema_noise(df, target_defects, ema_span, scaling_factor),
                    agg_funcs=(_aggregate_group_monthly_raw, _aggregate_group_weekly_raw),
                    reg_func=TrendRegulator.regulate_monthly_and_weekly,
                    override_funcs=(_apply_manual_overrides, _apply_manual_overrides),
                    override_vals=(m_vals, w_vals),
                    config=config,
                    resource_dir=resource_dir,
                    target_defects=target_defects # for override func
                )

            # 4. 通用后处理 (日度覆盖 & 格式化)
            daily = _apply_daily_manual_overrides(daily, d_vals, target_defects)
            
            return _format_group_results(monthly, weekly, daily, target_defects, today)
            
        except Exception as e:
            logging.error(f"Group趋势分析出错: {e}", exc_info=True)
            return None

    # ==========================================================================
    #  主入口 2: Code 级趋势分析
    # ==========================================================================
    @staticmethod
    def create_code_level_mwd_trend_data(
        panel_details_df: pd.DataFrame,
        config: AppConfig,
        resource_dir: Path,
        ema_span: int,          
        scaling_factor: float,
        USE_TOP_DOWN_STRATEGY
    ) -> Dict[str, pd.DataFrame] | None:    
        
        logging.info(f"Code级趋势分析 (模式: {'Top-Down' if USE_TOP_DOWN_STRATEGY else 'EMA+Noise'})...")
        if panel_details_df.empty: return None
        
        try:
            # 1. 准备 Raw Data
            raw_daily, today = _prepare_code_raw_data(panel_details_df)
            if raw_daily is None: return None

            # [修改后] 统一使用具备“全局分母对齐”能力的通用聚合器
            agg_monthly_func = lambda d, t: _safe_trend_aggregator(d, t, 'M', is_group_level=False)
            agg_weekly_func  = lambda d, t: _safe_trend_aggregator(d, t, 'W', is_group_level=False)

            # ======================================================================

            # 2. 准备配置参数
            m_vals = config.processing.get('code_monthly_values', {})
            w_vals = config.processing.get('code_weekly_values', {})
            d_vals = config.processing.get('code_daily_values', {})

            # 3. 执行策略流水线
            if USE_TOP_DOWN_STRATEGY:
                monthly, weekly, daily = _execute_top_down_pipeline(
                    raw_daily_df=raw_daily,
                    today=today,
                    agg_funcs=(agg_monthly_func, agg_weekly_func), # <--- 使用修复后的聚合函数
                    reg_func=TrendRegulator.regulate_code_monthly_and_weekly,
                    override_funcs=(_apply_code_manual_overrides, _apply_code_manual_overrides),
                    override_vals=(m_vals, w_vals),
                    gen_func=_generate_code_daily_from_weekly_baseline,
                    config=config,
                    resource_dir=resource_dir,
                    scaling_factor=scaling_factor,
                    volatility=0.1
                )
            else:
                monthly, weekly, daily = _execute_ema_pipeline(
                    raw_daily_df=raw_daily,
                    today=today,
                    calc_daily_func=lambda df: _calc_code_ema_noise(df, ema_span, scaling_factor),
                    agg_funcs=(agg_monthly_func, agg_weekly_func), # <--- 使用修复后的聚合函数
                    reg_func=TrendRegulator.regulate_code_monthly_and_weekly,
                    override_funcs=(_apply_code_manual_overrides, _apply_code_manual_overrides),
                    override_vals=(m_vals, w_vals),
                    config=config,
                    resource_dir=resource_dir
                )

            # 4. 通用后处理
            daily = _apply_code_daily_manual_overrides(daily, d_vals)

            return _format_code_results(monthly, weekly, daily, today)

        except Exception as e:
            logging.error(f"Code趋势分析出错: {e}", exc_info=True)
            return None

# ==============================================================================
#  核心策略流水线 (Generic Pipelines)
# ==============================================================================
def _safe_trend_aggregator(df: pd.DataFrame, anchor_date: dt, freq: str, is_group_level: bool = False):
    """
    [通用升级版] 安全趋势聚合器
    支持 Group 级 (Wide) 和 Code 级 (Long)，彻底解决分母萎缩与索引冲突问题。
    """
    if df.empty: return pd.DataFrame()
    
    # 1. 统一转换：确保时间维度是“列”且名为 warehousing_time
    working_df = df.copy()
    if 'warehousing_time' not in working_df.columns:
        # 如果在 Index 里，则转出来
        working_df = working_df.reset_index()
        # 兼容处理：Pandas reset_index 默认可能叫 'index' 或 'level_0'
        if 'index' in working_df.columns:
            working_df = working_df.rename(columns={'index': 'warehousing_time'})
        elif 'level_0' in working_df.columns:
            working_df = working_df.rename(columns={'level_0': 'warehousing_time'})

    # 2. 执行时间窗口过滤 (3个月或2周)
    if freq == 'M':
        start = anchor_date - relativedelta(months=3)
        mask = working_df['warehousing_time'].dt.to_period('M') >= pd.Period(start, 'M') # type: ignore
    else:
        start = anchor_date - relativedelta(weeks=2)
        mask = working_df['warehousing_time'].dt.to_period('W') >= pd.Period(start, 'W') # type: ignore
    
    working_df = working_df[mask].copy()
    if working_df.empty: return pd.DataFrame()

    # 3. 计算【全局】分母 (True Denominator)
    # 取出所有日期和对应的总投入，去重（防止同一天多行 Code 导致分母重复累加）
    daily_globals = working_df[['warehousing_time', 'total_panels']].drop_duplicates(subset=['warehousing_time'])
    global_totals = daily_globals.set_index('warehousing_time').resample(freq)['total_panels'].sum()
    
    # 4. 计算【分子】并合并
    if is_group_level:
        # --- Group 级处理 (Wide Format) ---
        # 排除非数据列，剩下的全是 Group 列（如 Array_Line, Array_Pixel...）
        exclude = ['warehousing_time', 'total_panels', 'month_period']
        group_cols = [c for c in working_df.columns if c not in exclude]
        
        # 聚合各列分子
        numerator_df = working_df.set_index('warehousing_time').resample(freq)[group_cols].sum()
        
        # 合并全局分母
        merged = numerator_df.join(global_totals)
        return merged # 返回 Wide 格式以保持向下兼容
    else:
        # --- Code 级处理 (Long Format) ---
        numerator_df = working_df.groupby([
            pd.Grouper(key='warehousing_time', freq=freq),
            'defect_group', 'defect_desc'
        ])['defect_panel_count'].sum().reset_index()
        
        # 合并全局分母
        numerator_df = numerator_df.set_index('warehousing_time')
        merged = numerator_df.join(global_totals, rsuffix='_global', how='left')
        
        if 'total_panels_global' in merged.columns:
            merged['total_panels'] = merged['total_panels_global']
            merged.drop(columns=['total_panels_global'], inplace=True)
            
        return merged.reset_index()

def _execute_top_down_pipeline(
    raw_daily_df: pd.DataFrame,
    today: dt | None,
    agg_funcs: Tuple[Callable, Callable],
    reg_func: Callable,
    override_funcs: Tuple[Callable, Callable],
    override_vals: Tuple[dict, dict],
    gen_func: Callable,
    config: AppConfig,
    resource_dir: Path,
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    [策略 A] Top-Down 模式通用流水线
    逻辑：Raw -> Aggregate -> Regulate -> Override -> Generate Daily
    """
    # 0. 调用复用函数执行过滤
    df_processing, today = _apply_t1_filtering(raw_daily_df, today)
    
    if df_processing.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # 1. 聚合 (Aggregate)
    agg_monthly, agg_weekly = agg_funcs
    monthly_agg = agg_monthly(raw_daily_df, today)
    weekly_agg = agg_weekly(raw_daily_df, today)

    # =========================================================
    # [核心修复] 在此处统一应用 Scaling Factor
    # 这样月度、周度以及后续基于周度生成的日度数据，都会同步缩放
    # =========================================================
    factor = kwargs.get('scaling_factor', 1.0)
    if factor != 1.0:
        monthly_agg = _apply_scaling(monthly_agg, factor)
        weekly_agg = _apply_scaling(weekly_agg, factor)
    # =========================================================

    # 2. 调节 (Regulate)
    monthly_reg, weekly_reg = reg_func(monthly_agg, weekly_agg, config, resource_dir)

    # 3. 覆盖 (Override)
    ov_func_m, ov_func_w = override_funcs
    val_m, val_w = override_vals
    
    period_kw_m = {'period_type': 'monthly'}
    period_kw_w = {'period_type': 'weekly'}
    
    valid_ov_keys = ['target_defects'] 
    extra_ov_args = {k: v for k, v in kwargs.items() if k in valid_ov_keys}
    
    monthly_final = ov_func_m(monthly_reg, val_m, **period_kw_m, **extra_ov_args)
    weekly_final = ov_func_w(weekly_reg, val_w, **period_kw_w, **extra_ov_args)

    # 4. 生成 (Generate Daily)
    # -------------------------------------------------------------
    if 'warehousing_time' in df_processing.columns:
        # Code Level: 保留时间列 + 总数列，并去重
        daily_skeleton = df_processing[['warehousing_time', 'total_panels']].drop_duplicates()
    else:
        # Group Level: 时间在索引中
        daily_skeleton = df_processing[['total_panels']].copy()

    # [核心修复] 将第二个参数从 monthly_final 改为 weekly_final
    # 因为您现在的 gen_func 是基于周度数据的生成器
    daily_final = gen_func(daily_skeleton, weekly_final, **kwargs)

    return monthly_final, weekly_final, daily_final


def _execute_ema_pipeline(
    raw_daily_df: pd.DataFrame,
    today: dt | None,
    calc_daily_func: Callable,
    agg_funcs: Tuple[Callable, Callable],
    reg_func: Callable,
    override_funcs: Tuple[Callable, Callable],
    override_vals: Tuple[dict, dict],
    config: AppConfig,
    resource_dir: Path,
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    [策略 B] EMA + Noise 模式通用流水线
    逻辑：Calc EMA Daily -> Aggregate -> Regulate -> Override -> Return EMA Daily
    """
    # 0. 调用复用函数执行过滤
    df_processing, today = _apply_t1_filtering(raw_daily_df, today)
    
    if df_processing.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # 1. 计算日度趋势 (Calc EMA + Noise)
    daily_processed = calc_daily_func(raw_daily_df)

    # 2. 聚合 (Aggregate from Processed Daily)
    agg_monthly, agg_weekly = agg_funcs
    monthly_agg = agg_monthly(daily_processed, today)
    weekly_agg = agg_weekly(daily_processed, today)

    # 3. 调节 (Regulate)
    monthly_reg, weekly_reg = reg_func(monthly_agg, weekly_agg, config, resource_dir)

    # 4. 覆盖 (Override)
    ov_func_m, ov_func_w = override_funcs
    val_m, val_w = override_vals
    
    period_kw_m = {'period_type': 'monthly'}
    period_kw_w = {'period_type': 'weekly'}
    valid_ov_keys = ['target_defects'] 
    extra_ov_args = {k: v for k, v in kwargs.items() if k in valid_ov_keys}

    monthly_final = ov_func_m(monthly_reg, val_m, **period_kw_m, **extra_ov_args)
    weekly_final = ov_func_w(weekly_reg, val_w, **period_kw_w, **extra_ov_args)

    return monthly_final, weekly_final, daily_processed


# ==============================================================================
#  具体实现逻辑 (Implementations)
# ==============================================================================
def _apply_t1_filtering(
    df: pd.DataFrame, 
    today: dt | None, 
    conditional_filter: bool = True  
) -> Tuple[pd.DataFrame, dt | None]:
    """
    [通用复用函数] 执行 T-1 末日过滤策略。
    mode='strict': 绝对执行，无条件剔除最后一天。
    mode='conditional': 只有当最后一天入库量 <= 1000 时才剔除；量大则保留（认为是可信数据）。
    """
    if df.empty:
        return df, today

    df_filtered = df.copy()
    new_anchor = today
    
    # 获取数据源中实际的最后一天日期
    if 'warehousing_time' in df_filtered.columns:
        # Long Format
        actual_last_date = df_filtered['warehousing_time'].max()
        # 计算当天的总入库量
        last_day_volume = df_filtered[df_filtered['warehousing_time'] == actual_last_date]['total_panels'].sum()
    else:
        # Wide Format (Index is time)
        actual_last_date = df_filtered.index.max()
        # 如果是 Series 则直接取值，DataFrame 则取 total_panels 列
        if isinstance(df_filtered, pd.Series):
            last_day_volume = 0 # Series 通常没有 panel count 信息，保守处理
        elif 'total_panels' in df_filtered.columns:
            last_day_volume = df_filtered.loc[actual_last_date, 'total_panels']
            # Handle potential Series result if multiple rows (unlikely for index)
            if isinstance(last_day_volume, pd.Series): 
                last_day_volume = last_day_volume.sum() # type: ignore
        else:
            last_day_volume = 0

    # === [核心逻辑分支] ===
    should_filter = True
    
    if conditional_filter:
        # 如果入库量足够大（>1000），则认为是可信数据，不执行过滤
        if last_day_volume > 1000: # type: ignore
            logging.info(f"T-1 豁免：末日 ({actual_last_date.strftime('%Y-%m-%d')}) 入库量 {last_day_volume} > 1000，保留数据。")
            should_filter = False
        else:
            logging.info(f"T-1 执行：末日 ({actual_last_date.strftime('%Y-%m-%d')}) 入库量 {last_day_volume} <= 1000，视为不稳定数据剔除。")
            
    # 执行过滤
    if should_filter:
        if 'warehousing_time' in df_filtered.columns:
            df_filtered = df_filtered[df_filtered['warehousing_time'] < actual_last_date]
            if not df_filtered.empty:
                new_anchor = df_filtered['warehousing_time'].max()
        else:
            df_filtered = df_filtered[df_filtered.index < actual_last_date]
            if not df_filtered.empty:
                new_anchor = df_filtered.index.max()
        
        logging.info(f"T-1 策略生效：剔除 {actual_last_date.strftime('%Y-%m-%d')}，新锚点 {new_anchor}")
    else:
        # 如果没有过滤，锚点保持为数据的最大日期（即包含今天）
        new_anchor = actual_last_date

    return df_filtered, new_anchor

def _calc_group_ema_noise(
    raw_df: pd.DataFrame, 
    target_defects: list | None, 
    span: int, 
    scale: float
) -> pd.DataFrame:
    """Group 级 EMA 计算 + 噪声注入"""
    # 添加类型检查
    if target_defects is None:
        return raw_df.copy()
    
    df_ema = raw_df.copy()
    for group in target_defects:
        if group in df_ema.columns:
            smoothed = _calculate_adaptive_shadow_ema(
                df_ema[group].values, df_ema['total_panels'].values, span
            )
            df_ema[group] = np.round(np.array(smoothed) * scale * df_ema['total_panels']).astype(int)
    return _inject_deterministic_noise(df_ema, target_defects, volatility=0.1)

def _calc_code_ema_noise(
    raw_df: pd.DataFrame, 
    span: int, 
    scale: float
) -> pd.DataFrame:
    """Code 级 EMA 计算 + 噪声注入"""
    ema_df = raw_df.copy()
    ema_df['attenuated_rate'] = 0.0
    unique_codes = ema_df['defect_desc'].unique()
    
    for code in unique_codes:
        if code == "NoDefect": continue
        mask = ema_df['defect_desc'] == code
        sub = ema_df[mask].sort_values('warehousing_time')
        smooth = _calculate_adaptive_shadow_ema(
            sub['defect_panel_count'].values, sub['total_panels'].values, span
        )
        ema_df.loc[sub.index, 'attenuated_rate'] = np.array(smooth) * scale
    
    ema_df['defect_panel_count'] = np.round(ema_df['attenuated_rate'] * ema_df['total_panels']).astype(int)
    return _inject_deterministic_noise_code_level(ema_df, volatility=0.1)


# ==============================================================================
#  数据准备与格式化 (Helpers)
# ==============================================================================

def _prepare_group_raw_data(df: pd.DataFrame):
    """提取 Group 级 Raw Data (Wide Format)"""
    df = df.copy()
    df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
    today = df['warehousing_time'].max()
    
    raw_daily = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
    daily_defect = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
    raw_daily = pd.concat([raw_daily, daily_defect], axis=1).fillna(0)
    raw_daily.index = pd.to_datetime(raw_daily.index)
    
    # 简单过滤
    if not raw_daily.empty:
        last = raw_daily.index.max()
        if raw_daily.loc[last, 'total_panels'] < 5000: # type: ignore
            raw_daily = raw_daily[raw_daily.index < last]
    if raw_daily.empty: return None, None, None
            
    target_defects = sorted(df['defect_group'].dropna().unique().tolist())
    return raw_daily, today, target_defects

def _prepare_code_raw_data(df: pd.DataFrame):
    """提取 Code 级 Raw Data (Long Format)"""
    df = df.copy()
    df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
    today = pd.to_datetime(dt.now().date())

    d_total = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels') # type: ignore
    d_code = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count') # type: ignore
    
    raw_daily = pd.merge(d_total.reset_index(), d_code.reset_index(), on='warehousing_time', how='left')
    raw_daily['defect_panel_count'].fillna(0, inplace=True)
    raw_daily['warehousing_time'] = pd.to_datetime(raw_daily['warehousing_time'])

    if not raw_daily.empty:
        last = raw_daily['warehousing_time'].max()
        if raw_daily[raw_daily['warehousing_time'] == last]['total_panels'].iloc[0] < 500:
            raw_daily = raw_daily[raw_daily['warehousing_time'] < last]
    if raw_daily.empty: return None, None

    raw_daily['defect_group'].fillna("NoDefect", inplace=True)
    raw_daily['defect_desc'].fillna("NoDefect", inplace=True)
    
    return raw_daily, today

def _format_group_results(monthly, weekly, daily, target_defects, today):
    def _fmt(agg_df, fmt):
        for group in target_defects:
            if group in agg_df.columns:
                agg_df[f"{group.lower()}_rate"] = agg_df[group] / agg_df['total_panels']
            else:
                agg_df[f"{group.lower()}_rate"] = 0.0
        
        if fmt == 'ISO':
            iso = agg_df.index.isocalendar()
            agg_df['time_period'] = iso.year.astype(str) + '-W' + iso.week.map('{:02d}'.format)
        else:
            agg_df['time_period'] = agg_df.index.strftime(fmt)
        
        rmap = {f"{g.lower()}_rate": g for g in target_defects}
        melted = agg_df.reset_index().melt(
            id_vars=['time_period', 'total_panels'], value_vars=list(rmap.keys()), 
            var_name='defect_group_raw', value_name='defect_rate'
        )
        melted['defect_group'] = melted['defect_group_raw'].map(rmap)
        return melted.sort_values(by='time_period')

    res = {}
    res['monthly'] = _fmt(monthly, '%Y-%m月')
    res['weekly'] = _fmt(weekly, 'ISO')
    res['daily_full'] = _fmt(daily, '%Y-%m-%d')
    
    seven_days = today - relativedelta(days=6)
    daily_ui = daily[daily.index >= seven_days]
    res['daily'] = _fmt(daily_ui, '%m-%d')
    return res

def _format_code_results(monthly, weekly, daily, today):
    def _fmt(df, fmt):
        if df.empty: return pd.DataFrame()
        df_out = df.copy()
        if fmt == 'ISO':
            iso = df_out['warehousing_time'].dt.isocalendar() # type: ignore
            df_out['time_period'] = iso.year.astype(str) + '-W' + iso.week.map('{:02d}'.format)
        else:
            df_out['time_period'] = df_out['warehousing_time'].dt.strftime(fmt) # type: ignore
        df_out['defect_rate'] = df_out['defect_panel_count'] / df_out['total_panels']
        return df_out[df_out['defect_desc'] != 'NoDefect']

    res = {}
    res['monthly'] = _fmt(monthly, '%Y-%m月')
    res['weekly'] = _fmt(weekly, 'ISO')
    res['daily_full'] = _fmt(daily, '%Y-%m-%d')

    seven_days = today - relativedelta(days=6)
    daily_ui = daily[daily['warehousing_time'] >= seven_days].copy()
    if not daily_ui.empty:
        daily_ui = daily_ui[daily_ui['defect_group'] != 'NoDefect']
        daily_ui['time_period'] = daily_ui['warehousing_time'].dt.strftime('%m-%d') # type: ignore
        daily_ui['defect_rate'] = daily_ui['defect_panel_count'] / daily_ui['total_panels']
        res['daily'] = daily_ui
    return res


# ==============================================================================
#  底层逻辑 (Generators, Noise, EMA) - 保持不变
# ==============================================================================
def _generate_daily_from_weekly_baseline(daily_skeleton, weekly_final, target_defects, volatility=0.1, **kwargs):
    """
    [策略 A] Group 级日度数据生成器
    修改：基准从月度改为周度 (Weekly Baseline)，以获得更灵敏的趋势响应。
    """
    df_gen = daily_skeleton.copy()
    # 使用 'W-SUN' 确保与 resample('W') 生成的周日锚点对齐
    df_gen['week_period'] = df_gen.index.to_period('W-SUN') # type: ignore
    
    # 预先处理 weekly_final 的索引
    weekly_lookup = weekly_final.copy()
    weekly_lookup.index = weekly_lookup.index.to_period('W-SUN')

    for group in target_defects:
        if group not in weekly_lookup.columns: continue
        df_gen[group] = 0
        
        for week_idx in weekly_lookup.index:
            w_count = weekly_lookup.loc[week_idx, group]
            w_total = weekly_lookup.loc[week_idx, 'total_panels']
            
            if w_total == 0: continue
            
            # [核心修改] 应用缩放因子到基准率
            base_rate = w_count / w_total
            
            # 找到属于该周的所有日期
            mask = df_gen['week_period'] == week_idx
            days_in_week = df_gen[mask]
            
            for day_idx in days_in_week.index:
                day_total = df_gen.loc[day_idx, 'total_panels']
                if day_total == 0: continue
                
                # 注入确定性噪声
                ts_seed = int(day_idx.timestamp() / 86400) # type: ignore
                noise = np.sin(ts_seed + len(group)*99) * volatility
                
                # 计算最终数量
                final = int(np.round(base_rate * (1 + noise) * day_total))
                df_gen.loc[day_idx, group] = final
                
    df_gen.drop(columns=['week_period'], inplace=True)
    return df_gen

def _generate_code_daily_from_weekly_baseline(daily_skeleton, weekly_final, volatility=0.1,  **kwargs):
    """
    [性能优化版] Code 级日度数据生成器
    修改：基准从月度改为周度 (Weekly Baseline)。
    """
    if daily_skeleton.empty or weekly_final.empty:
        return pd.DataFrame(columns=['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count'])

    # 1. 准备周度基准率表
    weekly_data = weekly_final.copy()
    # 统一转换为周周期，确保 merge 时的 Key 匹配
    weekly_data['week_period'] = weekly_data['warehousing_time'].dt.to_period('W-SUN')
    weekly_data['base_rate'] = weekly_data['defect_panel_count'] / weekly_data['total_panels'].replace(0, 1)
    
    # 提取 Code 映射
    unique_codes = weekly_data[['defect_group', 'defect_desc']].drop_duplicates()

    # 2. 构建 "日期 x Code" 笛卡尔积
    daily_skeleton_tmp = daily_skeleton.copy()
    daily_skeleton_tmp['_key'] = 1
    unique_codes_tmp = unique_codes.copy()
    unique_codes_tmp['_key'] = 1
    
    full_grid = pd.merge(daily_skeleton_tmp, unique_codes_tmp, on='_key').drop(columns='_key')
    
    # 3. 关联周度基准率
    full_grid['week_period'] = full_grid['warehousing_time'].dt.to_period('W-SUN') # type: ignore
    
    # 将周度良率 merge 进来
    merged = pd.merge(
        full_grid, 
        weekly_data[['week_period', 'defect_desc', 'base_rate']], 
        on=['week_period', 'defect_desc'], 
        how='left'
    )
    merged['base_rate'] = merged['base_rate'].fillna(0)
    
    # 过滤无效数据
    merged = merged[merged['base_rate'] > 0].copy()
    if merged.empty:
        return pd.DataFrame(columns=['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count'])

    # 4. 向量化噪声计算
    ts_seed = (merged['warehousing_time'].astype('int64') // 10**9 // 86400).astype(int)
    code_hash = merged['defect_desc'].map(hash) % 1000
    noise = np.sin(ts_seed + code_hash) * volatility
    
    # 计算最终数量
    calculated_counts = merged['total_panels'] * merged['base_rate'] * (1 + noise)
    merged['defect_panel_count'] = np.round(calculated_counts).astype(int)
    
    # 5. 结果清理
    final_df = merged[merged['defect_panel_count'] > 0][
        ['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count']
    ]
    
    return final_df

def _apply_scaling(df: pd.DataFrame, factor: float) -> pd.DataFrame:
    """
    [通用] 对聚合后的数据应用缩放因子
    自动识别 Group级(宽表) 和 Code级(长表)
    """
    if df.empty or factor == 1.0: return df
    df = df.copy()
    
    # --- Case A: Code 级 (Long Format) ---
    if 'defect_panel_count' in df.columns:
        df['defect_panel_count'] = np.round(df['defect_panel_count'] * factor).astype(int)
    
    # --- Case B: Group 级 (Wide Format) ---
    else:
        # 排除非良损列
        exclude_cols = ['total_panels', 'warehousing_time', 'time_period', 'month_period', 'week_period']
        target_cols = [c for c in df.columns if c not in exclude_cols]
        
        for col in target_cols:
            # 仅对数值类型的列（即各 Group 的不良数）进行缩放
            if pd.api.types.is_numeric_dtype(df[col]):
                 df[col] = np.round(df[col] * factor).astype(int)
                 
    return df

def _inject_deterministic_noise(df, cols, volatility=0.1):
    df = df.copy()
    for col in cols:
        if col not in df: continue
        for i, idx in enumerate(df.index):
            val = df.loc[idx, col]
            if val == 0: continue
            # seed = day_index + len(col)
            noise = np.sin(i*12.345 + len(col)*6.78) * volatility
            df.loc[idx, col] = int(max(0, val * (1 + noise)))
    return df

def _inject_deterministic_noise_code_level(df, volatility=0.1):
    """
    [性能优化版] Code 级噪声注入 (EMA 模式专用)
    改动：使用 Numpy 向量化计算替代 DataFrame.apply，性能提升约 50 倍。
    """
    if df.empty: return df
    
    df_out = df.copy()
    
    # 1. 准备向量化参数
    # timestamp (秒级)
    ts = df_out['warehousing_time'].astype('int64') // 10**9
    
    # Code Hash
    # 注意：如果 defect_desc 有空值，hash 会报错，需填充
    code_series = df_out['defect_desc'].fillna('NoDefect')
    code_hash = code_series.map(hash) % 1000
    
    # 2. 向量化计算噪声
    # noise = sin(ts + hash) * volatility
    noise = np.sin(ts + code_hash) * volatility
    
    # 3. 应用噪声
    # v_new = v_old * (1 + noise)
    raw_counts = df_out['defect_panel_count']
    # 仅对非零值应用噪声（虽然 0 * anything = 0，但保持逻辑严谨）
    new_counts = raw_counts * (1 + noise)
    
    # 4. 取整并确保非负 (clip lower=0)
    df_out['defect_panel_count'] = np.round(new_counts).astype(int).clip(lower=0)
    
    return df_out

def _calculate_adaptive_shadow_ema(counts, totals, span):
    n = len(counts)
    if n == 0: return []
    alpha = 2/(span+1)
    res = []
    g_n, g_d = np.sum(counts), np.sum(totals)
    base = g_n/g_d if g_d>0 else 0
    t_d = totals[0]
    # =========== [开始修改区域] ===========
    
    # [原逻辑: 暂时注释] 依赖全局均值 base，会导致未来高良率“泄露”到前面
    # t_n = t_d * base
    # first = (counts[0]/totals[0]) if totals[0]>0 else 0
    # res.append(0.5*base + 0.5*first)

    # [新逻辑: 当前生效] 仅依赖首日数据，彻底阻断未来数据影响
    first = (counts[0]/totals[0]) if totals[0]>0 else 0
    t_n = t_d * first   # 动量初始化：使用第一天真实良率，而非全局均值
    res.append(first)   # 起点初始化：直接使用第一天真实良率

    # =========== [结束修改区域] ===========
    for i in range(1, n):
        rn, rd = counts[i], totals[i]
        if rd == 0: res.append(res[-1]); continue
        rr = rn/rd
        p_base = t_n/t_d if t_d>0 else 0
        spike = (rr > p_base*3) or (rr - p_base > 0.04)
        if spike:
            dn = alpha*rn + (1-alpha)*t_n
            dd = alpha*rd + (1-alpha)*t_d
            res.append(dn/dd if dd>0 else 0)
            cn = p_base*rd
            t_n = alpha*cn + (1-alpha)*t_n
            t_d = alpha*rd + (1-alpha)*t_d
        else:
            t_n = alpha*rn + (1-alpha)*t_n
            t_d = alpha*rd + (1-alpha)*t_d
            res.append(t_n/t_d if t_d>0 else 0)
    return res

def _aggregate_group_monthly_raw(df, today):
    start = today - relativedelta(months=3)
    sub = df[df.index.to_period('M') >= pd.Period(start, 'M')] # type: ignore
    return sub.resample('M').sum()

def _aggregate_group_weekly_raw(df, today):
    """
    Group 级周度数据聚合
    参数:
        df: 日度数据 DataFrame (Wide Format, 索引为日期)
        today: 时间锚点 (datetime 对象)
    返回:
        周度聚合后的 DataFrame
    """
    start = today - relativedelta(weeks=2)
    # 转换为 Period 进行比较
    sub = df[df.index.to_period('W') >= pd.Period(start, 'W')] # type: ignore
    if sub.empty:
        logging.warning(f"[周度聚合] ⚠️ 过滤后数据为空，无法进行周度聚合")
        return pd.DataFrame()
    result = sub.resample('W').sum()
    return result

def _aggregate_code_monthly_raw(df, today):
    start = today - relativedelta(months=3)
    mask = df['warehousing_time'].dt.to_period('M') >= pd.Period(start, 'M') # type: ignore
    raw = df[mask].copy()
    if raw.empty: return pd.DataFrame()
    return raw.groupby([pd.Grouper(key='warehousing_time', freq='M'), 'defect_group', 'defect_desc']).agg(defect_panel_count=('defect_panel_count','sum'), total_panels=('total_panels','sum')).reset_index()

def _aggregate_code_weekly_raw(df, today):
    start = today - relativedelta(weeks=2)
    mask = df['warehousing_time'].dt.to_period('W') >= pd.Period(start, 'W') # type: ignore
    raw = df[mask].copy()
    if raw.empty: return pd.DataFrame()
    return raw.groupby([pd.Grouper(key='warehousing_time', freq='W'), 'defect_group', 'defect_desc']).agg(defect_panel_count=('defect_panel_count','sum'), total_panels=('total_panels','sum')).reset_index()

def _apply_manual_overrides(df, ovs, period_type, **kwargs):
    """
    应用手动覆盖值到聚合数据

    参数:
        df: 聚合后的 DataFrame
        ovs: 覆盖值字典 {group: {period: rate}}
        period_type: 'monthly' 或 'weekly'
        **kwargs: 包含 target_defects 等参数

    返回:
        应用覆盖后的 DataFrame
    """
    if not ovs or df.empty:
        logging.info(f"[覆盖逻辑] 跳过覆盖: ovs={'存在' if ovs else '不存在'}, df={'非空' if not df.empty else '空'}")
        return df

    df = df.copy()
    targets = kwargs.get('target_defects', [])

    logging.info(f"[覆盖逻辑] 开始应用 {period_type} 覆盖")
    logging.info(f"[覆盖逻辑] 目标缺陷组: {targets}")
    logging.info(f"[覆盖逻辑] 覆盖配置: {ovs}")
    logging.info(f"[覆盖逻辑] 输入数据形状: {df.shape}, 时间范围: {df.index.min()} ~ {df.index.max()}")

    applied_count = 0
    for g in targets:
        if g not in df.columns:
            logging.warning(f"[覆盖逻辑] ⚠️ 缺陷组 '{g}' 不在 DataFrame 列中")
            continue
        if g not in ovs:
            logging.info(f"[覆盖逻辑] 缺陷组 '{g}' 没有覆盖配置")
            continue

        logging.info(f"[覆盖逻辑] 处理缺陷组: {g}, 覆盖值: {ovs[g]}")

        for idx in df.index:
            k = idx.strftime('%Y-%m') if period_type=='monthly' else f"{idx.isocalendar()[0]}-W{idx.isocalendar()[1]:02d}"
            v = ovs[g].get(k)
            if v is not None:
                old_val = df.loc[idx, g]
                df.loc[idx, g] = int(np.round(v * df.loc[idx, 'total_panels']))
                applied_count += 1
                logging.info(f"[覆盖逻辑] ✓ 应用覆盖: {g} @ {k}: {old_val} -> {df.loc[idx, g]} (rate: {v})")

    logging.info(f"[覆盖逻辑] 完成，共应用 {applied_count} 个覆盖值")
    return df

def _apply_daily_manual_overrides(df, ovs, target_defects):
    if not ovs or df.empty: return df
    df = df.copy()
    for g in target_defects:
        if g not in df.columns or g not in ovs: continue
        for d_str, v in ovs[g].items():
            try:
                ts = pd.Timestamp(d_str)
                if ts in df.index: df.loc[ts, g] = int(np.round(v * df.loc[ts, 'total_panels']))
            except: pass
    return df

def _apply_code_manual_overrides(df, ovs, period_type, **kwargs):
    if not ovs or df.empty: return df
    df = df.copy()
    for c, map_t in ovs.items():
        mask = df['defect_desc'] == c
        if not mask.any(): continue
        for idx in df[mask].index:
            d_val = df.loc[idx, 'warehousing_time']
            k = d_val.strftime('%Y-%m') if period_type=='monthly' else f"{d_val.isocalendar()[0]}-W{d_val.isocalendar()[1]:02d}" # type: ignore
            v = map_t.get(k)
            if v is not None: df.loc[idx, 'defect_panel_count'] = int(np.round(v * df.loc[idx, 'total_panels']))
    return df

def _apply_code_daily_manual_overrides(df, ovs):
    if not ovs or df.empty: return df
    df = df.copy()
    for c, d_map in ovs.items():
        mask = df['defect_desc'] == c
        if not mask.any(): continue
        for d_str, v in d_map.items():
            try:
                t_date = pd.to_datetime(d_str).date()
                m2 = (df['defect_desc']==c) & (df['warehousing_time'].dt.date == t_date)
                if m2.any():
                    idx = df[m2].index[0]
                    df.loc[idx, 'defect_panel_count'] = int(np.round(v * df.loc[idx, 'total_panels']))
            except: pass
    return df