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
    # ==========================================================================
    #  主入口 1: Group 级趋势分析 (V3.1 - Code 注入 & 完整流水线保留)
    # ==========================================================================
    @staticmethod
    def create_mwd_trend_data(
        panel_details_df: pd.DataFrame, 
        mwd_code_data: Dict[str, pd.DataFrame] | None,
        config: AppConfig,
        scaling_factor: float,
        USE_TOP_DOWN_STRATEGY: bool,
        volatility: float = 0.1,
    ) -> Dict[str, pd.DataFrame] | None:
        
        logging.info(f"Group级趋势分析 (模式: Code源注入 -> {'Top-Down' if USE_TOP_DOWN_STRATEGY else 'EMA'})...")
        if panel_details_df.empty or not mwd_code_data: return None
        
        try:
            # 0. 准备真实的每日 total_panels 骨架，并获取 target_defects
            raw_daily, today, target_defects = _prepare_group_raw_data(panel_details_df)
            if raw_daily is None: return None
            
            # T-1 过滤 (保留末日剔除能力)
            df_processing, today = _apply_t1_filtering(raw_daily, today)
            if df_processing.empty: return None

            m_vals = config.processing.get('group_monthly_values', {})
            w_vals = config.processing.get('group_weekly_values', {})
            d_vals = config.processing.get('group_daily_values', {})

            # ------------------------------------------------------------------
            # [核心逻辑]: 将 Code 级长表聚合后，注入到 Group 级宽表骨架中
            # 这替代了图1中的 “步骤1: 聚合出原始数据”
            # ------------------------------------------------------------------
            def _inject_code_to_skeleton(freq: str, code_df_key: str):
                # 借助原始聚合器生成带有正确时间索引 (DatetimeIndex/PeriodIndex) 的空骨架
                skeleton = _safe_trend_aggregator(df_processing, today, freq, is_group_level=True) # type: ignore
                code_df = mwd_code_data.get(code_df_key)
                
                if skeleton.empty or code_df is None or code_df.empty: 
                    return skeleton
                
                res = skeleton.copy()
                # 遍历真实存在的时间周期，从 Code 聚合表里捞取不良数覆盖
                for idx in res.index:
                    if freq == 'W':
                        iso = idx.isocalendar() # type: ignore
                        k = f"{iso[0]}-W{iso[1]:02d}"
                    else:
                        k = idx.strftime('%Y-%m月') # type: ignore
                        
                    sub = code_df[code_df['time_period'] == k]
                    for g in target_defects:
                        if g in res.columns:
                            # 累加该 Group 下所有 Code 的不良数
                            val = sub[sub['defect_group'] == g]['defect_panel_count'].sum()
                            res.loc[idx, g] = val
                return res

            if USE_TOP_DOWN_STRATEGY:
                # 【图1 - Step 1】: 获取注入了 Code 数据的月/周基准 (Source of Truth)
                raw_monthly = _inject_code_to_skeleton('M', 'monthly')
                raw_weekly = _inject_code_to_skeleton('W', 'weekly')

                # 【图1 - Step 2】: 倍率缩放 (如配置为1.0则相当于不缩放)
                if scaling_factor != 1.0:
                    raw_monthly = _apply_scaling(raw_monthly, scaling_factor)
                    raw_weekly = _apply_scaling(raw_weekly, scaling_factor)

                # 【图1 - Step 3】: 智能调节 (兜底压制异常波动)
                reg_m, reg_w = TrendRegulator.regulate_monthly_and_weekly(
                    raw_monthly, raw_weekly
                )

                # 【图1 - Step 4】: 周度数据覆盖 (来自 YAML)
                final_weekly = _apply_manual_overrides(reg_w, w_vals, 'weekly', target_defects=target_defects)

                # 【图1 - Step 5】: 生成日度数据 (基于周度)
                daily_skeleton = df_processing[['total_panels']].copy()
                final_daily = _generate_daily_from_weekly_baseline(
                    daily_skeleton, final_weekly, target_defects, volatility
                )

                # 【图1 - Step 6】: 生成月度数据 (基于生成的日度数据重聚合，保证数学自洽)
                reaggregated_monthly = _safe_trend_aggregator(final_daily, today, 'M', is_group_level=True) # type: ignore

                # 【图1 - Step 7】: 月度数据覆盖 (来自 YAML)
                final_monthly = _apply_manual_overrides(reaggregated_monthly, m_vals, 'monthly', target_defects=target_defects)

                monthly, weekly, daily = final_monthly, final_weekly, final_daily

            else:
                # [EMA 模式兼容处理]
                def _inject_code_daily():
                    skeleton = df_processing[['total_panels']].copy()
                    code_daily = mwd_code_data.get('daily_full')
                    if skeleton.empty or code_daily is None or code_daily.empty: return skeleton
                    res = skeleton.copy()
                    for g in target_defects: res[g] = 0
                    for idx in res.index:
                        k = idx.strftime('%Y-%m-%d')
                        sub = code_daily[code_daily['time_period'] == k]
                        for g in target_defects:
                            res.loc[idx, g] = sub[sub['defect_group'] == g]['defect_panel_count'].sum()
                    return res
                
                daily_processed = _inject_code_daily()
                monthly_agg = _aggregate_group_monthly_raw(daily_processed, today)
                weekly_agg = _aggregate_group_weekly_raw(daily_processed, today)

                monthly_reg, weekly_reg = TrendRegulator.regulate_monthly_and_weekly(
                    monthly_agg, weekly_agg
                )

                monthly_final = _apply_manual_overrides(monthly_reg, m_vals, 'monthly', target_defects=target_defects)
                weekly_final = _apply_manual_overrides(weekly_reg, w_vals, 'weekly', target_defects=target_defects)

                monthly, weekly, daily = monthly_final, weekly_final, daily_processed

            # 最终格式化 (完美保留 7 天视图补全逻辑)
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
        ema_span: int,          
        scaling_factor: float,
        USE_TOP_DOWN_STRATEGY: bool,
        volatility: float = 0.2,
        warning_lines: dict = None  # type: ignore
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
                    scaling_factor=scaling_factor,
                    volatility=volatility,
                    warning_lines=warning_lines # [新增]
                )
            else:
                monthly, weekly, daily = _execute_ema_pipeline(
                    raw_daily_df=raw_daily,
                    today=today,
                    calc_daily_func=lambda df: _calc_code_ema_noise(df, ema_span, scaling_factor, volatility),
                    agg_funcs=(agg_monthly_func, agg_weekly_func), # <--- 使用修复后的聚合函数
                    reg_func=TrendRegulator.regulate_code_monthly_and_weekly,
                    override_funcs=(_apply_code_manual_overrides, _apply_code_manual_overrides),
                    override_vals=(m_vals, w_vals),
                    warning_lines=warning_lines # [新增]
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
# src/vivo_project/core/mwd_trend_processor.py
def _execute_top_down_pipeline(
    raw_daily_df: pd.DataFrame,
    today: dt | None,
    agg_funcs: Tuple[Callable, Callable],
    reg_func: Callable,
    override_funcs: Tuple[Callable, Callable],
    override_vals: Tuple[dict, dict],
    gen_func: Callable,
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    [策略 A] Top-Down 模式通用流水线 (V2.0 - 串行一致性版)
    逻辑流：Raw -> Weekly -> Daily (Generated) -> Monthly (Re-aggregated)
    解决月度与周度/日度数据因统计周期错位导致的逻辑割裂问题。
    """
    # 0. T-1 过滤
    df_processing, today = _apply_t1_filtering(raw_daily_df, today)
    
    if df_processing.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    agg_monthly_func, agg_weekly_func = agg_funcs

    # ==========================================================================
    # Phase 1: 确立周度基准 (The Baseline)
    # ==========================================================================
    
    # 1.1 聚合出原始周度数据 (同时聚合月度仅供 Regulator 参考)
    raw_monthly = agg_monthly_func(df_processing, today)
    raw_weekly = agg_weekly_func(df_processing, today)

    # 1.2 应用缩放 (Scaling)
    factor = kwargs.get('scaling_factor', 1.0)
    if factor != 1.0:
        raw_monthly = _apply_scaling(raw_monthly, factor)
        raw_weekly = _apply_scaling(raw_weekly, factor)

    # 1.3 智能调节 (Regulation)
    # 注意：reg_func 内部会对比 monthly 和 weekly 的趋势
    # 但我们后续只使用 regulated_weekly 作为生成源
    _, regulated_weekly = reg_func(raw_monthly, raw_weekly, **kwargs)

    # 1.4 人工覆盖 (Override - Weekly)
    ov_func_m, ov_func_w = override_funcs # 覆盖函数
    val_m, val_w = override_vals # 覆盖值
    
    # 从kwargs中筛选出'target_defects'这一参数
    period_kw_w = {'period_type': 'weekly'} # 进行周度覆盖
    valid_ov_keys = ['target_defects'] 
    extra_ov_args = {k: v for k, v in kwargs.items() if k in valid_ov_keys}
    
    # 得到【最终周度数据】(这是整个链路的 Source of Truth)
    final_weekly = ov_func_w(regulated_weekly, val_w, **period_kw_w, **extra_ov_args)

    # ==========================================================================
    # Phase 2: 生成日度数据 (Generation)
    # ==========================================================================
    
    # 构造日度骨架
    if 'warehousing_time' in df_processing.columns:
        # Code Level: 保留时间列 + 总数列，并去重
        daily_skeleton = df_processing[['warehousing_time', 'total_panels']].drop_duplicates()
    else:
        # Group Level: 时间在索引中
        daily_skeleton = df_processing[['total_panels']].copy()
    
    # 基于【最终周度】生成【最终日度】
    final_daily = gen_func(daily_skeleton, final_weekly, **kwargs)

    # ==========================================================================
    # Phase 3: 重构月度数据 (Re-aggregation)
    # ==========================================================================
    
    # 3.1 基于生成的日度数据，重新聚合出月度数据
    # 注意：agg_monthly_func 内部会执行 filtering (last 3 months)，这与我们的预期一致
    reaggregated_monthly = agg_monthly_func(final_daily, today)
    
    # 3.2 人工覆盖 (Override - Monthly)
    # 允许用户在最后环节强行修正月度数据 (Override 优先级最高)
    period_kw_m = {'period_type': 'monthly'}
    
    final_monthly = ov_func_m(reaggregated_monthly, val_m, **period_kw_m, **extra_ov_args)

    return final_monthly, final_weekly, final_daily

def _execute_ema_pipeline(
    raw_daily_df: pd.DataFrame,
    today: dt | None,
    calc_daily_func: Callable,
    agg_funcs: Tuple[Callable, Callable],
    reg_func: Callable,
    override_funcs: Tuple[Callable, Callable],
    override_vals: Tuple[dict, dict],
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
    monthly_reg, weekly_reg = reg_func(monthly_agg, weekly_agg, **kwargs)

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

    # 2. 执行时间窗口过滤 (3个月)
    start = anchor_date - relativedelta(months=3)
    mask = working_df['warehousing_time'].dt.to_period(freq) >= pd.Period(start, freq)
    
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
    
def _apply_t1_filtering(
    df: pd.DataFrame, 
    today: dt | None, 
    conditional_filter: bool = True  
) -> Tuple[pd.DataFrame, dt | None]:
    """
    [通用复用函数] 执行 T-1 末日过滤策略。
    """
    if df.empty:
        return df, today

    df_filtered = df.copy()
    new_anchor = today
    
    # 获取数据源中实际的最后一天日期
    if 'warehousing_time' in df_filtered.columns:
        actual_last_date = df_filtered['warehousing_time'].max()
        last_day_volume = df_filtered[df_filtered['warehousing_time'] == actual_last_date]['total_panels'].sum()
    else:
        actual_last_date = df_filtered.index.max()
        if isinstance(df_filtered, pd.Series):
            last_day_volume = 0
        elif 'total_panels' in df_filtered.columns:
            last_day_volume = df_filtered.loc[actual_last_date, 'total_panels']
            if isinstance(last_day_volume, pd.Series): 
                last_day_volume = last_day_volume.sum() # type: ignore
        else:
            last_day_volume = 0

    # === [核心逻辑分支] ===
    should_filter = True
    
    # -------------------------------------------------------------------------
    # ✅ [新增逻辑]：时间距离防呆校验：判断 actual_last_date 距离现实世界中的“今天”过了多久
    # 如果已经超过 1 天，说明这一天早就结束了，不管入库多少片，都是既定事实，绝不能剔除！
    # -------------------------------------------------------------------------
    real_today = pd.to_datetime(dt.now().date())
    # 确保 actual_last_date 是 datetime 类型以计算差值
    target_date = pd.to_datetime(actual_last_date)
    days_diff = (real_today - target_date).days
    
    if days_diff > 1: # 如果是两天前或更早的数据，直接豁免
        should_filter = False
    elif conditional_filter and last_day_volume > 1000: # type: ignore
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
        new_anchor = actual_last_date

    return df_filtered, new_anchor

def _calc_group_ema_noise(
    raw_df: pd.DataFrame, 
    target_defects: list | None, 
    span: int, 
    scale: float,
    volatility: float
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
    return _inject_deterministic_noise(df_ema, target_defects, volatility)

def _calc_code_ema_noise(
    raw_df: pd.DataFrame, 
    span: int, 
    scale: float,
    volatility: float
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
    return _inject_deterministic_noise_code_level(ema_df, volatility)


# ==============================================================================
#  数据准备与格式化 (Helpers)
# ==============================================================================

def _prepare_group_raw_data(df: pd.DataFrame):
    """提取 Group 级 Raw Data (Wide Format)"""
    df = df.copy()
    df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
    today = df['warehousing_time'].max()
    
    # --- [新增调试日志: 抓出 2-26 消失的真相] ---
    try:
        logging.info(f"========== [DEBUG: 数据源时间边界排查] ==========")
        dates_in_df = df['warehousing_time'].dt.strftime('%Y-%m-%d').unique()
        logging.info(f"提取的数据总行数: {len(df)}")
        logging.info(f"数据源中【实际存在的最后日期】: {today.strftime('%Y-%m-%d')}")
        logging.info(f"数据源中最近的 5 个有效日期: {sorted(dates_in_df)[-5:]}")
        if '2026-02-26' not in dates_in_df:
            logging.warning("⚠️ 铁证: 数据库查询虽然截至今日本应包含 2-26，但返回的 Panel 明细数据中【完全没有】 2-26 的入库记录！")
        else:
            logging.info("结论: 存在 2-26 的数据。请检查后续过滤逻辑。")
        logging.info(f"=================================================")
    except: pass
    # ---------------------------------------------

    raw_daily = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
    daily_defect = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
    raw_daily = pd.concat([raw_daily, daily_defect], axis=1).fillna(0)
    raw_daily.index = pd.to_datetime(raw_daily.index)
            
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

    raw_daily['defect_group'].fillna("NoDefect", inplace=True)
    raw_daily['defect_desc'].fillna("NoDefect", inplace=True)
    
    return raw_daily, today

def _format_group_results(monthly, weekly, daily, target_defects, today):
    """
    统一数据格式化与最终的“不良率”计算
    
    【核心逻辑说明】：
    这是整个数据流水线的最后一环。在这个函数之前，所有的数据表（Wide Format）
    里存的都是纯粹的“不良面板数量”。
    本函数的职责是：
    1. 计算最终展示用的“不良率”。
    2. 将“宽表 (Wide Format: 列是不同Group)” 融化(Melt)为 
       “长表 (Long Format: 一行代表一个Group的率)”，以适配 Plotly 和 Streamlit 的标准数据结构。
    """
    def _fmt(agg_df, fmt):
        # --- 步骤 1: 最终计算不良率 ---
        for group in target_defects:
            if group in agg_df.columns:
                # 【计算点】: 新增一列 `{group_name}_rate`，其值为: 该组不良数 / 总入库数
                agg_df[f"{group.lower()}_rate"] = agg_df[group] / agg_df['total_panels']
            else:
                agg_df[f"{group.lower()}_rate"] = 0.0
        
        # --- 步骤 2: 格式化时间标签 ---
        if fmt == 'ISO':
            iso = agg_df.index.isocalendar()
            agg_df['time_period'] = iso.year.astype(str) + '-W' + iso.week.map('{:02d}'.format)
        else:
            agg_df['time_period'] = agg_df.index.strftime(fmt)
        
        # --- 步骤 3: 宽表转长表 (Melt) ---
        # 创建一个映射字典，例如: {'array_line_rate': 'Array_Line'}
        rmap = {f"{g.lower()}_rate": g for g in target_defects}
        
        # 使用 pd.melt 将所有的 rate 列“融化”成两列：变量名列(defect_group_raw) 和 值列(defect_rate)
        # 这样处理后，画图时直接将 x 指定为 time_period, y 指定为 defect_rate, color 指定为 defect_group 即可
        melted = agg_df.reset_index().melt(
            id_vars=['time_period', 'total_panels'], # 保留作为维度的列
            value_vars=list(rmap.keys()),            # 需要融化的率列
            var_name='defect_group_raw',             # 融化后的名称列
            value_name='defect_rate'                 # 融化后的数值列
        )
        
        # 将内部小写的 rate 列名映射回标准的大写 Group 名称
        melted['defect_group'] = melted['defect_group_raw'].map(rmap)
        
        return melted.sort_values(by='time_period')

    res = {}
    # 分别对月、周、日全量数据进行格式化转换
    res['monthly'] = _fmt(monthly, '%Y-%m月')
    res['weekly'] = _fmt(weekly, 'ISO')
    res['daily_full'] = _fmt(daily, '%Y-%m-%d')
    
    # --- 步骤 4: 专门为 UI 展示切片 ---
    # 看板中通常不需要展示过去 90 天的日度柱状图，所以单独切出最近 7 天供 UI 快速渲染
    # [核心修复2]: 强制为 UI 补齐严格的 7 天日度数据
    seven_days = today - relativedelta(days=6)
    full_7_days = pd.date_range(start=seven_days, end=today, freq='D')
    
    daily_ui = daily[daily.index >= seven_days].copy()
    
    # 强制 reindex，缺失的日期会被自动插入并填充为 NaN
    daily_ui = daily_ui.reindex(full_7_days)
    # 用 0 填充入库量
    daily_ui['total_panels'] = daily_ui['total_panels'].fillna(0)
    # 用 0 填充不良数
    for g in target_defects:
        if g not in daily_ui.columns:
            daily_ui[g] = 0
        else:
            daily_ui[g] = daily_ui[g].fillna(0)
            
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
            
        # 增加防御：防止分母为 0 导致报错
        df_out['defect_rate'] = np.where(df_out['total_panels'] > 0, df_out['defect_panel_count'] / df_out['total_panels'], 0.0)
        return df_out[df_out['defect_desc'] != 'NoDefect']

    res = {}
    res['monthly'] = _fmt(monthly, '%Y-%m月')
    
    # =====================================================================
    # [核心修改] 仅针对 Weekly 进行“最近3周”保底补全
    # =====================================================================
    w_df = _fmt(weekly, 'ISO')
    if not w_df.empty:
        # 1. 计算目标 3 周的 ISO 标签 (如 '2026-W09', '2026-W08', '2026-W07')
        target_weeks = []
        for i in range(3):
            iso = (today - relativedelta(weeks=i)).isocalendar()
            target_weeks.append(f"{iso[0]}-W{iso[1]:02d}")
        target_weeks = sorted(target_weeks)
        
        # 2. 提取当前存在的所有 Code
        valid_codes = w_df[['defect_group', 'defect_desc']].drop_duplicates()
        
        # 3. 从带有 NoDefect 的原始 weekly 中提取真实的周度入库数，防止分母丢失
        raw_w = weekly.copy()
        iso_raw = raw_w['warehousing_time'].dt.isocalendar()
        raw_w['time_period'] = iso_raw.year.astype(str) + '-W' + iso_raw.week.map('{:02d}'.format)
        weekly_totals = raw_w.drop_duplicates('time_period').set_index('time_period')['total_panels'].to_dict()

        # 4. 探伤并补洞
        new_rows = []
        for w in target_weeks:
            for _, r in valid_codes.iterrows():
                grp = r['defect_group']
                desc = r['defect_desc']
                # 如果某周某 Code 缺失，则补入 0 值
                if not ((w_df['time_period'] == w) & (w_df['defect_desc'] == desc)).any():
                    new_rows.append({
                        'time_period': w,
                        'defect_group': grp,
                        'defect_desc': desc,
                        'total_panels': weekly_totals.get(w, 0), # 即使不良为0，如果有入库也要填入真实入库数
                        'defect_panel_count': 0,
                        'defect_rate': 0.0,
                        'warehousing_time': pd.NaT # 补洞数据，时间留空即可
                    })
        
        if new_rows:
            w_df = pd.concat([w_df, pd.DataFrame(new_rows)], ignore_index=True)
            w_df = w_df.sort_values(by=['time_period', 'defect_group', 'defect_desc']).reset_index(drop=True)
            
    res['weekly'] = w_df
    # =====================================================================

    res['daily_full'] = _fmt(daily, '%Y-%m-%d')

    seven_days = today - relativedelta(days=6)
    daily_ui = daily[daily['warehousing_time'] >= seven_days].copy()
    if not daily_ui.empty:
        daily_ui = daily_ui[daily_ui['defect_group'] != 'NoDefect']
        daily_ui['time_period'] = daily_ui['warehousing_time'].dt.strftime('%m-%d') # type: ignore
        # 增加防御：防止分母为 0
        daily_ui['defect_rate'] = np.where(daily_ui['total_panels'] > 0, daily_ui['defect_panel_count'] / daily_ui['total_panels'], 0.0)
        res['daily'] = daily_ui
    else:
        res['daily'] = pd.DataFrame()
        
    return res

# ==============================================================================
#  底层逻辑 (Generators, Noise, EMA) - 保持不变
# ==============================================================================
def _generate_daily_from_weekly_baseline(daily_skeleton, weekly_final, target_defects, volatility, **kwargs):
    """
    [策略 A] Group 级日度数据生成器
    修改：增加随机种子扰动因子，解决 sin 函数周期性导致的“伪趋势”问题。
    """
    df_gen = daily_skeleton.copy()
    df_gen['week_period'] = df_gen.index.to_period('W-SUN') # type: ignore
    
    weekly_lookup = weekly_final.copy()
    weekly_lookup.index = weekly_lookup.index.to_period('W-SUN')

    for group in target_defects:
        if group not in weekly_lookup.columns: continue
        df_gen[group] = 0
        
        for week_idx in weekly_lookup.index:
            w_count = weekly_lookup.loc[week_idx, group]
            w_total = weekly_lookup.loc[week_idx, 'total_panels']
            if w_total == 0: continue
            
            # 直接计算基准率
            base_rate = w_count / w_total
            
            mask = df_gen['week_period'] == week_idx
            days_in_week = df_gen[mask]
            
            for day_idx in days_in_week.index:
                day_total = df_gen.loc[day_idx, 'total_panels']
                if day_total == 0: continue
                
                # [Fix] 确定性白噪声生成
                # 原逻辑: ts_seed 连续递增导致 sin 呈现平滑波浪趋势
                # 新逻辑: 引入大质数乘法因子(1234567)，将连续的时间打散，实现“相邻两天不相关”
                ts_seed = int(day_idx.timestamp() / 86400) # type: ignore
                
                # 伪随机哈希算法: sin(time * Large_Prime + Group_Hash)
                scramble_factor = 1234567 
                noise_seed = (ts_seed * scramble_factor) + (hash(group) % 9999)
                
                # 这样生成的 noise 就是围绕 0 上下剧烈跳动的，而非平滑过渡
                noise = np.sin(noise_seed) * volatility
                
                # 计算最终数量
                final = int(np.round(base_rate * (1 + noise) * day_total))
                df_gen.loc[day_idx, group] = final
                
    df_gen.drop(columns=['week_period'], inplace=True)
    return df_gen

def _generate_code_daily_from_weekly_baseline(daily_skeleton, weekly_final, volatility, **kwargs):
    """
    [性能优化版] Code 级日度数据生成器
    修改：增加随机种子扰动因子，解决 sin 函数周期性导致的“伪趋势”问题。
    """
    if daily_skeleton.empty or weekly_final.empty:
        return pd.DataFrame(columns=['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count'])

    weekly_data = weekly_final.copy()
    weekly_data['week_period'] = weekly_data['warehousing_time'].dt.to_period('W-SUN')
    
    weekly_data['base_rate'] = weekly_data['defect_panel_count'] / weekly_data['total_panels'].replace(0, 1)
    
    unique_codes = weekly_data[['defect_group', 'defect_desc']].drop_duplicates()

    # 2. 构建 "日期 x Code" 笛卡尔积
    daily_skeleton_tmp = daily_skeleton.copy()
    daily_skeleton_tmp['_key'] = 1
    unique_codes_tmp = unique_codes.copy()
    unique_codes_tmp['_key'] = 1
    
    full_grid = pd.merge(daily_skeleton_tmp, unique_codes_tmp, on='_key').drop(columns='_key')
    
    # 3. 关联周度基准率
    full_grid['week_period'] = full_grid['warehousing_time'].dt.to_period('W-SUN') # type: ignore
    
    merged = pd.merge(
        full_grid, 
        weekly_data[['week_period', 'defect_desc', 'base_rate']], 
        on=['week_period', 'defect_desc'], 
        how='left'
    )
    merged['base_rate'] = merged['base_rate'].fillna(0)
    merged = merged[merged['base_rate'] > 0].copy()
    
    if merged.empty:
        return pd.DataFrame(columns=['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count'])

    # 4. [Fix] 向量化噪声计算 (高频扰动)
    # 同样引入大数乘法，打散时间连续性
    ts_vector = (merged['warehousing_time'].astype('int64') // 10**9 // 86400).astype(int)
    scramble_factor = 999983 # 大质数
    
    code_hash = merged['defect_desc'].map(hash) % 10000
    
    # 公式: sin(Time * Large_Prime + Code_Hash)
    # 这确保了同一 Code 在相邻两天的 noise 是完全随机独立的
    phase = (ts_vector * scramble_factor) + code_hash
    noise = np.sin(phase) * volatility
    
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

def _inject_deterministic_noise(df, cols, volatility):
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

def _inject_deterministic_noise_code_level(df, volatility):
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

def _calculate_adaptive_shadow_ema(counts, totals, span, use_global_init=True):
    """
    自适应 EMA 计算函数 (V2.0 - 双向稳态控制版)
    :param span: 平滑窗口大小
    :param use_global_init: 是否使用全局均值初始化
    """
    n = len(counts)
    if n == 0: return []
    alpha = 2/(span+1)
    res = []
    
    # 计算全局均值
    g_n, g_d = np.sum(counts), np.sum(totals)
    base = g_n/g_d if g_d>0 else 0
    
    t_d = totals[0]
    
    # 1. 初始化逻辑
    first_rate = (counts[0]/totals[0]) if totals[0]>0 else 0
    if use_global_init:
        t_n = t_d * base
        res.append(0.5 * base + 0.5 * first_rate)
    else:
        t_n = t_d * first_rate
        res.append(first_rate)

    # 2. 迭代计算
    for i in range(1, n):
        rn, rd = counts[i], totals[i]
        if rd == 0: res.append(res[-1]); continue
        
        rr = rn/rd
        p_base = t_n/t_d if t_d>0 else 0
        
        # ======================================================================
        # [核心优化] 双向异常检测 (Bi-directional Outlier Detection)
        # ======================================================================
        
        
        is_surge_abs = abs(rr - p_base) > 0.02 # A. 绝对值容差检测: 波动幅度不能超过 ±0.02 (2%)
        is_surge_ratio = (rr > p_base * 3.0) # B. 相对值容差检测: 波动幅度不能超过 3 倍
        is_plunge_ratio = (rr < p_base / 3.0) or (rr < 1e-4) # C. 相对值容差检测: 波动幅度不能超过 1/3 倍 或 非常小 (1e-4)

        is_abnormal = is_surge_abs or is_surge_ratio or is_plunge_ratio
        
        if is_abnormal:
            # [异常处理] 
            # 遇到暴涨或暴跌：
            # 1. 动量 (t_n, t_d) 强保持：只吸收极少量的当前数据 (alpha * p_base * rd)
            #    这相当于假设"真实情况"依然维持在 p_base 水平
            cn = p_base * rd 
            t_n = alpha * cn + (1-alpha) * t_n
            t_d = alpha * rd + (1-alpha) * t_d
            
            # 2. 输出值 (res) 弱跟随：允许显示一点点波动，避免线条完全死掉，
            #    但幅度会被 alpha (约0.1~0.3) 大幅削弱
            dn = alpha * rn + (1-alpha) * t_n
            dd = alpha * rd + (1-alpha) * t_d
            res.append(dn/dd if dd>0 else 0)
            
        else:
            # [正常处理]
            # 正常更新 EMA
            t_n = alpha * rn + (1-alpha) * t_n
            t_d = alpha * rd + (1-alpha) * t_d
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
    
    【核心逻辑说明】：
    虽然用户在配置文件 (ovs) 中输入的是“期望的覆盖不良率 (Rate)”，
    但为了保持整个流水线数据的“物质守恒”，本函数会将用户输入的“率”
    反向计算为具体的“不良数 (Count)”，并覆盖掉原有的不良数。

    参数:
        df: 聚合后的 DataFrame (内部包含 'total_panels' 列和各 Group 的不良数 列)
        ovs: 覆盖值字典 {group: {period: rate}} (用户配置的覆盖率)
        period_type: 'monthly' 或 'weekly' (用于决定时间Key的格式)
        **kwargs: 包含 target_defects 等参数

    返回:
        应用覆盖后的 DataFrame：df 中各 Group 列的数据依然被强制更新为**不良数**，而非不良率。
    """
    if not ovs or df.empty:
        logging.info(f"[覆盖逻辑] 跳过覆盖: ovs={'存在' if ovs else '不存在'}, df={'非空' if not df.empty else '空'}")
        return df

    df = df.copy()
    targets = kwargs.get('target_defects', [])
    applied_count = 0
    
    # 遍历需要处理的缺陷组 (如 Array_Line, OLED_Mura)
    for g in targets:
        if g not in df.columns:
            logging.warning(f"[覆盖逻辑] ⚠️ 缺陷组 '{g}' 不在 DataFrame 列中")
            continue
        if g not in ovs:
            logging.info(f"[覆盖逻辑] 缺陷组 '{g}' 没有覆盖配置")
            continue
            
        # 遍历 DataFrame 中的每一行（每个时间周期）
        for idx in df.index:
            # 1. 构造用于匹配字典的 Time Key (如 '2026-02' 或 '2026-W05')
            k = idx.strftime('%Y-%m') if period_type=='monthly' else f"{idx.isocalendar()[0]}-W{idx.isocalendar()[1]:02d}"
            
            # 2. 从用户的配置字典中尝试获取该周期的期望不良率
            v = ovs[g].get(k)
            
            # 3. 如果找到了覆盖率，执行核心的【率转数】逻辑
            if v is not None:
                old_val = df.loc[idx, g]
                
                # 【关键点】: 覆盖数 = 期望不良率(v) * 当期实际入库数(total_panels)，并四舍五入取整
                df.loc[idx, g] = int(np.round(v * df.loc[idx, 'total_panels']))
                
                applied_count += 1
                logging.info(f"[覆盖逻辑] ✓ 应用覆盖: {g} @ {k}: rate: {v}")
                
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