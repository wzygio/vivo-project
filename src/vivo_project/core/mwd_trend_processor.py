# src/vivo_project/core/mwd_trend_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Callable, Tuple
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
        mwd_code_data: Dict[str, pd.DataFrame] | None,
        config: AppConfig,
        scaling_factor: float,
        volatility: float = 0.1,
    ) -> Dict[str, pd.DataFrame] | None:
        
        logging.info("Group级趋势分析 (模式: 大一统混合流水线 Unified Hybrid Pipeline)...")
        if panel_details_df.empty or not mwd_code_data: return None
        
        try:
            raw_daily, last_day, target_defects = _prepare_group_raw_data(panel_details_df)
            if raw_daily is None: return None

            m_vals = config.processing.get('group_monthly_values', {})
            w_vals = config.processing.get('group_weekly_values', {})
            d_vals = config.processing.get('group_daily_values', {})

            def _inject_code_daily_as_base(df_proc: pd.DataFrame) -> pd.DataFrame:
                skeleton = df_proc[['total_panels']].copy()
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

            # [架构精简] 彻底废弃 Group 级趋势调节器适配器
            final_monthly, final_weekly, final_daily = _execute_unified_pipeline(
                raw_daily_df=raw_daily,
                last_day=last_day,
                calc_daily_ema_func=_inject_code_daily_as_base,
                agg_funcs=(
                    lambda d, t: _safe_trend_aggregator(d, t, 'M', is_group_level=True),
                    lambda d, t: _safe_trend_aggregator(d, t, 'W', is_group_level=True)
                ),
                reg_func=lambda d, **kw: d,                     
                override_funcs=(_apply_manual_overrides, _apply_manual_overrides),
                override_vals=(m_vals, w_vals),
                gen_daily_func=_generate_daily_from_weekly_baseline,
                scaling_factor=scaling_factor,
                volatility=volatility,
                target_defects=target_defects                   
            )

            daily = _apply_daily_manual_overrides(final_daily, d_vals, target_defects)
            return _format_group_results(final_monthly, final_weekly, daily, target_defects)
            
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
        volatility: float = 0.1,
        warning_lines: dict = None  # type: ignore
    ) -> Dict[str, pd.DataFrame] | None:    
        
        logging.info("Code级趋势分析 (模式: 大一统混合流水线 Unified Hybrid Pipeline)...")
        if panel_details_df.empty: return None
        
        try:
            raw_daily, last_day = _prepare_code_raw_data(panel_details_df)
            if raw_daily is None: return None

            agg_monthly_func = lambda d, t: _safe_trend_aggregator(d, t, 'M', is_group_level=False)
            agg_weekly_func  = lambda d, t: _safe_trend_aggregator(d, t, 'W', is_group_level=False)

            m_vals = config.processing.get('code_monthly_values', {})
            w_vals = config.processing.get('code_weekly_values', {})
            d_vals = config.processing.get('code_daily_values', {})

            monthly, weekly, daily = _execute_unified_pipeline(
                raw_daily_df=raw_daily,
                last_day=last_day,
                calc_daily_ema_func=lambda df: _calc_code_ema_noise(df, ema_span, scaling_factor, volatility),
                agg_funcs=(agg_monthly_func, agg_weekly_func),
                reg_func=TrendRegulator.regulate_code_daily_base,
                override_funcs=(_apply_code_manual_overrides, _apply_code_manual_overrides),
                override_vals=(m_vals, w_vals),
                gen_daily_func=_generate_code_daily_from_weekly_baseline,
                scaling_factor=1.0, 
                volatility=volatility,
                warning_lines=warning_lines
            )

            daily = _apply_code_daily_manual_overrides(daily, d_vals)
            return _format_code_results(monthly, weekly, daily)

        except Exception as e:
            logging.error(f"Code趋势分析出错: {e}", exc_info=True)
            return None
        
# ==============================================================================
#  核心策略流水线 (Generic Pipelines)
# ==============================================================================
def _execute_unified_pipeline(
    raw_daily_df: pd.DataFrame,
    last_day: dt | None,
    calc_daily_ema_func: Callable,
    agg_funcs: Tuple[Callable, Callable],
    reg_func: Callable,
    override_funcs: Tuple[Callable, Callable],
    override_vals: Tuple[dict, dict],
    gen_daily_func: Callable,
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    [重构 V4.0] 大一统混合流水线 (Unified Hybrid Pipeline)
    执行顺序翻转：EMA 洗底 -> 底层物理截断 -> 向上聚合 W/M -> 宏观定调与重塑
    """
    df_processing, last_day = _apply_t1_filtering(raw_daily_df, last_day)
    if df_processing.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    is_group = 'defect_desc' not in df_processing.columns
    df_processing = _pad_daily_data_to_today(df_processing, is_group)
    
    eval_last_day = pd.to_datetime(dt.now().date())
    
    # --- Step 1: EMA 洗底 ---
    ema_daily_base = calc_daily_ema_func(df_processing)
    
    # =========================================================================
    # 🛑 [核心重构] 先截断，再聚合
    # =========================================================================
    # --- Step 1.5: 底层数据软截断 (Regulation) ---
    reg_daily = reg_func(ema_daily_base, **kwargs) # 仅针对底层日度基底拦截压制
    
    # --- Step 2: 严格向上聚合 ---
    agg_monthly_func, agg_weekly_func = agg_funcs
    ov_func_m, ov_func_w = override_funcs
    val_m, val_w = override_vals
    
    # [物理定律保障]: 现在的 W 和 M 完全是由经过 Spec 截断的安全日度数据累加而成，杜绝数据倒挂
    reg_monthly = agg_monthly_func(reg_daily, eval_last_day) 
    reg_weekly = agg_weekly_func(reg_daily, eval_last_day)   
    
    factor = kwargs.get('scaling_factor', 1.0)
    if factor != 1.0:
        reg_monthly = _apply_scaling(reg_monthly, factor)
        reg_weekly = _apply_scaling(reg_weekly, factor)
        
    # --- Step 3: 人工定调 (周度为主控) ---
    period_kw_w = {'period_type': 'weekly'}
    valid_ov_keys = ['target_defects']
    extra_ov_args = {k: v for k, v in kwargs.items() if k in valid_ov_keys}
    
    final_weekly = ov_func_w(reg_weekly, val_w, **period_kw_w, **extra_ov_args)
    
    # --- Step 4: 降维重塑 (带精确旁路与防丢失拼接) ---
    final_daily = reg_daily.copy()
    
    if val_w: # 检测到覆盖配置
        overridden_keys = list(val_w.keys())
        logging.info(f"检测到人工覆盖指令，仅对以下目标启动重塑: {overridden_keys}")
        
        if 'warehousing_time' in df_processing.columns:
            # ================= [Code 级逻辑 (长表)] =================
            daily_skeleton = df_processing[['warehousing_time', 'total_panels']].drop_duplicates()
            
            # [核心修复 1] 连坐阻断：只截取被覆盖的 Code 送去重塑
            weekly_to_rebuild = final_weekly[final_weekly['defect_desc'].isin(overridden_keys)].copy()
            
            if not weekly_to_rebuild.empty:
                generated_daily = gen_daily_func(daily_skeleton, weekly_to_rebuild, **kwargs)
                
                if not generated_daily.empty:
                    # [核心修复 2] 黑洞阻断：彻底放弃 update()，改用物理剔除 + 物理追加 (concat)
                    # 先将旧的 EMA 大盘中被覆盖的 Code 连根拔起
                    final_daily = final_daily[~final_daily['defect_desc'].isin(overridden_keys)].copy()
                    # 再把重塑出来的（包含新增日期行）的完整新数据追加进去
                    final_daily = pd.concat([final_daily, generated_daily], ignore_index=True)
                    
        else:
            # ================= [Group 级逻辑 (宽表)] =================
            daily_skeleton = df_processing[['total_panels']].copy()
            
            # [核心修复 1] 连坐阻断：动态修改目标参数，让正弦波只重塑被覆盖的 Group 列
            kwargs_for_group = kwargs.copy()
            kwargs_for_group['target_defects'] = overridden_keys 
            
            generated_daily = gen_daily_func(daily_skeleton, final_weekly, **kwargs_for_group)
            
            if not generated_daily.empty:
                # [核心修复 2] 宽表直接按列暴力覆盖，不存在黑洞问题
                for g in overridden_keys:
                    if g in generated_daily.columns:
                        final_daily[g] = generated_daily[g]
    else:
        logging.info("未检测到覆盖指令，完美保留纯净 EMA 日度曲线。")
    
    # --- Step 5: 月度重构与定调 ---
    reaggregated_monthly = agg_monthly_func(final_daily, last_day)
    period_kw_m = {'period_type': 'monthly'}
    final_monthly = ov_func_m(reaggregated_monthly, val_m, **period_kw_m, **extra_ov_args)
    
    return final_monthly, final_weekly, final_daily

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
    last_day: dt | None, 
    conditional_filter: bool = True  
) -> Tuple[pd.DataFrame, dt | None]:
    """
    [通用复用函数] 执行 T-1 末日过滤策略。
    """
    if df.empty:
        return df, last_day

    df_filtered = df.copy()
    new_anchor = last_day
    
    # 获取数据源中实际的最后一天日期
    if 'warehousing_time' in df_filtered.columns:
        actual_last_date = df_filtered['warehousing_time'].max()
        # [Bug Fix] 防止 Code 级长表中的 total_panels 被错误累加，直接取单行的值即可
        last_day_volume = df_filtered[df_filtered['warehousing_time'] == actual_last_date]['total_panels'].iloc[0] 
    else:
        actual_last_date = df_filtered.index.max()
        if isinstance(df_filtered, pd.Series):
            last_day_volume = 0
        elif 'total_panels' in df_filtered.columns:
            last_day_volume = df_filtered.loc[actual_last_date, 'total_panels']
            if isinstance(last_day_volume, pd.Series): 
                # [Bug Fix] 针对宽表如果有重复索引防呆，取第一个有效值
                last_day_volume = last_day_volume.iloc[0] 
        else:
            last_day_volume = 0

    # === [核心逻辑分支] ===
    should_filter = True
    
    # -------------------------------------------------------------------------
    # ✅ [修复逻辑]：时间距离防呆校验
    # -------------------------------------------------------------------------
    real_last_day = pd.to_datetime(dt.now().date())
    target_date = pd.to_datetime(actual_last_date)
    days_diff = (real_last_day - target_date).days
    
    # [架构铁律修复]：如果 days_diff > 0，说明 actual_last_date 是昨天或更早。
    # 只要不是现实世界的今天，该日期的产出就是闭合的既定事实，无条件豁免，绝不剔除！
    if days_diff > 0: 
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

def _pad_daily_data_to_today(df: pd.DataFrame, is_group_level: bool) -> pd.DataFrame:
    """
    [核心新增] 在进入计算流水线前，将日期网格强行对齐到现实世界中的"今天"。
    保证 EMA 动量传递和向上聚合(周/月)在数学上的绝对连续性。
    """
    if df.empty: return df
    df_out = df.copy()
    real_today = pd.to_datetime(dt.now().date())

    if is_group_level:
        # --- Group Level (宽表补齐) ---
        has_dt_index = isinstance(df_out.index, pd.DatetimeIndex)
        if not has_dt_index:
            if 'warehousing_time' in df_out.columns:
                df_out.set_index('warehousing_time', inplace=True)
        
        min_date = df_out.index.min()
        full_dates = pd.date_range(start=min_date, end=real_today, freq='D')
        
        # 强行拉伸索引并用 0 填充没产出的日子
        df_out = df_out.reindex(full_dates).fillna(0)
        df_out.index.name = 'warehousing_time'
        return df_out
    else:
        # --- Code Level (长表笛卡尔积补齐) ---
        min_date = df_out['warehousing_time'].min()
        full_dates = pd.date_range(start=min_date, end=real_today, freq='D')

        # 提取当前数据中的所有 Code
        unique_codes = df_out[['defect_group', 'defect_desc']].drop_duplicates()
        
        # 构建连续的每日真实总产出 (没数据的日子补为0)
        daily_totals = df_out[['warehousing_time', 'total_panels']].drop_duplicates().set_index('warehousing_time')
        daily_totals = daily_totals.reindex(full_dates).fillna(0).reset_index()
        daily_totals.columns = ['warehousing_time', 'total_panels']

        # 构造笛卡尔积网格：每一天都包含所有的 Code
        daily_totals['_key'] = 1
        unique_codes['_key'] = 1
        full_grid = pd.merge(daily_totals, unique_codes, on='_key').drop(columns=['_key'])

        # 将真实数据 Merge 回网格
        merged = pd.merge(
            full_grid,
            df_out[['warehousing_time', 'defect_desc', 'defect_panel_count']],
            on=['warehousing_time', 'defect_desc'],
            how='left'
        )
        merged['defect_panel_count'] = merged['defect_panel_count'].fillna(0).astype(int)
        return merged
    

def _calc_code_ema_noise(
    raw_df: pd.DataFrame, 
    span: int, 
    scale: float,
    volatility: float
) -> pd.DataFrame:
    """Code 级 EMA 计算 + 噪声注入 (带 DEBUG 拦截器)"""
    ema_df = raw_df.copy()
    ema_df['attenuated_rate'] = 0.0
    unique_codes = ema_df['defect_desc'].unique()
    
    # [新增] 用于收集所有 Code 的 EMA 调试数据
    debug_frames = []
    
    for code in unique_codes:
        if code == "NoDefect": continue
        mask = ema_df['defect_desc'] == code
        sub = ema_df[mask].sort_values('warehousing_time')
        
        # 核心算法调用
        smooth = _calculate_adaptive_shadow_ema(
            sub['defect_panel_count'].values, sub['total_panels'].values, span
        )
        
        # =========================================================================
        # 🛑 [DEBUG 日志与 CSV 导出 - 增强版]
        # =========================================================================
        if debug_frames:
            try:
                debug_df = pd.concat(debug_frames, ignore_index=True)
                
                # 确保按时间升序，方便您在 Excel 画图
                debug_df['Date'] = pd.to_datetime(debug_df['Date'])
                debug_df = debug_df.sort_values(by=['Code', 'Date'])
                debug_df['Date'] = debug_df['Date'].dt.strftime('%Y-%m-%d')
                
                out_path = Path("logs/debug_ema_rates_full.csv")
                out_path.parent.mkdir(parents=True, exist_ok=True)
                
                export_df = debug_df.copy()
                # 将浮点数格式化为易读的百分比，避免科学计数法
                export_df['Raw_Rate'] = export_df['Raw_Rate'].apply(lambda x: f"{x:.5%}")
                export_df['EMA_Rate'] = export_df['EMA_Rate'].apply(lambda x: f"{x:.5%}")
                
                export_df.to_csv(out_path, index=False, encoding='utf-8-sig')
                logging.info(f"✅ [DEBUG] 完整近3个月 EMA 明细已导出至: {out_path.absolute()}")
            except Exception as e:
                logging.error(f"导出 EMA debug 数据失败: {e}")
        # =========================================================================

        ema_df.loc[sub.index, 'attenuated_rate'] = np.array(smooth) * scale
        
    # =========================================================================
    # 🛑 [DEBUG 日志与 CSV 导出]
    # =========================================================================
    if debug_frames:
        try:
            debug_df = pd.concat(debug_frames, ignore_index=True)
            
            # 1. 导出完整数据到 CSV，方便您在 Excel 中插入折线图进行直观对比
            out_path = Path("logs/debug_ema_rates.csv")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 将浮点数格式化为百分比字符串，提升 Excel 阅读体验
            export_df = debug_df.copy()
            export_df['Raw_Rate'] = export_df['Raw_Rate'].apply(lambda x: f"{x:.4%}")
            export_df['EMA_Rate'] = export_df['EMA_Rate'].apply(lambda x: f"{x:.4%}")
            export_df.to_csv(out_path, index=False, encoding='utf-8-sig')
            
            logging.info(f"✅ [DEBUG] EMA 算法明细已导出至: {out_path.absolute()}")
            
            # 2. 挑一个有数据的 Code，在控制台快速预览前 10 天的数据
            sample_code = debug_df['Code'].iloc[0]
            sample_df = export_df[export_df['Code'] == sample_code].head(10)
            logging.info(f"--- [DEBUG] EMA 抽样展示 (Code: {sample_code}, Span: {span}) ---\n{sample_df.to_string(index=False)}")
        except Exception as e:
            logging.error(f"导出 EMA debug 数据失败: {e}")
    # =========================================================================
    
    ema_df['defect_panel_count'] = np.round(ema_df['attenuated_rate'] * ema_df['total_panels']).astype(int)
    return _inject_deterministic_noise_code_level(ema_df, volatility)

# ==============================================================================
#  数据准备与格式化 (Helpers)
# ==============================================================================

def _prepare_group_raw_data(df: pd.DataFrame):
    """提取 Group 级 Raw Data (Wide Format)"""
    df = df.copy()
    df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
    last_day = df['warehousing_time'].max()
    
    # --- [新增调试日志: 抓出 2-26 消失的真相] ---
    try:
        logging.info(f"========== [DEBUG: 数据源时间边界排查] ==========")
        dates_in_df = df['warehousing_time'].dt.strftime('%Y-%m-%d').unique()
        logging.info(f"提取的数据总行数: {len(df)}")
        logging.info(f"数据源中【实际存在的最后日期】: {last_day.strftime('%Y-%m-%d')}")
        logging.info(f"数据源中最近的 5 个有效日期: {sorted(dates_in_df)[-5:]}")
    except: pass
    # ---------------------------------------------

    raw_daily = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame(name='total_panels') # type: ignore
    daily_defect = df.groupby([df['warehousing_time'].dt.date, 'defect_group'])['panel_id'].nunique().unstack(level='defect_group').fillna(0) # type: ignore
    raw_daily = pd.concat([raw_daily, daily_defect], axis=1).fillna(0)
    raw_daily.index = pd.to_datetime(raw_daily.index)
            
    target_defects = sorted(df['defect_group'].dropna().unique().tolist())
    return raw_daily, last_day, target_defects

def _prepare_code_raw_data(df: pd.DataFrame):
    """提取 Code 级 Raw Data (Long Format)"""
    df = df.copy()
    df['warehousing_time'] = pd.to_datetime(df['warehousing_time'], format='%Y%m%d')
    last_day = pd.to_datetime(dt.now().date())

    d_total = df.groupby(df['warehousing_time'].dt.date)['panel_id'].nunique().to_frame('total_panels') # type: ignore
    d_code = df.groupby([df['warehousing_time'].dt.date, 'defect_group', 'defect_desc'])['panel_id'].nunique().to_frame('defect_panel_count') # type: ignore
    
    raw_daily = pd.merge(d_total.reset_index(), d_code.reset_index(), on='warehousing_time', how='left')
    raw_daily['defect_panel_count'].fillna(0, inplace=True)
    raw_daily['warehousing_time'] = pd.to_datetime(raw_daily['warehousing_time'])

    raw_daily['defect_group'].fillna("NoDefect", inplace=True)
    raw_daily['defect_desc'].fillna("NoDefect", inplace=True)
    
    return raw_daily, last_day

def _format_group_results(monthly, weekly, daily, target_defects):
    """(V3 极简版) 仅负责 UI 格式化与尾部切片，不再做合成补齐"""
    def _fmt(agg_df, fmt, n_tail):
        if agg_df.empty: return pd.DataFrame()
        df = agg_df.copy()
        
        # 确保时间维度作为列
        if 'warehousing_time' not in df.columns:
            df = df.reset_index()
            if 'index' in df.columns: df.rename(columns={'index': 'warehousing_time'}, inplace=True)
            elif 'level_0' in df.columns: df.rename(columns={'level_0': 'warehousing_time'}, inplace=True)

        for group in target_defects:
            if group in df.columns:
                df[f"{group.lower()}_rate"] = np.where(df['total_panels'] > 0, df[group] / df['total_panels'], 0.0)
            else:
                df[f"{group.lower()}_rate"] = 0.0
                df[group] = 0 # 防御性填充，防止 melt 失败
        
        if fmt == 'ISO':
            iso = df['warehousing_time'].dt.isocalendar()
            df['time_period'] = iso.year.astype(str) + '-W' + iso.week.map('{:02d}'.format)
        else:
            df['time_period'] = df['warehousing_time'].dt.strftime(fmt)
        
        rmap = {f"{g.lower()}_rate": g for g in target_defects}
        melted = df.melt(
            id_vars=['time_period', 'total_panels'],
            value_vars=list(rmap.keys()),
            var_name='defect_group_raw',
            value_name='defect_rate'
        )
        melted['defect_group'] = melted['defect_group_raw'].map(rmap)
        
        # =========================================================================
        # 🛑 [核心修复 1] 时间切片截取逻辑重构
        # =========================================================================
        # 依赖原始的物理时间列(warehousing_time) 进行排序，然后提取去重后的时段字符串，
        # 彻底解决跨年时 '%m-%d' 字符串字典序（如 12-31 > 03-13）导致的截取错误。
        ordered_periods = df.sort_values('warehousing_time')['time_period'].drop_duplicates().tolist()
        target_periods = ordered_periods[-n_tail:] if len(ordered_periods) > n_tail else ordered_periods
        
        melted = melted[melted['time_period'].isin(target_periods)]
        melted['time_period'] = pd.Categorical(melted['time_period'], categories=target_periods, ordered=True)
        return melted.sort_values(by=['time_period', 'defect_group']).reset_index(drop=True)

    return {
        'monthly': _fmt(monthly, '%Y-%m月', 3),
        'weekly': _fmt(weekly, 'ISO', 3),
        'daily_full': _fmt(daily, '%Y-%m-%d', 9999), 
        'daily': _fmt(daily, '%m-%d', 7)
    }

def _format_code_results(monthly, weekly, daily):
    """(V3 极简版) 仅负责 UI 格式化与尾部切片"""
    def _fmt(df, fmt, n_tail):
        if df.empty: return pd.DataFrame()
        df_out = df.copy()
        if fmt == 'ISO':
            iso = df_out['warehousing_time'].dt.isocalendar()
            df_out['time_period'] = iso.year.astype(str) + '-W' + iso.week.map('{:02d}'.format)
        else:
            df_out['time_period'] = df_out['warehousing_time'].dt.strftime(fmt)
            
        df_out['defect_rate'] = np.where(df_out['total_panels'] > 0, df_out['defect_panel_count'] / df_out['total_panels'], 0.0)
        df_out = df_out[df_out['defect_desc'] != 'NoDefect']

        # =========================================================================
        # 🛑 [核心修复 2] Code 级同步修复时间切片逻辑
        # =========================================================================
        ordered_periods = df_out.sort_values('warehousing_time')['time_period'].drop_duplicates().tolist()
        target_periods = ordered_periods[-n_tail:] if len(ordered_periods) > n_tail else ordered_periods

        df_out = df_out[df_out['time_period'].isin(target_periods)]
        df_out['time_period'] = pd.Categorical(df_out['time_period'], categories=target_periods, ordered=True)
        return df_out.sort_values(by=['time_period', 'defect_group', 'defect_desc']).reset_index(drop=True)

    return {
        'monthly': _fmt(monthly, '%Y-%m月', 3),
        'weekly': _fmt(weekly, 'ISO', 3),
        'daily_full': _fmt(daily, '%Y-%m-%d', 9999),
        'daily': _fmt(daily, '%m-%d', 7)
    }

# ==============================================================================
#  底层逻辑 (Generators, Noise, EMA) - 保持不变
# ==============================================================================
def _generate_daily_from_weekly_baseline(daily_skeleton, weekly_final, target_defects, volatility, **kwargs):
    """
    [策略 A] Group 级日度数据生成器 (V4.1 - 绝对守恒版)
    引入“最大余额法 (Largest Remainder Method)”，确保每日随机波动的不良数之和，
    绝对等于当周的总不良数。
    """
    df_gen = daily_skeleton.copy()
    df_gen['week_period'] = df_gen.index.to_period('W-SUN') # type: ignore
    
    weekly_lookup = weekly_final.copy()
    weekly_lookup.index = weekly_lookup.index.to_period('W-SUN')

    for group in target_defects:
        if group not in weekly_lookup.columns: continue
        df_gen[group] = 0
        
        for week_idx in weekly_lookup.index:
            # [核心修正] 直接提取周目标数量，而不是基准率
            w_count = weekly_lookup.loc[week_idx, group]
            
            mask = df_gen['week_period'] == week_idx
            days_in_week = df_gen[mask]
            if days_in_week.empty: continue
            
            day_indices = days_in_week.index
            panels = days_in_week['total_panels'].values
            
            # 如果当周目标为0，或当周无产出，直接赋0跳过
            if w_count <= 0 or panels.sum() <= 0:
                df_gen.loc[day_indices, group] = 0
                continue
            
            # 1. 计算伪随机波动权重
            ts_seeds = np.array([int(d.timestamp() / 86400) for d in day_indices])
            scramble_factor = 1234567 
            stable_group_hash = sum(ord(c) for c in str(group))
            noise_seeds = (ts_seeds * scramble_factor) + (stable_group_hash % 9999)
            noises = np.sin(noise_seeds) * volatility
            
            # 2. 计算加权权重并归一化
            raw_weights = np.maximum(0, panels * (1 + noises))
            weight_sum = raw_weights.sum()
            
            if weight_sum > 0:
                norm_weights = raw_weights / weight_sum
            else:
                norm_weights = panels / panels.sum()
                
            # 3. 🎯 最大余额法分配 (确保总和绝对等于 w_count)
            exact_alloc = w_count * norm_weights
            int_alloc = np.floor(exact_alloc).astype(int)
            rem = int(w_count - int_alloc.sum())
            
            if rem > 0:
                frac = exact_alloc - int_alloc
                # 找出小数部分最大的前 rem 个元素的索引
                top_indices = np.argsort(frac)[-rem:]
                int_alloc[top_indices] += 1
                
            df_gen.loc[day_indices, group] = int_alloc
            
    df_gen.drop(columns=['week_period'], inplace=True)
    return df_gen

def _generate_code_daily_from_weekly_baseline(daily_skeleton, weekly_final, volatility, **kwargs):
    """
    [性能优化版] Code 级日度数据生成器 (V4.1 - 绝对守恒版)
    使用全向量化的“最大余额法”，在保持百万级数据处理性能的同时，
    确保单日数据之和与周度总基准绝对闭环对齐。
    """
    if daily_skeleton.empty or weekly_final.empty:
        return pd.DataFrame(columns=['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count'])

    weekly_data = weekly_final.copy()
    weekly_data['week_period'] = weekly_data['warehousing_time'].dt.to_period('W-SUN')
    
    unique_codes = weekly_data[['defect_group', 'defect_desc']].drop_duplicates()

    # 1. 构建 "日期 x Code" 笛卡尔积
    daily_skeleton_tmp = daily_skeleton.copy()
    daily_skeleton_tmp['_key'] = 1
    unique_codes_tmp = unique_codes.copy()
    unique_codes_tmp['_key'] = 1
    
    full_grid = pd.merge(daily_skeleton_tmp, unique_codes_tmp, on='_key').drop(columns='_key')
    full_grid['week_period'] = full_grid['warehousing_time'].dt.to_period('W-SUN') # type: ignore
    
    # 2. [核心修正] 合并周度目标绝对数量，而不是单纯的率
    weekly_targets = weekly_data[['week_period', 'defect_desc', 'defect_panel_count']].rename(
        columns={'defect_panel_count': 'target_w_count'}
    )
    
    merged = pd.merge(full_grid, weekly_targets, on=['week_period', 'defect_desc'], how='left')
    merged['target_w_count'] = merged['target_w_count'].fillna(0)
    
    # 3. 向量化噪声计算
    ts_vector = (merged['warehousing_time'].astype('int64') // 10**9 // 86400).astype(int)
    scramble_factor = 999983 
    def _stable_hash(s): return sum(ord(c) for c in str(s))
    code_hash = merged['defect_desc'].map(_stable_hash) % 10000
    phase = (ts_vector * scramble_factor) + code_hash
    noise = np.sin(phase) * volatility
    
    # 4. 计算分配权重
    merged['raw_weight'] = np.maximum(0, merged['total_panels'] * (1 + noise))
    weight_sums = merged.groupby(['week_period', 'defect_desc'])['raw_weight'].transform('sum')
    panel_sums = merged.groupby(['week_period', 'defect_desc'])['total_panels'].transform('sum')
    
    merged['norm_weight'] = np.where(
        weight_sums > 0, 
        merged['raw_weight'] / weight_sums,
        np.where(
            panel_sums > 0,
            merged['total_panels'] / panel_sums,
            0.0
        )
    )
    
    # 5. 🎯 [核心修正] 向量化最大余额法 (Vectorized Largest Remainder)
    merged['exact_alloc'] = merged['target_w_count'] * merged['norm_weight']
    merged['int_alloc'] = np.floor(merged['exact_alloc']).astype(int)
    merged['frac'] = merged['exact_alloc'] - merged['int_alloc']
    
    # 计算每个分组需要补齐的余数 (必定是 >= 0 的整数)
    rem_series = merged['target_w_count'] - merged.groupby(['week_period', 'defect_desc'])['int_alloc'].transform('sum')
    merged['rem'] = rem_series.astype(int)
    
    # 对每个分组内的小数部分进行降序排名 (遇到相同值时按出现顺序排)
    merged['frac_rank'] = merged.groupby(['week_period', 'defect_desc'])['frac'].rank(method='first', ascending=False)
    
    # 如果排名 <= 余数，则该天分得 1 个补偿不良数
    merged['defect_panel_count'] = merged['int_alloc'] + np.where(merged['frac_rank'] <= merged['rem'], 1, 0)
    
    # 6. 结果清理
    final_df = merged[['warehousing_time', 'total_panels', 'defect_group', 'defect_desc', 'defect_panel_count']]
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
    # ✅ 替换为稳定哈希：
    def _stable_hash(s): return sum(ord(c) for c in str(s))
    code_hash = code_series.map(_stable_hash) % 1000
    
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
    
    # 计算全局平均良损
    g_n, g_d = np.sum(counts), np.sum(totals)
    base = g_n/g_d if g_d>0 else 0
    
    t_d = totals[0]
    
    # 1. 初始化逻辑
    first_rate = (counts[0]/totals[0]) if totals[0]>0 else 0 # 计算第一天的良损
    if use_global_init: # 使用平均良损计算初始动量
        t_n = t_d * base
        res.append(0.5 * base + 0.5 * first_rate)
    else: # 使用第一天的良损计算初始动量(原始方法)
        t_n = t_d * first_rate
        res.append(first_rate)

    # 2. 迭代计算
    for i in range(1, n):
        rn, rd = counts[i], totals[i] # rn为当日真实不良数，rd为当日真实入库数
        if rd == 0: res.append(res[-1]); continue
        
        rr = rn/rd # 当日真实良损
        p_base = t_n/t_d if t_d>0 else 0 # 昨日良损(根据昨日动量计算)
        
        # ======================================================================
        # [核心优化] 双向异常检测 (Bi-directional Outlier Detection)
        # ======================================================================
        is_surge_abs = abs(rr - p_base) > 0.05 # A. 绝对值容差检测: 波动幅度不能超过 ±0.02 (2%)
        is_surge_ratio = (rr > p_base * 5.0) # B. 相对值容差检测: 波动幅度不能超过 3 倍
        is_plunge_ratio = (rr < p_base / 3.0) or (rr < 1e-4) # C. 相对值容差检测: 波动幅度不能超过 1/3 倍 或 非常小 (1e-4)

        is_abnormal = is_surge_abs or is_surge_ratio or is_plunge_ratio
        
        if is_abnormal:
            # [异常处理] 
            cn = p_base * rd # 使用昨日不良数(根据昨日动量计算)
            t_n = alpha * cn + (1-alpha) * t_n # 历史不良动量(不良数)
            t_d = alpha * rd + (1-alpha) * t_d # 历史入库动量(入库数)
            res.append(t_n/t_d if t_d>0 else 0)
            
        else:
            # [正常处理]
            t_n = alpha * rn + (1-alpha) * t_n
            t_d = alpha * rd + (1-alpha) * t_d
            res.append(t_n/t_d if t_d>0 else 0)
            
    return res

def _apply_manual_overrides(df, ovs, period_type, **kwargs):
    """
    应用手动覆盖值到聚合数据 (兼容用户不带前导零的输入)
    """
    if not ovs or df.empty:
        logging.info(f"[覆盖逻辑] 跳过覆盖: ovs={'存在' if ovs else '不存在'}, df={'非空' if not df.empty else '空'}")
        return df

    df = df.copy()
    targets = kwargs.get('target_defects', [])
    applied_count = 0
    
    # 遍历需要处理的缺陷组
    for g in targets:
        if g not in df.columns:
            logging.warning(f"[覆盖逻辑] ⚠️ 缺陷组 '{g}' 不在 DataFrame 列中")
            continue
        if g not in ovs:
            logging.info(f"[覆盖逻辑] 缺陷组 '{g}' 没有覆盖配置")
            continue
            
        # 遍历 DataFrame 中的每一行
        for idx in df.index:
            # 1. 构造用于匹配的 Time Key (支持标准填充格式与业务常用缩写格式) # 兼容 '2026-1' 与 '2026-01'
            if period_type == 'monthly':
                k_padded = idx.strftime('%Y-%m')             # 格式: '2026-01'
                k_unpadded = f"{idx.year}-{idx.month}"       # 格式: '2026-1'
            else:
                year, week, _ = idx.isocalendar()
                k_padded = f"{year}-W{week:02d}"             # 格式: '2026-W04'
                k_unpadded = f"{year}-W{week}"               # 格式: '2026-W4'
            
            # 2. 尝试获取期望不良率 (优先匹配标准格式，如果找不到再尝试匹配不带前导零的格式)
            v = ovs[g].get(k_padded)
            if v is None:
                v = ovs[g].get(k_unpadded)
            
            # 3. 如果找到了覆盖率，执行核心的【率转数】逻辑
            if v is not None:
                old_val = df.loc[idx, g]
                df.loc[idx, g] = int(np.round(v * df.loc[idx, 'total_panels']))
                applied_count += 1
                logging.info(f"[覆盖逻辑] ✓ 应用覆盖: {g} @ {k_padded}: rate: {v}")
                
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
            
            # 同样在 Code 级防呆处理前导零问题 # 同步修改以防御未来 Code 级长表的 Excel 错误
            if period_type == 'monthly':
                k_padded = d_val.strftime('%Y-%m')
                k_unpadded = f"{d_val.year}-{d_val.month}"
            else:
                year, week, _ = d_val.isocalendar()
                k_padded = f"{year}-W{week:02d}"
                k_unpadded = f"{year}-W{week}"
                
            v = map_t.get(k_padded)
            if v is None: 
                v = map_t.get(k_unpadded)
                
            if v is not None: 
                df.loc[idx, 'defect_panel_count'] = int(np.round(v * df.loc[idx, 'total_panels']))
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