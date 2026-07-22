# src/vivo_project/core/sheet_lot/simulation.py
import logging
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

def _distribute_sheet_from_lot(
    sheet_raw_results: Dict[str, Any],
    lot_results: Dict[str, Any],
    processing_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    [辅助函数 V5.3 - 泊松/多项式散布 + 软熔断版]
    利用多项式分布自然散布不良，并设定单卡最高不良率熔断线。
    完美兼顾自然的参差波动与“物理常识防呆”，杜绝平头哥与超高柱。
    """
    logging.info("开始执行 Sheet 级不良发牌调度 (Lot -> Sheet 泊松自然散布 V5.3)...")
    config = processing_config.get('sheet_hotspot_config', {})
    if not config.get('enable', False):
        return sheet_raw_results['code_level_details']
    
    seed = config.get('random_seed', 2026)
    rng = np.random.default_rng(seed)
    
    # 1. 获取全局花名册
    base_info = sheet_raw_results.get("group_level_summary_for_chart") 
    if base_info is None:
        return sheet_raw_results['code_level_details']
        
    if base_info.index.name == 'sheet_id':
        base_info = base_info.reset_index()
        
    req_cols = ['sheet_id', 'lot_id', 'total_panels', 'warehousing_time']
    if 'array_input_time' in base_info.columns: req_cols.append('array_input_time') 
    if 'pass_rate' in base_info.columns: req_cols.append('pass_rate')               
    sheet_roster = base_info[req_cols].copy()
    
    sim_sheet_codes = {}
    lot_code_details = lot_results.get("code_level_details", {})
    
    # 2. 遍历真理来源
    for group, df_lot in lot_code_details.items():
        if df_lot is None or df_lot.empty:
            sim_sheet_codes[group] = pd.DataFrame() 
            continue
            
        processed_codes_list = []
        
        for code_desc, df_lot_code in df_lot.groupby('defect_desc'):
            lot_tokens = df_lot_code[['lot_id', 'defect_panel_count', 'defect_group', 'defect_desc']]
            df_sheet_mod = pd.merge(sheet_roster, lot_tokens, on='lot_id', how='inner', suffixes=('', '_lot')) 
            
            if df_sheet_mod.empty: continue
                
            df_sheet_mod.rename(columns={'defect_panel_count': 'lot_token_count'}, inplace=True)
            df_sheet_mod['defect_panel_count'] = 0 
            
            # =================================================================
            # 🎲 [核心算法：多项式散布 + 软熔断]
            # =================================================================
            for lot_id, group_df in df_sheet_mod.groupby('lot_id'):
                token_count = group_df['lot_token_count'].iloc[0]
                if token_count <= 0: continue
                
                total_capacity = group_df['total_panels'].sum()
                if total_capacity <= 0: continue
                
                # A. 计算软熔断上限 (Soft Cap)
                # 设定单卡极限为平均良率的 2.5 倍 (或者至少允许 1 个，最多不超过自身容量)
                avg_rate = token_count / total_capacity
                cap_rate = min(avg_rate * 2.5, 1.0) 
                
                entity_capacities = group_df['total_panels'].to_numpy(dtype=float)
                max_allowed = np.ceil(entity_capacities * cap_rate).astype(int)
                max_allowed = np.clip(max_allowed, 1, entity_capacities) # type: ignore
                
                allocated_counts = np.zeros(len(group_df), dtype=int)
                remaining_tokens = int(token_count)
                
                # B. 散布与溢出重分配 (Scatter & Re-distribute)
                while remaining_tokens > 0:
                    # 找出还没爆满的 Sheet
                    valid_mask = allocated_counts < max_allowed
                    valid_indices = np.where(valid_mask)[0]
                    
                    if len(valid_indices) == 0: break # 全满了，强制停止
                        
                    # 按照剩余容量计算每次接球的概率
                    rem_capacities = max_allowed[valid_indices] - allocated_counts[valid_indices]
                    probs = rem_capacities / rem_capacities.sum()
                    
                    # 核心：多项式扔骰子 (一次性把剩下的不良按概率扔进篮子里)
                    draws = rng.multinomial(remaining_tokens, probs)
                    allocated_counts[valid_indices] += draws
                    
                    # C. 熔断截断：收回溢出的不良，下一轮重新发
                    overflow = allocated_counts - max_allowed
                    overflow_mask = overflow > 0
                    
                    if overflow_mask.any():
                        remaining_tokens = overflow[overflow_mask].sum() # 收回溢出
                        allocated_counts[overflow_mask] = max_allowed[overflow_mask] # 削平超高柱
                    else:
                        remaining_tokens = 0 # 完美发完
                
                df_sheet_mod.loc[group_df.index, 'defect_panel_count'] = allocated_counts
            # =================================================================
            
            df_sheet_mod['defect_rate'] = np.where(
                df_sheet_mod['total_panels'] > 0,
                df_sheet_mod['defect_panel_count'] / df_sheet_mod['total_panels'],
                0.0
            )
            
            final_cols = ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time', 'defect_group', 'defect_desc', 'defect_panel_count', 'defect_rate', 'total_panels', 'pass_rate']
            final_cols = [c for c in final_cols if c in df_sheet_mod.columns]
            
            if not df_sheet_mod.empty:
                processed_codes_list.append(df_sheet_mod[final_cols])
        
        if processed_codes_list:
            sim_sheet_codes[group] = pd.concat(processed_codes_list, ignore_index=True)
        else:
            sim_sheet_codes[group] = pd.DataFrame()
            
    return sim_sheet_codes
# ==============================================================================
#                      辅助函数：模拟数据
# ==============================================================================

def _expand_code_rows_to_positive_daily_entities(
    df_code: pd.DataFrame,
    base_info_df: pd.DataFrame,
    date_map_str: pd.Series,
    lookup_dict: Dict[str, float],
    entity_id_col: str,
    code_desc: Any,
) -> pd.DataFrame:
    """Add same-day entities when a Code has positive EMA daily rate."""
    if df_code.empty or not lookup_dict or entity_id_col not in base_info_df.columns:
        return df_code

    positive_dates = {str(date_key) for date_key, rate in lookup_dict.items() if float(rate) > 0}
    if not positive_dates:
        return df_code

    entity_date_map = {
        str(entity_id).strip(): date_key
        for entity_id, date_key in date_map_str.dropna().items()
    }
    roster = base_info_df.copy()
    roster["_entity_key"] = roster[entity_id_col].astype(str).str.strip()
    roster["_date_key"] = roster["_entity_key"].map(entity_date_map)
    roster = roster[roster["_date_key"].isin(positive_dates)].copy()
    if roster.empty:
        return df_code

    existing_entities = set(df_code[entity_id_col].astype(str).str.strip())
    roster = roster[~roster["_entity_key"].isin(existing_entities)].copy()
    if roster.empty:
        return df_code

    defect_group = df_code["defect_group"].dropna().iloc[0] if "defect_group" in df_code else ""
    new_rows = roster.drop(columns=["_entity_key", "_date_key"], errors="ignore")
    new_rows["defect_group"] = defect_group
    new_rows["defect_desc"] = code_desc
    new_rows["defect_panel_count"] = 0
    new_rows["defect_rate"] = 0.0

    for col in df_code.columns:
        if col not in new_rows.columns:
            new_rows[col] = np.nan

    return pd.concat([df_code, new_rows[df_code.columns]], ignore_index=True)


def _simulate_concentration(
    raw_results: Dict[str, Any],
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    processing_config: Dict[str, Any],
    entity_id_col: str = 'sheet_id'
) -> Dict[str, Any]:
    """
    [核心重构 V4.3 - 带深度调试导出 & 实体级微观扰动]
    引入稳定的微观随机噪声，打破同一天数据一模一样的“阶梯状”失真。
    """
    logging.info(f"开始执行 {entity_id_col} 级不良率模拟调度 (V4.3 - 微观扰动版)...")
    try:
        config = processing_config.get('sheet_hotspot_config', {})
        if not config.get('enable', False):
            return raw_results['code_level_details']
        
        sim_code_details = raw_results["code_level_details"].copy()
        base_info_df = raw_results.get("group_level_summary_for_chart")
        
        if base_info_df is None:
            logging.error("缺少基础汇总数据，模拟终止。")
            return sim_code_details

        # --- 0. 预先构建日期映射表 ---
        if entity_id_col not in base_info_df.columns and base_info_df.index.name == entity_id_col:
            base_info_temp = base_info_df.reset_index()
        else:
            base_info_temp = base_info_df
        
        date_map_raw = base_info_temp.drop_duplicates(subset=[entity_id_col]).set_index(entity_id_col)['warehousing_time']
        date_map_str = pd.to_datetime(date_map_raw, errors='coerce').dt.strftime('%Y%m%d')

        df_daily_ema = mwd_code_data.get('daily_full') if mwd_code_data else None

        # [新增修复] 初始化一个稳定的随机数生成器，保证每次刷新页面波动形态固定
        seed = config.get('random_seed', 2026)
        rng = np.random.default_rng(seed)

        # [新增] 调试数据收集器
        debug_lot_frames = []

        # --- 1. 开始遍历计算 ---
        for group, df_all_codes_in_group in sim_code_details.items():
            if df_all_codes_in_group.empty: continue
            
            processed_codes_list = []
            for code_desc, df_code in df_all_codes_in_group.groupby('defect_desc'):
                df_code_mod = df_code.copy()
                lookup_dict = {}
                if df_daily_ema is not None:
                    code_ema_data = df_daily_ema[df_daily_ema['defect_desc'] == code_desc].copy()
                    code_ema_data['date_key'] = code_ema_data['time_period'].astype(str).str.replace('-', '')
                    lookup_dict = code_ema_data.set_index('date_key')['defect_rate'].to_dict()

                df_code_mod = _expand_code_rows_to_positive_daily_entities(
                    df_code=df_code_mod,
                    base_info_df=base_info_temp,
                    date_map_str=date_map_str,
                    lookup_dict=lookup_dict,
                    entity_id_col=entity_id_col,
                    code_desc=code_desc,
                )
                df_code_mod['date_key'] = df_code_mod[entity_id_col].map(date_map_str)

                # 🚀 核心映射：获取当日大盘基准
                df_code_mod['daily_base_rate'] = df_code_mod['date_key'].map(lookup_dict).fillna(0.0)
                
                # =========================================================
                # 🚀 [核心修复：微观扰动] 
                # 为同一天内的每个 Lot/Sheet 赋予 ±30% 的随机浮动，打破阶梯状
                # =========================================================
                # 只有当基准率大于 0 时才施加扰动，提升计算效率
                mask_positive = df_code_mod['daily_base_rate'] > 0
                if mask_positive.any():
                    noise_factors = rng.uniform(0.8, 1.2, size=mask_positive.sum())
                    df_code_mod.loc[mask_positive, 'daily_base_rate'] *= noise_factors
                # =========================================================

                df_code_mod['defect_panel_count'] = np.round(
                    df_code_mod['total_panels'] * df_code_mod['daily_base_rate']
                ).astype(int)
                
                df_code_mod['defect_panel_count'] = np.minimum(
                    df_code_mod['defect_panel_count'], 
                    df_code_mod['total_panels']
                )
                
                df_code_mod['defect_rate'] = np.where(
                    df_code_mod['total_panels'] > 0,
                    df_code_mod['defect_panel_count'] / df_code_mod['total_panels'],
                    0.0
                )
                
                # =========================================================
                # 🛑 [DEBUG 收集器] 收集当前 Code 的模拟明细
                # =========================================================
                if entity_id_col == 'lot_id':
                    debug_df = df_code_mod[[
                        'lot_id', 'defect_desc', 'date_key', 'total_panels', 
                        'daily_base_rate', 'defect_panel_count', 'defect_rate'
                    ]].copy()
                    debug_lot_frames.append(debug_df)
                # =========================================================
                
                df_code_mod.drop(columns=['date_key', 'daily_base_rate'], inplace=True, errors='ignore')
                processed_codes_list.append(df_code_mod)
            
            if processed_codes_list:
                sim_code_details[group] = pd.concat(processed_codes_list, ignore_index=True)
                
        # =================================================================
        # 🛑 [DEBUG 导出]
        # =================================================================
        if debug_lot_frames and entity_id_col == 'lot_id':
            try:
                final_debug_df = pd.concat(debug_lot_frames, ignore_index=True)
                # 转为易读格式
                final_debug_df['daily_base_rate'] = final_debug_df['daily_base_rate'].apply(lambda x: f"{x:.5%}")
                final_debug_df['defect_rate'] = final_debug_df['defect_rate'].apply(lambda x: f"{x:.5%}")
                
                out_path = Path("output/logs/sheet_lot_processor-lot_rate.csv")
                out_path.parent.mkdir(parents=True, exist_ok=True)
                final_debug_df.to_csv(out_path, index=False, encoding='utf-8-sig')
                logging.info(f"✅ [DEBUG] Lot 模拟分配明细已导出至: {out_path.absolute()}")
            except Exception as e:
                logging.error(f"导出 Lot 模拟 debug 数据失败: {e}")
        # =================================================================
                
        return sim_code_details
        
    except Exception as e:
        logging.error(f"模拟调度失败: {e}", exc_info=True)
        return raw_results.get('code_level_details', {})

# ==============================================================================
#                      辅助函数：覆盖数据
# ==============================================================================
