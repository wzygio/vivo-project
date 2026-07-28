# src/vivo_project/core/sheet_lot/simulation.py
import logging
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd


def _to_iso_week_keys(values: pd.Series) -> pd.Series:
    """Convert compact or standard date values to normalized ISO week keys."""
    text_values = values.astype("string").str.strip()
    parsed = pd.to_datetime(text_values, format="%Y%m%d", errors="coerce")
    fallback_mask = parsed.isna() & text_values.notna()
    if fallback_mask.any():
        parsed.loc[fallback_mask] = pd.to_datetime(
            text_values.loc[fallback_mask], errors="coerce"
        )

    iso_calendar = parsed.dt.isocalendar()
    return (
        iso_calendar.year.astype("string").str.zfill(4)
        + "-W"
        + iso_calendar.week.astype("string").str.zfill(2)
    )


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

def _expand_code_rows_to_positive_period_entities(
    df_code: pd.DataFrame,
    base_info_df: pd.DataFrame,
    entity_period_map: pd.Series,
    lookup_dict: Dict[str, float],
    entity_id_col: str,
    code_desc: Any,
) -> pd.DataFrame:
    """Add entities whose mapped trend period has a positive Code rate."""
    if df_code.empty or not lookup_dict or entity_id_col not in base_info_df.columns:
        return df_code

    positive_periods = {
        str(period_key)
        for period_key, rate in lookup_dict.items()
        if float(rate) > 0
    }
    if not positive_periods:
        return df_code

    normalized_entity_period_map = {
        str(entity_id).strip(): period_key
        for entity_id, period_key in entity_period_map.dropna().items()
    }
    roster = base_info_df.copy()
    roster["_entity_key"] = roster[entity_id_col].astype(str).str.strip()
    roster["_period_key"] = roster["_entity_key"].map(normalized_entity_period_map)
    roster = roster[roster["_period_key"].isin(positive_periods)].copy()
    if roster.empty:
        return df_code

    existing_entities = set(df_code[entity_id_col].astype(str).str.strip())
    roster = roster[~roster["_entity_key"].isin(existing_entities)].copy()
    if roster.empty:
        return df_code

    defect_group = df_code["defect_group"].dropna().iloc[0] if "defect_group" in df_code else ""
    new_rows = roster.drop(columns=["_entity_key", "_period_key"], errors="ignore")
    new_rows["defect_group"] = defect_group
    new_rows["defect_desc"] = code_desc
    new_rows["defect_panel_count"] = 0
    new_rows["defect_rate"] = 0.0

    for col in df_code.columns:
        if col not in new_rows.columns:
            new_rows[col] = np.nan

    return pd.concat([df_code, new_rows[df_code.columns]], ignore_index=True)


def _expand_code_rows_to_positive_daily_entities(
    df_code: pd.DataFrame,
    base_info_df: pd.DataFrame,
    date_map_str: pd.Series,
    lookup_dict: Dict[str, float],
    entity_id_col: str,
    code_desc: Any,
) -> pd.DataFrame:
    """Compatibility wrapper for callers using the former daily helper."""
    return _expand_code_rows_to_positive_period_entities(
        df_code=df_code,
        base_info_df=base_info_df,
        entity_period_map=date_map_str,
        lookup_dict=lookup_dict,
        entity_id_col=entity_id_col,
        code_desc=code_desc,
    )


def _allocate_weekly_defect_tokens(
    df_code: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.Series:
    """Allocate each week's integer defect total across its eligible entities."""
    allocated = pd.Series(0, index=df_code.index, dtype="int64")

    for _, week_indices in df_code.groupby("week_key", sort=False).groups.items():
        week_frame = df_code.loc[week_indices]
        base_rate = float(week_frame["weekly_base_rate"].iloc[0])
        if base_rate <= 0:
            continue

        capacities = (
            pd.to_numeric(week_frame["total_panels"], errors="coerce")
            .fillna(0)
            .clip(lower=0)
            .astype(int)
            .to_numpy()
        )
        total_capacity = int(capacities.sum())
        target_total = min(
            int(np.rint(total_capacity * base_rate)),
            total_capacity,
        )
        if target_total <= 0:
            continue

        weights = capacities.astype(float) * rng.uniform(
            0.8,
            1.2,
            size=len(week_frame),
        )
        week_allocation = np.zeros(len(week_frame), dtype=int)
        remaining = target_total

        while remaining > 0:
            available = capacities - week_allocation
            active = available > 0
            if not active.any():
                break

            active_weights = np.where(active, weights, 0.0)
            if active_weights.sum() <= 0:
                active_weights = available.astype(float)

            exact_shares = remaining * active_weights / active_weights.sum()
            additions = np.minimum(
                np.floor(exact_shares).astype(int),
                available,
            )
            week_allocation += additions
            remaining -= int(additions.sum())
            if remaining <= 0:
                break

            available = capacities - week_allocation
            fractional = exact_shares - np.floor(exact_shares)
            candidates = np.flatnonzero(available > 0)
            if not len(candidates):
                break

            order = candidates[
                np.argsort(-fractional[candidates], kind="stable")
            ]
            for position in order:
                if remaining <= 0:
                    break
                week_allocation[position] += 1
                remaining -= 1

        allocated.loc[week_indices] = week_allocation

    return allocated


def _simulate_concentration(
    raw_results: Dict[str, Any],
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    processing_config: Dict[str, Any],
    entity_id_col: str = 'sheet_id'
) -> Dict[str, Any]:
    """
    Lot 级模拟以所属 ISO 周的 Code MWD 良损为基准。

    每个 Lot 继续使用固定种子的微观随机扰动，避免同一周内完全同值。
    优先读取三个月完整的 ``weekly_full``；``weekly`` 仅作为旧调用方兼容。
    完整周度中未命中的期间按 0 处理，不回退到原始 Lot 或日度趋势。
    """
    logging.info(
        "开始执行 %s 级不良率模拟调度（ISO 周基准 + 微观扰动）...",
        entity_id_col,
    )
    try:
        config = processing_config.get('sheet_hotspot_config', {})
        if not config.get('enable', False):
            return raw_results['code_level_details']
        
        sim_code_details = raw_results["code_level_details"].copy()
        base_info_df = raw_results.get("group_level_summary_for_chart")

        df_weekly = None
        if mwd_code_data:
            df_weekly = mwd_code_data.get("weekly_full")
            if df_weekly is None or df_weekly.empty:
                df_weekly = mwd_code_data.get("weekly")
        weekly_columns = {"time_period", "defect_desc", "defect_rate"}
        if (
            df_weekly is None
            or df_weekly.empty
            or not weekly_columns.issubset(df_weekly.columns)
        ):
            logging.warning(
                "Weekly MWD trend is unavailable; preserving raw %s values.",
                entity_id_col,
            )
            return {
                group: frame.copy()
                for group, frame in raw_results["code_level_details"].items()
            }
        
        if base_info_df is None:
            logging.error("缺少基础汇总数据，模拟终止。")
            return sim_code_details

        # --- 0. 预先构建实体所属 ISO 周映射表 ---
        if entity_id_col not in base_info_df.columns and base_info_df.index.name == entity_id_col:
            base_info_temp = base_info_df.reset_index()
        else:
            base_info_temp = base_info_df
        
        date_map_raw = base_info_temp.drop_duplicates(subset=[entity_id_col]).set_index(entity_id_col)['warehousing_time']
        week_map_str = _to_iso_week_keys(date_map_raw)

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
                code_weekly_data = df_weekly[
                    df_weekly["defect_desc"] == code_desc
                ].copy()
                if "defect_group" in code_weekly_data.columns:
                    code_weekly_data = code_weekly_data[
                        code_weekly_data["defect_group"] == group
                    ]
                code_weekly_data["week_key"] = (
                    code_weekly_data["time_period"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )
                code_weekly_data["defect_rate"] = pd.to_numeric(
                    code_weekly_data["defect_rate"], errors="coerce"
                ).fillna(0.0)
                lookup_dict = code_weekly_data.set_index("week_key")[
                    "defect_rate"
                ].to_dict()
                if not lookup_dict:
                    processed_codes_list.append(df_code_mod)
                    continue

                df_code_mod = _expand_code_rows_to_positive_period_entities(
                    df_code=df_code_mod,
                    base_info_df=base_info_temp,
                    entity_period_map=week_map_str,
                    lookup_dict=lookup_dict,
                    entity_id_col=entity_id_col,
                    code_desc=code_desc,
                )
                df_code_mod["week_key"] = df_code_mod[entity_id_col].map(
                    week_map_str
                )

                # 核心映射：获取 Lot 所属 ISO 周的 Code 基准。
                df_code_mod["weekly_base_rate"] = (
                    df_code_mod["week_key"].map(lookup_dict).fillna(0.0)
                )
                
                # 先锁定 Code × 周的整数总量，再按 Lot 面积与 ±20% 权重分配。
                # 避免低良损率在每个 Lot 上独立四舍五入后全部归零。
                df_code_mod["defect_panel_count"] = _allocate_weekly_defect_tokens(
                    df_code_mod,
                    rng,
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
                        'lot_id', 'defect_desc', 'week_key', 'total_panels',
                        'weekly_base_rate', 'defect_panel_count', 'defect_rate'
                    ]].copy()
                    debug_lot_frames.append(debug_df)
                # =========================================================
                
                df_code_mod.drop(
                    columns=["week_key", "weekly_base_rate"],
                    inplace=True,
                    errors="ignore",
                )
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
                final_debug_df['weekly_base_rate'] = final_debug_df['weekly_base_rate'].apply(lambda x: f"{x:.5%}")
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
