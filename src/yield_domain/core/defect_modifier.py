import logging
import pandas as pd
from typing import Dict

@staticmethod
def apply_defect_multipliers(panel_df: pd.DataFrame, multipliers: Dict[str, float]) -> pd.DataFrame:
    """
    根据给定的倍率字典，调整特定 defect_desc 的不良 Panel 数量。
    [升级版] 解决 nunique() 去重降维问题，生成防碰撞的伪造 ID。
    """
    if not multipliers:
        return panel_df

    logging.info(f"开始应用不良倍率调整: {multipliers}")
    
    codes_to_process = list(multipliers.keys())
    df_to_process = panel_df[panel_df['defect_desc'].isin(codes_to_process)]
    df_unaffected = panel_df[~panel_df['defect_desc'].isin(codes_to_process)]
    
    processed_dfs = []
    for code, factor in multipliers.items():
        df_code = df_to_process[df_to_process['defect_desc'] == code]
        original_count = len(df_code)
        if original_count == 0:
            continue
            
        target_count = int(original_count * factor)
        
        if target_count < original_count:
            # 缩小情况：随机抽样 (没问题，直接用)
            df_sampled = df_code.sample(n=target_count, random_state=42)
            processed_dfs.append(df_sampled)
            logging.info(f"Code '{code}': 不良数从 {original_count} 下调至 {target_count} (倍率: {factor})。")
            
        elif target_count > original_count:
            # 放大情况：重复采样 + 伪造唯一 ID
            repeat_times = target_count // original_count
            remainder = target_count % original_count
            
            repeated_dfs = [df_code] # 第 1 份是原始数据，无需改 ID
            
            # 复制完整的倍数 (从第 2 份开始需要改 ID)
            for i in range(1, repeat_times):
                df_copy = df_code.copy()
                # 强行给 panel_id 加上防碰撞后缀，骗过 downstream 的 nunique()
                df_copy['panel_id'] = df_copy['panel_id'].astype(str) + f"_SIM_X{i}"
                repeated_dfs.append(df_copy)
            
            # 复制余数部分
            if remainder > 0:
                df_rem = df_code.sample(n=remainder, random_state=42).copy()
                df_rem['panel_id'] = df_rem['panel_id'].astype(str) + f"_SIM_REM"
                repeated_dfs.append(df_rem)
                
            df_repeated_final = pd.concat(repeated_dfs, ignore_index=True)
            processed_dfs.append(df_repeated_final)
            logging.info(f"Code '{code}': 不良数从 {original_count} 强行上调至 {target_count} (已生成伪造唯一ID)。")
            
        else:
            # 不变情况
            processed_dfs.append(df_code)

    final_df = pd.concat([df_unaffected] + processed_dfs, ignore_index=True)
    logging.info(f"不良倍率调整完成，数据从 {len(panel_df)} 行变为 {len(final_df)} 行。")
    return final_df



def _build_weight_maps(all_lots: list, specific_configs: dict, default_weight: int) -> dict:
    """[内部工具] 为每个Code创建Lot权重字典"""
    weight_maps = {}
    for code, lot_weights in specific_configs.items():
        weights = [lot_weights.get(lot, default_weight) for lot in all_lots]
        weight_maps[code] = (all_lots, weights)
    
    # 为所有未配置的Code创建默认权重
    default_weights = [default_weight] * len(all_lots)
    weight_maps['default'] = (all_lots, default_weights)
    return weight_maps


def _get_dispersion_target(row: pd.Series, context: dict) -> pd.Series:
    """
    [内部工具 - V2.0] 
    对单行Panel应用确定性哈希，进行加权抽样和重构。
    返回一个包含【三个】新ID的Series，以便覆盖原始列。
    """
    try:
        original_panel_id = row['panel_id']
        defect_desc = row['defect_desc']
        
        # 1. 获取此Code对应的“概率转盘”
        lot_list, weights = context['weight_maps'].get(defect_desc, context['weight_maps']['default'])
        
        # 2. 创建确定性种子
        seed_str = f"{original_panel_id}-{row['batch_no']}-{context['salt']}"
        seed = hash(seed_str)
        
        # 3. 执行确定性加权抽样 (选择Lot)
        import random
        rng = random.Random(seed)
        new_lot_id = rng.choices(lot_list, weights=weights, k=1)[0]
        
        # 4. 从新Lot的Sheet“宇宙”中随机选一个Sheet
        available_sheets = context['lot_to_sheets_map'].get(new_lot_id)
        if not available_sheets or len(available_sheets) == 0:
            # 安全保护：如果新Lot没有Sheet，则不修改
            return pd.Series([original_panel_id, row['lot_id'], row['sheet_id']])
        
        new_sheet_id = rng.choice(available_sheets)
        
        # 5. 重构Panel ID (保留后4位)
        new_panel_id = f"{new_sheet_id}{original_panel_id[11:]}"
        
        # 6. [核心修正] 返回一个包含所有新ID的Series
        return pd.Series([new_panel_id, new_lot_id, new_sheet_id])

    except Exception:
        return pd.Series([row['panel_id'], row['lot_id'], row['sheet_id']])
