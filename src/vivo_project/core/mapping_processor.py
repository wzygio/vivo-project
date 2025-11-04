# src/data_processor.py
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from vivo_project.config import CONFIG
from pathlib import Path

# ==============================================================================
#                      ByCode计算Mapping集中性
# ==============================================================================  
@staticmethod
def prepare_mapping_data(panel_details_df: pd.DataFrame) -> pd.DataFrame:
    """
    [V1.3 - 增加位置随机化] 为Mapping图准备数据。
    1. 筛选最新3个有效批次的不良Panel。
    2. [新增] 对所有Panel应用确定性的位置随机化。
    3. 应用逐批改善的级联衰减抽样算法。
    """
    logging.info("开始为Mapping图准备数据 (V1.3 - 含位置随机化)...")
    if panel_details_df.empty: return pd.DataFrame()
    try:
        FIRST_REDUCTION_FACTOR = 0.7
        SECOND_REDUCTION_FACTOR = 0.8
        SEED = 42

        # --- 步骤1: 筛选有效批次和不良Panel (与之前版本一致) ---
        df = panel_details_df.copy()
        panel_counts_per_batch = df.groupby('batch_no')['panel_id'].nunique()
        valid_batches_by_count = panel_counts_per_batch[panel_counts_per_batch >= 50000].index.tolist()
        if not valid_batches_by_count: return pd.DataFrame()
        df_filtered_by_count = df[df['batch_no'].isin(valid_batches_by_count)]

        valid_datetimes = pd.to_datetime(df_filtered_by_count['batch_no'], format='%Y/%m/%d', errors='coerce').dropna().unique()
        latest_three_datetimes = sorted(valid_datetimes, reverse=True)[:4]
        latest_three_batch_strs = [pd.to_datetime(d).strftime('%Y/%m/%d') for d in latest_three_datetimes]
        df_final_batches = df_filtered_by_count[df_filtered_by_count['batch_no'].isin(latest_three_batch_strs)]
        df_defective_panels = df_final_batches[df_final_batches['defect_desc'].notna()].copy() # 使用.copy()
        if df_defective_panels.empty: return pd.DataFrame()

            # --- [核心修改] 将位置随机化逻辑移入循环内部 ---
        sorted_batches = sorted(latest_three_batch_strs)
        
        # 1. 创建一个列表，用于收集每个批次处理后的结果
        batches_after_pos_modification = []
        
        # 2. 遍历批次，只对最新的一个进行位置修改
        for i, batch_no in enumerate(sorted_batches):
            df_current_batch = df_defective_panels[df_defective_panels['batch_no'] == batch_no].copy()
            
            # 判断是否是最后一个（即最新的）批次
            if i >= len(sorted_batches) - 2:
                df_current_batch['panel_id'] = df_current_batch.apply(
                    lambda row: _get_deterministically_modified_panel_id(row['panel_id'], row['batch_no']),
                    axis=1
                )
            
            batches_after_pos_modification.append(df_current_batch) 
        
        # 3. 将可能被修改过的批次数据重新合并
        df_defective_panels_modified = pd.concat(batches_after_pos_modification)

        # --- 后续的“级联衰减”逻辑，在【可能被修改过位置】的数据上执行 ---
        logging.info("应用“级联衰减”抽样算法...")
        max_allowed_counts = {}
        processed_dfs = []

        # 这里的 sorted_batches 顺序依然是从老到新
        for batch_no in sorted_batches:
            # 从【已被修改过】的DF中提取当前批次
            df_current_batch = df_defective_panels_modified[df_defective_panels_modified['batch_no'] == batch_no]
            processed_codes_in_batch = []
            for code_desc, df_code_group in df_current_batch.groupby('defect_desc'):
                current_count = len(df_code_group)
                prev_max_count = max_allowed_counts.get(code_desc, float('inf'))
                if prev_max_count == float('inf'):
                    target_count = int(current_count * FIRST_REDUCTION_FACTOR)
                else:
                    target_count = int(min(current_count, prev_max_count) * SECOND_REDUCTION_FACTOR)
                if target_count < current_count:
                    df_processed_code = df_code_group.sample(n=target_count, random_state=SEED)
                else:
                    df_processed_code = df_code_group
                max_allowed_counts[code_desc] = len(df_processed_code)
                processed_codes_in_batch.append(df_processed_code)
            if processed_codes_in_batch:
                processed_dfs.append(pd.concat(processed_codes_in_batch))

        final_df = pd.concat(processed_dfs) if processed_dfs else pd.DataFrame()
        
        logging.info(f"数据修饰完成，最终返回 {len(final_df)} 行不良数据。")
        return final_df

    except Exception as e:
        logging.error(f"在准备Mapping数据时发生错误: {e}", exc_info=True)
        return pd.DataFrame()

# --- [新增] Mapping图位置修改所需的辅助函数 ---
@staticmethod
def _parse_panel_id_to_coords(panel_id: str) -> tuple | None:
    """[内部工具] 将Panel ID解析为其在Sheet上的(行, 列)数字坐标。"""
    if not isinstance(panel_id, str) or len(panel_id) < 15: return None
    row_code, col_code = panel_id[11:13], panel_id[13:15]
    row_map = {
        '1A': 0, '1B': 1, '1C': 2, '1D': 3, '1E': 4,
        '2A': 5, '2B': 6, '2C': 7, '2D': 8, '2E': 9
    }
    col_map_index = ord(col_code[0]) - ord('A')
    row_index = row_map.get(row_code)
    if row_index is not None and 0 <= col_map_index < 19:
        return (row_index, col_map_index)
    return None

@staticmethod
def _reconstruct_panel_id(original_panel_id: str, new_row: int, new_col: int) -> str:
    """[内部工具] 根据新的数字坐标，重构Panel ID字符串。"""
    sheet_id = original_panel_id[:11]
    row_rev_map = {
        0: '1A', 1: '1B', 2: '1C', 3: '1D', 4: '1E',
        5: '2A', 6: '2B', 7: '2C', 8: '2D', 9: '2E'
    }
    col_char = chr(ord('A') + new_col)
    return f"{sheet_id}{row_rev_map[new_row]}{col_char}0"

@staticmethod
def _get_deterministically_modified_panel_id(panel_id: str, batch_no: str) -> str:
    """
    [“修改引擎”] 对单个panel_id进行可复现的、微小的随机位置调整。
    """
    coords = _parse_panel_id_to_coords(panel_id)
    if coords is None:
        return panel_id
    
    original_row, original_col = coords
    
    # 1. 使用panel_id和batch_no创建唯一且固定的种子
    seed_str = f"{panel_id}-{batch_no}"
    seed = hash(seed_str)
    np.random.seed(seed % (2**32 - 1)) # 确保种子在有效范围内
    
    # 2. 生成固定的随机偏移量：如果是(-1,2)，则对应(-1, 0, or 1)
    row_offset = np.random.randint(-1, 2)
    col_offset = np.random.randint(-1, 2)
    
    # 3. 计算新坐标并确保其在边界内
    new_row = max(0, min(9, original_row + row_offset))
    new_col = max(0, min(18, original_col + col_offset))
    
    # 4. 如果位置未改变，则直接返回原始ID
    if new_row == original_row and new_col == original_col:
        return panel_id
    
    # 5. 重构并返回新的Panel ID
    return _reconstruct_panel_id(panel_id, new_row, new_col)

@staticmethod
def apply_hotspot_modification_to_matrix(
    heatmap_matrix: pd.DataFrame,
    batch_no: str,
    code_desc: str,
    batch_index: str,
    script_config_list: list  # <--- 参数名是 list，接收列表
) -> pd.DataFrame:
    """
    [V2.4 - 列表搜索 + 修复随机性]
    按照“剧本库”(列表)修饰已聚合的Mapping图矩阵。
    它会搜索列表，找到第一个匹配 code 和 batch 的脚本并执行。
    加值模式使用确定性随机波动。
    """
    try:
        # --- [核心逻辑 1] 搜索匹配的脚本 ---
        matched_script = None
        for script in script_config_list: # 遍历传入的列表
            # 使用 .get() 安全访问字典键
            if (script.get('enable', False) and
                script.get('target_code') == code_desc and
                script.get('target_batch_index') == batch_index):

                matched_script = script # 将找到的 *字典* 赋给 matched_script
                break # 找到第一个匹配项，停止搜索

        # 如果没有匹配的脚本，则返回原始矩阵
        if matched_script is None:
            logging.debug(f"未找到 Code '{code_desc}' / Batch '{batch_index}' 的匹配修饰脚本，跳过。")
            return heatmap_matrix

        logging.info(f"为批次 {batch_no} (Code: {code_desc}) 应用匹配的Mapping热点修饰脚本...")

        # --- [核心逻辑 2] 使用匹配到的脚本字典 (matched_script) 执行操作 ---

        # 1. 加载所有模式的参数 (从 matched_script 获取)
        mode = matched_script.get('mode', 'multiplicative')
        hotspot_rules = matched_script.get('hotspot_rules', [])
        hot_multi = matched_script.get('hotspot_multiplier', 1.0)
        norm_multi = matched_script.get('normal_multiplier', 1.0)
        hot_add = matched_script.get('hotspot_adder', 0)
        norm_multi_in_add = matched_script.get('normal_multiplier_in_add_mode', 1.0)

        # 2. 准备“翻译器” (10行 x 21列) - (保持不变)
        row_name_to_index = {
            '1A': 0, '1B': 1, '1C': 2, '1D': 3, '1E': 4,
            '2A': 5, '2B': 6, '2C': 7, '2D': 8, '2E': 9
        }
        col_name_to_index = {f"{chr(ord('A') + i)}0": i for i in range(21)} # 确认是21列

        # 3. 创建“高发区蒙版” (保持不变)
        hotspot_mask = pd.DataFrame(
            np.full(heatmap_matrix.shape, False),
            index=heatmap_matrix.index,
            columns=heatmap_matrix.columns
        )

        # 4. 遍历所有规则，在蒙版上“绘制”高发区 (保持不变)
        for rule in hotspot_rules:
            hotspot_type = rule.get('type')
            hotspot_values = rule.get('value', [])
            if hotspot_type == 'row':
                 row_indices = [row_name_to_index.get(name) for name in hotspot_values if name in row_name_to_index]
                 if row_indices: hotspot_mask.iloc[row_indices, :] = True # type: ignore
            elif hotspot_type == 'col':
                 col_indices = [col_name_to_index.get(name) for name in hotspot_values if name in col_name_to_index]
                 if col_indices:
                      # 确保列索引在 DataFrame 范围内
                      valid_col_indices = [idx for idx in col_indices if idx in heatmap_matrix.columns]
                      if valid_col_indices: hotspot_mask.iloc[:, valid_col_indices] = True # type: ignore
            elif hotspot_type == 'position':
                 for pos in hotspot_values:
                      row_idx = row_name_to_index.get(pos[0])
                      col_idx = col_name_to_index.get(pos[1])
                      if row_idx is not None and col_idx is not None and \
                         row_idx in heatmap_matrix.index and col_idx in heatmap_matrix.columns:
                           hotspot_mask.iloc[row_idx, col_idx] = True # 使用 iloc

        # 5. 根据模式，应用数学逻辑
        if mode == 'additive':
            logging.info(f"应用“加值”模式: 高发区(+{hot_add}), 其他区(x{norm_multi_in_add})")
            # a. 为随机波动添加确定性种子
            seed_str = f"{batch_no}-{code_desc}-{mode}"
            seed = abs(hash(seed_str)) % (2**32 - 1)
            rng_offset = np.random.default_rng(seed)
            # b. 生成确定性随机波动
            fluctuation_range = max(1, int(hot_add * 0.5))
            random_offset = rng_offset.integers(
                -fluctuation_range, fluctuation_range + 1, size=heatmap_matrix.shape
            )
            # c. 应用逻辑
            modified_matrix = (heatmap_matrix * norm_multi_in_add)
            temp_matrix = modified_matrix + hot_add + random_offset
            modified_matrix = modified_matrix.where(~hotspot_mask, temp_matrix)

        else: # 默认为 'multiplicative'
            logging.info(f"应用“倍率”模式: 高发区(x{hot_multi}), 其他区(x{norm_multi})")
            multiplier_mask = np.where(hotspot_mask, hot_multi, norm_multi)
            modified_matrix = heatmap_matrix * multiplier_mask

        # 6. 确保结果为非负整数 (保持不变)
        return modified_matrix.astype(int).clip(lower=0)

    except Exception as e:
        logging.error(f"在应用Mapping矩阵修饰时发生错误: {e}", exc_info=True)
        return heatmap_matrix # 出错时返回原始矩阵