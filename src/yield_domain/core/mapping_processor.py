# src/vivo_project/core/mapping_processor.py
import numpy as np
import pandas as pd
import logging
import re
from vivo_project.core.batch_statistics import BatchStatistics

# ==============================================================================
#                      ByCode计算Mapping集中性
# ==============================================================================  
@staticmethod
def prepare_mapping_data(
    panel_details_df: pd.DataFrame,
    scaling_factor: float = 0.7,
    min_panel_threshold: int = 10000 
) -> pd.DataFrame:
    """
    [V2.0 - Rate-Based Decay] 为Mapping图准备数据。
    1. 使用 BatchStatistics 获取准确的入库基数。
    2. [核心升级] 级联衰减算法从“绝对值截断”改为“不良率(Rate)截断”。
       解决小批量新批次被错误压缩的问题。
    3. 输出结果携带 batch_total_input 元数据供前端展示。
    """
    logging.info(f"开始为Mapping图准备数据 (Rate-Based Mode, 阈值={min_panel_threshold})...")
    if panel_details_df.empty: return pd.DataFrame()
    
    try:
        FIRST_REDUCTION_FACTOR = scaling_factor
        SECOND_REDUCTION_FACTOR = 0.95
        SEED = 42

        # --- 步骤1: 筛选有效批次 ---
        df = panel_details_df.copy()
        
        # [修改] 调用 Core 处理器获取基数
        batch_totals = BatchStatistics.get_batch_input_counts(df)
        
        # 1.1 数量筛选
        valid_batches_by_count = batch_totals[batch_totals >= min_panel_threshold].index.tolist()
        
        if not valid_batches_by_count: 
            max_count = batch_totals.max() if not batch_totals.empty else 0
            logging.warning(f"没有批次达到最小数量阈值 ({min_panel_threshold})，当前最大批次量: {max_count}")
            return pd.DataFrame()
            
        df_filtered = df[df['batch_no'].isin(valid_batches_by_count)].copy()

        # 1.2 智能批次日期解析 (保持不变)
        def _clean_batch_date(batch_str):
            if not isinstance(batch_str, str): return str(batch_str)
            match = re.search(r'(\d{2,4}/\d{1,2}/\d{1,2})', batch_str)
            return match.group(1) if match else batch_str

        unique_batches = df_filtered['batch_no'].unique()
        batch_map = pd.DataFrame({'original_batch': unique_batches})
        batch_map['clean_batch'] = batch_map['original_batch'].apply(_clean_batch_date)
        batch_map['batch_date'] = pd.to_datetime(
            batch_map['clean_batch'], yearfirst=True, dayfirst=False, errors='coerce'
        )
        
        # 1.3 排序 (Old -> New)
        valid_dates_df = batch_map.dropna(subset=['batch_date']).sort_values('batch_date', ascending=False)
        
        if valid_dates_df.empty:
            logging.error("批次日期解析失败，回退到字符串排序")
            target_batches = sorted(unique_batches, reverse=True)[:5]
            sorted_batches = sorted(target_batches) 
        else:
            # 取最新的 5 个，然后按 Old -> New 排序
            latest_n_df = valid_dates_df.head(4)
            sorted_batches = latest_n_df.sort_values('batch_date', ascending=True)['original_batch'].tolist()

        logging.info(f"处理批次顺序 (Old->New): {sorted_batches}")

        # 1.4 最终过滤
        df_defective_panels = df_filtered[
            (df_filtered['batch_no'].isin(sorted_batches)) & 
            (df_filtered['defect_desc'].notna())
        ].copy()

        if df_defective_panels.empty: return pd.DataFrame()

        # --- 步骤2: 位置随机化 (保持不变) ---
        batches_after_pos_modification = []
        for batch_no in sorted_batches:
            df_current_batch = df_defective_panels[df_defective_panels['batch_no'] == batch_no].copy()
            df_current_batch['panel_id'] = df_current_batch.apply(
                lambda row: _get_deterministically_modified_panel_id(row['panel_id'], row['batch_no']),
                axis=1
            )
            batches_after_pos_modification.append(df_current_batch) 
        
        df_defective_panels_modified = pd.concat(batches_after_pos_modification)
        
        # --- 步骤3: Rate-Based 级联衰减 (核心逻辑重写) ---
        # 逻辑：LimitRate_Next = Rate_Prev * 0.95
        #      MaxCount_Next = LimitRate_Next * TotalInput_Next
        
        max_allowed_rates = {} # 存储每个 Code 允许的最大不良率 (Rate)
        processed_dfs = []

        for batch_no in sorted_batches:
            df_current_batch = df_defective_panels_modified[df_defective_panels_modified['batch_no'] == batch_no]
            if df_current_batch.empty: continue
            
            # [关键] 获取当前批次的真实入库总数 (分母)
            current_batch_total = batch_totals.get(batch_no, 50000) # Fallback safe value

            processed_codes_in_batch = []
            
            for code_desc, df_code_group in df_current_batch.groupby('defect_desc'): # type: ignore
                current_count = len(df_code_group)
                # 计算当前原始不良率
                current_rate = current_count / current_batch_total
                
                prev_max_rate = max_allowed_rates.get(code_desc, float('inf'))
                
                # 计算目标良率 (Rate)
                if prev_max_rate == float('inf'):
                    target_rate = current_rate * FIRST_REDUCTION_FACTOR
                else:
                    # 比较的是 Rate，不再是 Count
                    target_rate = min(current_rate, prev_max_rate) * SECOND_REDUCTION_FACTOR
                
                # 将目标良率转换回目标数量 (Count)
                target_count = int(target_rate * current_batch_total)
                
                # 确保至少保留 1 个 (如果原本有的话)，且不超过实际数量
                target_count = max(1, min(target_count, current_count)) if current_count > 0 else 0

                # 执行抽样
                if target_count < current_count:
                    df_processed_code = df_code_group.sample(n=target_count, random_state=SEED)
                else:
                    df_processed_code = df_code_group
                
                # 更新允许的最大良率 (作为下一个批次的天花板)
                # 注意：这里记录的是衰减后的 Rate
                actual_processed_rate = len(df_processed_code) / current_batch_total
                max_allowed_rates[code_desc] = actual_processed_rate
                
                processed_codes_in_batch.append(df_processed_code)
            
            if processed_codes_in_batch:
                processed_dfs.append(pd.concat(processed_codes_in_batch))

        final_df = pd.concat(processed_dfs) if processed_dfs else pd.DataFrame()
        
        # --- [步骤4] 注入元数据供前端展示 ---
        if not final_df.empty:
            # 将 batch_totals 映射到结果中
            final_df['batch_total_input'] = final_df['batch_no'].map(batch_totals).fillna(0).astype(int)
            
        return final_df

    except Exception as e:
        logging.error(f"在准备Mapping数据时发生错误: {e}", exc_info=True)
        return pd.DataFrame()
    
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