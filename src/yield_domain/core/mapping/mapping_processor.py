# src/vivo_project/core/mapping_processor.py
import pandas as pd
import logging
import re
from src.yield_domain.core.batch_statistics import BatchStatistics
from src.yield_domain.core.mapping.hotspot_modification import apply_hotspot_modification_to_matrix
from src.yield_domain.core.mapping.panel_position import (
    get_deterministically_modified_panel_id as _get_deterministically_modified_panel_id,
    parse_panel_id_to_coords as _parse_panel_id_to_coords,
    reconstruct_panel_id as _reconstruct_panel_id,
)

# ==============================================================================
#                      ByCode计算Mapping集中性
# ==============================================================================  
@staticmethod
def prepare_mapping_data(
    panel_details_df: pd.DataFrame,
    scaling_factor: float,
    min_panel_threshold: int = 0
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
            latest_n_df = valid_dates_df.head(5)
            sorted_batches = latest_n_df.sort_values('batch_date', ascending=True)['original_batch'].tolist()

        logging.info(f"处理批次顺序 (Old->New): {sorted_batches}")

        # 1.4 最终过滤
        df_defective_panels = df_filtered[
            (df_filtered['batch_no'].isin(sorted_batches)) & 
            (df_filtered['defect_desc'].notna())
        ].copy()

        if df_defective_panels.empty: return pd.DataFrame()

        # --- 步骤2: 位置随机化 (保持不变) ---
        batches_after_pos_modification: list[pd.DataFrame] = []
        for batch_no in sorted_batches:
            df_current_batch = df_defective_panels[df_defective_panels['batch_no'] == batch_no].copy()
            df_current_batch['panel_id'] = df_current_batch.apply(
                lambda row: _get_deterministically_modified_panel_id(row['panel_id'], row['batch_no']),
                axis=1
            )
            batches_after_pos_modification.append(df_current_batch) 
        
        if not batches_after_pos_modification:
            return pd.DataFrame()

        df_defective_panels_modified = pd.concat(batches_after_pos_modification)
        
        # --- 步骤3: Rate-Based 级联衰减 (完美级联版) ---
        max_allowed_rates = {} # 存储每个 Code 的理论天花板
        processed_dfs = []

        for batch_no in sorted_batches:
            df_current_batch = df_defective_panels_modified[df_defective_panels_modified['batch_no'] == batch_no]
            if df_current_batch.empty: continue
            
            current_batch_total = batch_totals.get(batch_no, 50000) 

            processed_codes_in_batch = []
            
            for code_desc, df_code_group in df_current_batch.groupby('defect_desc'): 
                current_count = len(df_code_group)
                current_rate = current_count / (current_batch_total or 1)
                
                prev_max_rate = max_allowed_rates.get(code_desc, float('inf'))
                
                # =========================================================
                # [完美级联逻辑] 让“天花板”自身随时间衰减，无视真实良率的波动
                # =========================================================
                if prev_max_rate == float('inf'):
                    # 最老批次：确立初始天花板
                    target_rate = current_rate * FIRST_REDUCTION_FACTOR
                    max_allowed_rates[code_desc] = target_rate 
                else:
                    # 后续批次：天花板严格按照 SECOND_REDUCTION_FACTOR 逐级递减
                    # 比如 0.95 -> 0.9025 -> 0.857
                    new_ceiling = prev_max_rate * SECOND_REDUCTION_FACTOR
                    
                    # 当前批次只能在“真实率”和“新天花板”之间取极小值
                    target_rate = min(current_rate, new_ceiling)
                    
                    # 核心：把这个按比例算出来的【纯理论新天花板】存回去！
                    # 而不是存入 target_rate 或 current_rate！
                    max_allowed_rates[code_desc] = new_ceiling
                
                # 将目标良率转换回目标数量
                target_count = int((target_rate or 0) * (current_batch_total or 0))
                target_count = max(1, min(target_count, current_count)) if current_count > 0 else 0

                # 执行抽样
                if target_count < current_count:
                    df_processed_code = df_code_group.sample(n=target_count, random_state=SEED)
                else:
                    df_processed_code = df_code_group
                
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
    
