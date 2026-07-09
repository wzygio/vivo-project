# src/vivo_project/core/sheet_lot/aggregation.py
import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

def _calculate_lot_base_info_with_median_time(
    panel_details_df: pd.DataFrame,
    array_input_times_df: pd.DataFrame | None  # 不再接收 full_sheet_info
) -> pd.DataFrame:
    if panel_details_df.empty: return pd.DataFrame()
    try:
        panel_df_with_dt = panel_details_df.copy()
        panel_df_with_dt['warehousing_datetime'] = pd.to_datetime(
            panel_df_with_dt['warehousing_time'], format='%Y%m%d', errors='coerce'
        )
        panel_df_with_dt.dropna(subset=['warehousing_datetime'], inplace=True)
        if panel_df_with_dt.empty: return pd.DataFrame()
        
        lot_base_agg = panel_df_with_dt.groupby('lot_id').agg(
            total_panels=('panel_id', 'nunique'), 
            warehousing_time_median=('warehousing_datetime', lambda x: x.quantile(0.75))
        ).reset_index()
        lot_base_agg['warehousing_time'] = lot_base_agg['warehousing_time_median'].dt.strftime('%Y%m%d').fillna('') # type: ignore
        lot_base_info_df = lot_base_agg[['lot_id', 'total_panels', 'warehousing_time']]
        
        # [独立提取时间] 直接从原生 array_times_df 中提取
        lot_array_times = None
        if array_input_times_df is not None and not array_input_times_df.empty:
            temp_df = array_input_times_df.copy()
            # 截取前9位得到 lot_id
            temp_df['lot_id'] = temp_df['sheet_id'].astype(str).str[:9]
            lot_array_times = temp_df.groupby('lot_id')['array_input_time'].max().reset_index()
            
        if lot_array_times is not None:
            lot_base_info_df = pd.merge(lot_base_info_df, lot_array_times, on='lot_id', how='left')
        else:
            lot_base_info_df['array_input_time'] = pd.NaT
            
        logging.info(f"成功聚合了 {len(lot_base_info_df)} 个 Lot 的基础信息 (含 array_input_time)。")
        return lot_base_info_df
    except Exception as e:
        logging.error(f"计算 Lot 基础信息时发生错误: {e}", exc_info=True)
        return pd.DataFrame()

def _calculate_raw_rates(
    panel_details_df_filtered: pd.DataFrame,
    base_info_df_filtered: pd.DataFrame, 
    target_defects: list,
    entity_id_col: str
) -> Dict[str, Any] | None:
    """
    [辅助函数 - 通用 V2.2 - 回归原始版] 
    策略：完全回归到 V1.6 的逻辑（只保留有不良记录的 Lot），
    仅增加了一行列名去重代码以修复 ValueError。
    """
    logging.info(f"开始计算{entity_id_col}级原始不良率 (回归原始逻辑)...")
    
    if base_info_df_filtered.index.name != entity_id_col:
        logging.error(f"索引不匹配: 期望 '{entity_id_col}'，实际 '{base_info_df_filtered.index.name}'")
        return None

    try:
        # --- 步骤 1: 计算分子 (仅含 count > 0 的记录) ---
        code_numerators = pd.DataFrame(columns=[entity_id_col, 'defect_group', 'defect_desc', 'defect_panel_count'])
        if not panel_details_df_filtered.empty:
            code_numerators = panel_details_df_filtered.groupby(
                [entity_id_col, 'defect_group', 'defect_desc']
            )['panel_id'].nunique().reset_index(name='defect_panel_count')

        # --- 步骤 2: 准备 Group 级数据 ---
        group_numerators = pd.DataFrame()
        if not code_numerators.empty:
            group_numerators = code_numerators.groupby([entity_id_col, 'defect_group'])['defect_panel_count'].sum()
        
        group_numerators_df = group_numerators.unstack(level='defect_group').fillna(0)
        group_summary_df = base_info_df_filtered.join(group_numerators_df, how='left').fillna(0)
        final_group_df = group_summary_df.reset_index()

        # 计算 Group Rate
        rate_cols = []
        for defect_type in target_defects:
            count_col_name = defect_type
            if count_col_name not in final_group_df.columns: final_group_df[count_col_name] = 0
            new_count_col_name = f"{defect_type.lower()}_count"
            final_group_df.rename(columns={count_col_name: new_count_col_name}, inplace=True, errors='ignore')
            
            rate_col_name = f"{defect_type.lower()}_rate"
            if new_count_col_name in final_group_df.columns and 'total_panels' in final_group_df.columns:
                final_group_df[rate_col_name] = np.where(
                    final_group_df['total_panels'] > 0,
                    final_group_df[new_count_col_name] / final_group_df['total_panels'], 0
                )
            else:
                final_group_df[rate_col_name] = 0.0
            rate_cols.append(rate_col_name)

        # --- 步骤 3: 准备 Code 级数据 ---
            
        # a. 准备基础信息
        base_info_for_code = base_info_df_filtered.reset_index()
        
        # [这是唯一的新增修改] 防止 'lot_id' 重复导致的 ValueError
        base_cols_for_code = [entity_id_col]
        if 'lot_id' in base_info_for_code.columns and entity_id_col != 'lot_id': base_cols_for_code.append('lot_id')
        for col in ['warehousing_time', 'array_input_time', 'total_panels', 'pass_rate']:
            if col in base_info_for_code.columns: base_cols_for_code.append(col)
        # 去重
        base_cols_for_code = list(dict.fromkeys(base_cols_for_code))
        
        # 清理 base_info_for_code 重复列
        if base_info_for_code.columns.duplicated().any():
            base_info_for_code = base_info_for_code.loc[:, ~base_info_for_code.columns.duplicated()]
            
        # 确保列存在
        base_cols_for_code = [c for c in base_cols_for_code if c in base_info_for_code.columns]
        base_info_subset_for_code = base_info_for_code[base_cols_for_code].drop_duplicates(subset=[entity_id_col])

        # b. 清理 code_numerators
        if code_numerators.columns.duplicated().any():
            code_numerators = code_numerators.loc[:, ~code_numerators.columns.duplicated()]

        # c. 执行 Merge (原始逻辑：以 code_numerators 为主)
        # 这保证了只保留有不良记录的 Lot，绝不会产生 0 值空位
        if code_numerators.empty:
            all_codes_with_base = pd.DataFrame()
        else:
            if entity_id_col in code_numerators.columns and entity_id_col in base_info_subset_for_code.columns:
                all_codes_with_base = pd.merge(
                    code_numerators,              # 左表！
                    base_info_subset_for_code,    # 右表
                    on=entity_id_col,
                    how='left'
                )
            else:
                logging.error(f"缺少连接键 '{entity_id_col}'，无法合并。")
                return None

        # d. 计算 Rate
        if all_codes_with_base.empty:
            all_codes_with_base['defect_rate'] = np.nan
        else:
            all_codes_with_base['total_panels'] = all_codes_with_base['total_panels'].fillna(0)
            all_codes_with_base['defect_rate'] = np.where(
                all_codes_with_base['total_panels'] > 0,
                all_codes_with_base['defect_panel_count'] / all_codes_with_base['total_panels'],
                0.0
            )

        # --- 步骤 4: 分组整理 ---
        # 直接调用原始的 _prepare_code_level_details (不需要额外的过滤逻辑了)
        code_level_details_dict = _prepare_code_level_details(
            all_codes_with_base=all_codes_with_base,
            target_defects=target_defects,
            entity_id_col=entity_id_col
        )

        return {
            "group_level_summary_for_table": final_group_df.fillna(0),
            "group_level_summary_for_chart": final_group_df,
            "code_level_details": code_level_details_dict
        }

    except Exception as e:
        logging.error(f"计算{entity_id_col}级原始不良率时出错: {e}", exc_info=True)
        return None

# 请务必也替换这个辅助函数，确保它也是原始纯净版
def _prepare_code_level_details(
    all_codes_with_base: pd.DataFrame, 
    target_defects: list,             
    entity_id_col: str                
) -> Dict[str, pd.DataFrame]:
    """
    [辅助函数 V1.0 - 原始版] 
    """
    code_level_details_dict = {}
    
    detail_cols_ordered = [
        entity_id_col, 'lot_id', 'warehousing_time', 'array_input_time',
        'defect_group', 'defect_desc', 'defect_panel_count', 'defect_rate',
        'total_panels', 'pass_rate'
    ]

    for group in target_defects:
        subset_df = pd.DataFrame()
        if not all_codes_with_base.empty and 'defect_group' in all_codes_with_base.columns:
                subset_df = all_codes_with_base.loc[all_codes_with_base['defect_group'] == group].copy()

        final_cols_temp = [col for col in detail_cols_ordered if col in subset_df.columns]
        final_cols = list(dict.fromkeys(final_cols_temp))

        if subset_df.empty:
            code_level_details_dict[group] = pd.DataFrame(columns=final_cols)
            continue 

        if subset_df.columns.duplicated().any():
                subset_df = subset_df.loc[:, ~subset_df.columns.duplicated()]
        
        if entity_id_col not in subset_df.columns:
                code_level_details_dict[group] = subset_df
                continue

        final_code_df_subset = subset_df[[c for c in final_cols if c in subset_df.columns]]

        try:
            final_code_df_subset_reset = final_code_df_subset.reset_index(drop=True)
            sort_keys = [key for key in [entity_id_col, 'defect_rate'] if key in final_code_df_subset.columns]
            
            if len(sort_keys) == 2: 
                code_level_details_dict[group] = final_code_df_subset_reset.sort_values(
                    by=sort_keys, ascending=[True, False]
                )
            elif len(sort_keys) == 1:
                code_level_details_dict[group] = final_code_df_subset_reset.sort_values(by=sort_keys[0])
            else:
                code_level_details_dict[group] = final_code_df_subset_reset

        except ValueError:
                code_level_details_dict[group] = final_code_df_subset

    return code_level_details_dict

def _get_desc_to_group_map(panel_details_df: pd.DataFrame) -> dict:
    """
    [辅助函数 V1.0] 从 Panel 数据构建 defect_desc 到 defect_group 的映射字典。
    """
    if panel_details_df is None or panel_details_df.empty or \
       'defect_desc' not in panel_details_df.columns or \
       'defect_group' not in panel_details_df.columns:
        logging.warning("无法构建 Desc -> Group 映射，Panel 数据无效或缺少列。")
        return {}
    try:
        # 去重并处理 NaN
        mapping_df = panel_details_df[['defect_desc', 'defect_group']].dropna().drop_duplicates(subset=['defect_desc'])
        desc_to_group = mapping_df.set_index('defect_desc')['defect_group'].to_dict()
        logging.info(f"成功构建了 {len(desc_to_group)} 条 Desc -> Group 映射。")
        return desc_to_group
    except Exception as e:
        logging.error(f"构建 Desc -> Group 映射时出错: {e}", exc_info=True)
        return {}

# --- 重聚合 ---
def _reaggregate_groups_from_codes(
        sim_code_details: Dict[str, pd.DataFrame],
        raw_base_info_df: pd.DataFrame,
        target_defects: list,
        entity_id_col: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        [辅助函数 V1.8 - 包含自愈机制] 重聚合 Group 数据。
        修复逻辑：在聚合前，先检查 sim_code_details 中是否存在 raw_base_info_df 缺失的 Entity (如覆盖插入的 Sheet)，
        如果有，则从 Code 数据中提取元数据反向补全基础信息，防止 Left Join 导致数据丢失。
        """
        logging.info(f"模拟/覆盖完成，正在重新聚合 {entity_id_col} 的 Group 级数据 (含基础信息完整性检查)...")
        
        if not sim_code_details or all(df.empty for df in sim_code_details.values()):
            return pd.DataFrame(), pd.DataFrame()

        try:
            # 1. 合并所有不良明细数据 (这是最新的事实数据)
            all_simulated_entities = pd.concat(sim_code_details.values(), ignore_index=True)
            if all_simulated_entities.empty:
                return pd.DataFrame(), pd.DataFrame()

            # --- [关键修复: 基础信息自愈] ---
            # 准备基础信息的 Master Copy
            if entity_id_col not in raw_base_info_df.columns and raw_base_info_df.index.name != entity_id_col:
                 # 极端情况防御
                 base_info_master = raw_base_info_df.copy()
            else:
                 # 确保 ID 是列
                 base_info_master = raw_base_info_df.reset_index() if raw_base_info_df.index.name == entity_id_col else raw_base_info_df.copy()

            # 提取现有的 ID 集合 (转字符串以防类型不匹配)
            if entity_id_col in base_info_master.columns:
                existing_ids = set(base_info_master[entity_id_col].astype(str).str.strip())
            else:
                existing_ids = set()

            # 从 Code 数据中提取所有出现的 ID
            if entity_id_col in all_simulated_entities.columns:
                active_ids_series = all_simulated_entities[entity_id_col].astype(str).str.strip()
                # 找出 "黑户" (在 Code 中有，但 Base 中没有的 ID)
                missing_mask = ~active_ids_series.isin(existing_ids)
                
                if missing_mask.any():
                    missing_ids = active_ids_series[missing_mask].unique()
                    logging.warning(f"检测到 {len(missing_ids)} 个实体 (如 {missing_ids[:3]}...) 在基础信息中缺失，正在从不良明细中恢复元数据...")
                    
                    # 提取元数据列 (在 _override_rates 中我们已经确保新行包含这些列)
                    meta_cols = [entity_id_col, 'total_panels', 'pass_rate', 'warehousing_time', 'lot_id', 'array_input_time']
                    meta_cols = list(dict.fromkeys(meta_cols)) # [新增修复]：强制去重，防止 entity_id_col 和 lot_id 重复！
                    
                    # 只提取存在的列
                    available_meta_cols = [c for c in meta_cols if c in all_simulated_entities.columns]
                    
                    # 提取并去重
                    recovered_rows = all_simulated_entities.loc[missing_mask, available_meta_cols].drop_duplicates(subset=[entity_id_col])
                    
                    # 追加到主名册
                    base_info_master = pd.concat([base_info_master, recovered_rows], ignore_index=True)
                    logging.info("基础信息补全完成。")
            # -------------------------------

            # 2. 准备 Group 级分子 (Pivot)
            group_numerators = all_simulated_entities.groupby([entity_id_col, 'defect_group'])['defect_panel_count'].sum()
            group_numerators_df = group_numerators.unstack(level='defect_group').fillna(0)

            # 3. 准备最终的基础信息 (用于 Join)
            # 此时 base_info_master 已经包含了所有需要的人
            base_cols_to_keep = [entity_id_col, 'total_panels', 'pass_rate']
            for col in ['lot_id', 'warehousing_time', 'array_input_time']:
                if col in base_info_master.columns and col not in base_cols_to_keep:
                    base_cols_to_keep.append(col)
            
            # 清理并设置索引
            base_info_subset_df = base_info_master[base_cols_to_keep].drop_duplicates(subset=[entity_id_col]).set_index(entity_id_col)

            # 4. 执行 Join (现在 Left Join 安全了，因为左边包含了所有新 ID)
            group_summary_df = base_info_subset_df.join(group_numerators_df, how='left').fillna(0)
            final_group_df = group_summary_df.reset_index()

            # 5. 计算比率 (Rate)
            rate_cols = []
            for defect_type in target_defects:
                count_col_name = defect_type
                if count_col_name not in final_group_df.columns: 
                    final_group_df[count_col_name] = 0
                
                new_count_col_name = f"{defect_type.lower()}_count"
                final_group_df.rename(columns={count_col_name: new_count_col_name}, inplace=True)
                
                rate_col_name = f"{defect_type.lower()}_rate"
                # 避免分母为0
                final_group_df[rate_col_name] = np.where(
                    final_group_df['total_panels'] > 0,
                    final_group_df[new_count_col_name] / final_group_df['total_panels'], 
                    0
                )
                rate_cols.append(rate_col_name)

            # 6. 准备 UI 格式
            final_ui_columns_base = [entity_id_col, 'pass_rate']
            for col in ['lot_id', 'warehousing_time', 'array_input_time']:
                if col in final_group_df.columns and col not in final_ui_columns_base:
                    final_ui_columns_base.append(col)
            
            final_ui_columns = final_ui_columns_base + rate_cols
            final_ui_columns = [col for col in final_ui_columns if col in final_group_df.columns]
            
            group_level_for_ui = final_group_df.reindex(columns=final_ui_columns).fillna(0)

            return group_level_for_ui, final_group_df

        except Exception as e:
            logging.error(f"重聚合 Group 数据 ({entity_id_col}) 时出错: {e}", exc_info=True)
            return pd.DataFrame(), pd.DataFrame()

# ==============================================================================
#                      辅助函数：处理截断
# ==============================================================================
