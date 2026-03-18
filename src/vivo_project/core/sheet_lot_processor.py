# src/vivo_project/core/sheet_lot_processor.py
import pandas as pd
import numpy as np
import logging
import comtypes.client
import comtypes
from pathlib import Path
from typing import Dict, Any, Optional
from collections import defaultdict
from datetime import datetime

# [Refactor] 移除全局 CONFIG, PROJECT_ROOT, RESOURCE_DIR
from vivo_project.config_model import AppConfig

# ==============================================================================
#             ByCode计算Sheet级不良率
# ==============================================================================
@staticmethod
def calculate_sheet_defect_rates(
    panel_details_df: pd.DataFrame,
    array_input_times_df: pd.DataFrame,
    lot_results: Dict[str, Any], # 接收 Lot 结果
    config: AppConfig,          
    product_dir: Path,         
) -> Dict[str, Any] | None:
    """(V5.0) Sheet 级完全听命于 Lot 发牌"""
    logging.info("开始Sheet级计算 (Lot局域分发 -> 覆盖 模式)...")
    try:
        # 1. 基础信息聚合 
        agg_rules = {'panel_id': 'nunique', 'lot_id': 'first', 'warehousing_time': 'first'}
        sheet_base = panel_details_df.groupby('sheet_id').agg(agg_rules).rename(columns={'panel_id': 'total_panels'})
        if sheet_base.index.name == 'sheet_id': sheet_base = sheet_base.reset_index()

        if not array_input_times_df.empty:
            if sheet_base.index.name == 'sheet_id': sheet_base = sheet_base.reset_index()
            sheet_base = pd.merge(sheet_base, array_input_times_df, on='sheet_id', how='left')
        else:
            sheet_base['array_input_time'] = pd.NaT

        # 2. 过滤 
        sheet_base_filtered = _filter_by_pass_rate(sheet_base.copy(), 190, 0, "sheet")
        if sheet_base_filtered.empty: return None
        
        valid_ids = sheet_base_filtered['sheet_id'].unique()
        panel_df_filtered = panel_details_df[panel_details_df['sheet_id'].isin(valid_ids)]

        # 3. 原始计算
        target_defects = sorted(panel_details_df['defect_group'].dropna().unique().tolist())
        raw_results = _calculate_raw_rates(
            panel_details_df_filtered=panel_df_filtered,
            base_info_df_filtered=sheet_base_filtered.set_index('sheet_id'),
            target_defects=target_defects,
            entity_id_col='sheet_id'
        )
        if not raw_results: return None

        # =====================================================================
        # 🛑 [DEBUG] 奉命行事：核查 L3MR5C037 的真实 Sheet 存活情况
        # =====================================================================
        target_lot = "L3MR5C037"
        if target_lot in sheet_base_filtered['lot_id'].values:
            base_sheets = sheet_base_filtered[sheet_base_filtered['lot_id'] == target_lot]['sheet_id'].tolist()
            logging.warning(f"🔍 [追踪验证] 基础花名册中 '{target_lot}' 共有 {len(base_sheets)} 张物理 Sheet: {base_sheets}")

            # 检查这批 Sheet 在 raw_results['code_level_details'] 里的存活情况 (即 V5.1 的发牌名单)
            survivors = set()
            for g, df_code in raw_results['code_level_details'].items():
                if not df_code.empty and 'lot_id' in df_code.columns:
                    survivors.update(df_code[df_code['lot_id'] == target_lot]['sheet_id'].tolist())
            logging.warning(f"🔍 [追踪验证] 经历了左连接后，该 Lot 在原始缺陷表中存活的 Sheet 仅有 {len(survivors)} 张: {list(survivors)}")
            logging.warning(f"🔍 [结论推导] 如果运行 V5.1，不良将被全部砸在这 {len(survivors)} 张 Sheet 上。")
        # =====================================================================
        
        # 🚀 4. 分发数据 (核心变动：彻底取代 _simulate_concentration)
        sim_code_details = _distribute_sheet_from_lot(
            sheet_raw_results=raw_results, 
            lot_results=lot_results, 
            processing_config=config.processing
        )
        
        # 此时的 current_code_details 已经是完美的、且截断过的数据
        current_code_details = sim_code_details

        # --- 6. 应用覆盖 (Override) ---
        # [Refactor] 从 config.paths 中获取 FileResource 对象
        override_res = config.paths.get('rate_override_config')
        
        override_file_path = None
        override_sheet_name = ""
        
        if override_res:
             override_file_path = product_dir / override_res.file_name
             override_sheet_name = override_res.sheet_name or ""
        
        override_df, _ = _load_override_excel(
            override_file_path, 
            override_sheet_name
        )
        
        desc_map = _get_desc_to_group_map(panel_details_df)
        
        final_code_details = _override_rates(
            simulated_code_details_dict=current_code_details,
            override_data_df=override_df,
            entity_id_col='sheet_id',
            desc_to_group_map=desc_map
        )

        # --- 7. 重聚合 (Re-aggregate) ---
        base_info_reagg = raw_results['group_level_summary_for_chart']
        if base_info_reagg.index.name != 'sheet_id': base_info_reagg = base_info_reagg.reset_index()

        ui_df, chart_df = _reaggregate_groups_from_codes(
            sim_code_details=final_code_details,
            raw_base_info_df=base_info_reagg,
            target_defects=target_defects,
            entity_id_col='sheet_id'
        )

        final_results = {
            "group_level_summary_for_table": ui_df,
            "group_level_summary_for_chart": chart_df,
            "code_level_details": final_code_details
        }
        
        logging.info("Sheet级计算完成。")
        return final_results

    except Exception as e:
        logging.error(f"Sheet级计算异常: {e}", exc_info=True)
        return None


# ==============================================================================
#                       ByCode计算Lot级不良率
# ==============================================================================
@staticmethod
def calculate_lot_defect_rates(
    panel_details_df: pd.DataFrame,
    array_input_times_df: pd.DataFrame, # 接收时间表
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    config: AppConfig,
    product_dir: Path,
    warning_lines: Optional[Dict[str, dict]] = None
) -> Dict[str, Any] | None:
    """(V5.0) 独立执行 Lot 级数据模拟"""
    logging.info("开始Lot级计算 (独立模拟 -> 截断 -> 覆盖 模式)...")
    try:
        lot_base = _calculate_lot_base_info_with_median_time(panel_details_df, array_input_times_df)
        if lot_base.empty: return None

        lot_base_filtered = _filter_by_pass_rate(lot_base.copy(), 190 * 30, 0.2, "Lot")
        if lot_base_filtered.empty: return None
        
        valid_lots = lot_base_filtered['lot_id'].unique()
        panel_df_lot = panel_details_df[panel_details_df['lot_id'].isin(valid_lots)]

        target_defects = sorted(panel_details_df['defect_group'].dropna().unique().tolist())
        raw_lot_results = _calculate_raw_rates(
            panel_details_df_filtered=panel_df_lot,
            base_info_df_filtered=lot_base_filtered.set_index('lot_id'),
            target_defects=target_defects,
            entity_id_col='lot_id'
        )
        if not raw_lot_results: return None

        # 🚀 4. 模拟 (离散Token分配版 _simulate_concentration，它在大容器 Lot 级会非常稳定，因为四舍五入的损耗极小）
        sim_lot_codes = _simulate_concentration(
            raw_results=raw_lot_results, 
            mwd_code_data=mwd_code_data, 
            processing_config=config.processing, 
            entity_id_col='lot_id'
        )
        if not isinstance(sim_lot_codes, dict): sim_lot_codes = raw_lot_results['code_level_details']
        
        current_lot_results = raw_lot_results.copy()
        current_lot_results['code_level_details'] = sim_lot_codes

        # --- 5. 截断 (Capping) ---
        capping_cfg = config.processing.get('defect_capping', {})
        
        if capping_cfg.get('enable', True):
            capped_results = _apply_defect_capping(
                results_dict=current_lot_results,
                warning_lines=warning_lines or {}
            )
            current_code_details = capped_results['code_level_details']
        else:
            # [修复]: 原代码这里少写了 ['code_level_details']，已补充
            current_code_details = current_lot_results['code_level_details']

        # --- 6. 覆盖 (Override) ---
        override_res = config.paths.get('rate_override_config')
        
        override_file_path = None
        override_sheet_name = ""
        
        if override_res:
                override_file_path = product_dir / override_res.file_name
                override_sheet_name = override_res.sheet_name or ""

        override_sheet_df, _= _load_override_excel(
            override_file_path, 
            override_sheet_name
        )
        
        override_lot_avg = _calculate_lot_override_rate_heuristic(
            override_df=override_sheet_df,
            lot_base_info_df=lot_base,
            mwd_code_data=mwd_code_data
        )
        
        desc_map = _get_desc_to_group_map(panel_details_df)
        
        final_code_details = _override_rates(
            simulated_code_details_dict=current_code_details,
            override_data_df=override_lot_avg,
            entity_id_col='lot_id',
            desc_to_group_map=desc_map
        )

        # --- 7. 重聚合 ---
        base_info_reagg = raw_lot_results['group_level_summary_for_chart']
        if base_info_reagg.index.name != 'lot_id': base_info_reagg = base_info_reagg.reset_index()

        ui_df, chart_df = _reaggregate_groups_from_codes(
            sim_code_details=final_code_details,
            raw_base_info_df=base_info_reagg,
            target_defects=target_defects,
            entity_id_col='lot_id'
        )

        return {
            "group_level_summary_for_table": ui_df,
            "group_level_summary_for_chart": chart_df,
            "code_level_details": final_code_details
        }

    except Exception as e:
        logging.error(f"Lot级计算异常: {e}", exc_info=True)
        return None

# ==============================================================================
#                      辅助函数：计算数据
# ==============================================================================
@staticmethod
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

@staticmethod
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
@staticmethod
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

@staticmethod
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
                
                entity_capacities = group_df['total_panels'].values
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
@staticmethod
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
                
                df_code_mod['date_key'] = df_code_mod[entity_id_col].map(date_map_str)
                
                lookup_dict = {}
                if df_daily_ema is not None:
                    code_ema_data = df_daily_ema[df_daily_ema['defect_desc'] == code_desc].copy()
                    code_ema_data['date_key'] = code_ema_data['time_period'].astype(str).str.replace('-', '')
                    lookup_dict = code_ema_data.set_index('date_key')['defect_rate'].to_dict()

                # 🚀 核心映射：获取当日大盘基准
                df_code_mod['daily_base_rate'] = df_code_mod['date_key'].map(lookup_dict).fillna(0.0)
                
                # =========================================================
                # 🚀 [核心修复：微观扰动] 
                # 为同一天内的每个 Lot/Sheet 赋予 ±30% 的随机浮动，打破阶梯状
                # =========================================================
                # 只有当基准率大于 0 时才施加扰动，提升计算效率
                mask_positive = df_code_mod['daily_base_rate'] > 0
                if mask_positive.any():
                    # 生成 0.7 到 1.3 之间的随机因子
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
                
                out_path = Path("logs/debug_lot_simulation.csv")
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
@staticmethod
def _load_override_excel(
    override_file_path: Optional[Path],
    override_sheet_name: str
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    [Refactor] 接收完整的 Path 对象
    """
    if not override_file_path or not override_sheet_name:
        return None, None
        
    logging.info(f"--- [COM Loader] 开始加载覆盖数据 (文件: '{override_file_path.name}') ---")
    abs_path = str(override_file_path.resolve())

    if not override_file_path.exists():
        logging.error(f"[COM] 文件不存在: {abs_path}")
        return None, None

    # --- COM 初始化 ---
    try:
        comtypes.CoInitialize()
    except:
        pass 

    excel_app = None
    workbook = None
    
    try:
        # [逻辑保持不变，仅路径来源变了]
        logging.info("[COM] 正在启动 Excel 应用程序实例...")
        excel_app = comtypes.client.CreateObject("Excel.Application")
        excel_app.Visible = False
        excel_app.DisplayAlerts = False 

        logging.info(f"[COM] 正在打开工作簿: {abs_path}")
        workbook = excel_app.Workbooks.Open(abs_path)

        try:
            sheet = workbook.Sheets(override_sheet_name)
        except Exception:
            logging.error(f"[COM] 找不到名为 '{override_sheet_name}' 的 Sheet 页。")
            return None, None

        raw_data = sheet.UsedRange.Value()
        
        if not raw_data or len(raw_data) < 2:
            logging.warning("[COM] Excel 数据为空或只有表头。")
            return None, None

        logging.info(f"[COM] 成功通过 Excel 提取数据，共 {len(raw_data)} 行。")

        header = raw_data[0]
        rows = raw_data[1:]
        
        rows_cleaned = []
        for row in rows:
            rows_cleaned.append(list(row) if row else [None]*len(header))

        df = pd.DataFrame(rows_cleaned, columns=list(header))

        expected_cols = ['lot_id', 'sheet_id', 'override_rate', 'defect_desc']
        
        df.columns = [str(c).strip() for c in df.columns]
        
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"[COM] 缺少必需列: {missing_cols}。实际列: {df.columns.to_list()}")
            return None, None

        if df['override_rate'].dtype == 'object':
             df['override_rate'] = df['override_rate'].astype(str).str.rstrip('%')
             df['override_rate'] = pd.to_numeric(df['override_rate'], errors='coerce')
             if df['override_rate'].mean() > 1.0:
                 df['override_rate'] = df['override_rate'] / 100.0

        df['defect_desc'] = df['defect_desc'].astype(str).str.strip()
        df.dropna(subset=expected_cols, inplace=True)
        
        lot_override_df = df.groupby(['lot_id', 'defect_desc'])['override_rate'].mean().reset_index()
        lot_override_df.rename(columns={'override_rate': 'override_rate_avg'}, inplace=True)

        return df[expected_cols], lot_override_df[['lot_id', 'defect_desc', 'override_rate_avg']]

    except Exception as e:
        logging.error(f"[COM] Excel 读取失败: {e}", exc_info=True)
        return None, None

    finally:
        if workbook:
            try:
                workbook.Close(False)
            except: pass
        if excel_app:
            try:
                excel_app.Quit()
            except: pass
        try:
            comtypes.CoUninitialize()
        except: pass

@staticmethod
def _calculate_lot_override_rate_heuristic(
    override_df: pd.DataFrame,
    lot_base_info_df: pd.DataFrame,
    mwd_code_data: Dict[str, pd.DataFrame] | None
) -> pd.DataFrame:
    """
    [新增 V1.0 - 启发式公式] 计算 Lot 级覆盖良损。
    公式: LotRate = 当月良损 + (同卡Sheet良损之和) / (30 + 同卡Sheet数)
    """
    logging.info("开始使用启发式公式计算 Lot 级覆盖不良率...")
    
    if override_df is None or override_df.empty:
        return pd.DataFrame()

    try:
        # 1. 计算 "同卡Sheet良损和" (Sum) 和 "同卡Sheet数" (Count)
        # -----------------------------------------------------------
        # 按 Lot 和 缺陷描述分组聚合
        lot_stats = override_df.groupby(['lot_id', 'defect_desc'])['override_rate'].agg(
            rate_sum='sum',
            sheet_count='count'
        ).reset_index()
        
        # 2. 准备 "当月良损" (Base Rate)
        # -----------------------------------------------------------
        # 需要先获取每个 Lot 的时间，以便匹配月度数据
        if lot_base_info_df is not None and not lot_base_info_df.empty:
            # 仅保留需要的列
            lot_dates = lot_base_info_df[['lot_id', 'warehousing_time']].drop_duplicates()
            # 合并时间信息到统计表
            lot_stats = pd.merge(lot_stats, lot_dates, on='lot_id', how='left')
        else:
            lot_stats['warehousing_time'] = pd.NaT
            logging.warning("缺少 Lot 基础信息，无法匹配当月良损，将默认当月良损为 0。")

        # 将时间转换为 YYYY-MM 格式以匹配 mwd_code_data
        lot_stats['time_period'] = pd.to_datetime(
            lot_stats['warehousing_time'], format='%Y%m%d', errors='coerce'
        ).dt.strftime('%Y-%m月')
        
        # 从 mwd_code_data 中提取月度基准
        monthly_map = {}
        if mwd_code_data and 'monthly' in mwd_code_data:
            df_monthly = mwd_code_data['monthly']
            if not df_monthly.empty and {'time_period', 'defect_desc', 'defect_rate'}.issubset(df_monthly.columns):
                # 构建查找字典: (时间, 描述) -> 率
                # 预处理：确保 rate 是 float
                df_monthly['defect_rate'] = pd.to_numeric(df_monthly['defect_rate'], errors='coerce').fillna(0)
                monthly_map = df_monthly.set_index(['time_period', 'defect_desc'])['defect_rate'].to_dict()
            else:
                logging.warning("月度趋势数据格式不正确或为空。")

        # 定义查找函数
        def get_base_rate(row):
            key = (row.get('time_period'), row['defect_desc'])
            return monthly_map.get(key, 0.0)

        # 应用查找
        lot_stats['base_rate'] = lot_stats.apply(get_base_rate, axis=1)
        
        # 3. 应用最终公式
        # -----------------------------------------------------------
        # 公式: Base + Sum / (30 + Count)
        # 注意: 平滑因子 30 是硬编码的经验值
        smoothing_factor = 30
        lot_stats['override_rate_avg'] = lot_stats['base_rate'] + (
            lot_stats['rate_sum'] / (float(smoothing_factor) + lot_stats['sheet_count'])
        )
        
        logging.info(f"Lot 级覆盖率计算完成，共计算 {len(lot_stats)} 条记录。")
        
        # 返回符合 _override_rates 预期的格式: [lot_id, defect_desc, override_rate_avg]
        return lot_stats[['lot_id', 'defect_desc', 'override_rate_avg']]

    except Exception as e:
        logging.error(f"使用启发式公式计算 Lot 覆盖率时出错: {e}", exc_info=True)
        # 出错时返回空 DF，避免中断主流程
        return pd.DataFrame()

@staticmethod
def _override_rates(
        simulated_code_details_dict: Dict[str, pd.DataFrame],
        override_data_df: pd.DataFrame | None,
        entity_id_col: str,
        desc_to_group_map: dict
    ) -> Dict[str, pd.DataFrame]:
        """
        [核心函数 V1.8 - 物理常识防呆版] 使用外部数据覆盖模拟的不良率。
        彻底移除了无脑的“暴力插入”。现在系统会严格审查实体是否在当前时间窗口的物理基座中存活。
        """
        if override_data_df is None or override_data_df.empty:
            logging.info(f"无覆盖数据提供 ({entity_id_col} 级别)，跳过覆盖步骤。")
            return simulated_code_details_dict

        # --- 动态定义必需列 ---
        rate_col_name = 'override_rate' if entity_id_col == 'sheet_id' else 'override_rate_avg'
        required_cols = ['lot_id', 'defect_desc', rate_col_name]
        if entity_id_col == 'sheet_id': required_cols.append('sheet_id')
        required_cols = list(dict.fromkeys(required_cols)) 

        missing_cols = [col for col in required_cols if col not in override_data_df.columns]
        if missing_cols:
            logging.error(f"覆盖数据 DataFrame ({entity_id_col}) 缺少必需列: {missing_cols}，无法执行覆盖。")
            return simulated_code_details_dict

        logging.info(f"开始使用外部数据覆盖 {entity_id_col} 级别的不良率 (严格审查模式)...")
        
        # --- [审计准备] ---
        all_config_ids = set(override_data_df[entity_id_col].astype(str).str.strip().unique())
        processed_ids = set()
        watchlist = ['L3MR5A0B023', 'L3MR5A0B026']

        final_results_dict = {group: df.copy() for group, df in simulated_code_details_dict.items() if df is not None}
        total_replaced_count = 0
        total_inserted_count = 0

        # --- 准备模板与物理花名册 ---
        all_sim_df_list = [df for df in final_results_dict.values() if not df.empty]
        if not all_sim_df_list:
            logging.error("无法执行任何操作，因为当前时间窗口内无任何物理基底数据。")
            return simulated_code_details_dict
        
        all_sim_df = pd.concat(all_sim_df_list, ignore_index=True)
        
        # 🛑 [核心防御: 提取当前时间窗口的合法实体名册]
        valid_entity_ids = set(all_sim_df[entity_id_col].astype(str).str.strip().unique())
        
        generic_template_row = all_sim_df.iloc[0]
        lot_specific_templates = all_sim_df.drop_duplicates(subset=['lot_id']).set_index('lot_id')
        
        new_rows_to_add_by_group = defaultdict(list)
        processed_indices = set()

        # --- 遍历覆盖 DataFrame ---
        for index, override_row in override_data_df.iterrows():
            target_desc = str(override_row['defect_desc']).strip()
            target_entity_id = str(override_row[entity_id_col]).strip() 
            target_lot_id = override_row['lot_id']
            override_rate = override_row[rate_col_name]

            is_target_trace = target_entity_id in watchlist
            if is_target_trace:
                logging.warning(f"!!! [追踪] 发现 Excel 指令 ID: {target_entity_id}")
                logging.warning(f"    - 缺陷描述: '{target_desc}'")

            # =================================================================
            # 🛑 [防呆拦截拦截机制生效]
            # 拒绝跨时空的幽灵实体插入，捍卫物质守恒定律！
            # =================================================================
            if target_entity_id not in valid_entity_ids:
                if is_target_trace:
                    logging.error(f"    -> [拦截] 实体 '{target_entity_id}' 在当前数据底座中物理不存在！拒绝凭空捏造。")
                else:
                    logging.debug(f"跳过覆盖: 实体 '{target_entity_id}' 在当前窗口内不存在或已被过滤。")
                continue # 直接跳过，不计入 processed_ids

            # 1. 查找目标 Group
            target_group = desc_to_group_map.get(target_desc)
            if not target_group:
                continue

            # 2. 查找目标 Group 的 DataFrame
            target_df = final_results_dict.get(target_group)
            if target_df is None:
                continue 

            # 3. 匹配行
            match_mask = pd.Series(False, index=target_df.index)
            if not target_df.empty:
                match_mask = (target_df[entity_id_col].astype(str).str.strip() == target_entity_id) & \
                             (target_df['defect_desc'] == target_desc)

            matched_indices = target_df.index[match_mask]

            # 4. 执行
            if not matched_indices.empty:
                # --- 替换 ---
                if is_target_trace: logging.warning(f"    -> [动作] 找到匹配缺陷记录，执行替换。")
                target_df.loc[matched_indices, 'defect_rate'] = override_rate
                if 'total_panels' in target_df.columns:
                    panels = target_df.loc[matched_indices, 'total_panels']
                    new_counts = np.maximum(0, np.round(override_rate * panels)).astype(int)
                    target_df.loc[matched_indices, 'defect_panel_count'] = new_counts
                    if index not in processed_indices:
                        total_replaced_count += len(matched_indices)
                        processed_indices.add(index)
                        processed_ids.add(target_entity_id) 
            else:
                # --- 插入 (仅针对合法存在的实体插入它原先没有的缺陷) ---
                if is_target_trace: logging.warning(f"    -> [动作] 该合法实体未包含该缺陷，准备插入新缺陷记录。")
                
                # [优化]: 寻找最精确的模板行，优先抓取该实体自身的物理基础信息(如时间、过货率)
                entity_rows = all_sim_df[all_sim_df[entity_id_col].astype(str).str.strip() == target_entity_id]
                if not entity_rows.empty:
                    template_row = entity_rows.iloc[0]
                elif target_lot_id in lot_specific_templates.index:
                    template_row = lot_specific_templates.loc[target_lot_id]
                else:
                    template_row = generic_template_row

                try:
                    template_panels = float(template_row.get('total_panels', 1))
                    if template_panels == 0: template_panels = 1.0

                    new_row = {
                        'sheet_id': target_entity_id if entity_id_col == 'sheet_id' else (template_row.get('sheet_id', '') if 'sheet_id' in template_row else ''),
                        'lot_id': target_lot_id,
                        'defect_desc': target_desc,
                        'defect_rate': override_rate,
                        'defect_group': target_group,
                        'total_panels': template_panels,
                        'defect_panel_count': np.maximum(0, np.round(override_rate * template_panels)).astype(int),
                        'warehousing_time': template_row.get('warehousing_time', ''),
                        'array_input_time': template_row.get('array_input_time', pd.NaT),
                        'pass_rate': template_row.get('pass_rate', 0.0)
                    }
                    
                    if entity_id_col == 'lot_id': new_row['lot_id'] = target_entity_id 
                    
                    target_df_cols = target_df.columns.to_list()
                    if not target_df_cols and (target_group not in new_rows_to_add_by_group): 
                         target_df_cols = [col for col in ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time', 'defect_group', 'defect_desc', 'defect_panel_count', 'defect_rate', 'total_panels', 'pass_rate'] if col in new_row]
                         if final_results_dict.get(target_group) is None or final_results_dict.get(target_group).empty: # type: ignore
                             final_results_dict[target_group] = pd.DataFrame(columns=target_df_cols)
                    
                    new_row_filtered = {k: v for k, v in new_row.items() if k in target_df_cols}
                    new_rows_to_add_by_group[target_group].append(new_row_filtered)
                    
                    if index not in processed_indices:
                        total_inserted_count += 1
                        processed_indices.add(index)
                        processed_ids.add(target_entity_id) 
                        
                except Exception as insert_err:
                    logging.error(f"构建插入行失败 (ID: {target_entity_id}): {insert_err}", exc_info=True)

        # --- 合并新行 ---
        if new_rows_to_add_by_group:
            for group, new_rows in new_rows_to_add_by_group.items():
                if new_rows:
                    df_new = pd.DataFrame(new_rows)
                    target_df = final_results_dict.get(group)
                    if target_df is None: target_df = pd.DataFrame(columns=df_new.columns)
                    final_results_dict[group] = pd.concat([target_df, df_new], ignore_index=True).where(pd.notna, None) # type: ignore

        # --- [最终审计报告] ---
        failed_ids = all_config_ids - processed_ids
        
        logging.info(f"覆盖审计完成: Excel中共配置 {len(all_config_ids)} 个ID，成功应用 {len(processed_ids)} 个，拦截防呆 {len(failed_ids)} 个。")
        
        if failed_ids:
            failed_list = sorted(list(failed_ids))
            logging.warning("========== [物理防呆拦截名单] ==========")
            logging.warning(f"以下 ID 由于在当前数据底座中不存在，已被系统拒绝凭空捏造: {failed_list[:20]}")
            logging.warning("=======================================")

        return final_results_dict

@staticmethod
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
@staticmethod
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
@staticmethod
def _filter_by_pass_rate(
    base_df: pd.DataFrame,
    denominator: float,
    threshold: float,
    entity_name: str = "sheet"
) -> pd.DataFrame:
    """
    [辅助函数 - 通用] 按过货率筛选。
    """
    logging.info(f"开始进行{entity_name}过货率筛选 (阈值 >= {threshold:.1%})...")
    if 'total_panels' not in base_df.columns:
            logging.error(f"无法进行过货率筛选，基础 DataFrame 缺少 'total_panels' 列。")
            return pd.DataFrame() # 返回空以示失败
    if denominator <= 0:
        logging.error("过货率筛选的分母不能为零或负数。")
        return pd.DataFrame()

    base_df['pass_rate'] = base_df['total_panels'] / denominator
    original_count = len(base_df)
    df_filtered = base_df[base_df['pass_rate'] >= threshold].copy() # 使用 .copy() 避免 SettingWithCopyWarning
    filtered_count = len(df_filtered)
    logging.info(f"过货率筛选完成：从 {original_count} 个{entity_name}中筛选出 {filtered_count} 个。")
    return df_filtered

@staticmethod
def _apply_defect_capping(
    results_dict: Dict[str, Any],
    warning_lines: Optional[Dict[str, dict]] = None
) -> Dict[str, Any]:
    """
    [辅助函数 V3.0 - 单一职责：Code级精准截断] 
    仅对 Code 级数据应用专属的 Spec 区间截断。
    彻底抛弃 Group 级数据的同步截断，Group 数据将在后续通过严密的向上聚合得出，以捍卫物质守恒。
    """
    logging.info("开始对 Code 级不良率进行精准 Spec 截断处理...")
    
    if not isinstance(results_dict, dict) or "code_level_details" not in results_dict:
        logging.error("传递给 _apply_defect_capping 的输入不包含 code_level_details，无法截断。")
        return results_dict

    warning_lines = warning_lines or {}
    base_seed = 101
    
    try:
        # [核心重构] 我们只关心 Code 级数据，无视并抛弃原有的 Group 级视图
        dict_code_details = results_dict["code_level_details"].copy()
        rng_capping_code = np.random.default_rng(base_seed + 99)

        for group, df_code in dict_code_details.items():
            if df_code is not None and not df_code.empty and 'defect_rate' in df_code.columns:
                df_code_mod = df_code.copy()
                
                # 定义行级处理函数：直接计算最终数量
                def _row_capper(row):
                    code_name = str(row.get('defect_desc', '')).strip()
                    spec_dict = warning_lines.get(code_name) or {}
                    spec_upper = spec_dict.get('upper', 1.0)
                    spec_lower = spec_dict.get('lower', 0.0)
                    
                    return _apply_random_cap_and_floor(
                        rate=row['defect_rate'],
                        panels=row.get('total_panels', 0.0),
                        current_count=row.get('defect_panel_count', 0),
                        upper_threshold=spec_upper,
                        lower_threshold=spec_lower,
                        rng=rng_capping_code
                    )

                # 🛑 核心修复：直接把函数结果赋值给不良数量列
                df_code_mod['defect_panel_count'] = df_code_mod.apply(_row_capper, axis=1)

                # 🛑 物理铁律：根据截断/托底后的真实整数 Panel 数量，反推最终的微观 Rate
                df_code_mod['defect_rate'] = np.where(
                    df_code_mod['total_panels'] > 0,
                    df_code_mod['defect_panel_count'] / df_code_mod['total_panels'],
                    0.0
                )

                dict_code_details[group] = df_code_mod

        logging.info("Code 级不良率精准截断/托底处理完成。")

        # 封装结果
        final_capped_results = results_dict.copy()
        final_capped_results["code_level_details"] = dict_code_details
        
        # 🛑 [架构防御] 强行删除旧的 Group 数据，逼迫主流水线使用 _reaggregate_groups_from_codes
        final_capped_results.pop("group_level_summary_for_table", None)
        final_capped_results.pop("group_level_summary_for_chart", None)
        
        return final_capped_results

    except Exception as e:
        logging.error(f"在应用 Code 级截断时发生错误: {e}", exc_info=True)
        return results_dict

@staticmethod
def _apply_random_cap_and_floor(
    rate: float,
    panels: int,
    current_count: int,
    upper_threshold: float,
    lower_threshold: float,
    rng: np.random.Generator
) -> int:
    """
    [辅助函数 V2.0 - 软截断] 
    当 rate 超标时，返回 [Limit * 0.8, Limit] 之间的随机值，
    确保截断后的数据依然呈现自然的随机波动，而非死板的直线。
    """
    if rate > upper_threshold:
        # 上限保护：在 Spec 的 80% ~ 95% 之间随机浮动
        safe_rate = rng.uniform(upper_threshold * 0.8, upper_threshold * 0.9)
        new_count = int(np.floor(safe_rate * panels))
        return min(new_count, int(current_count)) # 物理铁律：压制后的数量绝不能比原来更高
        
    # elif 0 < rate < lower_threshold:
    #     # 下限保护 (保持不变)
    #     safe_rate = max(0, rng.uniform(lower_threshold * 1.1, lower_threshold * 1.2))
    #     new_count = int(np.ceil(safe_rate * panels))
    #     return max(new_count, int(current_count))
    else:
        return int(current_count)