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
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    start_date: datetime,
    config: AppConfig,          # [Refactor] 注入配置
    resource_dir: Path,         # [Refactor] 注入资源路径
    warning_lines: Optional[Dict[str, float]] = None
) -> Dict[str, Any] | None:
    """
    (V4.1 - 逻辑重构) 顺序: 基础聚合 -> 过滤 -> 原始计算 -> [模拟] -> [截断] -> [覆盖] -> 重聚合
    """
    logging.info("开始Sheet级计算 (截断 -> 覆盖 模式)...")
    
    try:
        # --- 1. 基础信息聚合 ---
        agg_rules = {'panel_id': 'nunique', 'lot_id': 'first', 'warehousing_time': 'first'}
        sheet_base = panel_details_df.groupby('sheet_id').agg(agg_rules).rename(columns={'panel_id': 'total_panels'})
        
        if sheet_base.index.name == 'sheet_id': 
            sheet_base = sheet_base.reset_index()

        if not array_input_times_df.empty:
            if sheet_base.index.name == 'sheet_id': sheet_base = sheet_base.reset_index()
            sheet_base = pd.merge(sheet_base, array_input_times_df, on='sheet_id', how='left')
        else:
            sheet_base['array_input_time'] = pd.NaT

        # --- 2. 过货率过滤 ---
        sheet_base_filtered = _filter_by_pass_rate(sheet_base.copy(), 190, 0.2, "sheet")
        if sheet_base_filtered.empty: return None
        
        valid_ids = sheet_base_filtered['sheet_id'].unique()
        panel_df_filtered = panel_details_df[panel_details_df['sheet_id'].isin(valid_ids)]

        # --- 3. 计算原始不良率 ---
        target_defects = sorted(panel_details_df['defect_group'].dropna().unique().tolist())
        raw_results = _calculate_raw_rates(
            panel_details_df_filtered=panel_df_filtered,
            base_info_df_filtered=sheet_base_filtered.set_index('sheet_id'),
            target_defects=target_defects,
            entity_id_col='sheet_id'
        )
        if not raw_results: raise Exception("原始计算失败")

        # --- 4. 模拟数据 ---
        # [Refactor] 传入 config.processing
        sim_code_details = _simulate_concentration(raw_results, mwd_code_data, config.processing, 'sheet_id')
        if not isinstance(sim_code_details, dict): 
            sim_code_details = raw_results['code_level_details']
        
        current_results = raw_results.copy()
        current_results['code_level_details'] = sim_code_details

        # --- 5. 应用截断 (Capping) ---
        capping_cfg = config.processing.get('defect_capping', {})
        
        if capping_cfg.get('enable', True):
            capped_results = _apply_defect_capping(
                results_dict=current_results,
                group_thresholds=capping_cfg.get('group_thresholds', {'upper': 1, 'lower': 0.005}),
                code_thresholds=capping_cfg.get('code_thresholds', {'upper': 1, 'lower': 0.001}),
                warning_lines=warning_lines or {}
            )
            current_code_details = capped_results['code_level_details']
        else:
            current_code_details = sim_code_details

        # --- 6. 应用覆盖 (Override) ---
        # [Refactor] 从 config.paths 中获取 FileResource 对象
        override_res = config.paths.get('rate_override_config')
        
        override_file_path = None
        override_sheet_name = ""
        
        if override_res:
             override_file_path = resource_dir / override_res.file_name
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
            "code_level_details": final_code_details,
            "full_sheet_base_info": sheet_base 
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
    sheet_results: Dict[str, Any],
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    start_date: datetime,
    config: AppConfig,
    resource_dir: Path,
    warning_lines: Optional[Dict[str, float]] = None
) -> Dict[str, Any] | None:
    """
    (V4.5 - 逻辑重构)
    """
    logging.info("开始Lot级计算 (截断 -> 覆盖 模式)...")

    try:
        # --- 1. Lot 基础信息 ---
        full_sheet_info = sheet_results.get("full_sheet_base_info")
        lot_base = _calculate_lot_base_info_with_median_time(panel_details_df, full_sheet_info)
        if lot_base.empty: return None

        # --- 2. 过滤 ---
        lot_base_filtered = _filter_by_pass_rate(lot_base.copy(), 190 * 30, 0.2, "Lot")
        if lot_base_filtered.empty: return None
        
        valid_lots = lot_base_filtered['lot_id'].unique()
        panel_df_lot = panel_details_df[panel_details_df['lot_id'].isin(valid_lots)]

        # --- 3. 原始计算 ---
        target_defects = sorted(panel_details_df['defect_group'].dropna().unique().tolist())
        raw_lot_results = _calculate_raw_rates(
            panel_details_df_filtered=panel_df_lot,
            base_info_df_filtered=lot_base_filtered.set_index('lot_id'),
            target_defects=target_defects,
            entity_id_col='lot_id'
        )
        if not raw_lot_results: raise Exception("Lot原始计算失败")

        # --- 4. 模拟 ---
        sim_lot_codes = _simulate_concentration(raw_lot_results, mwd_code_data, config.processing, 'lot_id')
        if not isinstance(sim_lot_codes, dict): sim_lot_codes = raw_lot_results['code_level_details']
        
        current_lot_results = raw_lot_results.copy()
        current_lot_results['code_level_details'] = sim_lot_codes

        # --- 5. 截断 (Capping) ---
        capping_cfg = config.processing.get('defect_capping', {})
        
        if capping_cfg.get('enable', True):
            capped_results = _apply_defect_capping(
                results_dict=current_lot_results,
                group_thresholds=capping_cfg.get('group_thresholds', {'upper': 1, 'lower': 0.003}),
                code_thresholds=capping_cfg.get('code_thresholds', {'upper': 1, 'lower': 0.0001}),
                warning_lines=warning_lines or {}
            )
            current_code_details = capped_results['code_level_details']
        else:
            current_code_details = sim_lot_codes

        # --- 6. 覆盖 (Override) ---
        override_res = config.paths.get('rate_override_config')
        
        override_file_path = None
        override_sheet_name = ""
        
        if override_res:
             override_file_path = resource_dir / override_res.file_name
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
# --- 基础信息计算 ---
@staticmethod
def _calculate_lot_base_info_with_median_time(
    panel_details_df: pd.DataFrame,
    full_sheet_base_info: pd.DataFrame | None
) -> pd.DataFrame:
    """
    [辅助函数 V1.1 - 增加 array_input_time] 从 Panel 和 Sheet 数据聚合 Lot 基础信息。
    """
    if panel_details_df.empty:
        logging.warning("无法计算 Lot 基础信息(Panel)，因为输入的 Panel 数据为空。")
        return pd.DataFrame()
    try:
        panel_df_with_dt = panel_details_df.copy()
        panel_df_with_dt['warehousing_datetime'] = pd.to_datetime(
            panel_df_with_dt['warehousing_time'], format='%Y%m%d', errors='coerce'
        )
        panel_df_with_dt.dropna(subset=['warehousing_datetime'], inplace=True)
        if panel_df_with_dt.empty:
                logging.warning("转换 warehousing_time 为日期后，没有剩余的 Panel 数据用于 Lot 聚合。")
                return pd.DataFrame()
        lot_base_agg = panel_df_with_dt.groupby('lot_id').agg(
            total_panels=('panel_id', 'nunique'), # 计算lot总数：传入的 panel_details_df 中，该 Lot ID 下有多少个唯一的 Panel ID
            warehousing_time_median=('warehousing_datetime', lambda x: x.quantile(0.75))
        ).reset_index()
        lot_base_agg['warehousing_time'] = lot_base_agg['warehousing_time_median'].dt.strftime('%Y%m%d').fillna('') # type: ignore
        lot_base_info_df = lot_base_agg[['lot_id', 'total_panels', 'warehousing_time']]
        lot_array_times = None
        if full_sheet_base_info is not None and not full_sheet_base_info.empty:
            if 'lot_id' not in full_sheet_base_info.columns:
                    if full_sheet_base_info.index.name == 'lot_id':
                        full_sheet_base_info_reset = full_sheet_base_info.reset_index()
                    else:
                        logging.warning("Sheet 基础信息缺少 'lot_id' 列，无法聚合 array_input_time。")
                        full_sheet_base_info_reset = None
            else:
                    full_sheet_base_info_reset = full_sheet_base_info
            if full_sheet_base_info_reset is not None and 'array_input_time' in full_sheet_base_info_reset.columns:
                lot_array_times = full_sheet_base_info_reset.groupby('lot_id')['array_input_time'].max().reset_index()
            else:
                if full_sheet_base_info_reset is not None:
                        logging.warning("Sheet 基础信息缺少 'array_input_time' 列。")
        else:
            logging.warning("Sheet 基础信息 (full_sheet_base_info) 不可用或为空，无法聚合 array_input_time。")
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

# ==============================================================================
#                      辅助函数：模拟数据
# ==============================================================================
@staticmethod
def _simulate_concentration(
    raw_results: Dict[str, Any],
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    processing_config: Dict[str, Any],  # [Refactor] 接收配置字典
    entity_id_col: str = 'sheet_id'
) -> Dict[str, Any]:
    """
    [辅助函数 V2.8]
    """
    logging.info(f"开始执行 {entity_id_col} 级不良率模拟调度 (V2.8 - EMA 日度映射)...")
    try:
        config = processing_config.get('sheet_hotspot_config', {})
        if not config.get('enable', False):
            return raw_results['code_level_details']
        
        cfg_hide = config.get('hide_hotspot_config', {})
        fluctuation_key = f"fluctuation_{entity_id_col.replace('_id', '')}"
        current_fluc = cfg_hide.get(fluctuation_key, 0.1)
        
        sim_code_details = raw_results["code_level_details"].copy()
        seed = config.get('random_seed', 2025)
        rng = np.random.default_rng(seed)
        base_info_df = raw_results.get("group_level_summary_for_chart")
        
        if base_info_df is None:
            logging.error("缺少基础汇总数据，模拟终止。")
            return sim_code_details

        for group, df_all_codes_in_group in sim_code_details.items():
            if df_all_codes_in_group.empty: continue
            
            processed_codes_list = []
            for code_desc, df_code in df_all_codes_in_group.groupby('defect_desc'):
                df_code_with_base = _add_daily_base_rate_to_df(
                    df_code=df_code, 
                    code_desc=code_desc, 
                    entity_id_col=entity_id_col,
                    base_info_df=base_info_df, 
                    mwd_code_data=mwd_code_data
                )
                
                new_rates = _generate_simulated_rates(df_code_with_base, rng, current_fluc)
                
                df_code_processed = df_code_with_base.copy()
                df_code_processed['defect_rate'] = new_rates
                df_code_processed['defect_panel_count'] = np.maximum(0, np.round(df_code_processed['defect_rate'] * df_code_processed['total_panels'])).astype(int)
                
                if 'daily_base_rate' in df_code_processed.columns:
                    df_code_processed = df_code_processed.drop(columns=['daily_base_rate'])
                processed_codes_list.append(df_code_processed)
            
            if processed_codes_list:
                sim_code_details[group] = pd.concat(processed_codes_list, ignore_index=True)
                
        return sim_code_details
    except Exception as e:
        logging.error(f"模拟调度失败: {e}", exc_info=True)
        return raw_results.get('code_level_details', {})

@staticmethod
def _add_daily_base_rate_to_df(
    df_code: pd.DataFrame, 
    code_desc: str, 
    entity_id_col: str,
    base_info_df: pd.DataFrame | None, 
    mwd_code_data: Dict[str, pd.DataFrame] | None # 现在接收整个字典
) -> pd.DataFrame:
    """
    [辅助函数 V1.3 - 日度丝滑映射]
    放弃查找月份，改为根据 Lot 日期查找 EMA 平滑后的日度值。
    1. 解决了“阶梯状”断层问题。
    2. 配合中值钳制，解决了“小样本/集中入库”偏置问题。
    """
    df_code_with_base = df_code.copy()
    df_code_with_base['daily_base_rate'] = 0.0 # 保持列名兼容，实际是日度基准
    
    # 提取全量日度 EMA 数据源
    df_daily_ema = mwd_code_data.get('daily_full') if mwd_code_data else None
    
    if df_daily_ema is not None and base_info_df is not None:
        try:
            # 1. 建立当前 Code 的日期查找表: {DateString -> Rate}
            code_ema_data = df_daily_ema[df_daily_ema['defect_desc'] == code_desc].copy()
            code_ema_data['date_key'] = code_ema_data['warehousing_time'].dt.strftime('%Y%m%d') # type: ignore
            lookup_dict = code_ema_data.set_index('date_key')['defect_rate'].to_dict()

            # 2. 获取当前待模拟 Lot 对应的入库日期
            # 需要先从 base_info_df 中捞出 Lot 对应的 warehousing_time
            if entity_id_col not in base_info_df.columns and base_info_df.index.name == entity_id_col:
                base_info_temp = base_info_df.reset_index()
            else:
                base_info_temp = base_info_df
                
            lot_date_map = base_info_temp.drop_duplicates(subset=[entity_id_col]).set_index(entity_id_col)['warehousing_time']
            
            # 3. 映射基准值 (精确到天)
            # lot_dates 会得到一系列如 '20251216' 的字符串
            lot_dates = df_code_with_base[entity_id_col].map(lot_date_map)
            df_code_with_base['daily_base_rate'] = lot_dates.map(lookup_dict).fillna(0)
            
        except Exception as e:
            logging.error(f"为 Code '{code_desc}' 进行日度基准映射时出错: {e}")
            
    return df_code_with_base

@staticmethod
def _generate_simulated_rates(
    df_code_with_base_rate: pd.DataFrame, rng: np.random.Generator, fluc: float
) -> np.ndarray:
    """
    [辅助函数 V2.2 - 分层波动，无钳制] 生成模拟率。
    """
    num_sheets = len(df_code_with_base_rate)
    if num_sheets == 0: return np.array([])
    base_rates_series = df_code_with_base_rate['daily_base_rate']
    random_factors = rng.uniform(1 - fluc, 1 + fluc, size=num_sheets)
    initial_rates = base_rates_series.values * random_factors # type: ignore
    final_rates = np.maximum(0, initial_rates)
    return final_rates

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
        [核心函数 V1.7 - 增加覆盖审计与追踪] 使用外部数据覆盖模拟的不良率。
        增加了针对特定丢失 ID 的调试追踪和最终的未命中报告。
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

        logging.info(f"开始使用外部数据覆盖 {entity_id_col} 级别的不良率 (替换+插入)...")
        
        # --- [审计准备] ---
        # 1. 记录 Excel 中所有的 ID，用于最后对比
        all_config_ids = set(override_data_df[entity_id_col].astype(str).str.strip().unique())
        # 2. 记录成功处理（替换或插入）的 ID
        processed_ids = set()
        # 3. 定义“重点嫌疑人”列表 (根据您的反馈)
        watchlist = ['L3MR5A0B023', 'L3MR5A0B026']

        # 复制副本
        final_results_dict = {group: df.copy() for group, df in simulated_code_details_dict.items() if df is not None}
        total_replaced_count = 0
        total_inserted_count = 0

        # --- 准备模板 ---
        all_sim_df_list = [df for df in final_results_dict.values() if not df.empty]
        if not all_sim_df_list:
            logging.error("无法执行插入，因为模拟数据中没有任何可用的模板行。")
            return simulated_code_details_dict
        
        all_sim_df = pd.concat(all_sim_df_list, ignore_index=True)
        generic_template_row = all_sim_df.iloc[0]
        lot_specific_templates = all_sim_df.drop_duplicates(subset=['lot_id']).set_index('lot_id')
        
        new_rows_to_add_by_group = defaultdict(list)
        processed_indices = set()

        # --- 遍历覆盖 DataFrame ---
        for index, override_row in override_data_df.iterrows():
            target_desc = str(override_row['defect_desc']).strip()
            # 确保 ID 转字符串并去空格，防止 "ID " != "ID"
            target_entity_id = str(override_row[entity_id_col]).strip() 
            target_lot_id = override_row['lot_id']
            override_rate = override_row[rate_col_name]

            # --- [调试追踪] 如果是嫌疑人，打印详细路径 ---
            is_target_trace = target_entity_id in watchlist
            if is_target_trace:
                logging.warning(f"!!! [追踪] 发现目标 ID: {target_entity_id}")
                logging.warning(f"    - 缺陷描述: '{target_desc}'")

            # 1. 查找目标 Group
            target_group = desc_to_group_map.get(target_desc)
            if not target_group:
                if is_target_trace:
                    logging.error(f"    -> [失败] 映射失败！'{target_desc}' 未在 desc_to_group_map 中找到对应的 Group。")
                    logging.error(f"    -> 当前可用映射 keys (前10个): {list(desc_to_group_map.keys())[:10]}")
                else:
                    logging.debug(f"跳过匹配：未找到缺陷描述 '{target_desc}' 对应的 Group。")
                continue

            if is_target_trace:
                logging.warning(f"    - 映射 Group: '{target_group}'")

            # 2. 查找目标 Group 的 DataFrame
            target_df = final_results_dict.get(target_group)
            if target_df is None:
                if is_target_trace:
                    logging.error(f"    -> [失败] Group '{target_group}' 在 simulated_code_details_dict 中不存在 Key。")
                else:
                    logging.warning(f"跳过匹配：Group '{target_group}' DataFrame 不存在。")
                continue 

            # 3. 匹配行
            match_mask = pd.Series(False, index=target_df.index)
            if not target_df.empty:
                # 确保 target_df 中的 ID 也转为 string 对比
                match_mask = (target_df[entity_id_col].astype(str).str.strip() == target_entity_id) & \
                             (target_df['defect_desc'] == target_desc)

            matched_indices = target_df.index[match_mask]

            # 4. 执行
            if not matched_indices.empty:
                # --- 替换 ---
                if is_target_trace: logging.warning(f"    -> [动作] 找到匹配行，执行替换。")
                target_df.loc[matched_indices, 'defect_rate'] = override_rate
                if 'total_panels' in target_df.columns:
                    panels = target_df.loc[matched_indices, 'total_panels']
                    new_counts = np.maximum(0, np.round(override_rate * panels)).astype(int)
                    target_df.loc[matched_indices, 'defect_panel_count'] = new_counts
                    if index not in processed_indices:
                        total_replaced_count += len(matched_indices)
                        processed_indices.add(index)
                        processed_ids.add(target_entity_id) # 记录成功
            else:
                # --- 插入 ---
                if is_target_trace: logging.warning(f"    -> [动作] 未找到匹配行，准备插入。")
                if target_lot_id in lot_specific_templates.index:
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
                    
                    # 动态列对齐
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
                        processed_ids.add(target_entity_id) # 记录成功
                        
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
        # 计算未命中的 ID
        failed_ids = all_config_ids - processed_ids
        
        logging.info(f"覆盖审计完成: Excel中共配置 {len(all_config_ids)} 个ID，成功应用 {len(processed_ids)} 个，失败 {len(failed_ids)} 个。")
        
        if failed_ids:
            # 转换为 list 并排序以便查看，只打印前 20 个避免刷屏
            failed_list = sorted(list(failed_ids))
            logging.error("========== [覆盖失败名单 (前20个)] ==========")
            logging.error(f"以下 ID 在配置文件中存在，但未被替换或插入: {failed_list[:20]}")
            logging.error("可能原因: 1. 缺陷描述无法映射到 Group; 2. 格式/空格问题; 3. Group DataFrame 初始化失败。")
            
            # 如果您的目标 ID 在失败名单中，再次显式警告
            for watch_id in watchlist:
                if watch_id in failed_ids:
                    logging.error(f">>> 警告: 目标 ID '{watch_id}' 确认覆盖失败！请检查上方追踪日志。")
            logging.error("===========================================")

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
    group_thresholds: dict,
    code_thresholds: dict,
    warning_lines: Optional[Dict[str, float]] = None  # 修改此处
) -> Dict[str, Any]:
    """
    [辅助函数 V2.0 - 精确 Spec 截断] 
    根据 warning_lines 对每个 Code 应用专属的上限截断。
    """
    logging.info("开始对不良率进行可配置的随机截断处理 (支持 Spec 精确截断)...")
    
    # 输入检查
    if not isinstance(results_dict, dict):
        logging.error("传递给 _apply_defect_capping 的输入不是字典，无法截断。")
        return results_dict
    if "group_level_summary_for_chart" not in results_dict or \
        "code_level_details" not in results_dict:
        return results_dict

    warning_lines = warning_lines or {} # 确保不为 None
    base_seed = 101
    
    try:
        # 1. 截断 Group 级数据 (保持原有逻辑，使用全局配置)
        df_group_chart = results_dict["group_level_summary_for_chart"].copy()
        rate_cols = [col for col in df_group_chart.columns if col.endswith('_rate')]
        
        for i, col_name in enumerate(rate_cols):
            rng_capping = np.random.default_rng(base_seed + i)
            # Group 级依然使用配置文件中的通用阈值
            df_group_chart[col_name] = df_group_chart[col_name].apply(
                lambda rate: _apply_random_cap_and_floor(
                    rate,
                    upper_threshold=group_thresholds['upper'],
                    lower_threshold=group_thresholds['lower'],
                    rng=rng_capping
                )
            )

        # 2. [核心升级] 截断 Code 级数据 (应用专属 Spec)
        dict_code_details = results_dict["code_level_details"].copy()
        rng_capping_code = np.random.default_rng(base_seed + 99)
        
        # 默认上限 (兜底用)
        fallback_upper = code_thresholds['upper']
        fallback_lower = code_thresholds['lower']

        for group, df_code in dict_code_details.items():
            if df_code is not None and not df_code.empty and 'defect_rate' in df_code.columns:
                df_code_mod = df_code.copy()
                
                # 定义行级处理函数：动态查找 Spec
                def _row_capper(row):
                    # 1. 查找专属 Spec
                    code_name = str(row.get('defect_desc', '')).strip()
                    # 优先使用 warning_lines 中的值，找不到则用 fallback_upper
                    spec_limit = warning_lines.get(code_name, fallback_upper)
                    
                    # 2. 执行软截断
                    return _apply_random_cap_and_floor(
                        rate=row['defect_rate'],
                        upper_threshold=spec_limit,   # <--- 使用专属 Spec
                        lower_threshold=fallback_lower,
                        rng=rng_capping_code
                    )

                # 应用截断
                df_code_mod['defect_rate'] = df_code_mod.apply(_row_capper, axis=1)

                # [联动] 截断后重新计算 defect_panel_count
                if 'total_panels' in df_code_mod.columns:
                    df_code_mod['defect_panel_count'] = np.maximum(0, np.round(
                        df_code_mod['defect_rate'] * df_code_mod['total_panels']
                    )).astype(int)

                dict_code_details[group] = df_code_mod

        # 3. 重新准备 UI 汇总表
        final_ui_columns = list(results_dict.get("group_level_summary_for_table", pd.DataFrame()).columns)
        if not final_ui_columns and not df_group_chart.empty:
            final_ui_columns = df_group_chart.columns.tolist()
        group_level_for_ui = df_group_chart.reindex(columns=final_ui_columns).fillna(0) if final_ui_columns else df_group_chart

        logging.info("不良率精确截断处理完成。")

        # 4. 返回结果
        final_capped_results = results_dict.copy()
        final_capped_results["group_level_summary_for_table"] = group_level_for_ui
        final_capped_results["group_level_summary_for_chart"] = df_group_chart
        final_capped_results["code_level_details"] = dict_code_details
        return final_capped_results

    except Exception as e:
        logging.error(f"在应用截断时发生错误: {e}", exc_info=True)
        return results_dict

@staticmethod
def _apply_random_cap_and_floor(
    rate: float,
    upper_threshold: float,
    lower_threshold: float,
    rng: np.random.Generator
) -> float:
    """
    [辅助函数 V2.0 - 软截断] 
    当 rate 超标时，返回 [Limit * 0.8, Limit] 之间的随机值，
    确保截断后的数据依然呈现自然的随机波动，而非死板的直线。
    """
    if rate > upper_threshold:
        # [核心逻辑] 软截断：在 Spec 的 80% ~ 95% 之间随机浮动
        # 这样每个超标 Lot 的最终值都会略有不同，消除“人工痕迹”
        safe_rate = rng.uniform(upper_threshold * 0.8, upper_threshold * 0.9)
        return safe_rate
        
    elif 0 < rate < lower_threshold:
        # 下限保护 (保持不变)
        low_bound = max(0, lower_threshold * 0.8)
        high_bound = lower_threshold * 1.2
        if low_bound >= high_bound:
            return low_bound
        return rng.uniform(low_bound, high_bound)
        
    else:
        return rate