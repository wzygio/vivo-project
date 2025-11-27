# src/vivo_project/core/sheet_lot_processor.py
import pandas as pd
import numpy as np
import logging, sys, io
from pathlib import Path
from typing import Dict, Any
from collections import defaultdict
import comtypes.client
import comtypes

from vivo_project.config import CONFIG, DATA_DIR, PROJECT_ROOT, RESOURCE_DIR
from vivo_project.utils.utils import save_dict_to_excel # 假设 save_dict_to_excel 在这里

# ==============================================================================
#              ByCode计算Sheet级不良率 (V3.9 - 集成覆盖+探针)
# ==============================================================================
@staticmethod
def calculate_sheet_defect_rates(
    panel_details_df: pd.DataFrame,
    target_defects: list,
    array_input_times_df: pd.DataFrame,
    mwd_code_data: Dict[str, pd.DataFrame] | None
) -> Dict[str, Any] | None:
    """
    (V3.9 - 添加覆盖探针, 修正调用)
    按顺序执行 过滤 -> 计算原始 -> [模拟] -> [覆盖] -> 重聚合 -> 截断 六个步骤。
    """
    logging.info("开始执行Sheet级不良率完整业务流程 (V3.9 - 添加覆盖探针)...")
    debug_output_dir = PROJECT_ROOT / "data" / "processed"

    try:
        # --- 步骤 1: 聚合 Sheet 基础信息 ---
        logging.info("步骤1: 聚合 Sheet 基础信息...")
        aggregation_rules = {'panel_id': 'nunique', 'lot_id': 'first', 'warehousing_time': 'first'}
        sheet_base_info_df = panel_details_df.groupby('sheet_id').agg(aggregation_rules)
        sheet_base_info_df = sheet_base_info_df.rename(columns={'panel_id': 'total_panels'})
        if not array_input_times_df.empty:
            sheet_array_times = array_input_times_df.copy()
            # 确保 sheet_id 是列以便合并
            if sheet_base_info_df.index.name == 'sheet_id':
                    sheet_base_info_df = sheet_base_info_df.reset_index()
            sheet_base_info_df = pd.merge(sheet_base_info_df, sheet_array_times, on='sheet_id', how='left')
            # 合并后可以重新设置索引，如果后续流程需要
            # sheet_base_info_df = sheet_base_info_df.set_index('sheet_id')
        else:
                # 确保 array_input_time 列存在，即使为空
                sheet_base_info_df['array_input_time'] = pd.NaT

        # --- 步骤 2: 过滤 Sheet ---
        logging.info("步骤2: 过滤 Sheet...")
        # 注意：_filter_by_pass_rate 需要 total_panels 列，确保它存在
        sheet_base_info_filtered = _filter_by_pass_rate(
            base_df=sheet_base_info_df.copy(), # 传入包含 total_panels 的 DF
            denominator=190,
            threshold=0.9,
            entity_name="sheet"
        )
        if sheet_base_info_filtered.empty:
            logging.warning("Sheet 过货率筛选后无剩余数据。")
            return None
        valid_entities = sheet_base_info_filtered['sheet_id'].unique() # 从过滤后的结果获取有效 ID
        panel_details_df_filtered = panel_details_df[panel_details_df['sheet_id'].isin(valid_entities)]


        # --- 步骤 3: 计算原始不良率 ---
        logging.info("步骤3: 计算原始 Sheet 级不良率...")
        # 传递过滤后的基础信息，确保设置为 index 以便 join
        raw_results = _calculate_raw_rates(
            panel_details_df_filtered=panel_details_df_filtered,
            base_info_df_filtered=sheet_base_info_filtered.set_index('sheet_id'), # <-- 设 index
            target_defects=target_defects,
            entity_id_col='sheet_id'
        )
        if raw_results is None: raise Exception("原始不良率计算步骤失败。")


        # --- 步骤 4: 模拟数据 ---
        logging.info("步骤4: 应用 Sheet 级不良率模拟...")
        sim_code_details = _simulate_concentration(
            raw_results=raw_results,
            mwd_code_data=mwd_code_data,
            entity_id_col='sheet_id'
        )
        if sim_code_details is None or not isinstance(sim_code_details, dict): # 增加类型检查
                logging.warning("Sheet 级不良率模拟失败或返回无效格式，将尝试在原始数据上应用覆盖。")
                sim_code_details = raw_results['code_level_details']


        # --- 探针 1: 保存覆盖前的数据 ---
        try:
            save_dict_to_excel(
                data_dict=sim_code_details,
                output_dir=debug_output_dir,
                filename="debug_sheet_before_override.xlsx"
            )
        except Exception as save_err:
                logging.error(f"[调试探针] 保存覆盖前数据失败: {save_err}")


        # --- [核心修改] 步骤 5: 应用覆盖逻辑 (传入 desc_to_group_map) ---
        logging.info("步骤5: 应用 Sheet 级不良率覆盖...")
        # a. 加载覆盖数据
        override_config = CONFIG.get('processing', {}).get('rate_override_config', {})
        override_sheet_df, _ = _load_override_excel(
            override_file=override_config.get('override_file', ''),
            override_sheet_name=override_config.get('override_sheet_name', '')
        )


        # b. [新增] 获取 Desc -> Group 映射
        #    使用 panel_details_df (未过滤) 来构建最全的映射
        desc_to_group_map = _get_desc_to_group_map(panel_details_df)
        # c. 执行覆盖
        overridden_code_details = _override_rates(
                simulated_code_details_dict=sim_code_details,
                override_data_df=override_sheet_df,
                entity_id_col='sheet_id',
                desc_to_group_map=desc_to_group_map # <--- 传递映射
        )

        # --- 探针 2: 保存覆盖后的数据 ---
        try:
            save_dict_to_excel(
                data_dict=overridden_code_details, # <-- 保存覆盖后的字典
                output_dir=debug_output_dir,
                filename="debug_sheet_after_override.xlsx" # <-- 不同文件名
            )
        except Exception as save_err:
                logging.error(f"[调试探针] 保存覆盖后数据失败: {save_err}")

        # --- 步骤 6: 从 [覆盖后] 的 Code 数据重新聚合 Group 数据 ---
        logging.info("步骤6: 从覆盖后的 Code 级数据重聚合 Group 级数据...")
        # 确保传递给 _reaggregate_groups_from_codes 的 raw_base_info_df 包含必要列且索引正确
        base_info_for_reagg = raw_results['group_level_summary_for_chart']
        # 如果索引不是 sheet_id, 重置它
        if base_info_for_reagg.index.name != 'sheet_id':
                base_info_for_reagg = base_info_for_reagg.reset_index()

        sim_group_ui, sim_group_chart = _reaggregate_groups_from_codes(
            sim_code_details=overridden_code_details,
            raw_base_info_df=base_info_for_reagg, # <-- 使用准备好的基础信息
            target_defects=target_defects,
            entity_id_col='sheet_id' # <-- 确保传递 sheet_id
        )
        overridden_results = {
            "group_level_summary_for_table": sim_group_ui,
            "group_level_summary_for_chart": sim_group_chart,
            "code_level_details": overridden_code_details
        }

        # --- 步骤 7: 截断 ---
        logging.info("步骤7: 应用 Sheet 级不良率截断...")
        group_level_thresholds_sheet = {'upper': 0.60, 'lower': 0.000}
        code_level_thresholds_sheet = {'upper': 0.30, 'lower': 0.000}
        final_results = _apply_defect_capping(
            overridden_results,
            group_level_thresholds_sheet,
            code_level_thresholds_sheet
        )

        # 将原始完整的 Sheet 基础信息添加到最终结果 (如果需要)
        # 确保 sheet_base_info_df 包含 sheet_id 作为列或索引
        if 'sheet_id' not in sheet_base_info_df.columns and sheet_base_info_df.index.name != 'sheet_id':
                final_results['full_sheet_base_info'] = sheet_base_info_df.reset_index()
        else:
                final_results['full_sheet_base_info'] = sheet_base_info_df


        logging.info("Sheet级业务规则应用完成 (V3.9 - 添加覆盖探针)。")
        return final_results

    except Exception as e:
        logging.error(f"在执行Sheet级业务规则时发生错误: {e}", exc_info=True)
        return None

# ==============================================================================
#                      ByCode计算Lot级不良率 (V4.4 - 集成 Lot 覆盖)
# ==============================================================================
@staticmethod
def calculate_lot_defect_rates(
    panel_details_df: pd.DataFrame,
    sheet_results: Dict[str, Any],
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    target_defects: list
) -> Dict[str, Any] | None:
    """
    (V4.4 - 集成 Lot 覆盖, 修正调用)
    执行 计算原始 -> 过滤 -> [模拟] -> [覆盖] -> 重聚合 -> 截断 的串行流程。
    """
    logging.info("开始执行Lot级不良率完整业务流程 (V4.4 - 集成 Lot 覆盖)...")

    try:
        # --- 步骤 1: 计算 Lot 基础信息 ---
        logging.info("步骤1: 计算 Lot 级基础信息...")
        full_sheet_base_info = sheet_results.get("full_sheet_base_info")
        lot_base_info_df = _calculate_lot_base_info_with_median_time(
            panel_details_df, full_sheet_base_info
        )
        if lot_base_info_df.empty: return None

        # --- 步骤 2: 过滤 Lot ---
        logging.info("步骤2: 过滤 Lot...")
        lot_base_info_filtered = _filter_by_pass_rate(
                base_df=lot_base_info_df.copy(), denominator=190 * 30, threshold=0.10, entity_name="Lot"
        )
        if lot_base_info_filtered.empty: return None

        # --- 步骤 3: 计算原始 Lot 级不良率 ---
        logging.info("步骤3: 计算原始 Lot 级不良率...")
        valid_lot_ids = lot_base_info_filtered['lot_id'].unique()
        panel_details_df_filtered_for_lot = panel_details_df[panel_details_df['lot_id'].isin(valid_lot_ids)]
        raw_lot_results = _calculate_raw_rates(
            panel_details_df_filtered=panel_details_df_filtered_for_lot,
            base_info_df_filtered=lot_base_info_filtered.set_index('lot_id'), # <-- 设 index
            target_defects=target_defects,
            entity_id_col='lot_id'
        )
        if raw_lot_results is None: raise Exception("Lot级原始不良率计算失败。")

        # --- 步骤 4: 模拟数据 ---
        logging.info("步骤4: 应用 Lot 级不良率模拟...")
        simulated_lot_code_details = _simulate_concentration(
            raw_results=raw_lot_results,
            mwd_code_data=mwd_code_data,
            entity_id_col='lot_id' # <-- 指定 lot_id
        )
        if simulated_lot_code_details is None or not isinstance(simulated_lot_code_details, dict):
                logging.warning("Lot 级不良率模拟失败或返回无效格式，将尝试在原始数据上应用覆盖。")
                simulated_lot_code_details = raw_lot_results['code_level_details']

        # --- [核心修改] 步骤 5: 应用 Lot 级覆盖逻辑 (传入 desc_to_group_map) ---
        logging.info("步骤5: 应用 Lot 级不良率覆盖...")
        # a. 加载覆盖数据
        override_config = CONFIG.get('processing', {}).get('rate_override_config', {})
        _, override_lot_avg_df = _load_override_excel(
            override_file=override_config.get('override_file', ''),
            override_sheet_name=override_config.get('override_sheet_name', '')
        )

        # b. [新增] 获取 Desc -> Group 映射
        desc_to_group_map = _get_desc_to_group_map(panel_details_df)
        # c. 执行覆盖
        overridden_lot_code_details = _override_rates(
                simulated_code_details_dict=simulated_lot_code_details,
                override_data_df=override_lot_avg_df, # <-- 使用 Lot 平均覆盖数据
                entity_id_col='lot_id',
                desc_to_group_map=desc_to_group_map # <--- 传递映射
        )

        # --- 步骤 6: 从 [覆盖后] 的 Code 数据重新聚合 Group 数据 ---
        logging.info("步骤6: 从覆盖后的 Code 级数据重聚合 Group 级数据...")
        # 确保传递给 _reaggregate_groups_from_codes 的 raw_base_info_df 包含必要列且索引正确
        base_info_for_reagg_lot = raw_lot_results['group_level_summary_for_chart']
        if base_info_for_reagg_lot.index.name != 'lot_id':
                base_info_for_reagg_lot = base_info_for_reagg_lot.reset_index()

        sim_group_ui, sim_group_chart = _reaggregate_groups_from_codes(
            sim_code_details=overridden_lot_code_details, # <-- 使用覆盖后数据
            raw_base_info_df=base_info_for_reagg_lot, # <-- 使用准备好的基础信息
            target_defects=target_defects,
            entity_id_col='lot_id' # <-- 指定 lot_id
        )
        overridden_lot_results = {
            "group_level_summary_for_table": sim_group_ui,
            "group_level_summary_for_chart": sim_group_chart,
            "code_level_details": overridden_lot_code_details
        }

        # --- 步骤 7: 截断 ---
        logging.info("步骤7: 应用 Lot 级不良率截断...")
        group_level_thresholds = {'upper': 0.02, 'lower': 0.003}
        code_level_thresholds = {'upper': 0.009, 'lower': 0.0001}
        final_results = _apply_defect_capping(
            overridden_lot_results,
            group_level_thresholds,
            code_level_thresholds
        )

        logging.info("Lot级业务规则应用完成 (V4.4 - 集成 Lot 覆盖)。")
        return final_results

    except Exception as e:
        logging.error(f"在执行Lot级业务规则时发生错误: {e}", exc_info=True)
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
            total_panels=('panel_id', 'nunique'),
            warehousing_time_median=('warehousing_datetime', lambda x: x.quantile(0.75))
        ).reset_index()
        lot_base_agg['warehousing_time'] = lot_base_agg['warehousing_time_median'].dt.strftime('%Y%m%d').fillna('')
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

    

# --- 计算原始不良率 ---
@staticmethod
def _calculate_raw_rates(
    panel_details_df_filtered: pd.DataFrame,
    base_info_df_filtered: pd.DataFrame, # 期望 entity_id_col 是索引
    target_defects: list,
    entity_id_col: str
) -> Dict[str, Any] | None:
    """
    [辅助函数 - 通用 V1.6 - 重构 Code 明细准备 + 包含探针] 计算原始不良率。
    """
    logging.info(f"开始计算{entity_id_col}级原始不良率...")
    if base_info_df_filtered.index.name != entity_id_col:
            logging.error(f"传递给 _calculate_raw_rates 的 base_info_df_filtered 索引不是 '{entity_id_col}'。")
            return None

    try:
        # --- 步骤 1: 计算 Code 级分子 ---
        code_numerators = pd.DataFrame(columns=[entity_id_col, 'defect_group', 'defect_desc', 'defect_panel_count'])
        if not panel_details_df_filtered.empty:
                code_numerators = panel_details_df_filtered.groupby(
                    [entity_id_col, 'defect_group', 'defect_desc']
                )['panel_id'].nunique().reset_index(name='defect_panel_count')
        else:
                logging.warning(f"用于计算原始不良率的 Panel 数据为空 ({entity_id_col})。")

        # --- 步骤 2: 准备 Group 级数据 ---
        group_numerators_df = pd.DataFrame()
        if not code_numerators.empty:
                group_numerators = code_numerators.groupby([entity_id_col, 'defect_group'])['defect_panel_count'].sum()
                group_numerators_df = group_numerators.unstack(level='defect_group').fillna(0)
        # 使用 join 合并
        group_summary_df = base_info_df_filtered.join(group_numerators_df, how='left').fillna(0)
        final_group_df = group_summary_df.reset_index() # entity_id_col 成为列

        # 计算 Group 级不良率
        rate_cols = []
        for defect_type in target_defects:
            count_col_name = defect_type # Group name from unstack
            # 确保 Group 列存在 (即使全为 0)
            if count_col_name not in final_group_df.columns:
                final_group_df[count_col_name] = 0

            # 重命名 count 列
            new_count_col_name = f"{defect_type.lower()}_count"
            # 使用 errors='ignore' 避免在列不存在时报错 (虽然上面已添加)
            final_group_df.rename(columns={count_col_name: new_count_col_name}, inplace=True, errors='ignore')

            # 计算 rate 列
            rate_col_name = f"{defect_type.lower()}_rate"
            # 确保 new_count_col_name 存在 (重命名可能失败) 且 total_panels 存在
            if new_count_col_name in final_group_df.columns and 'total_panels' in final_group_df.columns:
                    final_group_df[rate_col_name] = np.where(
                        final_group_df['total_panels'] > 0,
                        final_group_df[new_count_col_name] / final_group_df['total_panels'],
                        0
                    )
                    rate_cols.append(rate_col_name)
            else:
                    logging.warning(f"无法计算 Group Rate '{rate_col_name}'，缺少列 '{new_count_col_name}' 或 'total_panels'。")
                    final_group_df[rate_col_name] = 0.0 # 创建列并填充 0
                    rate_cols.append(rate_col_name) # 仍然添加到列表


        # --- 步骤 3: 准备 Code 级数据 (合并 + 计算 Rate) ---
        # a. 准备基础信息 DataFrame
        base_info_for_code = base_info_df_filtered.reset_index()
        base_cols_for_code = [entity_id_col]
        if 'lot_id' in base_info_for_code.columns and entity_id_col != 'lot_id': base_cols_for_code.append('lot_id')
        for col in ['warehousing_time', 'array_input_time', 'total_panels', 'pass_rate']:
                if col in base_info_for_code.columns: base_cols_for_code.append(col)
        base_cols_for_code = list(dict.fromkeys(base_cols_for_code))
        # 清理 base_info_for_code 重复列
        if base_info_for_code.columns.duplicated().any():
                logging.warning(f"基础信息 DataFrame (base_info_for_code) 包含重复列名，将尝试清理...")
                base_info_for_code = base_info_for_code.loc[:, ~base_info_for_code.columns.duplicated()]
                base_cols_for_code = [col for col in base_cols_for_code if col in base_info_for_code.columns]
        if entity_id_col not in base_info_for_code.columns:
                logging.error(f"清理重复列后，基础信息 DataFrame 缺少 '{entity_id_col}' 列。")
                return None
        base_info_subset_for_code = base_info_for_code[base_cols_for_code].drop_duplicates(subset=[entity_id_col])


        # b. 清理 code_numerators
        if code_numerators.columns.duplicated().any():
            logging.warning(f"Code 计数 DataFrame (code_numerators) 包含重复列名，将尝试清理...")
            code_numerators = code_numerators.loc[:, ~code_numerators.columns.duplicated()]
        # 确保 entity_id_col 存在
        if entity_id_col not in code_numerators.columns and not code_numerators.empty:
            logging.error(f"清理重复列后，Code 计数 DataFrame 缺少 '{entity_id_col}' 列。")
            all_codes_with_base = pd.DataFrame() # 创建空 DF 继续
        else:
            # c. 执行 Merge
            all_codes_with_base = pd.DataFrame() # 初始化为空
            if not code_numerators.empty and not base_info_subset_for_code.empty: # 确保两个 DF 都有内容
                if entity_id_col in code_numerators.columns and entity_id_col in base_info_subset_for_code.columns:
                    all_codes_with_base = pd.merge(
                        code_numerators,
                        base_info_subset_for_code,
                        on=entity_id_col,
                        how='left'
                    )
                else:
                    logging.error(f"无法执行 Merge，因为 '{entity_id_col}' 列在输入 DataFrame 中缺失。")
            elif code_numerators.empty:
                logging.warning(f"Code 计数 DataFrame 为空 ({entity_id_col})，Merge 结果将为空。")

        # d. 计算 Code 级不良率
        if all_codes_with_base.empty:
            logging.warning(f"DataFrame 'all_codes_with_base' 为空 ({entity_id_col})，无法计算 Code 级不良率。")
            all_codes_with_base['defect_rate'] = np.nan # 添加空列
        elif 'total_panels' not in all_codes_with_base.columns or 'defect_panel_count' not in all_codes_with_base.columns:
                logging.error(f"DataFrame 'all_codes_with_base' 缺少 'total_panels' 或 'defect_panel_count' 列，无法计算 Code 级不良率。")
                all_codes_with_base['defect_rate'] = np.nan
        else:
            all_codes_with_base['defect_rate'] = np.where(
                    (all_codes_with_base['total_panels'].notna()) & (all_codes_with_base['total_panels'] > 0),
                    all_codes_with_base['defect_panel_count'] / all_codes_with_base['total_panels'],
                    0
            )

        # --- 步骤 4: 调用新辅助函数准备 Code 级明细字典 ---
        code_level_details_dict = _prepare_code_level_details(
            all_codes_with_base=all_codes_with_base, # 传入包含 defect_rate 的 DF
            target_defects=target_defects,
            entity_id_col=entity_id_col
        )


        # --- 步骤 5: 准备 UI 汇总表 ---
        final_ui_columns_base = [entity_id_col, 'pass_rate']
        for col in ['lot_id', 'warehousing_time', 'array_input_time']:
                if col in final_group_df.columns and col not in final_ui_columns_base:
                    final_ui_columns_base.append(col)
        final_ui_columns = final_ui_columns_base + rate_cols
        final_ui_columns = [col for col in final_ui_columns if col in final_group_df.columns]
        group_level_for_ui = final_group_df.reindex(columns=final_ui_columns).fillna(0)

        # --- 返回结果 ---
        return {
            "group_level_summary_for_table": group_level_for_ui,
            "group_level_summary_for_chart": final_group_df,
            "code_level_details": code_level_details_dict
        }
    except Exception as e:
        logging.error(f"在计算{entity_id_col}级原始不良率时发生错误: {e}", exc_info=True)
        return None

 # --- [新增] 辅助函数：准备 Code 级明细字典 ---
@staticmethod
def _prepare_code_level_details(
    all_codes_with_base: pd.DataFrame, # 输入: 合并了基础信息的 Code 数据
    target_defects: list,             # 输入: 目标 Group 列表
    entity_id_col: str                # 输入: 'sheet_id' 或 'lot_id'
) -> Dict[str, pd.DataFrame]:
    """
    [新-辅助函数 V1.0 - 包含探针] 从合并后的 Code 数据准备 code_level_details 字典。
    包含按 Group 拆分、选择列、处理空组和排序的逻辑。
    """
    code_level_details_dict = {}
    logging.info(f"--- 开始准备 Code 级明细字典 ({entity_id_col}) ---")

    # 定义最终需要的列顺序
    detail_cols_ordered = [
        entity_id_col, 'lot_id', 'warehousing_time', 'array_input_time',
        'defect_group', 'defect_desc', 'defect_panel_count', 'defect_rate',
        'total_panels', 'pass_rate'
    ]

    # 确保所有 target_defects 都在字典中作为 key 存在
    for group in target_defects:
        logging.debug(f"处理 Group: {group} ({entity_id_col})")
        subset_df = pd.DataFrame() # 初始化为空
        if not all_codes_with_base.empty and 'defect_group' in all_codes_with_base.columns:
                # 使用 .loc 明确筛选，避免 SettingWithCopyWarning
                subset_df = all_codes_with_base.loc[all_codes_with_base['defect_group'] == group].copy()
        else:
                logging.warning(f"无法为 Group '{group}' ({entity_id_col}) 筛选数据，源 DataFrame 为空或缺少 'defect_group' 列。")

        # 只保留实际存在的列，并确保唯一性
        final_cols_temp = [col for col in detail_cols_ordered if col in subset_df.columns]
        final_cols = list(dict.fromkeys(final_cols_temp)) # 去重

        if subset_df.empty:
            logging.debug(f"Group '{group}' ({entity_id_col}) 没有数据，创建空 DataFrame。")
            code_level_details_dict[group] = pd.DataFrame(columns=final_cols)
            continue # 处理下一个 group

        # --- 清理 subset_df (最后的保险) ---
        if subset_df.columns.duplicated().any():
                logging.warning(f"[准备 Code 明细] DataFrame subset_df (Group: {group}) 包含重复列，将强制清理！")
                subset_df = subset_df.loc[:, ~subset_df.columns.duplicated()]
                # 重新确定 final_cols
                final_cols_temp = [col for col in detail_cols_ordered if col in subset_df.columns]
                final_cols = list(dict.fromkeys(final_cols_temp))

        # 确保 entity_id_col 存在于最终列列表中
        if entity_id_col not in final_cols:
                logging.error(f"[准备 Code 明细] 最终列列表缺少 '{entity_id_col}'，无法排序 Group '{group}'。将返回未排序数据。")
                code_level_details_dict[group] = subset_df # 返回未排序（可能包含多余列）
                continue # 处理下一个 group

        # 选择最终列
        final_code_df_subset = subset_df[final_cols]

        # 执行排序
        try:
            # 重置索引并丢弃旧索引，确保索引是默认的 RangeIndex
            final_code_df_subset_reset = final_code_df_subset.reset_index(drop=True)

            # 检查排序键是否存在
            sort_keys = [key for key in [entity_id_col, 'defect_rate'] if key in final_code_df_subset.columns]
            if len(sort_keys) == 2: # 只有当两个键都存在时才按两者排序
                code_level_details_dict[group] = final_code_df_subset_reset.sort_values(
                    by=sort_keys, ascending=[True, False]
                )
            elif len(sort_keys) == 1:
                code_level_details_dict[group] = final_code_df_subset_reset.sort_values(by=sort_keys[0])
            else:
                code_level_details_dict[group] = final_code_df_subset_reset # 无法排序，返回重置索引后的

        except ValueError as sort_error:
                # 捕获排序错误（例如重复列错误）
                logging.error(f"!!! 在排序 Group '{group}' ({entity_id_col}) 时遇到 ValueError: {sort_error}")
                logging.error(f"    DataFrame 列名: {final_code_df_subset.columns.to_list()}")
                code_level_details_dict[group] = final_code_df_subset # 返回未排序

    logging.info(f"--- 完成准备 Code 级明细字典 ({entity_id_col}) ---")
    return code_level_details_dict
    
# ==============================================================================
#                      辅助函数：模拟数据
# ==============================================================================
@staticmethod
def _simulate_concentration(
    raw_results: Dict[str, Any],
    mwd_code_data: Dict[str, pd.DataFrame] | None,
    entity_id_col: str = 'sheet_id'
) -> Dict[str, Any]:
    """
    [辅助函数 V2.7 - 分层波动] 调度器。
    """
    logging.info(f"开始执行 {entity_id_col} 级不良率模拟调度 (V2.7 - 分层波动)...")
    try:
        config = CONFIG.get('processing', {}).get('sheet_hotspot_config', {})
        if not config.get('enable', False):
            logging.info(f"{entity_id_col} 级不良率模拟未启用，跳过此步骤。")
            return raw_results['code_level_details']
        cfg_hide = config.get('hide_hotspot_config', {})
        fluctuation_key = f"fluctuation_{entity_id_col.replace('_id', '')}"
        current_fluc = cfg_hide.get(fluctuation_key)
        if current_fluc is None:
            default_fluc = 0.1
            logging.warning(f"在 config.yaml 中未找到 '{fluctuation_key}'，将使用默认波动 {default_fluc}。")
            current_fluc = default_fluc
        logging.info(f"为 {entity_id_col} 应用波动幅度: {current_fluc}")
        
        df_monthly = None
        if mwd_code_data and mwd_code_data.get('monthly') is not None:
                df_monthly = mwd_code_data['monthly'].copy()
                if not {'defect_desc', 'time_period', 'defect_rate'}.issubset(df_monthly.columns):
                    logging.error("月度趋势数据缺少必要列，无法进行动态基准模拟。")
                    df_monthly = None
                else:
                    df_monthly['defect_rate'] = pd.to_numeric(df_monthly['defect_rate'], errors='coerce').fillna(0)
        else: logging.warning("未找到月度趋势数据，无法进行动态基准模拟。")
        
        sim_code_details = raw_results["code_level_details"].copy()
        seed = config.get('random_seed', 2025)
        rng = np.random.default_rng(seed)
        base_info_df = raw_results.get("group_level_summary_for_chart")
        if base_info_df is not None:
                if entity_id_col not in base_info_df.columns:
                    if entity_id_col == base_info_df.index.name:
                        base_info_df = base_info_df.reset_index()
                    else:
                        base_info_df = None
        else: logging.error(f"未找到 {entity_id_col} 级汇总数据，无法进行动态基准模拟。")
        
        for group, df_all_codes_in_group in sim_code_details.items():
            if df_all_codes_in_group.empty: continue
            processed_codes_list = []
            if entity_id_col not in df_all_codes_in_group.columns:
                    processed_codes_list.append(df_all_codes_in_group)
                    continue
            for code_desc, df_code in df_all_codes_in_group.groupby('defect_desc'):
                df_code_with_base = _add_monthly_base_rate_to_df(
                    df_code=df_code, code_desc=code_desc, entity_id_col=entity_id_col,
                    base_info_df=base_info_df, df_monthly=df_monthly
                )
                new_rates = _generate_simulated_rates(
                    df_code_with_base_rate=df_code_with_base, rng=rng, fluc=current_fluc
                )
                df_code_processed = df_code_with_base.copy()
                df_code_processed['defect_rate'] = new_rates
                df_code_processed['defect_panel_count'] = np.maximum(0, np.round(df_code_processed['defect_rate'] * df_code_processed['total_panels'])).astype(int)
                if 'monthly_base_rate' in df_code_processed.columns:
                        df_code_processed = df_code_processed.drop(columns=['monthly_base_rate'])
                processed_codes_list.append(df_code_processed)
            if processed_codes_list:
                    try:
                        sim_code_details[group] = pd.concat(processed_codes_list, ignore_index=True)
                    except ValueError:
                        sim_code_details[group] = pd.DataFrame()
        return sim_code_details
    except Exception as e:
        logging.error(f"在执行 {entity_id_col} 级不良率模拟调度时发生错误: {e}", exc_info=True)
        return raw_results.get('code_level_details', {})

@staticmethod
def _add_monthly_base_rate_to_df(
    df_code: pd.DataFrame, code_desc: str, entity_id_col: str,
    base_info_df: pd.DataFrame | None, df_monthly: pd.DataFrame | None
) -> pd.DataFrame:
    """
    [辅助函数 V1.1 - 优化时间合并] 添加月度基准。
    """
    df_code_with_base = df_code.copy()
    df_code_with_base['monthly_base_rate'] = 0.0
    if df_monthly is not None and base_info_df is not None:
        try:
            base_info_df_for_map = None
            if entity_id_col not in base_info_df.columns and base_info_df.index.name != entity_id_col: pass
            elif 'warehousing_time' not in base_info_df.columns: pass
            else:
                base_info_df_for_map = base_info_df.drop_duplicates(subset=[entity_id_col]).set_index(entity_id_col)['warehousing_time']
            warehousing_times = None
            if base_info_df_for_map is not None:
                warehousing_times = df_code_with_base[entity_id_col].map(base_info_df_for_map)
            if warehousing_times is not None and not warehousing_times.isnull().all():
                warehousing_time_dt = pd.to_datetime(warehousing_times, format='%Y%m%d', errors='coerce')
                valid_time_mask = warehousing_time_dt.notna()
                df_code_with_base['time_period'] = ''
                df_code_with_base.loc[valid_time_mask, 'time_period'] = warehousing_time_dt[valid_time_mask].dt.strftime('%Y-%m月')
            else:
                df_code_with_base['time_period'] = ''
            monthly_lookup = df_monthly[df_monthly['defect_desc'] == code_desc].set_index('time_period')['defect_rate']
            df_code_with_base['monthly_base_rate'] = df_code_with_base['time_period'].map(monthly_lookup).fillna(0)
            df_code_with_base = df_code_with_base.drop(columns=['time_period'], errors='ignore')
        except Exception as merge_err:
            logging.error(f"为 {entity_id_col} / Code '{code_desc}' 匹配月度基准时出错: {merge_err}", exc_info=True)
            df_code_with_base['monthly_base_rate'] = 0.0
            df_code_with_base = df_code_with_base.drop(columns=['time_period'], errors='ignore')
    else: logging.warning(f"未找到基准信息或月度数据，无法为 {entity_id_col} / Code '{code_desc}' 添加月度基准。")
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
    base_rates_series = df_code_with_base_rate['monthly_base_rate']
    random_factors = rng.uniform(1 - fluc, 1 + fluc, size=num_sheets)
    initial_rates = base_rates_series.values * random_factors # type: ignore
    final_rates = np.maximum(0, initial_rates)
    return final_rates

# ==============================================================================
#                      辅助函数：覆盖数据
# ==============================================================================

@staticmethod
def _load_override_excel(
    override_file: str,
    override_sheet_name: str
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    [新-辅助函数 V2.0 - COM 终极方案] 
    利用 Excel 应用程序本身来读取数据，完美绕过 BadZipFile 和文件锁。
    """
    logging.info(f"--- [COM Loader] 开始加载覆盖数据 (文件: '{override_file}') ---")

    if not override_file or not override_sheet_name:
        return None, None

    file_path = RESOURCE_DIR / override_file
    abs_path = str(file_path.resolve()) # COM 必须使用绝对路径

    if not file_path.exists():
        logging.error(f"[COM] 文件不存在: {abs_path}")
        return None, None

    # --- COM 初始化 ---
    try:
        comtypes.CoInitialize()
    except:
        pass # 忽略重复初始化的错误

    excel_app = None
    workbook = None
    
    try:
        logging.info("[COM] 正在启动 Excel 应用程序实例...")
        # 1. 启动 Excel (静默模式)
        excel_app = comtypes.client.CreateObject("Excel.Application")
        excel_app.Visible = False
        excel_app.DisplayAlerts = False 

        # 2. 打开工作簿 (这一步是关键，Excel 进程有权限读取文件)
        logging.info(f"[COM] 正在打开工作簿: {abs_path}")
        workbook = excel_app.Workbooks.Open(abs_path)

        # 3. 找到指定 Sheet
        try:
            sheet = workbook.Sheets(override_sheet_name)
        except Exception:
            logging.error(f"[COM] 找不到名为 '{override_sheet_name}' 的 Sheet 页。")
            return None, None

        # 4. 直接从内存获取数据 (二维元组)
        raw_data = sheet.UsedRange.Value()
        
        if not raw_data or len(raw_data) < 2:
            logging.warning("[COM] Excel 数据为空或只有表头。")
            return None, None

        logging.info(f"[COM] 成功通过 Excel 提取数据，共 {len(raw_data)} 行。")

        # 5. 转换为 Pandas DataFrame
        header = raw_data[0]
        rows = raw_data[1:]
        
        # 规整数据，防止某些行为空导致长度不一致
        rows_cleaned = []
        for row in rows:
            rows_cleaned.append(list(row) if row else [None]*len(header))

        df = pd.DataFrame(rows_cleaned, columns=list(header))

        # --- 以下是标准的数据清洗逻辑 ---
        expected_cols = ['lot_id', 'sheet_id', 'override_rate', 'defect_desc']
        
        # 清理列名空格
        df.columns = [str(c).strip() for c in df.columns]
        
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"[COM] 缺少必需列: {missing_cols}。实际列: {df.columns.to_list()}")
            return None, None

        # 处理百分比/数值
        if df['override_rate'].dtype == 'object':
             df['override_rate'] = df['override_rate'].astype(str).str.rstrip('%')
             df['override_rate'] = pd.to_numeric(df['override_rate'], errors='coerce')
             # 如果 Excel 返回的是 '50' 代表 50%，则需要除以 100
             # 如果 Excel 返回的是 0.5，则不需要
             # 简单的启发式判断：如果大多数值 > 1，说明是百分数分子
             if df['override_rate'].mean() > 1.0:
                 df['override_rate'] = df['override_rate'] / 100.0

        df['defect_desc'] = df['defect_desc'].astype(str).str.strip()
        
        # 移除空行
        df.dropna(subset=expected_cols, inplace=True)
        
        # 计算 Lot 平均值
        lot_override_df = df.groupby(['lot_id', 'defect_desc'])['override_rate'].mean().reset_index()
        lot_override_df.rename(columns={'override_rate': 'override_rate_avg'}, inplace=True)

        return df[expected_cols], lot_override_df[['lot_id', 'defect_desc', 'override_rate_avg']]

    except Exception as e:
        logging.error(f"[COM] Excel 读取失败: {e}", exc_info=True)
        return None, None

    finally:
        # 6. 清理资源
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
def _override_rates(
    simulated_code_details_dict: Dict[str, pd.DataFrame],
    override_data_df: pd.DataFrame | None,
    entity_id_col: str,
    desc_to_group_map: dict
) -> Dict[str, pd.DataFrame]:
    """
    [核心函数 V1.6 - 修正 Lot 覆盖检查] 使用外部数据覆盖模拟的不良率和不良数。
    遍历覆盖文件：匹配则替换，不匹配则按 Lot 模板插入。
    """
    if override_data_df is None or override_data_df.empty:
        logging.info(f"无覆盖数据提供 ({entity_id_col} 级别)，跳过覆盖步骤。")
        return simulated_code_details_dict

    # --- [核心修改] 动态定义必需列 ---
    rate_col_name = 'override_rate' if entity_id_col == 'sheet_id' else 'override_rate_avg'
    
    # 通用必需列
    required_cols = ['lot_id', 'defect_desc', rate_col_name]
    # 根据层级添加特定 ID 列
    if entity_id_col == 'sheet_id':
         required_cols.append('sheet_id')

    # 确保列表唯一
    required_cols = list(dict.fromkeys(required_cols)) 

    # 检查必需列是否存在
    missing_cols = [col for col in required_cols if col not in override_data_df.columns]
    if missing_cols:
         logging.error(f"覆盖数据 DataFrame ({entity_id_col}) 缺少必需列: {missing_cols}，无法执行覆盖。")
         return simulated_code_details_dict # 返回原始模拟结果
    # --- [修改结束] ---

    logging.info(f"开始使用外部数据覆盖 {entity_id_col} 级别的不良率 (替换+插入)...")
    
    # --- [核心修改] 创建副本以进行修改 ---
    final_results_dict = {group: df.copy() for group, df in simulated_code_details_dict.items() if df is not None}
    total_replaced_count = 0
    total_inserted_count = 0

    # --- 准备模板查找表 (逻辑不变) ---
    all_sim_df_list = [df for df in final_results_dict.values() if not df.empty]
    if not all_sim_df_list:
         logging.error("无法执行插入，因为模拟数据中没有任何可用的模板行。")
         return simulated_code_details_dict
         
    all_sim_df = pd.concat(all_sim_df_list, ignore_index=True)
    if all_sim_df.empty:
         logging.error("无法执行插入，因为合并后的模拟数据为空，无法创建模板。")
         return simulated_code_details_dict
    
    generic_template_row = all_sim_df.iloc[0]
    lot_specific_templates = all_sim_df.drop_duplicates(subset=['lot_id']).set_index('lot_id')
    
    new_rows_to_add_by_group = defaultdict(list)
    processed_indices = set()

    # --- 遍历覆盖 DataFrame 的每一行 (逻辑不变) ---
    for index, override_row in override_data_df.iterrows():
        target_desc = override_row['defect_desc']
        target_entity_id = override_row[entity_id_col]
        target_lot_id = override_row['lot_id']
        override_rate = override_row[rate_col_name]

        # 1. 查找目标 Group (不变)
        target_group = desc_to_group_map.get(target_desc)
        if not target_group:
            logging.debug(f"跳过匹配：未找到缺陷描述 '{target_desc}' 对应的 Group。")
            continue

        # 2. 查找目标 Group 的 DataFrame (不变)
        target_df = final_results_dict.get(target_group)
        if target_df is None:
            logging.warning(f"跳过匹配：Group '{target_group}' DataFrame 不存在。")
            continue # 如果 Group 键不存在，跳过

        # 3. 在目标 DataFrame 中查找匹配行 (替换逻辑) (不变)
        match_mask = pd.Series(False, index=target_df.index)
        if not target_df.empty:
            try:
                match_mask = (target_df[entity_id_col].astype(str) == str(target_entity_id)) & \
                             (target_df['defect_desc'] == target_desc)
            except KeyError:
                 logging.warning(f"跳过匹配：尝试在 Group '{target_group}' DataFrame 中匹配时缺少列。")

        matched_indices = target_df.index[match_mask]

        # 4. 根据是否匹配，执行替换或准备插入 (不变)
        if not matched_indices.empty:
            # --- [替换逻辑] (不变) ---
            target_df.loc[matched_indices, 'defect_rate'] = override_rate
            if 'total_panels' in target_df.columns:
                panels = target_df.loc[matched_indices, 'total_panels']
                new_counts = np.maximum(0, np.round(override_rate * panels)).astype(int)
                target_df.loc[matched_indices, 'defect_panel_count'] = new_counts
                if index not in processed_indices:
                     total_replaced_count += len(matched_indices)
                     processed_indices.add(index)
            else: logging.warning(f"跳过匹配：Group '{target_group}' DataFrame 缺少 'total_panels' 列。")

        else:
            # --- [插入逻辑] (不变) ---
            logging.debug(f"未找到匹配行，准备插入新行。")
            
            if target_lot_id in lot_specific_templates.index:
                template_row = lot_specific_templates.loc[target_lot_id]
            else:
                logging.warning(f"Lot ID '{target_lot_id}' (来自 {target_entity_id}) 在模拟数据中无模板，将使用通用模板。")
                template_row = generic_template_row

            try:
                template_panels = float(template_row.get('total_panels', 1))
                if template_panels == 0: template_panels = 1.0

                new_row = {
                    # [修正] 确保 sheet_id (如果适用) 被正确获取
                    'sheet_id': override_row['sheet_id'] if entity_id_col == 'sheet_id' else (template_row.get('sheet_id', '') if 'sheet_id' in template_row else ''), # Lot 级别插入时，sheet_id 用模板的或为空
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
                
                # [修正] 当 entity_id_col 是 'lot_id' 时，新行中也需要 'lot_id'
                # (上面的 new_row 构造已包含 sheet_id 和 lot_id)
                if entity_id_col == 'lot_id':
                     new_row['lot_id'] = target_entity_id # 确保 entity_id 列的值是正确的
                
                
                target_df_cols = target_df.columns.to_list()
                if not target_df_cols and (target_group not in new_rows_to_add_by_group): # 如果目标 DF 是空的，并且是第一次添加
                     # 从 new_row 动态确定列
                     target_df_cols = [col for col in ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time', 'defect_group', 'defect_desc', 'defect_panel_count', 'defect_rate', 'total_panels', 'pass_rate'] if col in new_row]
                     # 如果 final_results_dict[target_group] 是 None 或空 DF，需要用新列创建它
                     if final_results_dict.get(target_group) is None or final_results_dict.get(target_group).empty: # type: ignore
                          final_results_dict[target_group] = pd.DataFrame(columns=target_df_cols)
                
                new_row_filtered = {k: v for k, v in new_row.items() if k in target_df_cols}
                
                new_rows_to_add_by_group[target_group].append(new_row_filtered)
                if index not in processed_indices:
                     total_inserted_count += 1
                     processed_indices.add(index)

            except Exception as insert_err:
                 logging.error(f"构建用于插入的新行时失败 (ID: {target_entity_id}, Code: {target_desc}): {insert_err}", exc_info=True)


    # --- 合并新行 (不变) ---
    if new_rows_to_add_by_group:
        logging.info(f"正在将 {total_inserted_count} 条新记录插入到结果中...")
        for group, new_rows in new_rows_to_add_by_group.items():
            if new_rows:
                df_new = pd.DataFrame(new_rows)
                target_df = final_results_dict.get(group) # 获取可能已更新的 DF
                if target_df is None: # 如果 Group 之前不存在
                     target_df = pd.DataFrame(columns=df_new.columns)
                
                # 确保列对齐
                final_results_dict[group] = pd.concat([target_df, df_new], ignore_index=True).where(pd.notna, None)


    # --- [核心修改] 移除零匹配检查 ---
    if total_replaced_count == 0 and total_inserted_count == 0 and not override_data_df.empty:
        logging.warning(f"数据覆盖操作完成 ({entity_id_col} 级别)，但 0 条记录被替换，0 条记录被插入。请检查 Excel 文件内容是否与数据匹配。")
        # 不再抛出 ValueError

    logging.info(f"不良率覆盖完成，共替换 {total_replaced_count} 条记录，插入 {total_inserted_count} 条记录。")
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
    [辅助函数 V1.1 - 通用版] 重聚合 Group 数据。
    """
    logging.info(f"模拟/覆盖完成，正在重新聚合 {entity_id_col} 的 Group 级数据...")
    if not sim_code_details or all(df.empty for df in sim_code_details.values()):
            empty_df = pd.DataFrame()
            return empty_df, empty_df
    try: # 添加 try-except 块增加健壮性
        all_simulated_entities = pd.concat(sim_code_details.values(), ignore_index=True)
        if all_simulated_entities.empty:
                empty_df = pd.DataFrame()
                return empty_df, empty_df
        group_numerators = all_simulated_entities.groupby([entity_id_col, 'defect_group'])['defect_panel_count'].sum()
        group_numerators_df = group_numerators.unstack(level='defect_group').fillna(0)
        if entity_id_col not in raw_base_info_df.columns:
            base_info_df_indexed = raw_base_info_df.reset_index()
        else:
            base_info_df_indexed = raw_base_info_df
        base_cols_to_keep = [entity_id_col, 'total_panels', 'pass_rate']
        for col in ['lot_id', 'warehousing_time', 'array_input_time']:
            if col in base_info_df_indexed.columns and col not in base_cols_to_keep:
                base_cols_to_keep.append(col)
        base_cols_to_keep = [col for col in base_cols_to_keep if col in base_info_df_indexed.columns]
        base_info_subset_df = base_info_df_indexed[base_cols_to_keep].drop_duplicates(subset=[entity_id_col]).set_index(entity_id_col)
        group_summary_df = base_info_subset_df.join(group_numerators_df, how='left').fillna(0)
        final_group_df = group_summary_df.reset_index()
        rate_cols = []
        for defect_type in target_defects:
            count_col_name = defect_type
            if count_col_name not in final_group_df.columns: final_group_df[count_col_name] = 0
            new_count_col_name = f"{defect_type.lower()}_count"
            final_group_df.rename(columns={count_col_name: new_count_col_name}, inplace=True)
            rate_col_name = f"{defect_type.lower()}_rate"
            final_group_df[rate_col_name] = np.where(
                final_group_df['total_panels'] > 0,
                final_group_df[new_count_col_name] / final_group_df['total_panels'], 0
            )
            rate_cols.append(rate_col_name)
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
            empty_df = pd.DataFrame()
            return empty_df, empty_df # 返回空 DF 避免后续错误

# ==============================================================================
#                      辅助函数：处理截断
# ==============================================================================
@staticmethod
def _filter_by_pass_rate(
    base_df: pd.DataFrame,
    denominator: float,
    threshold: float = 0.9,
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

# --- 截断 ---
@staticmethod
def _apply_defect_capping(
    results_dict: Dict[str, Any], # 修改变量名以反映输入是处理后的结果
    group_thresholds: dict,
    code_thresholds: dict
) -> Dict[str, Any]:
    """
    [辅助函数 - 通用 V1.1] 应用不良率截断 (对处理后的结果)。
    增加对输入字典结构的健壮性检查。
    """
    logging.info("开始对不良率进行可配置的随机截断处理...")
    # [新增] 输入检查
    if not isinstance(results_dict, dict):
            logging.error("传递给 _apply_defect_capping 的输入不是字典，无法截断。")
            return results_dict # 返回原始输入或引发错误
    if "group_level_summary_for_chart" not in results_dict or \
        "code_level_details" not in results_dict or \
        not isinstance(results_dict["code_level_details"], dict):
            logging.error("传递给 _apply_defect_capping 的字典结构不完整，无法截断。")
            return results_dict

    try:
        base_seed = 101
        # 1. 截断 Group 级数据
        df_group_chart = results_dict["group_level_summary_for_chart"].copy()
        rate_cols = [col for col in df_group_chart.columns if col.endswith('_rate')]
        for i, col_name in enumerate(rate_cols):
            # 使用确定性种子以保证可复现性
            # np.random.seed(base_seed + i) # 旧方法，会影响全局
            rng_capping = np.random.default_rng(base_seed + i) # 使用独立生成器
            df_group_chart[col_name] = df_group_chart[col_name].apply(
                lambda rate: _apply_random_cap_and_floor( # 调用时加上类名
                    rate,
                    upper_threshold=group_thresholds['upper'],
                    lower_threshold=group_thresholds['lower'],
                    rng=rng_capping # 传递生成器
                )
            )

        # 2. 截断 Code 级数据
        dict_code_details = results_dict["code_level_details"].copy() # 操作副本
        rng_capping_code = np.random.default_rng(base_seed + 99) # Code 级使用一个种子
        for group, df_code in dict_code_details.items():
            if df_code is not None and not df_code.empty and 'defect_rate' in df_code.columns:
                df_code_mod = df_code.copy()
                df_code_mod['defect_rate'] = df_code_mod['defect_rate'].apply(
                    lambda rate: _apply_random_cap_and_floor( # 调用时加上类名
                        rate,
                        upper_threshold=code_thresholds['upper'],
                        lower_threshold=code_thresholds['lower'],
                        rng=rng_capping_code # 传递生成器
                    )
                )
                # [新增] 截断后重新计算不良数
                if 'total_panels' in df_code_mod.columns:
                        df_code_mod['defect_panel_count'] = np.maximum(0, np.round(
                            df_code_mod['defect_rate'] * df_code_mod['total_panels']
                        )).astype(int)

                dict_code_details[group] = df_code_mod # 更新字典中的 DataFrame

        # 3. 重新准备 UI 汇总表 (基于已截断的 Group 数据)
        final_ui_columns = list(results_dict.get("group_level_summary_for_table", pd.DataFrame()).columns) # 从原始结果获取列顺序
        if not final_ui_columns and not df_group_chart.empty: # Fallback
                final_ui_columns = df_group_chart.columns.tolist()

        group_level_for_ui = df_group_chart.reindex(columns=final_ui_columns).fillna(0) if final_ui_columns else df_group_chart

        logging.info("不良率随机截断处理完成。")

        # 4. 构建并返回最终结果字典
        final_capped_results = results_dict.copy() # 复制原始字典结构
        final_capped_results["group_level_summary_for_table"] = group_level_for_ui
        final_capped_results["group_level_summary_for_chart"] = df_group_chart
        final_capped_results["code_level_details"] = dict_code_details
        return final_capped_results

    except Exception as e:
        logging.error(f"在应用截断时发生错误: {e}", exc_info=True)
        return results_dict # 出错时返回未截断的结果


@staticmethod
def _apply_random_cap_and_floor(
    rate: float,
    upper_threshold: float,
    lower_threshold: float,
    rng: np.random.Generator # <--- 接收生成器实例
    ) -> float:
    """
    [辅助函数 V1.1 - 使用独立 RNG] 对单个不良率数值应用可复现的随机上下限截断。
    """
    if rate > upper_threshold:
        # 使用传入的 rng 实例生成随机数
        return rng.uniform(upper_threshold * 0.7, upper_threshold * 1.3)
    elif 0 < rate < lower_threshold:
            # 对于下限，确保波动范围有意义
            low_bound = max(0, lower_threshold * 0.8) # 确保不低于0
            high_bound = lower_threshold * 1.2
            if low_bound >= high_bound: # 如果下限计算有问题，返回一个固定值
                return low_bound
            return rng.uniform(low_bound, high_bound)
    else:
        return rate