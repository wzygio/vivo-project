# src/vivo_project/core/sheet_lot_processor.py
import logging
from typing import Any, Dict, Optional

import pandas as pd

# [Refactor] 移除全局 CONFIG, PROJECT_ROOT, RESOURCE_DIR
from src.shared_kernel.config_model import AppConfig
from src.yield_domain.core.sheet_lot.aggregation import (
    _calculate_lot_base_info_with_median_time,
    _calculate_raw_rates,
    _get_desc_to_group_map,
    _reaggregate_groups_from_codes,
)
from src.yield_domain.core.sheet_lot.capping import (
    _apply_defect_capping,
    _apply_random_cap_and_floor,  # noqa: F401 - legacy compatibility export
    _filter_by_pass_rate,
)
from src.yield_domain.core.sheet_lot.overrides import (
    _calculate_lot_override_rate_heuristic,
    _override_rates,
)
from src.yield_domain.core.sheet_lot.simulation import (
    _distribute_sheet_from_lot,
    _simulate_concentration,
)


def _filter_lots_by_warehousing_window(
    lot_base: pd.DataFrame,
    as_of: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Keep lots warehoused on or after the first day of two months ago."""
    if lot_base.empty:
        return lot_base.copy()

    if 'warehousing_time' not in lot_base.columns:
        logging.warning("Lot data has no warehousing_time column; excluding all lots.")
        return lot_base.iloc[0:0].copy()

    reference_date = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
    cutoff_date = reference_date.normalize().replace(day=1) - pd.DateOffset(months=2)
    warehousing_dates = pd.to_datetime(
        lot_base['warehousing_time'], format='%Y%m%d', errors='coerce'
    )
    filtered_lot_base = lot_base.loc[warehousing_dates >= cutoff_date].copy()

    logging.info(
        "Applied lot warehousing window from %s: retained %d of %d lots.",
        cutoff_date.strftime('%Y-%m-%d'),
        len(filtered_lot_base),
        len(lot_base),
    )
    return filtered_lot_base

# ==============================================================================
#             ByCode计算Sheet级不良率
# ==============================================================================
@staticmethod
def calculate_sheet_defect_rates(
    panel_details_df: pd.DataFrame,
    array_input_times_df: pd.DataFrame,
    lot_results: Dict[str, Any], # 接收 Lot 结果
    config: AppConfig,          
    override_df: pd.DataFrame | None = None,
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
    warning_lines: Optional[Dict[str, dict]] = None,
    override_df: pd.DataFrame | None = None,
) -> Dict[str, Any] | None:
    """(V5.0) 独立执行 Lot 级数据模拟"""
    logging.info("开始Lot级计算 (独立模拟 -> 截断 -> 覆盖 模式)...")
    try:
        lot_base = _calculate_lot_base_info_with_median_time(panel_details_df, array_input_times_df)
        if lot_base.empty: return None

        lot_base = _filter_lots_by_warehousing_window(lot_base)
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

        override_lot_avg = _calculate_lot_override_rate_heuristic(
            override_df=override_df,
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
