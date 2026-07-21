# src/vivo_project/core/sheet_lot/capping.py
import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

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

def _apply_random_cap_and_floor(
    rate: float,
    panels: int | None = None,
    current_count: int | None = None,
    upper_threshold: float = 1.0,
    lower_threshold: float = 0.0,
    rng: np.random.Generator | None = None,
) -> int | float:
    """
    [辅助函数 V2.0 - 软截断] 
    当 rate 超标时，返回 [Limit * 0.8, Limit] 之间的随机值，
    确保截断后的数据依然呈现自然的随机波动，而非死板的直线。
    """
    rng = rng or np.random.default_rng()

    if rate > upper_threshold:
        # 上限保护：在 Spec 的 80% ~ 95% 之间随机浮动
        # Retain a ±5% tolerance around the spec so Lots may be slightly out of spec.
        safe_rate = rng.uniform(upper_threshold * 0.95, upper_threshold * 1.05)
        if panels is None or current_count is None:
            return float(safe_rate)

        new_count = int(np.floor(safe_rate * panels))
        return min(new_count, int(current_count)) # 物理铁律：压制后的数量绝不能比原来更高
        
    # elif 0 < rate < lower_threshold:
    #     # 下限保护 (保持不变)
    #     safe_rate = max(0, rng.uniform(lower_threshold * 1.1, lower_threshold * 1.2))
    #     new_count = int(np.ceil(safe_rate * panels))
    #     return max(new_count, int(current_count))
    else:
        if panels is None or current_count is None:
            return float(rate)
        return int(current_count)
