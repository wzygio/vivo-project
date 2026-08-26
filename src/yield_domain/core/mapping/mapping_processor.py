# src/vivo_project/core/mapping_processor.py
import pandas as pd
import logging
import math
import re
from typing import Any, Optional
from src.yield_domain.core.batch_statistics import BatchStatistics
from yield_domain.core.mapping.hotspot_modification import (
    apply_hotspot_modification_to_matrix,
    resolve_mapping_modification_plan,
)
from yield_domain.core.mapping.layout import resolve_mapping_layout
from yield_domain.core.mapping.panel_position import (
    get_deterministically_modified_panel_id as _get_deterministically_modified_panel_id,
    parse_panel_id_to_coords as _parse_panel_id_to_coords,
    reconstruct_panel_id as _reconstruct_panel_id,
)

# ==============================================================================
#                      月度缩放倍数（批次所属月份 × Code）
# ==============================================================================
MAX_MONTHLY_SCALE_FACTOR = 10.0


def _safe_monthly_factor(factor: object, code_desc: str, month: str) -> float:
    """拒绝异常或过大的倍率，避免单个配置触发 Mapping 行爆炸。"""
    try:
        parsed = float(factor)
    except (TypeError, ValueError):
        parsed = math.nan
    if not math.isfinite(parsed) or parsed < 0 or parsed > MAX_MONTHLY_SCALE_FACTOR:
        logging.error(
            "Mapping 月度倍率无效或超出防御上限 %.1f，按 1.0 处理: Code=%s, 月份=%s, 原值=%r",
            MAX_MONTHLY_SCALE_FACTOR,
            code_desc,
            month,
            factor,
        )
        return 1.0
    return parsed


def _apply_monthly_scale_factors(
    df: pd.DataFrame,
    batch_month_map: dict[str, str],
    monthly_factors: dict[tuple[str, str], float],
) -> pd.DataFrame:
    """在级联衰减之前，按 (defect_desc, 批次所属月份) 缩放不良行数。

    - f < 1：确定性抽样（random_state=42）；
    - f > 1：整倍复制 + 余数抽样，复制行 panel_id 加 `_SIM_M{i}` 后缀防碰撞；
    - 未命中因子（含批次日期无法解析）的行保持不变。
    """
    SEED = 42
    processed = []
    for (batch_no, code_desc), df_group in df.groupby(
        ["batch_no", "defect_desc"], sort=False, dropna=False
    ):
        month = batch_month_map.get(batch_no)
        raw_factor = monthly_factors.get((code_desc, month), 1.0) if month else 1.0
        factor = _safe_monthly_factor(raw_factor, str(code_desc), month or "unknown")
        current_count = len(df_group)
        target_count = int(current_count * factor)

        if target_count == current_count or factor == 1.0:
            processed.append(df_group)
        elif target_count < current_count:
            processed.append(df_group.sample(n=target_count, random_state=SEED))
        else:
            repeat_times = target_count // current_count
            remainder = target_count % current_count
            copies = [df_group]
            for i in range(1, repeat_times):
                df_copy = df_group.copy()
                df_copy["panel_id"] = df_copy["panel_id"].astype(str) + f"_SIM_M{i}"
                copies.append(df_copy)
            if remainder > 0:
                df_rem = df_group.sample(n=remainder, random_state=SEED).copy()
                df_rem["panel_id"] = df_rem["panel_id"].astype(str) + "_SIM_MREM"
                copies.append(df_rem)
            processed.append(pd.concat(copies, ignore_index=True))
    return pd.concat(processed, ignore_index=True) if processed else df


# ==============================================================================
#                      ByCode计算Mapping集中性
# ==============================================================================  
@staticmethod
def prepare_mapping_data(
    panel_details_df: pd.DataFrame,
    scaling_factor: float,
    min_panel_threshold: int = 0,
    hotspot_scripts: Optional[list[dict[str, Any]]] = None,
    product_code: Optional[str] = None,
    mapping_layout: Optional[dict[str, Any]] = None,
    monthly_factors: Optional[dict[tuple[str, str], float]] = None,
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
        resolved_layout = resolve_mapping_layout(mapping_layout)
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

        # --- 步骤2: 按唯一修饰方案执行默认位置修饰 ---
        batches_after_pos_modification: list[pd.DataFrame] = []
        total_batches = len(sorted_batches)
        for batch_position, batch_no in enumerate(sorted_batches):
            df_current_batch = df_defective_panels[df_defective_panels['batch_no'] == batch_no].copy()
            for code_desc in df_current_batch['defect_desc'].unique():
                modification_plan = resolve_mapping_modification_plan(
                    script_config_list=hotspot_scripts or [],
                    product_code=product_code,
                    code_desc=code_desc,
                    batch_no=batch_no,
                    batch_position=batch_position,
                    total_batches=total_batches,
                )
                if not modification_plan.applies_default_position_modification:
                    continue

                code_mask = df_current_batch['defect_desc'] == code_desc
                def _modify_panel_position(row: pd.Series) -> str:
                    if mapping_layout is None:
                        return _get_deterministically_modified_panel_id(
                            row['panel_id'],
                            row['batch_no'],
                        )
                    return _get_deterministically_modified_panel_id(
                        row['panel_id'],
                        row['batch_no'],
                        resolved_layout,
                    )

                df_current_batch.loc[code_mask, 'panel_id'] = df_current_batch.loc[
                    code_mask
                ].apply(_modify_panel_position, axis=1)
            batches_after_pos_modification.append(df_current_batch) 
        
        if not batches_after_pos_modification:
            return pd.DataFrame()

        df_defective_panels_modified = pd.concat(batches_after_pos_modification)
        
        # --- 步骤2.5: 月度缩放倍数（批次所属月份 × Code，级联衰减之前） ---
        if monthly_factors:
            batch_month_map = {
                row["original_batch"]: (
                    row["batch_date"].strftime("%Y-%m")
                    if pd.notna(row["batch_date"])
                    else None
                )
                for _, row in batch_map.iterrows()
            }
            df_defective_panels_modified = _apply_monthly_scale_factors(
                df_defective_panels_modified,
                batch_month_map,
                monthly_factors,
            )
        
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
    
