import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Optional

class AbnormalDetector:
    """
    异常波动检测器 (Core Domain Service)
    包含两套规则：
    1. 系统趋势检测：基于清洗后的月度数据 (环比翻倍/激增)。
    2. 基准报表检测：基于外部 Excel 的批次数据 (最新批次 vs 上一批次)。
    """

    # ==========================================================================
    #  逻辑 A: 系统内部月度趋势检测 (迁移自前端)
    # ==========================================================================
    @staticmethod
    def detect_system_trend_alerts(
        group_monthly: pd.DataFrame, 
        code_monthly: pd.DataFrame
    ) -> List[str]:
        alerts = []
        
        # 1. Group 级
        if group_monthly is not None and not group_monthly.empty:
            df_g = group_monthly.sort_values('time_period')
            for grp, sub_df in df_g.groupby('defect_group'):
                msg = AbnormalDetector._check_single_series_trend(sub_df, f"Group 预警 [{grp}]")
                if msg: alerts.append(msg)

        # 2. Code 级
        if code_monthly is not None and not code_monthly.empty:
            df_c = code_monthly.sort_values('time_period')
            for desc, sub_df in df_c.groupby('defect_desc'):
                msg = AbnormalDetector._check_single_series_trend(sub_df, f"Code 预警 [{desc}]")
                if msg: alerts.append(msg)
                
        return alerts

    @staticmethod
    def _check_single_series_trend(sub_df: pd.DataFrame, title_prefix: str) -> str | None:
        """内部辅助函数：检查单条时间序列的最后两个点"""
        if len(sub_df) < 2: return None
        
        curr_row = sub_df.iloc[-1]
        prev_row = sub_df.iloc[-2]
        
        r_curr = float(curr_row['defect_rate'])
        r_prev = float(prev_row['defect_rate'])
        
        # 规则: 翻倍(且基数>0.1%) 或 激增20%
        is_doubled = (r_curr > r_prev * 2) and (r_curr > 0.001)
        is_surged = (r_curr - r_prev > 0.002)
        
        if is_doubled or is_surged:
            reasons = []
            if is_doubled: reasons.append("环比翻倍")
            if is_surged: reasons.append("增幅>0.2%")
            
            return (f"📊 **{title_prefix}** (系统): {curr_row['time_period']} "
                    f"良损 {r_curr:.2%} vs 上月 {r_prev:.2%} -> {' & '.join(reasons)}")
        return None

    # ==========================================================================
    #  逻辑 B: 外部基准报表批次比对 (新需求)
    # ==========================================================================
    @staticmethod
    def detect_benchmark_batch_alerts(
        raw_df: pd.DataFrame, 
        target_groups: List[str], 
        target_codes: List[str]
    ) -> List[str]:
        """
        解析原始 Excel DataFrame，寻找最新两个有效批次并比对。
        """
        alerts = []
        if raw_df is None or raw_df.empty: return []

        try:
            # --- Step 1: 定位关键行与列 ---
            
            # 1.1 找到 "批次产出率" 行 (用于筛选有效列)
            # C 列 (Index 2)
            mask_yield = raw_df[2].astype(str).str.strip() == "批次产出率"
            if not mask_yield.any():
                logging.warning("基准报表中未找到 '批次产出率' 行")
                return []
            yield_row_idx = int(mask_yield.idxmax())
            
            # 1.2 找到 "批次号" 行 (用于获取批次名称)
            # [修改] D 列 (Index 3) 为 "批次/工单"
            mask_batch = raw_df[3].astype(str).str.strip() == "批次/工单"
            if mask_batch.any():
                batch_name_row = mask_batch.idxmax()
            else:
                # [兜底逻辑] 如果没找到明确标记，尝试向上回溯非空行
                logging.warning("未找到 '批次/工单' 标记行，尝试自动回溯...")
                batch_name_row = max(0, yield_row_idx - 1)
                # (此处保留之前的简单回溯作为最后的保险，或者直接报错)

            # 1.3 筛选有效列 (产出率 > 20%)
            valid_cols = []
            # 假设数据从第 5 列 (E列) 开始
            for col_idx in range(raw_df.shape[1] - 1, 4, -1):
                val = raw_df.iloc[yield_row_idx, col_idx]
                try:
                    val_float = float(val) # type: ignore
                    if val_float > 0.2:
                        valid_cols.append(col_idx)
                        if len(valid_cols) == 2: break
                except (ValueError, TypeError):
                    continue
            
            if len(valid_cols) < 2:
                return []

            col_curr, col_prev = valid_cols[0], valid_cols[1]
            
            # 获取批次名称
            batch_curr = str(raw_df.iloc[batch_name_row, col_curr]) # type: ignore
            batch_prev = str(raw_df.iloc[batch_name_row, col_prev]) # type: ignore

            # --- Step 2: 建立数据索引 (Group/Code -> Row) ---
            # 假设数据区域在 yield_row_idx 之后
            group_map = {}
            code_map = {}
            
            for r in range(yield_row_idx + 1, len(raw_df)):
                g_val = str(raw_df.iloc[r, 2]).strip() # C列 Group
                c_val = str(raw_df.iloc[r, 3]).strip() # D列 Code
                
                if g_val and g_val != 'nan': group_map[g_val] = r
                if c_val and c_val != 'nan': code_map[c_val] = r

            # --- Step 3: 执行比对 ---
            def check_row(row_idx, name, type_label):
                try:
                    v_c = float(raw_df.iloc[row_idx, col_curr]) # type: ignore
                    v_p = float(raw_df.iloc[row_idx, col_prev]) # type: ignore
                    
                    # 规则: 翻倍(基数>0.1%) 或 激增>20%
                    if (v_c > v_p * 2 and v_c > 0.001) or (v_c - v_p > 0.002):
                        return (f"🚨 **{type_label} 真实报表预警 [{name}]**: "
                                f"批次{batch_curr} ({v_c:.2%}) vs 批次{batch_prev} ({v_p:.2%}) -> 异常波动")
                except (ValueError, TypeError):
                    pass
                return None

            # Check Groups
            for grp in target_groups:
                if grp in group_map:
                    msg = check_row(group_map[grp], grp, "Group")
                    if msg: alerts.append(msg)

            # Check Codes
            for code in target_codes:
                if code in code_map:
                    msg = check_row(code_map[code], code, "Code")
                    if msg: alerts.append(msg)

        except Exception as e:
            logging.error(f"基准报表比对逻辑出错: {e}")
            
        return alerts