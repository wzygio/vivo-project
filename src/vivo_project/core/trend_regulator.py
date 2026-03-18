# src/vivo_project/core/trend_regulator.py
import pandas as pd
import numpy as np
import logging
from typing import Tuple, Any, cast, Optional
from datetime import datetime as dt
from pathlib import Path
from pandas import Series

from vivo_project.config_model import AppConfig
from vivo_project.core.abnormal_detector import AbnormalDetector
from vivo_project.infrastructure.data_loader import load_excel_report

class TrendRegulator:
    """
    智能趋势调节器 (Smart Alignment Regulator)
    核心逻辑：自查 -> 仲裁 -> 压制
    """
    @staticmethod
    def regulate_code_daily_base(
        daily_df: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Code 级智能调节 (V4.2 - 底层向量化区间截断)
        包含上限防爆表机制与下限防过低机制，维持工业稳态波动。
        """
        if daily_df.empty:
            return daily_df

        logging.info("启动 Code 级智能趋势调节器 (单一职责：底层向量化区间截断)...")

        warning_lines = kwargs.get('warning_lines', {})
        if not warning_lines:
            logging.warning("未获取到 warning_lines 规格线，Code 级截断被跳过。")
            return daily_df

        # =====================================================================
        # 🚀 向量化区间截断 (Vectorized Daily Capping & Flooring)
        # =====================================================================
        daily_regulated = daily_df.copy()
        
        # 0. 兼容性解析：提取上限与下限
        upper_limits = {}
        lower_limits = {}
        for code, limits in warning_lines.items():
            if isinstance(limits, dict):
                upper_limits[code] = limits.get('upper', 1.0)
                lower_limits[code] = limits.get('lower', 0.0)
            else:
                upper_limits[code] = float(limits)
                lower_limits[code] = 0.0
        
        # 1. 映射警戒线到 DataFrame
        daily_regulated['spec_limit_upper'] = daily_regulated['defect_desc'].map(upper_limits).fillna(1.0)
        daily_regulated['spec_limit_lower'] = daily_regulated['defect_desc'].map(lower_limits).fillna(0.0)
        
        # 2. 计算当前良率
        daily_regulated['current_rate'] = np.where(
            daily_regulated['total_panels'] > 0, 
            daily_regulated['defect_panel_count'] / daily_regulated['total_panels'], 
            0.0
        )
        
        # 提供一个全局稳定的 Hash 算子
        def _stable_hash(s): return sum(ord(c) for c in str(s))

        # ---------------------------------------------------------------------
        # 🛑 A. 上限压制 (Upper Capping)
        # ---------------------------------------------------------------------
        mask_exceed = daily_regulated['current_rate'] > daily_regulated['spec_limit_upper']
        capping_count = mask_exceed.sum()
        
        if capping_count > 0:
            exceed_df = daily_regulated[mask_exceed].copy()
            
            ts_vec = (exceed_df['warehousing_time'].astype('int64') // 10**9).astype(int)
            code_hash_vec = exceed_df['defect_desc'].map(_stable_hash)
            
            hash_val = ((ts_vec + code_hash_vec) % 10000) / 10000.0
            safe_rates = exceed_df['spec_limit_upper'] * 0.8 + (hash_val * exceed_df['spec_limit_upper'] * 0.1)
            
            new_counts = np.floor(safe_rates * exceed_df['total_panels']).astype(int)
            final_counts = np.minimum(new_counts, exceed_df['defect_panel_count'])
            daily_regulated.loc[mask_exceed, 'defect_panel_count'] = final_counts
            
            # 同步更新 current_rate 以供下限参考（虽不互斥，但保持数据一致性）
            daily_regulated.loc[mask_exceed, 'current_rate'] = safe_rates

        # ---------------------------------------------------------------------
        # 🛬 B. 下限托底 (Lower Flooring)
        # ---------------------------------------------------------------------
        # [物理铁律]: 如果不良本身就是 0 (毫无瑕疵)，绝对不能强行拔高伪造不良！
        # 只有在确有不良发生 (current_rate > 0)，且低于下限时，才进行托底。
        mask_below = (daily_regulated['current_rate'] > 0) & (daily_regulated['current_rate'] < daily_regulated['spec_limit_lower'])
        floor_count = mask_below.sum()
        
        if floor_count > 0:
            below_df = daily_regulated[mask_below].copy()
            
            ts_vec_l = (below_df['warehousing_time'].astype('int64') // 10**9).astype(int)
            code_hash_vec_l = below_df['defect_desc'].map(_stable_hash)
            
            # 使用略微不同的乘数因子，防止与上限形成完全同步的伪随机波形
            hash_val_l = ((ts_vec_l * 7 + code_hash_vec_l) % 10000) / 10000.0 
            
            # 托底保护：在 [Lower * 0.8, Lower * 1.2] 区间内自然波动
            safe_rates_l = below_df['spec_limit_lower'] * 1.0 + (hash_val_l * below_df['spec_limit_lower'] * 0.2)
            
            # 向上看齐，取 ceil 保证至少补充到位
            new_counts_l = np.ceil(safe_rates_l * below_df['total_panels']).astype(int)
            final_counts_l = np.maximum(new_counts_l, below_df['defect_panel_count'])
            daily_regulated.loc[mask_below, 'defect_panel_count'] = final_counts_l
            
        # ---------------------------------------------------------------------
        
        if capping_count > 0 or floor_count > 0:
            logging.info(f"[daily 维度] 向量化截断完成：压制超标 {capping_count} 处，托底过低 {floor_count} 处。")
        else:
            logging.info(f"[daily 维度] 底层数据安全，未触及上下限。")
            
        # 清理临时计算列
        daily_regulated.drop(columns=['spec_limit_upper', 'spec_limit_lower', 'current_rate'], inplace=True)

        return daily_regulated