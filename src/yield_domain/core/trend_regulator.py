# src/vivo_project/core/trend_regulator.py
import pandas as pd
import numpy as np
import logging

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
        Code 级智能调节。
        仅执行上限压制，避免低发 Code 被下限托底放大。
        """
        if daily_df.empty:
            return daily_df

        logging.info("启动 Code 级智能趋势调节器 (单一职责：上限压制)...")

        warning_lines = kwargs.get('warning_lines', {})
        if not warning_lines:
            logging.warning("未获取到 warning_lines 规格线，Code 级截断被跳过。")
            return daily_df

        # =====================================================================
        # 🚀 向量化上限截断 (Vectorized Daily Capping)
        # =====================================================================
        daily_regulated = daily_df.copy()
        
        # 0. 兼容性解析：只提取上限，显式忽略 lower
        upper_limits = {}
        for code, limits in warning_lines.items():
            if isinstance(limits, dict):
                upper_limits[code] = limits.get('upper', 1.0)
            else:
                upper_limits[code] = float(limits)
        
        # 1. 映射警戒线到 DataFrame
        daily_regulated['spec_limit_upper'] = daily_regulated['defect_desc'].map(upper_limits).fillna(1.0)
        
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
            
        # ---------------------------------------------------------------------
        upper_caps = np.floor(
            daily_regulated['spec_limit_upper'] * daily_regulated['total_panels']
        ).astype(int)
        daily_regulated['defect_panel_count'] = np.minimum(
            daily_regulated['defect_panel_count'].astype(int),
            upper_caps
        ).clip(lower=0)
        
        if capping_count > 0:
            logging.info(f"[daily 维度] 向量化上限截断完成：压制超标 {capping_count} 处。")
        else:
            logging.info("[daily 维度] 底层数据安全，未触及上限。")
            
        # 清理临时计算列
        daily_regulated.drop(columns=['spec_limit_upper', 'current_rate'], inplace=True)

        return daily_regulated
