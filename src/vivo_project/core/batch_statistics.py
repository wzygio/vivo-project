# src/vivo_project/core/batch_statistics.py
import pandas as pd
import logging

class BatchStatistics:
    """
    [Core Processor] 批次级统计计算器
    职责：计算每个批次的入库总数(Total Input)、不良数(Defect Count)及良损率(Yield Loss)。
    """

    @staticmethod
    def get_batch_input_counts(panel_df: pd.DataFrame) -> pd.Series:
        """
        计算每个批次的入库总数（去重后的 Panel 数量）。
        Returns:
            Series: index=batch_no, value=total_count
        """
        if panel_df.empty: return pd.Series(dtype=int)
        return panel_df.groupby('batch_no')['panel_id'].nunique()

    @staticmethod
    def calculate_batch_defect_stats(panel_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算 [Batch x Defect Code] 粒度的详细统计。
        
        Returns:
            DataFrame columns: 
            ['batch_no', 'defect_desc', 'defect_count', 'total_input', 'defect_rate']
        """
        if panel_df.empty: return pd.DataFrame()

        # 1. 计算分母：每个批次的入库总数
        batch_totals = BatchStatistics.get_batch_input_counts(panel_df)
        
        # 2. 计算分子：每个批次、每个 Code 的不良 Panel 数
        # 过滤掉良品 (defect_desc 为空或 NaN)
        defect_df = panel_df[panel_df['defect_desc'].notna()]
        if defect_df.empty: return pd.DataFrame()
        
        defect_counts = defect_df.groupby(['batch_no', 'defect_desc'])['panel_id'].nunique()
        
        # 3. 合并与计算
        stats_df = defect_counts.to_frame(name='defect_count').reset_index()
        stats_df['total_input'] = stats_df['batch_no'].map(batch_totals)
        stats_df['defect_rate'] = stats_df['defect_count'] / stats_df['total_input']
        
        return stats_df