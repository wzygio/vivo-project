# src/vivo_project/core/trend_regulator.py
import pandas as pd
import numpy as np
import logging
from typing import Tuple, Any, cast
from datetime import datetime as dt
from pathlib import Path

# [Refactor] 引入 AppConfig 模型，移除全局 CONFIG
from vivo_project.config_model import AppConfig
from vivo_project.core.abnormal_detector import AbnormalDetector
from vivo_project.infrastructure.data_loader import load_excel_report

class TrendRegulator:
    """
    智能趋势调节器 (Smart Alignment Regulator)
    核心逻辑：自查 -> 仲裁 -> 压制
    """

    @staticmethod
    def regulate_monthly_and_weekly(
        monthly_df: pd.DataFrame, 
        weekly_df: pd.DataFrame,
        config: AppConfig,
        resource_dir: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        检查月度数据的最新一月。
        如果系统显示恶化，但外部基准未恶化，则强制压平月度数据，并回溯修正周度数据。
        """
        if monthly_df.empty or len(monthly_df) < 2:
            return monthly_df, weekly_df

        logging.info("启动智能趋势调节器 (Smart Alignment)...")

        # 2. 定位需要检查的月份 (最后一行)
        last_month_idx = monthly_df.index[-1] # Timestamp
        prev_month_idx = monthly_df.index[-2]
        
        # 3. 遍历每个产品 (Column)
        target_groups = monthly_df.columns.tolist()
        if 'total_panels' in target_groups: target_groups.remove('total_panels')

        monthly_regulated = monthly_df.copy()
        weekly_regulated = weekly_df.copy()

        for group in target_groups:
            try:
                # A. 提取数值
                try:
                    raw_curr_panels = monthly_df.loc[last_month_idx, 'total_panels']
                    raw_prev_panels = monthly_df.loc[prev_month_idx, 'total_panels']
                    
                    curr_panels = float(cast(Any, raw_curr_panels))
                    prev_panels = float(cast(Any, raw_prev_panels))
                except (ValueError, TypeError):
                    logging.warning(f"[{group}] 无法提取 total_panels (非数值)，跳过。")
                    continue
                
                if curr_panels == 0 or prev_panels == 0: continue
                
                try:
                    raw_curr_count = monthly_df.loc[last_month_idx, group]
                    raw_prev_count = monthly_df.loc[prev_month_idx, group]
                    
                    curr_count = float(cast(Any, raw_curr_count))
                    prev_count = float(cast(Any, raw_prev_count))
                except (ValueError, TypeError):
                    curr_count = 0.0
                    prev_count = 0.0

                curr_rate = curr_count / curr_panels
                prev_rate = prev_count / prev_panels

                # B. Step 1: 内部自查
                if not AbnormalDetector.is_value_trend_abnormal(curr_rate, prev_rate):
                    continue 

                # D. Step 3: 逆向压制
                logging.warning(f"[{group}] 系统报警但外部基准稳定 -> 触发智能调节。")
                
                safe_delta = AbnormalDetector.THRESHOLD_SURGE_DELTA - 0.0001
                target_rate = prev_rate + safe_delta
                
                target_rate_doubling = (prev_rate * AbnormalDetector.THRESHOLD_DOUBLING_RATIO) - 0.0001
                target_rate = min(target_rate, target_rate_doubling)
                
                target_count = int(target_rate * curr_panels)
                correction_factor = target_count / curr_count if curr_count > 0 else 1.0
                
                if correction_factor >= 1.0: continue

                # E. 应用修正
                monthly_regulated.loc[last_month_idx, group] = target_count
                logging.warning(f"[智能调节-Group] {group} ({last_month_idx.strftime('%Y-%m')}): {curr_rate:.2%} -> {target_rate:.2%}")

            except Exception as e:
                logging.error(f"调节 Group {group} 时出错: {e}")
                continue

        return monthly_regulated, weekly_regulated


    @staticmethod
    def regulate_code_monthly_and_weekly(
        monthly_df: pd.DataFrame, 
        weekly_df: pd.DataFrame,
        config: AppConfig,
        resource_dir: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        [新增] Code 级智能调节
        """
        if monthly_df.empty or weekly_df.empty:
            return monthly_df, weekly_df

        logging.info("启动 Code 级智能趋势调节器...")

        # 2. 遍历每个 Code
        monthly_regulated = monthly_df.copy()
        weekly_regulated = weekly_df.copy()
        
        unique_codes = monthly_regulated['defect_desc'].unique()
        
        dates = monthly_regulated['warehousing_time'].sort_values().unique()
        if len(dates) < 2: return monthly_regulated, weekly_regulated
        
        last_month_date = dates[-1]
        prev_month_date = dates[-2]

        for code in unique_codes:
            if code == 'NoDefect': continue
            
            try:
                # A. 提取该 Code 的最近两个月数据
                mask_curr = (monthly_regulated['defect_desc'] == code) & (monthly_regulated['warehousing_time'] == last_month_date)
                mask_prev = (monthly_regulated['defect_desc'] == code) & (monthly_regulated['warehousing_time'] == prev_month_date)
                
                if not mask_curr.any() or not mask_prev.any(): continue
                
                try:
                    raw_curr_p = cast(pd.Series, monthly_regulated.loc[mask_curr, 'total_panels']).values[0]
                    raw_prev_p = cast(pd.Series, monthly_regulated.loc[mask_prev, 'total_panels']).values[0]
                    curr_panels = float(cast(Any, raw_curr_p))
                    prev_panels = float(cast(Any, raw_prev_p))
                    
                    raw_curr_c = cast(pd.Series, monthly_regulated.loc[mask_curr, 'defect_panel_count']).values[0]
                    raw_prev_c = cast(pd.Series, monthly_regulated.loc[mask_prev, 'defect_panel_count']).values[0]
                    curr_count = float(cast(Any, raw_curr_c))
                    prev_count = float(cast(Any, raw_prev_c))
                except (IndexError, ValueError, TypeError):
                    continue

                if curr_panels == 0 or prev_panels == 0: continue
                
                curr_rate = curr_count / curr_panels
                prev_rate = prev_count / prev_panels

                # B. Step 1: 内部自查
                if not AbnormalDetector.is_value_trend_abnormal(curr_rate, prev_rate):
                    continue 
                
                # D. Step 3: 逆向压制
                safe_delta = AbnormalDetector.THRESHOLD_SURGE_DELTA - 0.0001
                target_rate = prev_rate + safe_delta
                
                target_rate_doubling = (prev_rate * AbnormalDetector.THRESHOLD_DOUBLING_RATIO) - 0.0001
                target_rate = min(target_rate, target_rate_doubling)
                
                target_count = int(target_rate * curr_panels)
                correction_factor = target_count / curr_count if curr_count > 0 else 1.0
                
                if correction_factor >= 1.0: continue

                # E. 应用修正
                logging.info(f"[Code: {code}] 触发智能调节: {curr_rate:.2%} -> {target_rate:.2%}")

                monthly_regulated.loc[mask_curr, 'defect_panel_count'] = target_count
                logging.warning(f"[智能调节-Code] {code} ({last_month_date.strftime('%Y-%m')}): {curr_rate:.2%} -> {target_rate:.2%}")
                
            except Exception as e:
                logging.error(f"调节 Code {code} 时出错: {e}")
                continue
                
        return monthly_regulated, weekly_regulated