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

    # ==========================================================================
    #  Helper Functions (核心逻辑提取)
    # ==========================================================================
    @staticmethod
    def _parse_metrics(
        raw_curr_p: Any, raw_prev_p: Any, raw_curr_c: Any, raw_prev_c: Any
    ) -> Optional[Tuple[float, float, float, float, float, float]]:
        """
        [Helper] 统一提取并转换数值，计算良率。
        返回: (curr_panels, prev_panels, curr_count, prev_count, curr_rate, prev_rate) 或 None
        """
        try:
            # 统一转 float，兼容 numpy 类型和字符串
            curr_panels = float(raw_curr_p)
            prev_panels = float(raw_prev_p)
            
            if curr_panels == 0 or prev_panels == 0:
                return None
                
            curr_count = float(raw_curr_c)
            prev_count = float(raw_prev_c)

            curr_rate = curr_count / curr_panels
            prev_rate = prev_count / prev_panels
            
            return curr_panels, prev_panels, curr_count, prev_count, curr_rate, prev_rate
        except (ValueError, TypeError, IndexError):
            return None

    @staticmethod
    def _calculate_regulated_target(
        curr_rate: float, prev_rate: float, curr_panels: float, curr_count: float
    ) -> Optional[int]:
        """
        [Helper] 核心调节逻辑：检测异常 -> 计算压制后的目标数量。
        新规则：如果前值为 0，则跳过调整（保留原值），因为 0 不具备参考意义。
        """
        # [核心改动] 前值为 0 时认为基准无效，直接跳过调整，保留原始数据
        if prev_rate <= 0:
            return None

        # 1. 内部自查 (Abnormal Check)
        if not AbnormalDetector.is_value_trend_abnormal(curr_rate, prev_rate):
            return None

        # 2. 计算压制目标 (Regulation Calculation)
        # 目标 A: 激增压制 (防止超过前值 + 0.2%)
        safe_delta = AbnormalDetector.THRESHOLD_SURGE_DELTA - 0.0001
        target_rate_surge = prev_rate + safe_delta
        
        # 目标 B: 翻倍压制 (防止超过前值 * 2)
        target_rate_doubling = (prev_rate * AbnormalDetector.THRESHOLD_DOUBLING_RATIO) - 0.0001
        
        # 取两者中更严格（更小）的那个作为目标良率
        target_rate = min(target_rate_surge, target_rate_doubling)
        
        # 兜底保护：防止由于减法操作在极小值情况下产生负数
        target_rate = max(0.0, target_rate)
        
        target_count = int(target_rate * curr_panels)
        
        # 3. 修正因子检查 (确保是向下压低数据)
        if curr_count > 0:
            correction_factor = target_count / curr_count
            if correction_factor >= 1.0:
                return None
        else:
            return None

        return target_count
    # ==========================================================================
    #  Main Functions
    # ==========================================================================
    @staticmethod
    def regulate_monthly_and_weekly(
        monthly_df: pd.DataFrame, 
        weekly_df: pd.DataFrame,
        config: AppConfig,
        resource_dir: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Group 级智能调节 (Monthly & Weekly)
        """
        if monthly_df.empty or len(monthly_df) < 2:
            return monthly_df, weekly_df

        logging.info("启动智能趋势调节器 (Smart Alignment)...")

        monthly_regulated = monthly_df.copy()
        weekly_regulated = weekly_df.copy()

        # --- 1. 月度调节 ---
        last_month_idx = monthly_df.index[-1]
        prev_month_idx = monthly_df.index[-2]
        
        # 排除非数据列
        target_groups_m = [c for c in monthly_df.columns if c != 'total_panels']

        for group in target_groups_m:
            try:
                # 提取 (Group数据在宽表中直接通过 index, col 定位)
                metrics = TrendRegulator._parse_metrics(
                    monthly_df.loc[last_month_idx, 'total_panels'],
                    monthly_df.loc[prev_month_idx, 'total_panels'],
                    monthly_df.loc[last_month_idx, group],
                    monthly_df.loc[prev_month_idx, group]
                )
                if not metrics: continue
                
                curr_panels, _, curr_count, _, curr_rate, prev_rate = metrics
                
                # 计算
                target_count = TrendRegulator._calculate_regulated_target(
                    curr_rate, prev_rate, curr_panels, curr_count
                )
                
                # 应用
                if target_count is not None:
                    logging.info(f"[{group}] 系统报警但外部基准稳定 -> 触发智能调节。")
                    monthly_regulated.loc[last_month_idx, group] = target_count
                    logging.info(f"[智能调节-Group月度] {group} ({last_month_idx.strftime('%Y-%m')}): {curr_rate:.2%} -> {(target_count/curr_panels):.2%}")

            except Exception as e:
                logging.error(f"调节 Group 月度 {group} 时出错: {e}")
                continue

        # --- 2. 周度调节 ---
        if not weekly_df.empty and len(weekly_df) >= 2:
            last_week_idx = weekly_df.index[-1]
            prev_week_idx = weekly_df.index[-2]
            
            target_groups_w = [c for c in weekly_df.columns if c != 'total_panels']
            
            for group in target_groups_w:
                try:
                    # 提取 (注意：Weekly数据可能是Series对象，_parse_metrics 会自动处理 float 强转)
                    metrics = TrendRegulator._parse_metrics(
                        weekly_df.loc[last_week_idx, 'total_panels'],
                        weekly_df.loc[prev_week_idx, 'total_panels'],
                        weekly_df.loc[last_week_idx, group],
                        weekly_df.loc[prev_week_idx, group]
                    )
                    if not metrics: continue
                    
                    curr_panels, _, curr_count, _, curr_rate, prev_rate = metrics
                    
                    # 计算
                    target_count = TrendRegulator._calculate_regulated_target(
                        curr_rate, prev_rate, curr_panels, curr_count
                    )
                    
                    # 应用
                    if target_count is not None:
                        weekly_regulated.loc[last_week_idx, group] = target_count
                        logging.info(f"[智能调节-Group周度] {group} (本周:{curr_rate:.2%} vs 上周:{prev_rate:.2%}): {curr_rate:.2%} -> {target_count/curr_panels:.2%}")

                except Exception as e:
                    logging.error(f"调节 Group 周度 {group} 时出错: {e}")
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
        Code 级智能调节 (Monthly & Weekly)
        """
        if monthly_df.empty and weekly_df.empty:
            return monthly_df, weekly_df

        logging.info("启动 Code 级智能趋势调节器...")

        monthly_regulated = monthly_df.copy()
        weekly_regulated = weekly_df.copy()
        
        # --- 1. 月度调节 ---
        if not monthly_df.empty:
            unique_codes = monthly_regulated['defect_desc'].unique()
            dates = monthly_regulated['warehousing_time'].sort_values().unique()
            
            if len(dates) >= 2:
                last_date = dates[-1]
                prev_date = dates[-2]

                for code in unique_codes:
                    if code == 'NoDefect': continue
                    try:
                        mask_curr = (monthly_regulated['defect_desc'] == code) & (monthly_regulated['warehousing_time'] == last_date)
                        mask_prev = (monthly_regulated['defect_desc'] == code) & (monthly_regulated['warehousing_time'] == prev_date)
                        
                        if not mask_curr.any() or not mask_prev.any(): continue
                        
                        # 提取 (Code级数据需要先 .values[0] 取出标量)
                        # 使用 getattr/item 等方式确保取出的是标量，或者直接依赖 _parse_metrics 的强转能力
                        # 这里为了安全，显式取 .iloc[0]
                        metrics = TrendRegulator._parse_metrics(
                            monthly_regulated.loc[mask_curr, 'total_panels'].iloc[0], # type: ignore
                            monthly_regulated.loc[mask_prev, 'total_panels'].iloc[0], # type: ignore
                            monthly_regulated.loc[mask_curr, 'defect_panel_count'].iloc[0], # type: ignore
                            monthly_regulated.loc[mask_prev, 'defect_panel_count'].iloc[0] # type: ignore
                        )
                        if not metrics: continue
                        
                        curr_panels, _, curr_count, _, curr_rate, prev_rate = metrics
                        
                        # 计算
                        target_count = TrendRegulator._calculate_regulated_target(
                            curr_rate, prev_rate, curr_panels, curr_count
                        )
                        
                        # 应用
                        if target_count is not None:
                            logging.info(f"[智能调节-Code月度]{code}: {curr_rate:.2%} -> {(target_count/curr_panels):.2%}")
                            monthly_regulated.loc[mask_curr, 'defect_panel_count'] = target_count
                            
                    except Exception as e:
                        continue

        # --- 2. 周度调节 ---
        if not weekly_df.empty:
            unique_codes = weekly_regulated['defect_desc'].unique()
            weeks = weekly_regulated['warehousing_time'].sort_values().unique()
            
            if len(weeks) >= 2:
                last_date = weeks[-1]
                prev_date = weeks[-2]
                
                for code in unique_codes:
                    if code == 'NoDefect': continue
                    try:
                        mask_curr = (weekly_regulated['defect_desc'] == code) & (weekly_regulated['warehousing_time'] == last_date)
                        mask_prev = (weekly_regulated['defect_desc'] == code) & (weekly_regulated['warehousing_time'] == prev_date)
                        
                        if not mask_curr.any() or not mask_prev.any(): continue
                        
                        # 提取
                        metrics = TrendRegulator._parse_metrics(
                            weekly_regulated.loc[mask_curr, 'total_panels'].iloc[0], # type: ignore
                            weekly_regulated.loc[mask_prev, 'total_panels'].iloc[0], # type: ignore
                            weekly_regulated.loc[mask_curr, 'defect_panel_count'].iloc[0], # type: ignore
                            weekly_regulated.loc[mask_prev, 'defect_panel_count'].iloc[0] # type: ignore
                        )
                        if not metrics: continue
                        
                        curr_panels, _, curr_count, _, curr_rate, prev_rate = metrics
                        
                        # 计算
                        target_count = TrendRegulator._calculate_regulated_target(
                            curr_rate, prev_rate, curr_panels, curr_count
                        )
                        
                        # 应用
                        if target_count is not None:
                            weekly_regulated.loc[mask_curr, 'defect_panel_count'] = target_count
                            logging.info(f"[智能调节-Code周度] {code}: {curr_rate:.2%} -> {target_count/curr_panels:.2%}")

                    except Exception as e:
                        logging.error(f"调节周度 Code {code} 时出错: {e}")
                        continue

        return monthly_regulated, weekly_regulated