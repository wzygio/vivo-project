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
    智能趋势调节器 (Smart Alignment Regulator) - V3.0 全量递归版
    核心逻辑：全链路重放 (Full History Replay)
    """

    # ==========================================================================
    #  Helper Functions (保持不变，复用逻辑)
    # ==========================================================================
    @staticmethod
    def _parse_metrics(
        raw_curr_p: Any, raw_prev_p: Any, raw_curr_c: Any, raw_prev_c: Any
    ) -> Optional[Tuple[float, float, float, float, float, float]]:
        """[Helper] 统一提取并转换数值"""
        try:
            curr_panels = float(raw_curr_p)
            prev_panels = float(raw_prev_p)
            if curr_panels == 0 or prev_panels == 0: return None
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
    #  核心逻辑：全量序列化递归 (Sequential Recursive Logic)
    # ==========================================================================
    @staticmethod
    def _apply_sequential_regulation_group(df: pd.DataFrame, freq_name: str):
        """对 Group 级宽表进行全量递归调节"""
        if df.empty or len(df) < 2: return
        
        # 排除非数据列
        exclude_cols = ['total_panels', 'warehousing_time', 'time_period', 'month_period', 'week_period']
        target_groups = [c for c in df.columns if c not in exclude_cols]
        
        for group in target_groups:
            # 维护一个“上一次有效良率”的状态变量
            # 初始化为 None，表示序列的起点
            prev_valid_rate = None
            
            # 按时间顺序遍历每一行 (Full Replay)
            for idx in df.index:
                try:
                    curr_total = float(df.loc[idx, 'total_panels']) # type: ignore
                    if curr_total == 0: continue
                    
                    curr_count = float(df.loc[idx, group]) # type: ignore
                    curr_rate = curr_count / curr_total
                    
                    # 第一天没有“前值”，直接作为基准
                    if prev_valid_rate is None:
                        prev_valid_rate = curr_rate
                        continue
                    
                    # --- 核心递归逻辑 ---
                    # 使用“当前值”与“上一次修正后的值(prev_valid_rate)”进行比对
                    target_count = TrendRegulator._calculate_regulated_target(
                        curr_rate, prev_valid_rate, curr_total, curr_count
                    )
                    
                    if target_count is not None:
                        # [Hit] 发现异常，应用调节
                        df.loc[idx, group] = target_count
                        
                        # [关键点] 更新 prev_valid_rate 为“调节后的良率”
                        # 这样下一次循环时，就是基于这个平滑后的值进行比较
                        prev_valid_rate = target_count / curr_total
                        
                        # 仅在最后两个周期打印日志，避免全量刷屏
                        if idx == df.index[-1] or idx == df.index[-2]:
                            logging.warning(f"[智能调节-Group{freq_name}] {group}: {curr_rate:.2%} -> {prev_valid_rate:.2%}")
                    else:
                        # [Miss] 正常波动，更新前值为当前真实值
                        prev_valid_rate = curr_rate
                        
                except Exception:
                    continue

    @staticmethod
    def _apply_sequential_regulation_code(df: pd.DataFrame, freq_name: str):
        """对 Code 级长表进行全量递归调节"""
        if df.empty: return
        
        unique_codes = df['defect_desc'].unique()
        
        for code in unique_codes:
            if code == 'NoDefect': continue
            
            # 必须先按时间排序，确保递归顺序正确
            mask = df['defect_desc'] == code
            # 注意：这里我们只获取索引的顺序
            sorted_indices = df[mask].sort_values('warehousing_time').index
            
            if len(sorted_indices) < 2: continue
            
            prev_valid_rate = None
            
            for idx in sorted_indices:
                try:
                    curr_total = float(df.loc[idx, 'total_panels']) # type: ignore
                    if curr_total == 0: continue
                    
                    curr_count = float(df.loc[idx, 'defect_panel_count']) # type: ignore
                    curr_rate = curr_count / curr_total
                    
                    if prev_valid_rate is None:
                        prev_valid_rate = curr_rate
                        continue
                    
                    target_count = TrendRegulator._calculate_regulated_target(
                        curr_rate, prev_valid_rate, curr_total, curr_count
                    )
                    
                    if target_count is not None:
                        df.loc[idx, 'defect_panel_count'] = target_count
                        prev_valid_rate = target_count / curr_total
                        
                        # 简单判断是否是最近的数据才打日志
                        if idx == sorted_indices[-1]:
                             logging.info(f"[智能调节-Code{freq_name}] {code}: {curr_rate:.2%} -> {prev_valid_rate:.2%}")
                    else:
                        prev_valid_rate = curr_rate
                        
                except Exception:
                    continue

    # ==========================================================================
    #  Main Entry Points
    # ==========================================================================
    @staticmethod
    def regulate_monthly_and_weekly(
        monthly_df: pd.DataFrame, 
        weekly_df: pd.DataFrame,
        config: AppConfig,
        resource_dir: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Group 级智能调节 - 使用全量递归算法
        """
        logging.info("启动智能趋势调节器 (Sequential Recursive Mode)...")

        # 1. 复制数据，避免污染源数据
        monthly_regulated = monthly_df.copy() if not monthly_df.empty else pd.DataFrame()
        weekly_regulated = weekly_df.copy() if not weekly_df.empty else pd.DataFrame()

        # 2. 执行全量递归调节
        # 无论历史有多长，都从头算一遍，保证 W03 永远是基于 W02 算出来的
        if not monthly_regulated.empty:
            TrendRegulator._apply_sequential_regulation_group(monthly_regulated, "月度")
            
        if not weekly_regulated.empty:
            TrendRegulator._apply_sequential_regulation_group(weekly_regulated, "周度")

        return monthly_regulated, weekly_regulated

    @staticmethod
    def regulate_code_monthly_and_weekly(
        monthly_df: pd.DataFrame, 
        weekly_df: pd.DataFrame,
        config: AppConfig,
        resource_dir: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Code 级智能调节 - 使用全量递归算法
        """
        logging.info("启动 Code 级智能趋势调节器 (Sequential Recursive Mode)...")

        monthly_regulated = monthly_df.copy() if not monthly_df.empty else pd.DataFrame()
        weekly_regulated = weekly_df.copy() if not weekly_df.empty else pd.DataFrame()

        if not monthly_regulated.empty:
            TrendRegulator._apply_sequential_regulation_code(monthly_regulated, "月度")
            
        if not weekly_regulated.empty:
            TrendRegulator._apply_sequential_regulation_code(weekly_regulated, "周度")

        return monthly_regulated, weekly_regulated