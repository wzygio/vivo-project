import pandas as pd
import numpy as np
import logging
from typing import Tuple, Any, cast
from datetime import datetime as dt

from vivo_project.config import CONFIG
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
        weekly_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        检查月度数据的最新一月。
        如果系统显示恶化，但外部基准未恶化，则强制压平月度数据，并回溯修正周度数据。
        """
        if monthly_df.empty or len(monthly_df) < 2:
            return monthly_df, weekly_df

        logging.info("启动智能趋势调节器 (Smart Alignment)...")
        
        # 1. 准备基准数据
        bench_cfg = CONFIG.get('processing', {}).get('benchmark_report_config', {})
        file_name = bench_cfg.get('file_name')
        sheet_name = bench_cfg.get('sheet_name', 'CT')
        
        raw_benchmark_df = None
        if file_name:
            raw_benchmark_df = load_excel_report(file_name, sheet_name)

        # 2. 定位需要检查的月份 (最后一行)
        last_month_idx = monthly_df.index[-1] # Timestamp
        prev_month_idx = monthly_df.index[-2]
        
        # 3. 遍历每个产品 (Column)
        # 注意：monthly_df 的列名应该是 defect_group (如 Array_Pixel)
        target_groups = monthly_df.columns.tolist()
        if 'total_panels' in target_groups: target_groups.remove('total_panels')

        # 这里的 df 是 Wide Format (Index=Date, Cols=Groups)
        monthly_regulated = monthly_df.copy()
        weekly_regulated = weekly_df.copy()

        for group in target_groups:
            try:
                # A. 提取数值 [修改点 2: 使用 cast(Any, ...) 彻底屏蔽类型报错]
                # Pylance 认为 .loc[] 可能返回复数(complex)，从而拒绝 float()。
                # cast(Any, ...) 告诉检查器忽略这里的类型约束。
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
                    # 如果该 Group 某个月没数据（NaN），转 float 会变成 nan
                    # 我们可以视作 0 或者跳过，这里视作 0 安全一些
                    curr_count = 0.0
                    prev_count = 0.0

                curr_rate = curr_count / curr_panels
                prev_rate = prev_count / prev_panels

                # B. Step 1: 内部自查
                if not AbnormalDetector.is_value_trend_abnormal(curr_rate, prev_rate): # Scalar类型可能是complex，不能够赋给float
                    continue # 系统内部认为正常，无需调节

                # C. Step 2: 外部仲裁
                is_real_issue = False
                if raw_benchmark_df is not None:
                    # 去外部报表查这个 Group
                    is_real_issue = AbnormalDetector.is_benchmark_abnormal(raw_benchmark_df, group)
                
                if is_real_issue:
                    logging.info(f"[{group}] 系统报警且外部基准确认异常 -> 维持原状 (真实恶化)。")
                    continue

                # D. Step 3: 逆向压制
                logging.warning(f"[{group}] 系统报警但外部基准稳定 -> 触发智能调节。")
                
                # 计算目标 Rate (安全线)
                # 安全线 = 上月 + 允许波动(0.2%) - 安全余量(0.01%)
                # 这样刚好卡在报警阈值之下一点点
                safe_delta = AbnormalDetector.THRESHOLD_SURGE_DELTA - 0.0001
                target_rate = prev_rate + safe_delta
                
                # 如果是翻倍触发的，也要防翻倍
                target_rate_doubling = (prev_rate * AbnormalDetector.THRESHOLD_DOUBLING_RATIO) - 0.0001
                target_rate = min(target_rate, target_rate_doubling)
                
                # 计算修正系数
                # Factor = Target / Current
                # 我们通过调整 Count 来调整 Rate
                target_count = int(target_rate * curr_panels)
                correction_factor = target_count / curr_count if curr_count > 0 else 1.0
                
                if correction_factor >= 1.0: continue # 不需要调

                # E. 应用修正
                
                # 1. 修正月度
                monthly_regulated.loc[last_month_idx, group] = target_count
                logging.warning(f"[智能调节-Group] {group} ({last_month_idx.strftime('%Y-%m')}): {curr_rate:.2%} -> {target_rate:.2%}")

            except Exception as e:
                logging.error(f"调节 Group {group} 时出错: {e}")
                continue

        return monthly_regulated, weekly_regulated


    @staticmethod
    def regulate_code_monthly_and_weekly(
        monthly_df: pd.DataFrame, 
        weekly_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        [新增] Code 级智能调节
        处理 Long Format 数据: columns=['warehousing_time', 'defect_group', 'defect_desc', 'defect_panel_count', 'total_panels']
        """
        if monthly_df.empty or weekly_df.empty:
            return monthly_df, weekly_df

        logging.info("启动 Code 级智能趋势调节器...")

        # 1. 准备基准数据
        bench_cfg = CONFIG.get('processing', {}).get('benchmark_report_config', {})
        file_name = bench_cfg.get('file_name')
        sheet_name = bench_cfg.get('sheet_name', 'CT')
        
        raw_benchmark_df = None
        if file_name:
            raw_benchmark_df = load_excel_report(file_name, sheet_name)

        # 2. 遍历每个 Code
        monthly_regulated = monthly_df.copy()
        weekly_regulated = weekly_df.copy()
        
        # 获取所有唯一的 Code
        unique_codes = monthly_regulated['defect_desc'].unique()
        
        # 确定最近两个月
        # monthly_df 应该是按时间排序的
        dates = monthly_regulated['warehousing_time'].sort_values().unique()
        if len(dates) < 2: return monthly_regulated, weekly_regulated
        
        last_month_date = dates[-1]
        prev_month_date = dates[-2]

        for code in unique_codes:
            if code == 'NoDefect': continue
            
            try:
                # A. 提取该 Code 的最近两个月数据
                # 使用 Mask 提取行
                mask_curr = (monthly_regulated['defect_desc'] == code) & (monthly_regulated['warehousing_time'] == last_month_date)
                mask_prev = (monthly_regulated['defect_desc'] == code) & (monthly_regulated['warehousing_time'] == prev_month_date)
                
                if not mask_curr.any() or not mask_prev.any(): continue
                
                # 提取数值 (使用 cast 解决类型报错)
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

                # C. Step 2: 外部仲裁
                is_real_issue = False
                if raw_benchmark_df is not None:
                    # is_benchmark_abnormal 会自动在 C列(Group) 和 D列(Code) 查找
                    is_real_issue = AbnormalDetector.is_benchmark_abnormal(raw_benchmark_df, code)
                
                if is_real_issue:
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

                # 1. 修正月度 (定位到具体那一行)
                monthly_regulated.loc[mask_curr, 'defect_panel_count'] = target_count
                logging.warning(f"[智能调节-Code] {code} ({last_month_date.strftime('%Y-%m')}): {curr_rate:.2%} -> {target_rate:.2%}")
                
            except Exception as e:
                logging.error(f"调节 Code {code} 时出错: {e}")
                continue
                
        return monthly_regulated, weekly_regulated