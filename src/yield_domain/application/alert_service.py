import logging
from typing import Dict, List, Any
import pandas as pd
from pathlib import Path

# [Refactor] 引入配置模型，移除全局 CONFIG
from src.shared_kernel.config_model import AppConfig
from yield_domain.infrastructure.data_loader import load_excel_report
from yield_domain.core.abnormal_detector import AbnormalDetector

class AlertService:
    @staticmethod
    def get_dashboard_alerts(
        mwd_group_data: Dict[str, pd.DataFrame],
        mwd_code_data: Dict[str, pd.DataFrame],
        config: AppConfig,        # [Inject] 注入配置对象
        product_dir: Path        # [Inject] 注入资源目录
    ) -> List[str]:
        """
        获取所有看板相关的预警信息（系统计算 + 真实报表比对）。
        [V2.0] 增加目标 Group 过滤，只分析配置中关注的三大类。
        """
        all_alerts = []
        
        # 0. 获取配置的目标 Group
        # [Refactor] 使用对象属性访问
        target_defect_groups = config.data_source.target_defect_groups or []
        
        # 1. 获取系统计算数据的趋势预警
        # ----------------------------------------------------
        group_monthly = mwd_group_data.get('monthly')
        code_monthly = mwd_code_data.get('monthly')
        
        # [关键修改] 数据清洗：只保留目标 Group 的数据
        
        # A. 清洗 Group 数据
        valid_group_monthly = pd.DataFrame()
        if group_monthly is not None and not group_monthly.empty:
            valid_group_monthly = group_monthly[
                group_monthly['defect_group'].isin(target_defect_groups)
            ]

        # B. 清洗 Code 数据
        valid_code_monthly = pd.DataFrame()
        if code_monthly is not None and not code_monthly.empty:
            valid_code_monthly = code_monthly[
                code_monthly['defect_group'].isin(target_defect_groups)
            ]
        
        # 调用检测器
        system_alerts = AbnormalDetector.detect_system_trend_alerts(
            valid_group_monthly, 
            valid_code_monthly
        )
        all_alerts.extend(system_alerts)
        
        # --- 2. [新增] 周度趋势预警 ---
        group_weekly = mwd_group_data.get('weekly')
        code_weekly = mwd_code_data.get('weekly')

        # A. 清洗 Group 周度数据
        valid_group_weekly = pd.DataFrame()
        if group_weekly is not None and not group_weekly.empty:
            valid_group_weekly = group_weekly[
                group_weekly['defect_group'].isin(target_defect_groups)
            ]
            
        # B. 清洗 Code 周度数据
        valid_code_weekly = pd.DataFrame()
        if code_weekly is not None and not code_weekly.empty:
            valid_code_weekly = code_weekly[
                code_weekly['defect_group'].isin(target_defect_groups)
            ]

        # C. 调用检测器 (复用 detect_system_trend_alerts 逻辑，它也适用于周度数据的结构)
        # 注意：检测器内部生成的文案可能不包含“周度”字样，取决于 time_period 格式(如 2026-W05)
        # 如果需要区分，用户可以通过时间格式辨别，或者您可以扩展 detect_system_trend_alerts 的参数
        system_alerts_weekly = AbnormalDetector.detect_system_trend_alerts(
            valid_group_weekly,
            valid_code_weekly
        )
        all_alerts.extend(system_alerts_weekly)

        # 3. 获取外部基准报表的批次预警
        # ----------------------------------------------------
        # [Refactor] 从 config.processing 获取字典
        bench_cfg = config.processing.get('benchmark_report_config', {})
        file_name = bench_cfg.get('file_name')
        sheet_name = bench_cfg.get('sheet_name', 'CT')
        
        if file_name:
            # A. 加载外部文件
            # [Refactor] 显式构建完整路径
            file_path = product_dir / file_name
            raw_report_df = load_excel_report(file_path, sheet_name)
            
            if raw_report_df is not None:
                # B. 提取目标列表 (Context)
                target_groups_context = target_defect_groups
                
                # Code: 使用清洗后的 valid_code_monthly 中的 Code 列表
                target_codes_context = []
                if not valid_code_monthly.empty:
                    target_codes_context = valid_code_monthly['defect_desc'].unique().tolist()
                
                # C. 调用 Core 逻辑进行比对
                benchmark_alerts = AbnormalDetector.detect_benchmark_batch_alerts(
                    raw_report_df, 
                    target_groups_context, 
                    target_codes_context
                )
                all_alerts.extend(benchmark_alerts)
        
        return all_alerts