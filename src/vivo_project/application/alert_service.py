import logging
from typing import Dict, List, Any
import pandas as pd
from pathlib import Path

# [Refactor] 引入配置模型，移除全局 CONFIG
from vivo_project.config_model import AppConfig
from vivo_project.infrastructure.data_loader import load_excel_report
from vivo_project.core.abnormal_detector import AbnormalDetector

class AlertService:
    @staticmethod
    def get_dashboard_alerts(
        mwd_group_data: Dict[str, pd.DataFrame],
        mwd_code_data: Dict[str, pd.DataFrame],
        config: AppConfig,        # [Inject] 注入配置对象
        resource_dir: Path        # [Inject] 注入资源目录
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
        
        # 2. 获取外部基准报表的批次预警
        # ----------------------------------------------------
        # [Refactor] 从 config.processing 获取字典
        bench_cfg = config.processing.get('benchmark_report_config', {})
        file_name = bench_cfg.get('file_name')
        sheet_name = bench_cfg.get('sheet_name', 'CT')
        
        if file_name:
            # A. 加载外部文件
            # [Refactor] 显式构建完整路径
            file_path = resource_dir / file_name
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