from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from src.yield_domain.infrastructure.data_loader import load_excel_report
from src.yield_domain.core.abnormal_detector import AbnormalDetector
from src.yield_domain.application.alert_results import TREND_ALERT_SCOPES


class AlertService:
    @staticmethod
    def get_dashboard_alert_history(
        mwd_group_data: Dict[str, pd.DataFrame], mwd_code_data: Dict[str, pd.DataFrame],
    ) -> List[Dict[str, Any]]:
        """Publish enabled ISO-week periods using the existing pairwise detector.

        Current markers preserve the dashboard's latest-per-series semantics.
        No baseline is invented for a series with fewer than two observations.
        """
        def identity(record):
            return tuple("" if pd.isna(record.get(key)) else str(record[key]) for key in
                         ("level", "defect_group", "defect_desc", "time_period", "period_scope"))

        current = {identity(record) for record in AlertService.get_dashboard_alert_records(
            mwd_group_data, mwd_code_data)}
        history = []
        for scope in TREND_ALERT_SCOPES:
            group = mwd_group_data.get(f"{scope}_full", mwd_group_data.get(scope))
            code = mwd_code_data.get(f"{scope}_full", mwd_code_data.get(scope))
            frames = [frame for frame in (group, code) if frame is not None and not frame.empty]
            periods = sorted({str(value) for frame in frames for value in frame["time_period"].dropna()})
            for period in periods:
                prefixes = [frame.loc[frame["time_period"].astype(str).le(period)]
                            if frame is not None and not frame.empty else pd.DataFrame()
                            for frame in (group, code)]
                for record in AbnormalDetector.detect_system_trend_records(*prefixes):
                    if str(record["time_period"]) != period:
                        continue
                    record = {**record, "period_scope": scope}
                    history.append({**record, "is_current": identity(record) in current})
        return history

    @staticmethod
    def get_dashboard_alerts(
        mwd_group_data: Dict[str, pd.DataFrame],
        mwd_code_data: Dict[str, pd.DataFrame],
        product_dir: Path,
        benchmark_report_config: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        获取所有看板相关的预警信息（系统计算 + 真实报表比对）。

        输入趋势数据已经由底层 Yield 数据策略统一限定，本层不再读取配置或二次过滤。
        """
        all_alerts = []
        
        # 月度数据仍供图表及基准清单使用，趋势预警只检测已启用的 ISO 周。
        for scope in TREND_ALERT_SCOPES:
            group = mwd_group_data.get(scope)
            code = mwd_code_data.get(scope)
            all_alerts.extend(AbnormalDetector.detect_system_trend_alerts(
                group if group is not None else pd.DataFrame(),
                code if code is not None else pd.DataFrame(),
                period_scope=scope,
            ))

        # 3. 获取外部基准报表的批次预警
        # ----------------------------------------------------
        bench_cfg = benchmark_report_config or {}
        file_name = bench_cfg.get('file_name')
        sheet_name = bench_cfg.get('sheet_name', 'CT')
        
        if file_name:
            # A. 加载外部文件
            # [Refactor] 显式构建完整路径
            file_path = product_dir / file_name
            raw_report_df = load_excel_report(file_path, sheet_name)
            
            if raw_report_df is not None:
                group_monthly = mwd_group_data.get('monthly')
                code_monthly = mwd_code_data.get('monthly')
                valid_group_monthly = group_monthly if group_monthly is not None else pd.DataFrame()
                valid_code_monthly = code_monthly if code_monthly is not None else pd.DataFrame()
                # B. 提取目标列表 (Context)
                target_groups_context = []
                if not valid_group_monthly.empty:
                    target_groups_context = list(
                        dict.fromkeys(
                            valid_group_monthly['defect_group'].dropna().tolist()
                        )
                    )
                
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

    @staticmethod
    def get_dashboard_alert_records(
        mwd_group_data: Dict[str, pd.DataFrame],
        mwd_code_data: Dict[str, pd.DataFrame],
    ) -> List[Dict[str, Any]]:
        """
        获取结构化 ISO 周趋势预警记录，用于按 Defect Code 自动出图。

        判定口径与 get_dashboard_alerts 完全一致；每条记录额外带 period_scope
        （"weekly"）标识来源周期。基准报表比对的文本预警不在此返回。
        """
        records: List[Dict[str, Any]] = []
        for scope in TREND_ALERT_SCOPES:
            group_df = mwd_group_data.get(scope)
            code_df = mwd_code_data.get(scope)
            scope_records = AbnormalDetector.detect_system_trend_records(
                group_df if group_df is not None else pd.DataFrame(),
                code_df if code_df is not None else pd.DataFrame(),
            )
            for record in scope_records:
                record["period_scope"] = scope
            records.extend(scope_records)
        return records
