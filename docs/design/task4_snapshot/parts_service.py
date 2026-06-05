# src/equipment_domain/application/parts_service.py
"""
[应用服务层] 关键备件报表服务。

流水线: 加载 → 匹配 → 补全 → 钳制 → 计算进度
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.equipment_domain.infrastructure.data_loader import (
    load_spec_baseline,
    load_part_life_snapshot,
)
from src.equipment_domain.core.parts_matcher import build_and_match_all
from src.equipment_domain.core.data_completer import fill_missing_by_pairing, clamp_over_spec
from src.equipment_domain.core.parts_calculator import batch_calculate_progress_and_status

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class PartsReportViewModel:
    """关键备件报表视图模型"""

    report_df: pd.DataFrame
    total_count: int
    real_count: int           # 真实数据条数
    simulated_count: int      # 模拟补全条数
    over_count: int
    warning_count: int
    normal_count: int
    last_update: str


class PartsReportService:
    """关键备件报表服务"""

    @staticmethod
    @st.cache_data
    def get_report_data(
        _db_manager,
        baseline_path: str,
        snapshot_signature: str,
    ) -> PartsReportViewModel:
        """
        流水线: 加载 → 匹配 → 补全 → 钳制 → 计算进度
        """
        # 1-2. 加载 + 匹配
        spec_df = load_spec_baseline(baseline_path)
        snapshot_df = load_part_life_snapshot(_db_manager, spec_df)
        report_df = build_and_match_all(spec_df, snapshot_df)

        # 3. 补全缺失值
        report_df = fill_missing_by_pairing(report_df)

        # 4. 超规钳制
        report_df = clamp_over_spec(report_df)

        # 5. 计算进度 + 预警状态
        report_df = batch_calculate_progress_and_status(report_df)

        # 6. 统计
        real_count = int((report_df.get("数据来源", "") == "真实").sum())
        simulated_count = int((report_df.get("数据来源", "").str.contains("模拟", na=False)).sum())
        over_count = int((report_df["预警状态"] == "超规").sum())
        warning_count = int((report_df["预警状态"] == "预警").sum())
        normal_count = int((report_df["预警状态"] == "正常").sum())

        valid_times = report_df["测量时间"].dropna()
        if not valid_times.empty:
            parsed = pd.to_datetime(valid_times, errors="coerce").dropna()
            last_update = str(parsed.max()) if not parsed.empty else ""
        else:
            last_update = ""

        return PartsReportViewModel(
            report_df=report_df,
            total_count=len(report_df),
            real_count=real_count,
            simulated_count=simulated_count,
            over_count=over_count,
            warning_count=warning_count,
            normal_count=normal_count,
            last_update=last_update,
        )