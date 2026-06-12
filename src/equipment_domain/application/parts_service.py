# src/equipment_domain/application/parts_service.py
"""
[应用服务层] 关键备件报表服务。

职责:
1. 加载 CSV 规格基线 + 数据库快照数据
2. 委托 core 层进行批量匹配、进度计算、预警判定
3. 通过 @st.cache_data 实现 L2 缓存
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.equipment_domain.infrastructure.data_loader import (
    load_spec_baseline,
    load_part_life_snapshot,
    PartsRepository,
)
from src.equipment_domain.core.parts_matcher import build_and_match_all
from src.equipment_domain.core.parts_calculator import (
    apply_over_spec_alert_and_decoration,
    batch_calculate_progress_and_status,
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

logger = logging.getLogger(__name__)


# ==============================================================================
#  视图模型
# ==============================================================================


@dataclass
class PartsReportViewModel:
    """关键备件报表视图模型"""

    report_df: pd.DataFrame  # 最终报表 DataFrame
    total_count: int         # 总备件条数
    over_count: int          # 超规条数 (>100%)
    warning_count: int       # 预警条数 (>90%)
    normal_count: int        # 正常条数
    last_update: str         # 数据最后更新时间


# ==============================================================================
#  主服务
# ==============================================================================


class PartsReportService:
    """关键备件报表服务"""

    @staticmethod
    def safe_refresh_snapshots(
        _db_manager: "DatabaseManager",
        baseline_path: str,
    ) -> bool:
        """刷新关键备件 L1 Parquet 快照，不触碰 Streamlit L2 缓存。"""
        try:
            spec_df = load_spec_baseline(baseline_path)
            repo = PartsRepository(_db_manager, spec_df)
            snapshot_df = repo.get_snapshot(force_refresh=True)
            return not snapshot_df.empty
        except Exception as e:
            logger.error(f"关键备件快照刷新失败: {e}", exc_info=True)
            return False

    @staticmethod
    @st.cache_data  # L2 缓存（遵循项目红线纪律，不可移除）
    def get_report_data(
        _db_manager,
        baseline_path: str,
        snapshot_signature: str,
    ) -> PartsReportViewModel:
        """
        获取完整的关键备件报表数据。

        输出列（13 + 1 调试列）:
        厂别, 备件类型, 设备类型, 膜层, 制程, 寿命规格,
        站点, 机台号-腔室, 参数名称, 测量值, 测量时间,
        使用进度, 预警状态, [匹配参数名]

        Args:
            _db_manager: 数据库管理器实例（不参与缓存哈希）
            baseline_path: CSV 基线配置文件路径
            snapshot_signature: 缓存键失效信号

        Returns:
            PartsReportViewModel
        """
        # 1. 加载基线 CSV
        spec_df = load_spec_baseline(baseline_path)

        # 2. 查询数据库快照数据
        snapshot_df = load_part_life_snapshot(_db_manager, spec_df)

        # 3. 批量匹配（使用预建索引，O(n) 而非 O(n*m)）
        report_df = build_and_match_all(spec_df, snapshot_df)

        # 4. 先保留原始超规判断，再对展示测量值做修饰
        report_df = apply_over_spec_alert_and_decoration(
            report_df,
            group_cols=["厂别", "备件类型", "设备类型", "膜层", "制程", "寿命规格"],
        )

        # 5. 计算使用进度和预警状态
        report_df = batch_calculate_progress_and_status(report_df)

        # 6. 统计信息
        over_count = int((report_df["预警状态"] == "超规").sum())
        warning_count = int((report_df["预警状态"] == "预警").sum())
        normal_count = int((report_df["预警状态"] == "正常").sum())

        # 最后更新时间
        valid_times = report_df["测量时间"].dropna()
        if not valid_times.empty:
            parsed_times = pd.to_datetime(valid_times, errors="coerce")
            valid_parsed = parsed_times.dropna()
            last_update = str(valid_parsed.max()) if not valid_parsed.empty else ""
        else:
            last_update = ""

        return PartsReportViewModel(
            report_df=report_df,
            total_count=len(report_df),
            over_count=over_count,
            warning_count=warning_count,
            normal_count=normal_count,
            last_update=last_update,
        )
