# src/equipment_domain/application/parts_service.py
"""
[应用服务层] 关键备件报表服务。

职责:
1. 加载 CSV 规格基线 + 数据库最新值
2. 委托 core 层进行匹配、进度计算、预警判定
3. 通过 @st.cache_data 实现 L2 缓存
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import pandas as pd
import streamlit as st

from src.equipment_domain.infrastructure.data_loader import (
    load_spec_baseline,
    load_latest_part_life,
)
from src.equipment_domain.core.parts_matcher import find_matching_db_record
from src.equipment_domain.core.parts_calculator import batch_calculate_progress_and_status

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
    total_count: int  # 总备件条数
    warning_count: int  # 超预警条数
    last_update: str  # 数据最后更新时间


# ==============================================================================
#  主服务
# ==============================================================================


class PartsReportService:
    """关键备件报表服务"""

    @staticmethod
    @st.cache_data  # L2 缓存（遵循项目红线纪律，不可移除）
    def get_report_data(
        _db_manager,
        baseline_path: str,
        snapshot_signature: str,
    ) -> PartsReportViewModel:
        """
        获取完整的关键备件报表数据。

        [缓存策略]
        - 使用 @st.cache_data 装饰器实现 L2 缓存
        - _db_manager 以下划线开头，不参与缓存哈希计算
        - snapshot_signature 作为缓存键，CSV 文件 mtime 变化时自动失效

        Args:
            _db_manager: 数据库管理器实例（不参与缓存哈希）
            baseline_path: CSV 基线配置文件路径
            snapshot_signature: 缓存键失效信号（CSV 文件 mtime）

        Returns:
            PartsReportViewModel: 备件报表视图模型
        """
        # 1. 加载基线 CSV
        spec_df = load_spec_baseline(baseline_path)

        # 2. 查询数据库最新值
        latest_df = load_latest_part_life(_db_manager)

        # 3. 逐行匹配合并（委托 core 层匹配算法）
        matched_rows: list[dict] = []

        for _, spec_row in spec_df.iterrows():
            matched_db = find_matching_db_record(spec_row, latest_df)

            actual_value: Optional[float] = None
            measure_time: Optional[str] = None
            param_name: Optional[str] = None
            step_id: Optional[str] = None
            sub_equip_id: Optional[str] = None

            if matched_db is not None:
                actual_value = matched_db.get("value")
                measure_time_obj = matched_db.get("glass_start_time")
                if pd.notna(measure_time_obj):
                    measure_time = str(measure_time_obj)
                param_name = matched_db.get("param_name")
                step_id = matched_db.get("step_id")
                sub_equip_id = matched_db.get("sub_equip_id")

            matched_rows.append(
                {
                    "厂别": spec_row.get("厂别"),
                    "膜层": spec_row.get("膜层"),
                    "制程": spec_row.get("制程"),
                    "机台": spec_row.get("机台"),
                    "腔室": spec_row.get("腔室"),
                    "备件类型": spec_row.get("备件类型"),
                    "寿命规格": spec_row.get("寿命规格"),
                    "预警值": spec_row.get("预警值"),
                    "实际数据": actual_value,
                    "测量时间": measure_time,
                    "参数名称": param_name,
                    "站点": step_id,
                    "机台编号": sub_equip_id,
                }
            )

        report_df = pd.DataFrame(matched_rows)

        # 4. 计算使用进度和预警状态（委托 core 层计算逻辑）
        report_df = batch_calculate_progress_and_status(report_df)

        # 5. 统计信息
        warning_count = int((report_df["预警状态"] == "⚠️ 超预警").sum())

        # 最后更新时间
        valid_times = report_df["测量时间"].dropna()
        if not valid_times.empty:
            parsed_times = pd.to_datetime(valid_times, errors="coerce")
            valid_parsed = parsed_times.dropna()
            if not valid_parsed.empty:
                last_update = str(valid_parsed.max())
            else:
                last_update = ""
        else:
            last_update = ""

        return PartsReportViewModel(
            report_df=report_df,
            total_count=len(report_df),
            warning_count=warning_count,
            last_update=last_update,
        )
