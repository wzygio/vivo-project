# src/equipment_domain/application/parts_service.py
"""
[应用服务层] 关键备件报表服务。

职责:
1. 加载 CSV 规格基线 + 数据库最新值
2. 基于子串匹配（机台+腔室+备件类型）建立关联
3. 计算使用进度百分比和预警状态
4. 通过 @st.cache_data 实现 L2 缓存
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd
import streamlit as st

from src.equipment_domain.infrastructure.data_loader import (
    load_spec_baseline,
    load_latest_part_life,
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
    total_count: int  # 总备件条数
    warning_count: int  # 超预警条数
    last_update: str  # 数据最后更新时间


# ==============================================================================
#  内部匹配算法
# ==============================================================================


def _find_matching_db_record(
    spec_row: pd.Series,
    db_df: pd.DataFrame,
) -> Optional[pd.Series]:
    """
    对一条规格行，在 DB 结果中查找匹配的最近记录。

    匹配逻辑（AND 条件）:
    1. 机台匹配: spec.机台 是 db.sub_equip_id 的子串（大小写不敏感）
    2. 腔室匹配: spec.腔室 是 db.sub_equip_id 的子串
       - 若腔室含 '/' (如 P3/P4)，拆分为 ['P3','P4'] 任一匹配即可
    3. 备件类型匹配: spec.备件类型 是 db.param_name 的子串

    Args:
        spec_row: 一条基线规格行 (包含 机台, 腔室, 备件类型 列)
        db_df: 全部 DB 记录 DataFrame

    Returns:
        匹配到的 DB 记录 (Series, 取 glass_start_time 最新的那条)
        若无匹配返回 None
    """
    if db_df.empty:
        return None

    machine = str(spec_row.get("机台", "")).strip().upper()
    chambers_raw = str(spec_row.get("腔室", "")).strip()
    chambers = [ch.strip().upper() for ch in chambers_raw.split("/") if ch.strip()]
    part_type = str(spec_row.get("备件类型", "")).strip().upper()

    if not machine or not chambers or not part_type:
        return None

    def _matches(row: pd.Series) -> bool:
        sub_id = str(row.get("sub_equip_id", "")).upper()
        p_name = str(row.get("param_name", "")).upper()

        # 1. 机台匹配
        if machine not in sub_id:
            return False

        # 2. 腔室匹配（任一腔室标识匹配即可）
        if not any(ch in sub_id for ch in chambers):
            return False

        # 3. 备件类型匹配
        if part_type not in p_name:
            return False

        return True

    matched = db_df[db_df.apply(_matches, axis=1)]

    if matched.empty:
        return None

    # 取 glass_start_time 最新的那条
    valid_time_mask = matched["glass_start_time"].notna()
    if valid_time_mask.any():
        return matched.loc[matched["glass_start_time"].idxmax()]
    else:
        # 如果没有有效时间，取第一条
        return matched.iloc[0]


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

        # 3. 逐行匹配合并
        matched_rows: list[dict] = []

        for _, spec_row in spec_df.iterrows():
            matched_db = _find_matching_db_record(spec_row, latest_df)

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

        # 4. 计算使用进度和预警状态
        report_df["使用进度"] = (
            report_df["实际数据"] / report_df["寿命规格"] * 100
        )
        # 上限 clip 到 100%（避免进度条溢出）
        report_df["使用进度"] = report_df["使用进度"].clip(upper=100.0)
        # 无数据时使用进度为 0
        report_df["使用进度"] = report_df["使用进度"].fillna(0.0)

        report_df["预警状态"] = np.where(
            report_df["使用进度"] >= report_df["预警值"],
            "⚠️ 超预警",
            "✅ 正常",
        )

        # 5. 统计信息
        warning_count = int((report_df["预警状态"] == "⚠️ 超预警").sum())

        # 最后更新时间
        valid_times = report_df["测量时间"].dropna()
        if not valid_times.empty:
            # 解析时间并取最大值
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
