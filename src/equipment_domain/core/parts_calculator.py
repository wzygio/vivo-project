# src/equipment_domain/core/parts_calculator.py
"""
[核心业务逻辑] 关键备件使用进度与预警状态计算。

职责:
1. 计算单个备件的使用进度百分比
2. 批量计算 DataFrame 的使用进度列
3. 根据进度和预警阈值判定预警状态
4. 纯业务逻辑，不依赖外部 I/O
"""

from typing import Optional

import numpy as np
import pandas as pd


STATUS_WARNING = "⚠️ 超预警"
STATUS_NORMAL = "✅ 正常"


def calculate_usage_progress(
    actual_value: Optional[float],
    spec_limit: Optional[float],
) -> float:
    """
    计算单个备件的使用进度百分比。

    Args:
        actual_value: 实际测量值
        spec_limit: 寿命规格

    Returns:
        float: 使用进度百分比 (0.0 ~ 100.0)，无效输入返回 0.0
    """
    if actual_value is None or spec_limit is None:
        return 0.0
    if spec_limit <= 0:
        return 0.0
    progress = (actual_value / spec_limit) * 100.0
    return min(max(progress, 0.0), 100.0)


def calculate_warning_status(
    usage_progress: float,
    warning_threshold: float,
) -> str:
    """
    根据使用进度和预警阈值判定预警状态。

    Args:
        usage_progress: 使用进度百分比
        warning_threshold: 预警阈值百分比

    Returns:
        str: STATUS_WARNING 或 STATUS_NORMAL
    """
    if usage_progress >= warning_threshold:
        return STATUS_WARNING
    return STATUS_NORMAL


def batch_calculate_progress_and_status(report_df: pd.DataFrame) -> pd.DataFrame:
    """
    批量计算 DataFrame 中所有行的使用进度和预警状态。

    要求 DataFrame 包含以下列:
    - 实际数据: 实际测量值
    - 寿命规格: 备件额定寿命
    - 预警值: 触发预警的使用进度百分比阈值

    将添加/覆盖以下列:
    - 使用进度: 使用进度百分比 (0.0 ~ 100.0)
    - 预警状态: STATUS_WARNING 或 STATUS_NORMAL

    Args:
        report_df: 包含备件数据的 DataFrame

    Returns:
        pd.DataFrame: 添加了计算列的原 DataFrame（原地修改）
    """
    report_df["使用进度"] = (
        report_df["实际数据"] / report_df["寿命规格"] * 100
    )
    report_df["使用进度"] = report_df["使用进度"].clip(upper=100.0)
    report_df["使用进度"] = report_df["使用进度"].fillna(0.0)

    report_df["预警状态"] = np.where(
        report_df["使用进度"] >= report_df["预警值"],
        STATUS_WARNING,
        STATUS_NORMAL,
    )

    return report_df
