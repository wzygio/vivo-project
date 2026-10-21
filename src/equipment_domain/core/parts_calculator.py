# src/equipment_domain/core/parts_calculator.py
"""
[核心业务逻辑] 关键备件使用进度与预警状态计算。

职责:
1. 计算单个备件的使用进度百分比
2. 批量计算 DataFrame 的使用进度列
3. 根据进度和阈值判定预警状态（正常/预警/超规）
4. 纯业务逻辑，不依赖外部 I/O
"""

import math
from dataclasses import dataclass
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd


STATUS_OVER = "超规"
STATUS_WARNING = "预警"
STATUS_NORMAL = "正常"
STATUS_REPLACED = "已更换"
REPLACEMENT_PROGRESS_THRESHOLD = 98.0


@dataclass(frozen=True)
class PartsAlertPolicy:
    """Application-injected warning thresholds for critical parts."""

    warning_threshold: float
    over_threshold: float

    def __post_init__(self) -> None:
        if self.warning_threshold >= self.over_threshold:
            raise ValueError("warning_threshold must be lower than over_threshold")


RAW_VALUE_COLUMN = "原始测量值"
OVER_SPEC_COLUMN = "是否超规"
DECORATION_COLUMN = "数据修饰"
DECORATION_STATUS_ORIGINAL = "原始"
DECORATION_STATUS_REPLACED = "寿命到期更换"


def _coerce_float(value: object) -> Optional[float]:
    """将业务输入安全转为 float，无效值返回 None。"""
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric_value):
        return None
    return numeric_value


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
        float: 使用进度百分比 (0.0 ~ 无限)，无效输入返回 0.0
    """
    if actual_value is None or spec_limit is None:
        return 0.0
    if spec_limit <= 0:
        return 0.0
    return (actual_value / spec_limit) * 100.0


def is_over_spec(
    actual_value: Optional[float],
    spec_limit: Optional[float],
) -> bool:
    """
    判断原始测量值是否超过寿命规格线。

    Args:
        actual_value: 原始测量值
        spec_limit: 寿命规格

    Returns:
        bool: 原始测量值 > 规格线时返回 True
    """
    numeric_actual = _coerce_float(actual_value)
    numeric_spec = _coerce_float(spec_limit)
    if numeric_actual is None or numeric_spec is None:
        return False
    if numeric_spec <= 0:
        return False
    return numeric_actual > numeric_spec


def apply_lifetime_replacement(
    report_df: pd.DataFrame,
    *,
    value_col: str = "测量值",
    spec_col: str = "寿命规格",
    raw_value_col: str = RAW_VALUE_COLUMN,
    over_spec_col: str = OVER_SPEC_COLUMN,
    decoration_col: str = DECORATION_COLUMN,
) -> pd.DataFrame:
    """保留源值；进度严格超过98%时计算用测量值归零，输入和来源不改写。

    每次以调用方提供的源值重新判断，不把标记当作持久更换事件。
    各行独立处理，不执行旧超规修饰、前值增长或98%截断。
    最终测量值由进度计算完成后的更换日期投影生成。
    """
    if report_df.empty or value_col not in report_df or spec_col not in report_df:
        return report_df.copy()
    result = report_df.copy()
    result[raw_value_col] = result[value_col]
    result[over_spec_col] = [
        is_over_spec(value, spec)
        for value, spec in zip(result[value_col], result[spec_col])
    ]
    result[decoration_col] = DECORATION_STATUS_ORIGINAL
    values = pd.to_numeric(result[value_col], errors="coerce")
    specs = pd.to_numeric(result[spec_col], errors="coerce")
    replacement_due = (
        np.isfinite(values) & np.isfinite(specs) & specs.gt(0)
        & (values / specs * 100).gt(REPLACEMENT_PROGRESS_THRESHOLD)
    )
    result.loc[replacement_due, value_col] = 0.0
    result.loc[replacement_due, decoration_col] = DECORATION_STATUS_REPLACED
    return result


def calculate_warning_status(
    usage_progress: float,
    policy: PartsAlertPolicy,
) -> str:
    """
    根据使用进度判定预警状态。

    规则:
    - usage_progress > 配置超规阈值 → STATUS_OVER (超规)
    - usage_progress > 配置预警阈值 → STATUS_WARNING (预警)
    - 否则                   → STATUS_NORMAL (正常)

    Args:
        usage_progress: 使用进度百分比

    Returns:
        str: STATUS_OVER / STATUS_WARNING / STATUS_NORMAL
    """
    if usage_progress > policy.over_threshold:
        return STATUS_OVER
    if usage_progress > policy.warning_threshold:
        return STATUS_WARNING
    return STATUS_NORMAL


def apply_replacement_measurement_dates(
    report_df: pd.DataFrame, *, as_of_date: str | date,
) -> pd.DataFrame:
    """完成进度计算后，已更换行的测量值统一显示本次报告更换日期。"""
    result = report_df.copy()
    if result.empty or "预警状态" not in result or "测量值" not in result:
        return result
    replaced = result["预警状态"].eq(STATUS_REPLACED)
    if replaced.any():
        replacement_date = pd.Timestamp(as_of_date).strftime("%Y-%m-%d")
        # 数值列显式转为object，容纳日期文本；使用进度已经完成计算。
        result["测量值"] = result["测量值"].astype(object)
        result.loc[replaced, "测量值"] = replacement_date
    return result


def batch_calculate_progress_and_status(
    report_df: pd.DataFrame,
    policy: PartsAlertPolicy,
) -> pd.DataFrame:
    """
    批量计算 DataFrame 中所有行的使用进度和预警状态。

    要求 DataFrame 包含以下列:
    - 测量值: 实际测量值
    - 寿命规格: 备件额定寿命

    将添加/覆盖以下列:
    - 使用进度: 使用进度百分比
    - 预警状态: STATUS_OVER / STATUS_WARNING / STATUS_NORMAL

    Args:
        report_df: 包含备件数据的 DataFrame

    Returns:
        pd.DataFrame: 添加了计算列的新 DataFrame
    """
    result = report_df.copy()
    result["使用进度"] = (
        result["测量值"] / result["寿命规格"] * 100
    )
    result["使用进度"] = result["使用进度"].fillna(0.0)

    is_replaced = (
        result[DECORATION_COLUMN].eq(DECORATION_STATUS_REPLACED)
        if DECORATION_COLUMN in result.columns
        else pd.Series(False, index=result.index)
    )
    conditions = [
        is_replaced,
        result["使用进度"] > policy.over_threshold,
        result["使用进度"] > policy.warning_threshold,
    ]
    choices = [STATUS_REPLACED, STATUS_OVER, STATUS_WARNING]
    result["预警状态"] = np.select(conditions, choices, default=STATUS_NORMAL)

    return result
