# src/equipment_domain/core/parts_calculator.py
"""
[核心业务逻辑] 关键备件使用进度与预警状态计算。

职责:
1. 计算单个备件的使用进度百分比
2. 批量计算 DataFrame 的使用进度列
3. 根据进度和阈值判定预警状态（正常/预警/超规）
4. 纯业务逻辑，不依赖外部 I/O
"""

import hashlib
from typing import Optional, Sequence

import numpy as np
import pandas as pd


STATUS_OVER = "超规"
STATUS_WARNING = "预警"
STATUS_NORMAL = "正常"

# 阈值常量
WARNING_THRESHOLD = 90.0   # > 90% → 预警
OVER_THRESHOLD = 100.0     # > 100% → 超规

# 超规数据修饰常量
DECORATION_GROWTH_RATIO = 1.01
DECORATION_MIN_RATIO = 0.90
DECORATION_MAX_RATIO = 0.95
DISPLAY_PROGRESS_MAX_RATIO = 0.96

RAW_VALUE_COLUMN = "原始测量值"
OVER_SPEC_COLUMN = "是否超规"
DECORATION_COLUMN = "数据修饰"
DECORATION_STATUS_ORIGINAL = "原始"
DECORATION_STATUS_DECORATED = "超规修饰"
DECORATION_STATUS_DISPLAY_CAPPED = "进度上限修饰"


def _coerce_float(value: object) -> Optional[float]:
    """将业务输入安全转为 float，无效值返回 None。"""
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric_value):
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


def calculate_decorated_over_spec_value(
    actual_value: Optional[float],
    spec_limit: Optional[float],
    previous_value: Optional[float],
    spec_ratio: float = DECORATION_MIN_RATIO,
) -> float:
    """
    对超规测量值进行修饰。

    规则:
    - 未超规时返回原始测量值
    - 超规时返回 max(上一个有效值 * 1.01, ratio * 规格线)
    - ratio 会被约束在 [0.90, 0.95]

    Args:
        actual_value: 原始测量值
        spec_limit: 寿命规格
        previous_value: 上一个已输出的有效值
        spec_ratio: 规格线修饰比例

    Returns:
        float: 修饰后的测量值；无效输入返回 0.0
    """
    numeric_actual = _coerce_float(actual_value)
    numeric_spec = _coerce_float(spec_limit)
    if numeric_actual is None:
        return 0.0
    if numeric_spec is None or numeric_spec <= 0:
        return numeric_actual
    if numeric_actual <= numeric_spec:
        return numeric_actual

    bounded_ratio = min(max(spec_ratio, DECORATION_MIN_RATIO), DECORATION_MAX_RATIO)
    spec_candidate = numeric_spec * bounded_ratio
    numeric_previous = _coerce_float(previous_value)
    if numeric_previous is None:
        return spec_candidate
    return max(numeric_previous * DECORATION_GROWTH_RATIO, spec_candidate)


def cap_display_value_below_visible_100(
    actual_value: Optional[float],
    spec_limit: Optional[float],
) -> float:
    """
    将展示测量值限制在不会被整数百分比渲染成 100% 的范围内。

    Args:
        actual_value: 待展示测量值
        spec_limit: 寿命规格

    Returns:
        float: 若进度过高则压到 DISPLAY_PROGRESS_MAX_RATIO * 规格线
    """
    numeric_actual = _coerce_float(actual_value)
    numeric_spec = _coerce_float(spec_limit)
    if numeric_actual is None:
        return 0.0
    if numeric_spec is None or numeric_spec <= 0:
        return numeric_actual
    display_cap = numeric_spec * DISPLAY_PROGRESS_MAX_RATIO
    if numeric_actual > display_cap:
        return display_cap
    return numeric_actual


def _stable_spec_ratio(seed_value: object) -> float:
    """根据行标识稳定生成 [0.90, 0.95] 区间内的修饰比例。"""
    digest = hashlib.md5(str(seed_value).encode("utf-8")).hexdigest()
    normalized = int(digest[:8], 16) / 0xFFFFFFFF
    return DECORATION_MIN_RATIO + (
        normalized * (DECORATION_MAX_RATIO - DECORATION_MIN_RATIO)
    )


def _build_seed(row: pd.Series, row_index: object) -> str:
    """构造稳定修饰比例使用的业务种子。"""
    seed_parts = [
        str(row_index),
        str(row.get("厂别", "")),
        str(row.get("备件类型", "")),
        str(row.get("设备类型", "")),
        str(row.get("膜层", "")),
        str(row.get("制程", "")),
        str(row.get("站点", "")),
        str(row.get("机台号-腔室", "")),
        str(row.get("参数名称", "")),
        str(row.get("日期", "")),
    ]
    return "|".join(seed_parts)


def apply_over_spec_alert_and_decoration(
    report_df: pd.DataFrame,
    value_col: str = "测量值",
    spec_col: str = "寿命规格",
    raw_value_col: str = RAW_VALUE_COLUMN,
    over_spec_col: str = OVER_SPEC_COLUMN,
    decoration_col: str = DECORATION_COLUMN,
    group_cols: Optional[Sequence[str]] = None,
    sort_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    先记录原始超规状态，再对超过规格线的数据进行修饰。
    原始超规标记只用于触发修饰与审计，前端状态统一由修饰后的展示值计算。

    Args:
        report_df: 备件报表或趋势 DataFrame
        value_col: 测量值列名
        spec_col: 规格线列名
        raw_value_col: 原始测量值落地列名
        over_spec_col: 原始值是否超规的落地列名
        decoration_col: 数据修饰状态列名
        group_cols: 需要独立计算“上一个值”的分组列
        sort_col: 组内排序列，适用于趋势数据

    Returns:
        pd.DataFrame: 增加原始值、超规标记，并修饰 value_col 后的新 DataFrame
    """
    if report_df.empty:
        return report_df.copy()
    if value_col not in report_df.columns or spec_col not in report_df.columns:
        return report_df.copy()

    result = report_df.copy()
    result[raw_value_col] = result[value_col]
    result[over_spec_col] = [
        is_over_spec(value, spec)
        for value, spec in zip(result[value_col], result[spec_col])
    ]
    result[decoration_col] = DECORATION_STATUS_ORIGINAL

    work_df = result
    if sort_col and sort_col in result.columns:
        work_df = result.sort_values(sort_col, kind="mergesort")

    valid_group_cols = [
        col for col in (group_cols or [])
        if col in work_df.columns
    ]
    if valid_group_cols:
        group_iter = work_df.groupby(valid_group_cols, sort=False, dropna=False)
        groups = [group for _, group in group_iter]
    else:
        groups = [work_df]

    for group in groups:
        previous_value: Optional[float] = None
        for row_index, row in group.iterrows():
            actual_value = _coerce_float(row[value_col])
            spec_limit = _coerce_float(row[spec_col])
            if bool(row[over_spec_col]):
                ratio = _stable_spec_ratio(_build_seed(row, row_index))
                decorated_value = calculate_decorated_over_spec_value(
                    actual_value=actual_value,
                    spec_limit=spec_limit,
                    previous_value=previous_value,
                    spec_ratio=ratio,
                )
                capped_value = cap_display_value_below_visible_100(
                    actual_value=decorated_value,
                    spec_limit=spec_limit,
                )
                result.at[row_index, value_col] = capped_value
                result.at[row_index, decoration_col] = DECORATION_STATUS_DECORATED
                previous_value = capped_value
            elif actual_value is not None:
                capped_value = cap_display_value_below_visible_100(
                    actual_value=actual_value,
                    spec_limit=spec_limit,
                )
                if capped_value != actual_value:
                    result.at[row_index, value_col] = capped_value
                    result.at[row_index, decoration_col] = DECORATION_STATUS_DISPLAY_CAPPED
                previous_value = capped_value

    return result


def calculate_warning_status(
    usage_progress: float,
) -> str:
    """
    根据使用进度判定预警状态。

    规则:
    - usage_progress > 100% → STATUS_OVER (超规)
    - usage_progress > 90%  → STATUS_WARNING (预警)
    - 否则                   → STATUS_NORMAL (正常)

    Args:
        usage_progress: 使用进度百分比

    Returns:
        str: STATUS_OVER / STATUS_WARNING / STATUS_NORMAL
    """
    if usage_progress > OVER_THRESHOLD:
        return STATUS_OVER
    if usage_progress > WARNING_THRESHOLD:
        return STATUS_WARNING
    return STATUS_NORMAL


def batch_calculate_progress_and_status(report_df: pd.DataFrame) -> pd.DataFrame:
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
        pd.DataFrame: 添加了计算列的原 DataFrame（原地修改）
    """
    report_df["使用进度"] = (
        report_df["测量值"] / report_df["寿命规格"] * 100
    )
    report_df["使用进度"] = report_df["使用进度"].fillna(0.0)

    conditions = [
        report_df["使用进度"] > OVER_THRESHOLD,
        report_df["使用进度"] > WARNING_THRESHOLD,
    ]
    choices = [STATUS_OVER, STATUS_WARNING]
    report_df["预警状态"] = np.select(conditions, choices, default=STATUS_NORMAL)

    return report_df
