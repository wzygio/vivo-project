"""Shared sheet-OOS alert filtering for inline monitoring reports.

Alert semantics (see docs/PRD/PRD-2026-08-25-Inline自动预警中心.md):

- 单片异常口径固定为 `flag == FALSE`（用户确认释放真实值的超规片）；
  明细工作簿中的每一行本来就是超规片，`flag` 只是修饰决策。
- 预警范围默认上一 ISO 周；消费方可指定包含首尾两天的日期范围。

本模块只包含纯函数，不做任何文件或数据库 I/O。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import pandas as pd


def previous_iso_week_range(reference_date: date | pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the previous ISO week as a half-open [start, end) interval.

    ``start`` 为上周一 00:00，``end`` 为本周一 00:00（不包含）。
    与 ISO-8601 周历一致：参考日为周一时，上一周指紧邻的上一个完整周。
    """
    reference = pd.Timestamp(reference_date).normalize()
    this_monday = reference - pd.Timedelta(days=reference.weekday())
    return this_monday - pd.Timedelta(days=7), this_monday


def _is_false_flag(value: object) -> bool:
    """与 sheet_oos_decoration._parse_flag 语义对齐：仅“释放真实值”判定为 False。"""
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value is False
    text = str(value).strip().lower()
    return text in {"false", "0", "no", "n", "否", "不修饰", "不截断"}


def build_sheet_oos_alerts(
    detail_df: pd.DataFrame,
    *,
    time_column: str,
    reference_date: date | pd.Timestamp | None = None,
    excluded_param_name_contains: Sequence[str] = (),
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """筛选指定日期内 flag == FALSE 的明细，默认上一ISO周，按时间倒序。

    - ``flag`` 解析与修饰工作簿语义一致（True/Delete/空值均不报警）；
    - ``time_column`` 经 ``pd.to_datetime(errors="coerce")`` 归一化，
      解析失败或缺失的行不参与；
    - 输入为空或缺少必要列时返回空 DataFrame（保留原列结构）。
    - 独立预警排除项按参数名作不区分大小写的普通文本匹配，不改写 flag。
    """
    if detail_df is None or detail_df.empty:
        return detail_df.copy() if isinstance(detail_df, pd.DataFrame) else pd.DataFrame()
    if "flag" not in detail_df.columns or time_column not in detail_df.columns:
        return detail_df.iloc[0:0].copy()

    start, end = previous_iso_week_range(reference_date or date.today())
    start = pd.Timestamp(start_date).normalize() if start_date is not None else start
    end = pd.Timestamp(end_date).normalize() + pd.Timedelta(days=1) if end_date is not None else end
    if start >= end:
        raise ValueError('Invalid alert date range')
    result = detail_df.copy()
    result[time_column] = pd.to_datetime(result[time_column], errors="coerce")
    if isinstance(result[time_column].dtype, pd.DatetimeTZDtype):
        result[time_column] = result[time_column].dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
    mask = (
        result["flag"].map(_is_false_flag)
        & result[time_column].notna()
        & (result[time_column] >= start)
        & (result[time_column] < end)
    )
    if excluded_param_name_contains and "param_name" in result.columns:
        parameter_names = result["param_name"].fillna("").astype(str)
        for token in excluded_param_name_contains:
            if token.strip():
                mask &= ~parameter_names.str.contains(token.strip(), case=False, regex=False)
    return result.loc[mask].sort_values(time_column, ascending=False).reset_index(drop=True)
