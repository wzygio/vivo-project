"""Q-Time confirmed over-spec alert selection."""

from __future__ import annotations

import pandas as pd


FALSE_FLAG_TOKENS = {"false", "0", "no", "n", "否", "不修饰", "不截断"}


def _is_confirmed_real(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value is False
    return str(value).strip().lower() in FALSE_FLAG_TOKENS


def build_qtime_alerts(decoration: pd.DataFrame) -> pd.DataFrame:
    """Return flag=False Q-Time OOS rows, newest records first."""
    if decoration is None or decoration.empty or "flag" not in decoration.columns:
        if isinstance(decoration, pd.DataFrame):
            return decoration.iloc[0:0].copy()
        return pd.DataFrame()
    result = decoration.loc[decoration["flag"].map(_is_confirmed_real)].copy()
    if "timekey" in result.columns:
        result = result.sort_values("timekey", ascending=False, kind="stable")
    return result.reset_index(drop=True)
