"""Compact business-column presentation shared by AOI density warnings."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from app.utils.step_labels import format_step_label

_VALUE_COLUMNS = {
    "target_date": "当日", "target_density": "当日密度",
    "previous_date": "T-1有效日", "previous_density": "T-1密度",
    "two_days_before_date": "T-2有效日", "two_days_before_density": "T-2密度",
    "baseline_density": "基线5日均值",
}


def build_density_trend_alert_display(
    alerts: pd.DataFrame,
    indicator_columns: dict[str, str],
    step_desc_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    columns = {**indicator_columns, **_VALUE_COLUMNS}
    display = alerts[list(columns)].rename(columns=columns).copy()
    display["站点"] = display["站点"].map(lambda step: format_step_label(str(step), step_desc_map))
    display["预警原因"] = [
        "；".join(name for enabled, name in (
            (row.daily_alert, "当日超规"), (row.rising_alert, "连续两日上升"),
        ) if enabled)
        for row in alerts.itertuples(index=False)
    ]
    return display


def render_density_trend_alert_center(
    evaluation: pd.DataFrame, *, reference_date: date,
    indicator_columns: dict[str, str], step_desc_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Render only business fields and return raw keys for module-owned charts."""
    target = reference_date - timedelta(days=1)
    alerts = evaluation.loc[evaluation["is_alert"].eq(True)] if not evaluation.empty else evaluation
    with st.expander(f"趋势预警中心（当日 {target.isoformat()}）", expanded=not alerts.empty):
        if evaluation.empty:
            st.info("暂无趋势数据。")
        elif not alerts.empty:
            st.dataframe(
                build_density_trend_alert_display(alerts, indicator_columns, step_desc_map),
                hide_index=True, width="stretch",
                column_config={name: st.column_config.NumberColumn(format="%.4f")
                               for name in ("当日密度", "T-1密度", "T-2密度", "基线5日均值")},
            )
        elif evaluation["daily_evaluable"].any() or evaluation["rising_evaluable"].any():
            st.success("可评估项暂无预警。")
        else:
            st.info("数据不足。")
    return alerts
