"""Q-Time confirmed over-spec alert center."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def _alert_display(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame()
    display = alerts.copy()
    display["站点"] = (
        display["f_step"].astype(str)
        + " → "
        + display["t_step"].astype(str)
        + "｜"
        + display["step_desc"].astype(str)
    )
    time_text = display["timekey"].astype(str).str.slice(0, 14)
    display["过货时间"] = pd.to_datetime(
        time_text,
        format="%Y%m%d%H%M%S",
        errors="coerce",
    )
    return display.rename(
        columns={
            "lot_id": "Lot ID",
            "q_spec": "Q-Time规格(H)",
            "wait_time": "等待时长(H)",
            "over_hours": "超规时长(H)",
        }
    )[["站点", "Lot ID", "Q-Time规格(H)", "等待时长(H)", "超规时长(H)", "过货时间"]]


def render_qtime_alert_center(alerts: pd.DataFrame, *, total_lots: int) -> None:
    """Render confirmed real Q-Time violations in the current report window."""
    has_alerts = not alerts.empty
    with st.expander("Q-Time 超规预警中心", expanded=has_alerts):
        if has_alerts:
            alert_lots = alerts["lot_id"].nunique()
            st.error(f"检测到 {len(alerts)} 条已确认真实超规，涉及 {alert_lots} 个 Lot，请关注。")
            with st.container(horizontal=True):
                st.metric("当前过货 Lot", total_lots, border=True)
                st.metric("超规预警 Lot", alert_lots, border=True)
            st.dataframe(
                _alert_display(alerts),
                hide_index=True,
                width="stretch",
                column_config={
                    "Q-Time规格(H)": st.column_config.NumberColumn(format="%.0f"),
                    "等待时长(H)": st.column_config.NumberColumn(format="%.1f"),
                    "超规时长(H)": st.column_config.NumberColumn(format="%.1f"),
                    "过货时间": st.column_config.DatetimeColumn(format="YYYY/MM/DD HH:mm:ss"),
                },
            )
        else:
            st.success("当前查询窗口内未发现已确认真实超规（flag=False）的 Q-Time 记录。")
