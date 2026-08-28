"""Inline 报表共享的单片异常（Sheet OOS）预警中心组件。

四个 Inline 监控报表（SPC / CTQ / AOI_TT / AOI_RS）共用的预警 UI 与按键过滤逻辑：
- 预警口径：`flag == FALSE` 且时间 ∈ 上一 ISO 周（后端见
  ``src/inline_domain/core/shared/sheet_oos_alerts.py``）；
- 展示：预警中心 Expander 有警自动展开，无警显示正常态，数据不可用显示降级提示；
- 出图：``filter_report_by_alert_keys`` 按预警指标键精确过滤报表数据，
  供"自动预警指标图像" Expander 复用各模块既有图表渲染。
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.utils.step_labels import format_step_label


def filter_report_by_alert_keys(
    report_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
    key_map: dict[str, str],
) -> pd.DataFrame:
    """按预警键（中文列→英文列映射）精确过滤报表数据。

    与 SPC CPK 预警的 ``filter_spc_report_by_alerts`` 同语义：
    任一输入为空或键列缺失时返回空表；预警键去重后按 MultiIndex 精确匹配。
    """
    if report_df.empty or alerts_df.empty:
        return report_df.iloc[0:0].copy()

    alert_columns = set(key_map)
    report_columns = set(key_map.values())
    if not alert_columns.issubset(alerts_df.columns) or not report_columns.issubset(report_df.columns):
        return report_df.iloc[0:0].copy()

    alert_keys_df = (
        alerts_df[list(key_map)]
        .rename(columns=key_map)
        .astype(str)
        .drop_duplicates()
    )
    report_keys_df = report_df[list(key_map.values())].astype(str)
    alert_key_index = pd.MultiIndex.from_frame(alert_keys_df)
    report_key_index = pd.MultiIndex.from_frame(report_keys_df)
    return report_df.loc[report_key_index.isin(alert_key_index)].copy().reset_index(drop=True)


def build_sheet_oos_alert_display(
    alerts_df: pd.DataFrame,
    *,
    column_map: dict[str, str],
    output_columns: list[str],
) -> pd.DataFrame:
    """把英文预警明细重命名为中文展示表，缺失列跳过。"""
    if alerts_df.empty:
        return pd.DataFrame(columns=output_columns)
    available_map = {src: dst for src, dst in column_map.items() if src in alerts_df.columns}
    display = alerts_df.rename(columns=available_map)
    columns = [col for col in output_columns if col in display.columns]
    return display[columns].copy()


def render_sheet_oos_alert_center(
    alerts_df: pd.DataFrame,
    *,
    title: str,
    has_source_data: bool,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """渲染单片异常预警中心 Expander（有警自动展开）。"""
    has_alerts = not alerts_df.empty
    with st.expander(title, expanded=has_alerts):
        if has_alerts:
            st.error(f"检测到 {len(alerts_df)} 条单片异常预警（flag=已确认真实超规），请关注。")
            display_alerts_df = alerts_df
            if step_desc_map and "站点" in alerts_df.columns:
                display_alerts_df = alerts_df.copy()
                display_alerts_df["站点"] = display_alerts_df["站点"].map(
                    lambda step: format_step_label(step, step_desc_map)
                )
            st.dataframe(display_alerts_df, hide_index=True, use_container_width=True)
        elif has_source_data:
            st.success("上一周未发现已确认真实超规（flag=FALSE）的单片异常。")
        else:
            st.info("当前产品暂无单片异常明细数据。")
