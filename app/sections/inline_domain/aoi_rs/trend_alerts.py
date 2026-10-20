"""AOI_RS trend warning presentation; calculations belong to the application."""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
    AOI_RS_ALERT_KEY_MAP,
    AOI_RS_ALERT_PASS_THROUGH_KEY_MAP,
    render_aoi_rs_indicator_sections,
)
from app.sections.inline_domain.shared.alert_center import filter_report_by_alert_keys
from app.utils.step_labels import format_step_label

if TYPE_CHECKING:
    from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportViewModel

_DISPLAY_COLUMNS = {
    "factory": "厂别", "step_id": "站点", "rs_code": "RS Code",
    "target_date": "当日", "target_density": "当日密度",
    "previous_date": "T-1有效日", "previous_density": "T-1密度",
    "two_days_before_date": "T-2有效日", "two_days_before_density": "T-2密度",
    "baseline_density": "基线5日均值",
}


def build_aoi_rs_trend_alert_display(
    alerts: pd.DataFrame, step_desc_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Create a business-column whitelist before handing data to Streamlit."""
    display = alerts[list(_DISPLAY_COLUMNS)].rename(columns=_DISPLAY_COLUMNS).copy()
    display["站点"] = display["站点"].map(lambda step: format_step_label(str(step), step_desc_map))
    display["预警原因"] = [
        "；".join(name for enabled, name in (
            (row.daily_alert, "当日超规"), (row.rising_alert, "连续两日上升"),
        ) if enabled)
        for row in alerts.itertuples(index=False)
    ]
    return display


def render_aoi_rs_trend_alerts(
    report: AoiRsReportViewModel,
    *,
    reference_date: date,
    step_desc_map: dict[str, str] | None = None,
) -> None:
    """Show fixed-yesterday status and exact matching indicator charts."""
    evaluation = report.trend_evaluation_df
    target = reference_date - timedelta(days=1)
    alerts = evaluation.loc[evaluation["is_alert"].eq(True)] if not evaluation.empty else evaluation
    with st.expander(f"趋势预警中心（当日 {target.isoformat()}）", expanded=not alerts.empty):
        if evaluation.empty:
            st.info("暂无趋势数据。")
            return
        if not alerts.empty:
            st.dataframe(
                build_aoi_rs_trend_alert_display(alerts, step_desc_map),
                hide_index=True, width="stretch",
                column_config={
                    name: st.column_config.NumberColumn(format="%.4f")
                    for name in ("当日密度", "T-1密度", "T-2密度", "基线5日均值")
                },
            )
        elif evaluation["daily_evaluable"].any() or evaluation["rising_evaluable"].any():
            st.success("可评估项暂无预警。")
        else:
            st.info("数据不足。")
    if alerts.empty:
        return
    _render_alert_charts(report, alerts, reference_date, step_desc_map)


def _render_alert_charts(
    report: AoiRsReportViewModel,
    alerts: pd.DataFrame,
    end_date: date,
    step_desc_map: dict[str, str] | None,
) -> None:
    keys = alerts[list(AOI_RS_ALERT_KEY_MAP.values())].rename(
        columns={value: key for key, value in AOI_RS_ALERT_KEY_MAP.items()},
    )
    indicators = filter_report_by_alert_keys(report.indicators_df, keys, AOI_RS_ALERT_KEY_MAP)
    if indicators.empty:
        st.info("暂无图像。")
        return
    frames = {
        name: filter_report_by_alert_keys(getattr(report, name), keys, AOI_RS_ALERT_KEY_MAP)
        for name in ("rs_details_df", "lot_points_df", "sheet_points_df")
    }
    with st.expander(f"趋势预警指标图像（{len(indicators)} 个指标）", expanded=True):
        render_aoi_rs_indicator_sections(
            **frames,
            pass_through_df=filter_report_by_alert_keys(
                report.pass_through_df, keys, AOI_RS_ALERT_PASS_THROUGH_KEY_MAP,
            ),
            spec_df=report.spec_df, indicators_df=indicators, end_date=end_date,
            chart_key_prefix="aoi_rs_trend_alert", step_desc_map=step_desc_map,
        )
