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
    "target_date": "目标日", "target_density": "目标日密度",
    "previous_date": "上次有效日", "previous_density": "上次密度",
    "two_days_before_date": "再前有效日", "two_days_before_density": "再前密度",
    "baseline_density": "基线5日均值", "target_sheet_qty": "目标日过货量",
    "valid_days": "计算窗有效天数", "baseline_days": "基线有效天数",
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
    with st.expander(f"趋势预警中心（目标日 {target.isoformat()}）", expanded=not alerts.empty):
        st.caption(
            "目标日为昨天，密度单位为 RS/片。比较记录从目标日前7天内选取最近两次有效数据，允许跨日；"
            "基线取目标日前第2至第6天的正密度均值，跳过缺测和零值。"
            "当日超规：目标日密度同时大于上次密度及基线的1.2倍；"
            "连续两日上升：目标日大于上次密度的1.2倍，且上次大于再前密度的1.2倍。任一规则命中即预警。"
        )
        if evaluation.empty:
            st.info("当前产品暂无可评估的 AOI RS 趋势数据。")
            return
        daily_count = int(evaluation["daily_evaluable"].sum())
        rising_count = int(evaluation["rising_evaluable"].sum())
        insufficient = int((~(evaluation["daily_evaluable"] & evaluation["rising_evaluable"])).sum())
        st.caption(f"共 {len(evaluation)} 个指标；当日超规可评估 {daily_count} 个，连续两日上升可评估 {rising_count} 个。")
        if insufficient:
            st.info(f"{insufficient} 个指标的部分或全部规则数据不足，缺失日期不补零。")
        if not alerts.empty:
            st.warning(f"检测到 {len(alerts)} 个指标触发趋势预警，请关注。")
            st.dataframe(
                build_aoi_rs_trend_alert_display(alerts, step_desc_map),
                hide_index=True, width="stretch",
                column_config={
                    name: st.column_config.NumberColumn(format="%.4f")
                    for name in ("目标日密度", "上次密度", "再前密度", "基线5日均值")
                },
            )
        elif daily_count or rising_count:
            st.success("可评估规则未发现趋势预警。")
        else:
            st.info("目标日趋势数据不足，暂不能判断是否存在趋势预警。")
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
        st.warning("趋势预警指标暂无可绘制的 AOI RS 数据。")
        return
    frames = {
        name: filter_report_by_alert_keys(getattr(report, name), keys, AOI_RS_ALERT_KEY_MAP)
        for name in ("rs_details_df", "lot_points_df", "sheet_points_df")
    }
    with st.expander(f"趋势预警指标图像（{len(indicators)} 个指标）", expanded=True):
        st.caption("以下图像由趋势预警自动匹配，无需通过筛选器查询。")
        render_aoi_rs_indicator_sections(
            **frames,
            pass_through_df=filter_report_by_alert_keys(
                report.pass_through_df, keys, AOI_RS_ALERT_PASS_THROUGH_KEY_MAP,
            ),
            spec_df=report.spec_df, indicators_df=indicators, end_date=end_date,
            chart_key_prefix="aoi_rs_trend_alert", step_desc_map=step_desc_map,
        )
