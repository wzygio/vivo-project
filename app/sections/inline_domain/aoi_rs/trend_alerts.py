"""AOI_RS trend warning presentation; calculations belong to the application."""

from __future__ import annotations

from datetime import date
import logging
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
    AOI_RS_ALERT_KEY_MAP,
    AOI_RS_ALERT_PASS_THROUGH_KEY_MAP,
    render_aoi_rs_indicator_sections,
)
from app.sections.inline_domain.shared.alert_center import filter_report_by_alert_keys
from app.sections.inline_domain.shared.trend_alert_center import (
    build_density_trend_alert_display, render_density_trend_alert_center,
)

if TYPE_CHECKING:
    from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportViewModel

_INDICATOR_COLUMNS = {
    "factory": "厂别", "step_id": "站点", "rs_code": "RS Code",
}


def build_aoi_rs_trend_alert_display(
    alerts: pd.DataFrame, step_desc_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Create a business-column whitelist before handing data to Streamlit."""
    return build_density_trend_alert_display(alerts, _INDICATOR_COLUMNS, step_desc_map)


def render_aoi_rs_trend_alerts(
    report: AoiRsReportViewModel | None,
    *,
    reference_date: date,
    step_desc_map: dict[str, str] | None = None,
    evaluation_df: pd.DataFrame | None = None,
) -> None:
    """Show fixed-yesterday status and exact matching indicator charts."""
    alerts = render_density_trend_alert_center(
        evaluation_df if evaluation_df is not None else getattr(report, "trend_evaluation_df", pd.DataFrame()),
        reference_date=reference_date,
        indicator_columns=_INDICATOR_COLUMNS, step_desc_map=step_desc_map,
    )
    if alerts.empty or report is None:
        return
    try:
        _render_alert_charts(report, alerts, reference_date, step_desc_map)
    except Exception:
        logging.getLogger(__name__).exception("Optional AOI RS trend charts unavailable")
        st.warning("趋势预警图像暂不可用。")


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
