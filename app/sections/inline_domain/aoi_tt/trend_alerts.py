"""Total-only TT warnings with the existing particle-size chart breakdown."""
from __future__ import annotations

from datetime import date
import logging
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
    AOI_TT_ALERT_KEY_MAP, render_aoi_tt_indicator_sections,
)
from app.sections.inline_domain.shared.alert_center import filter_report_by_alert_keys
from app.sections.inline_domain.shared.trend_alert_center import render_density_trend_alert_center

if TYPE_CHECKING:
    from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportViewModel

_INDICATOR_COLUMNS = {
    "factory": "厂别", "step_id": "站点", "tt_name": "TT名称",
}


def render_aoi_tt_trend_alerts(
    report: AoiTtReportViewModel | None, *, reference_date: date,
    step_desc_map: dict[str, str] | None = None,
    evaluation_df: pd.DataFrame | None = None,
) -> None:
    alerts = render_density_trend_alert_center(
        evaluation_df if evaluation_df is not None else getattr(report, "trend_evaluation_df", pd.DataFrame()),
        reference_date=reference_date,
        indicator_columns=_INDICATOR_COLUMNS, step_desc_map=step_desc_map,
    )
    if alerts.empty or report is None:
        return
    try:
        keys = alerts[list(_INDICATOR_COLUMNS)].rename(columns=_INDICATOR_COLUMNS)
        details = filter_report_by_alert_keys(report.tt_details_df, keys, AOI_TT_ALERT_KEY_MAP)
        indicators = filter_report_by_alert_keys(report.indicators_df, keys, AOI_TT_ALERT_KEY_MAP)
        if details.empty or indicators.empty:
            st.info("暂无图像。")
            return
        count = len(keys.drop_duplicates())
        with st.expander(f"趋势预警指标图像（{count} 个指标）", expanded=True):
            render_aoi_tt_indicator_sections(
                tt_details_df=details, spec_df=report.spec_df, indicators_df=indicators,
                end_date=reference_date, step_desc_map=step_desc_map,
                chart_key_prefix="aoi_tt_trend_alert",
                aggregation_details_df=report.tt_details_df,
            )
    except Exception:
        logging.getLogger(__name__).exception("Optional AOI TT trend charts unavailable")
        st.warning("趋势预警图像暂不可用。")
