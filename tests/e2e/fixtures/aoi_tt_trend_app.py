"""No-database TT/particle warning fixture with overlapping chart areas."""
from datetime import date

import pandas as pd
import streamlit as st

from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
    render_aoi_tt_indicator_sections, render_aoi_tt_sheet_oos_alert_indicator_sections,
)
from app.sections.inline_domain.aoi_tt.trend_alerts import render_aoi_tt_trend_alerts
from src.inline_domain.application.aoi_tt.aoi_tt_service import AoiTtReportViewModel
from src.inline_domain.core.aoi_tt.aoi_tt_calculator import build_period_trend_df
from src.inline_domain.core.aoi_tt.trend_alerts import INDICATOR_KEYS, HISTORY_LOOKBACK_DAYS, evaluate_aoi_tt_trends

st.set_page_config(page_title="AOI TT 趋势验证", layout="wide")
st.title("AOI TT 趋势预警验证")
scenario = st.selectbox("场景", ["有预警", "跨日预警", "无预警", "仅粒径预警", "数据不足", "加载失败"])
show_manual = st.checkbox("同时展示其他报表区域")
reference = date(2026, 10, 20)
target = date(2026, 10, 19)

if scenario == "加载失败":
    st.error("AOI TT 报表加载失败，请稍后重试。")
else:
    dates = ["2026-10-13", "2026-10-16", "2026-10-19"] if scenario == "跨日预警" else ["2026-10-17", "2026-10-18", "2026-10-19"]
    quantities = [1, 2, 3] if scenario in ("有预警", "跨日预警") else [1, 1, 1]
    total_quantities = [10 * value for value in quantities]
    if scenario == "仅粒径预警":
        quantities = [1, 2, 3]
    details = pd.DataFrame([
        dict(prod_code="M678", factory="ARRAY", step_id="11620", tt_name=name,
             particle_size=size, start_time=pd.Timestamp(day), sheet_id=f"S{day}",
             lot_id=f"L{day}", tt_qty=qty)
        for name, size, values in [("TDSUM", "Total", total_quantities),
                                   ("TDSUM", "S", quantities), ("OTHER", "Total", [10, 10, 10])]
        for day, qty in zip(dates, values)
    ])
    if scenario == "数据不足":
        details = details.loc[details.start_time.eq(pd.Timestamp(target))].copy()
    candidates = details[["prod_code", *INDICATOR_KEYS]].drop_duplicates()
    indicators = candidates
    trend = build_period_trend_df(
        details, reference, daily_start_date=(pd.Timestamp(target) - pd.Timedelta(days=HISTORY_LOOKBACK_DAYS)).date(),
        daily_end_date=target,
    )
    report = AoiTtReportViewModel(
        tt_details_df=details, spec_df=pd.DataFrame(), indicators_df=indicators,
        trend_evaluation_df=evaluate_aoi_tt_trends(trend, candidates, reference_date=reference),
    )
    render_aoi_tt_trend_alerts(report, reference_date=reference, step_desc_map={"11620": "检测站"})
    if show_manual:
        selected = details.loc[details.tt_name.eq("TDSUM") & details.particle_size.eq("S")]
        selected_indicators = indicators.loc[indicators.tt_name.eq("TDSUM")]
        render_aoi_tt_indicator_sections(
            tt_details_df=selected, spec_df=pd.DataFrame(), indicators_df=selected_indicators, end_date=reference,
        )
        render_aoi_tt_sheet_oos_alert_indicator_sections(
            pd.DataFrame([{"厂别": "ARRAY", "站点": "11620", "TT名称": "TDSUM"}]),
            tt_details_df=details, spec_df=pd.DataFrame(), indicators_df=indicators, end_date=reference,
        )
