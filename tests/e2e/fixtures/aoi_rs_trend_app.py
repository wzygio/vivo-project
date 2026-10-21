"""No-database fixture for RS trend warning UI and chart verification."""

from datetime import date
from unittest.mock import patch

import pandas as pd
import streamlit as st

from app.sections.inline_domain.aoi_rs.trend_alerts import render_aoi_rs_trend_alerts
from src.inline_domain.application.aoi_rs.aoi_rs_service import AoiRsReportBuildError, AoiRsReportViewModel
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import build_lot_point_df, build_period_trend_df, build_sheet_point_df
from src.inline_domain.core.aoi_rs.trend_alerts import evaluate_aoi_rs_trends
from src.shared_kernel.config import ConfigLoader

st.set_page_config(page_title="AOI RS 趋势验证", layout="wide")
st.title("AOI RS 趋势预警验证")
scenario = st.selectbox("场景", ["有预警", "跨日预警", "无预警", "数据不足", "加载失败"])
reference = date(2026, 10, 19)

try:
    if scenario == "加载失败":
        raise AoiRsReportBuildError("fixture failure")
    dates = ["2026-10-16", "2026-10-17", "2026-10-18"]
    values = [10, 13, 17] if scenario in ("有预警", "跨日预警") else [10, 10, 10]
    if scenario == "跨日预警":
        values = [10, 20, 30]
    rows = [
        dict(prod_code="M678", factory="ARRAY", step_id="11629", rs_code=code,
             start_time=pd.Timestamp(day), sheet_id=f"S{day}", lot_id=f"L{day}", code_qty=value)
        for code, quantities in [("A1PPS", values), ("B1", [10, 10, 10])]
        for day, value in zip(
            ["2026-10-12", "2026-10-15", "2026-10-18"]
            if scenario == "跨日预警" and code == "A1PPS" else dates,
            quantities,
        )
    ]
    details = pd.DataFrame(rows)
    if scenario == "数据不足":
        details = details.loc[details.start_time.eq(pd.Timestamp("2026-10-18"))].copy()
    throughput = pd.DataFrame([
        dict(factory="ARRAY", step_id="11629", sheet_id=f"S{i}-{day}", start_time=pd.Timestamp(day))
        for day in sorted(details.start_time.dt.strftime("%Y-%m-%d").unique()) for i in range(100)
    ])
    indicators = details[["prod_code", "factory", "step_id", "rs_code"]].drop_duplicates()
    indicators["code_desc"] = indicators.rs_code.map({"A1PPS": "趋势样例", "B1": "稳定样例"})
    trend = build_period_trend_df(details, throughput, reference, daily_end_date=date(2026, 10, 18))
    lots = build_lot_point_df(details, throughput.assign(lot_id=throughput.start_time.map(lambda day: f"L{day.date()}")))
    sheets = build_sheet_point_df(details)
    report = AoiRsReportViewModel(
        rs_details_df=details, pass_through_df=throughput, spec_df=pd.DataFrame(),
        indicators_df=indicators, lot_points_df=lots, sheet_points_df=sheets,
        trend_evaluation_df=evaluate_aoi_rs_trends(trend, indicators, reference_date=reference),
    )
    with patch.object(ConfigLoader, "get_aoi_rs_special_decoration_factories", return_value=()):
        render_aoi_rs_trend_alerts(report, reference_date=reference, step_desc_map={"11629": "检测站"})
except AoiRsReportBuildError:
    st.error("AOI RS 报表加载失败，请稍后重试或点击页头“刷新缓存”。")
