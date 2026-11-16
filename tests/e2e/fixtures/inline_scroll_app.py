"""Pure in-memory charts through production CTQ/AOI rendering and SPC layout."""
from datetime import date
from pathlib import Path
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

st.set_page_config(page_title="Inline scroll fixture", layout="wide")
kind = st.query_params.get("kind", "ctq")
if kind not in {"spc", "ctq", "aoi_rs", "aoi_tt"}:
    st.stop()
st.title("Inline scroll fixture")
st.caption(f"Fixture kind: {kind}; in-memory chart data; no database or workbook access")
END = date(2026, 11, 13)


def measurements():
    return pd.DataFrame([
        dict(factory="ARRAY", prod_code="FIXTURE", step_id="12140", param_name=f"P{indicator}",
             sheet_id=f"S{day:02d}", lot_id=f"L{day // 3}",
             sheet_start_time=pd.Timestamp(f"2026-11-{day:02d} 08:00") + pd.Timedelta(minutes=site),
             site_name=str(site), unit_id="U1", main_process_unit_id=f"CH{day % 2}",
             param_value=1 + indicator * .01 + day * .002 + site * .01,
             usl=1.5, lsl=.5, ucl=1.3, lcl=.7, target=1.)
        for indicator in range(3) for day in range(2, 13) for site in range(1, 4)
    ])


def aoi_points(column, value_column):
    return pd.DataFrame([
        dict(factory="ARRAY", prod_code="FIXTURE", step_id="11629", sheet_id=f"S{day:02d}",
             lot_id=f"L{day // 3}", start_time=pd.Timestamp(f"2026-11-{day:02d} 08:00"),
             particle_size="Total", **{column: f"C{code}", value_column: day % 3 + code + 1})
        for code in range(3) for day in range(2, 13)
    ])


if kind in {"spc", "ctq"}:
    points = measurements()
    features = points.groupby(["factory", "prod_code", "step_id", "param_name", "sheet_id"], as_index=False).agg(
        sheet_start_time=("sheet_start_time", "min"), sheet_mean=("param_value", "mean"),
        sheet_min=("param_value", "min"), sheet_max=("param_value", "max"),
        usl=("usl", "first"), lsl=("lsl", "first"), ucl=("ucl", "first"), lcl=("lcl", "first"),
        target=("target", "first"),
    )
    if kind == "ctq":
        from app.sections.inline_domain.ctq.ctq_dashboard import (
            render_ctq_indicator_sections, render_ctq_sheet_oos_alert_indicator_sections,
        )
        st.header("Fixture ordinary charts")
        render_ctq_indicator_sections(features, points)
        st.header("Fixture alert charts")
        alerts = pd.DataFrame([{"厂别": "ARRAY", "站点": "12140", "参数名称": f"P{n}"} for n in range(3)])
        render_ctq_sheet_oos_alert_indicator_sections(alerts, features, points)
    else:
        from app.sections.inline_domain.spc.scroll_layout import render_spc_page_scroll_layout
        from app.sections.inline_domain.spc.spc_dashboard import _render_indicator_payload
        render_spc_page_scroll_layout()
        st.selectbox("Fixture factory", ["ARRAY"], key="spc_factory_filter")
        for section in ("ordinary", "alert"):
            st.header(f"Fixture {section} charts")
            for n in range(3):
                figure = go.Figure(go.Scatter(x=list(range(10)), y=[1 + (i % 4) * .1 for i in range(10)], mode="lines+markers"))
                figure.update_layout(height=400, title=f"SPC fixture {n}")
                _render_indicator_payload(dict(label=f"SPC fixture {n}", capability_table=pd.DataFrame(),
                                               fig1=figure, chamber_fig=go.Figure(figure), time_fig=go.Figure(figure)),
                                          chart_key_prefix=f"fixture_spc_{section}")
elif kind == "aoi_rs":
    from app.sections.inline_domain.aoi_rs.aoi_rs_dashboard import (
        render_aoi_rs_indicator_sections, render_aoi_rs_sheet_oos_alert_indicator_sections,
    )
    from src.inline_domain.core.aoi_rs.aoi_rs_calculator import build_lot_point_df, build_sheet_point_df
    points = aoi_points("rs_code", "code_qty")
    throughput = points[["factory", "step_id", "sheet_id", "lot_id", "start_time"]].drop_duplicates()
    indicators = points[["factory", "step_id", "rs_code"]].drop_duplicates()
    frames = dict(rs_details_df=points, pass_through_df=throughput, spec_df=pd.DataFrame(),
                  indicators_df=indicators, lot_points_df=build_lot_point_df(points, throughput),
                  sheet_points_df=build_sheet_point_df(points), end_date=END)
    st.header("Fixture ordinary charts")
    render_aoi_rs_indicator_sections(**frames)
    st.header("Fixture alert charts")
    alerts = pd.DataFrame([{"厂别": "ARRAY", "站点": "11629", "RS Code": f"C{n}"} for n in range(3)])
    render_aoi_rs_sheet_oos_alert_indicator_sections(alerts, **frames)
else:
    from app.sections.inline_domain.aoi_tt.aoi_tt_dashboard import (
        render_aoi_tt_indicator_sections, render_aoi_tt_sheet_oos_alert_indicator_sections,
    )
    points = aoi_points("tt_name", "tt_qty")
    indicators = points[["factory", "step_id", "tt_name"]].drop_duplicates()
    frames = dict(tt_details_df=points, spec_df=pd.DataFrame(), indicators_df=indicators, end_date=END)
    st.header("Fixture ordinary charts")
    render_aoi_tt_indicator_sections(**frames)
    st.header("Fixture alert charts")
    alerts = pd.DataFrame([{"厂别": "ARRAY", "站点": "11629", "TT名称": f"C{n}"} for n in range(3)])
    render_aoi_tt_sheet_oos_alert_indicator_sections(alerts, **frames)

st.caption("Fixture charts ready")
