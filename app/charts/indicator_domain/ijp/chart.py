"""Deterministic Plotly model for the IJP overflow By-day stacked ratio chart."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative

from src.indicator_domain.core.ijp.overflow import IJP_RS_CODES

TARGET_COLOR = "#ef4444"


def build_ijp_daily_figure(
    ratios: pd.DataFrame,
    target: float | None = None,
) -> go.Figure:
    """Build the day × RS_CODE 100% stacked ratio bars with an optional target line."""
    figure = go.Figure()
    frame = ratios.copy()
    days: list[str] = []
    if not frame.empty and {"day", "rs_code", "ratio"} <= set(frame.columns):
        frame["ratio"] = pd.to_numeric(frame["ratio"], errors="coerce")
        frame = frame.dropna(subset=["day", "rs_code"])
        days = sorted(frame["day"].astype(str).unique())
        codes = [code for code in IJP_RS_CODES if code in set(frame["rs_code"])]
        codes += sorted(set(frame["rs_code"]) - set(codes))
        for index, code in enumerate(codes):
            code_rows = frame[frame["rs_code"] == code].set_index(
                frame.loc[frame["rs_code"] == code, "day"].astype(str)
            )
            y_values = [
                float(code_rows["ratio"].get(day, 0.0)) * 100 for day in days
            ]
            figure.add_bar(
                x=days,
                y=y_values,
                name=code,
                marker_color=qualitative.Plotly[index % len(qualitative.Plotly)],
                hovertemplate="%{x}<br>" + code + " %{y:.1f}%<extra></extra>",
            )

    if target is not None and days:
        figure.add_scatter(
            x=days,
            y=[float(target)] * len(days),
            name="Target",
            mode="lines",
            line={"color": TARGET_COLOR, "width": 2, "dash": "dash"},
            hovertemplate=f"Target {float(target):.1f}%<extra></extra>",
        )

    figure.update_layout(
        title={"text": "OLED RS Overflow By天", "x": 0.5, "xanchor": "center"},
        height=430,
        margin={"l": 50, "r": 24, "t": 72, "b": 60},
        barmode="stack",
        bargap=0.35,
        legend={"orientation": "h", "x": 0.5, "xanchor": "center", "y": 1.02},
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        xaxis={"title": "日期", "showgrid": False},
        yaxis={
            "title": "CODE 占比（%）",
            "range": [0, 100],
            "ticksuffix": "%",
            "gridcolor": "#d7dde5",
            "zeroline": False,
        },
        hovermode="x unified",
    )
    return figure
