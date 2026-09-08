"""Deterministic Plotly model for the IJP overflow By-Glass-ID ratio chart."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative

from src.indicator_domain.core.ijp.overflow import IJP_RS_CODES


def build_ijp_glass_figure(
    ratios: pd.DataFrame,
    *,
    title: str = "OLED RS Overflow By Glass ID",
) -> go.Figure:
    """Build one RS_CODE percentage stack per Glass ID within a Printer."""
    figure = go.Figure()
    frame = ratios.copy()
    glass_ids: list[str] = []
    if not frame.empty and {"glass_id", "rs_code", "ratio"} <= set(frame.columns):
        frame["ratio"] = pd.to_numeric(frame["ratio"], errors="coerce")
        frame = frame.dropna(subset=["glass_id", "rs_code"])
        glass_ids = sorted(frame["glass_id"].astype(str).unique())
        codes = [code for code in IJP_RS_CODES if code in set(frame["rs_code"])]
        codes += sorted(set(frame["rs_code"]) - set(codes))
        for index, code in enumerate(codes):
            code_rows = frame[frame["rs_code"] == code].set_index(
                frame.loc[frame["rs_code"] == code, "glass_id"].astype(str)
            )
            y_values = [
                float(code_rows["ratio"].get(glass_id, 0.0)) * 100
                for glass_id in glass_ids
            ]
            figure.add_bar(
                x=glass_ids,
                y=y_values,
                name=code,
                marker_color=qualitative.Plotly[index % len(qualitative.Plotly)],
                hovertemplate="%{x}<br>" + code + " %{y:.1f}%<extra></extra>",
            )

    figure.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        height=430,
        margin={"l": 50, "r": 24, "t": 108, "b": 110},
        barmode="stack",
        bargap=0.35,
        legend={
            "orientation": "h",
            "x": 0.5,
            "xanchor": "center",
            "y": 1.02,
            "yanchor": "bottom",
        },
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        xaxis={
            "title": "Glass ID", "showgrid": False, "type": "category",
            "tickangle": -45, "automargin": True,
        },
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
