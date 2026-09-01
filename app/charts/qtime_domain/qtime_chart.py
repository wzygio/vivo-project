"""Deterministic Plotly model for the Q-Time Lot chart."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


BAR_COLOR = "#31c7c9"
SPEC_COLOR = "#ef4444"


def build_qtime_figure(details: pd.DataFrame) -> go.Figure:
    """Build the Lot wait-time bars and optional row-level specification line."""
    frame = details.copy()
    lot_ids = frame.get("lot_id", pd.Series(dtype="object")).fillna("").astype(str)
    wait_times = _numeric_column(frame, "wait_time")
    step_names = frame.get("step_desc", pd.Series(dtype="object")).dropna()
    series_name = str(step_names.iloc[0]) if not step_names.empty else "Q-Time"

    figure = go.Figure()
    figure.add_bar(
        x=lot_ids,
        y=wait_times,
        name=series_name,
        marker_color=BAR_COLOR,
        hovertemplate="Lot %{x}<br>等待时长 %{y:.2f} h<extra></extra>",
    )

    specs = _numeric_column(frame, "q_spec")
    if specs.notna().any():
        figure.add_scatter(
            x=lot_ids,
            y=specs,
            name="QTime规格",
            mode="lines",
            line={"color": SPEC_COLOR, "width": 2},
            hovertemplate="QTime规格 %{y:.2f} h<extra></extra>",
        )

    figure.update_layout(
        title={"text": "北极星QTime监控", "x": 0.5, "xanchor": "center"},
        height=430,
        margin={"l": 50, "r": 24, "t": 72, "b": 110},
        legend={"orientation": "h", "x": 0.5, "xanchor": "center", "y": 1.02},
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        bargap=0.35,
        xaxis={"title": "Lot ID", "tickangle": -90, "showgrid": False},
        yaxis={
            "title": "等待时长（小时）",
            "rangemode": "tozero",
            "gridcolor": "#d7dde5",
            "zeroline": False,
        },
        hovermode="x unified",
    )
    return figure


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")
