"""Deterministic Plotly model for the Q-Time Lot chart."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


BAR_COLOR = "#31c7c9"
BAR_COLORS = (BAR_COLOR, "#2563eb", "#7c3aed", "#0891b2", "#0f766e")
SPEC_COLOR = "#ef4444"


def build_qtime_figure(
    details: pd.DataFrame,
    *,
    title: str = "北极星QTime监控",
) -> go.Figure:
    """Build the Lot wait-time bars and optional row-level specification line."""
    frame = details.copy()
    if "step_desc" in frame:
        step_names = frame["step_desc"].fillna("Q-Time").astype(str)
    else:
        step_names = pd.Series("Q-Time", index=frame.index, dtype="object")
    frame = frame.assign(_step_name=step_names)
    step_groups = list(frame.groupby("_step_name", sort=False))

    figure = go.Figure()
    for index, (step_name, group) in enumerate(step_groups):
        figure.add_bar(
            x=_lot_ids(group),
            y=_numeric_column(group, "wait_time"),
            name=str(step_name),
            marker_color=BAR_COLORS[index % len(BAR_COLORS)],
            hovertemplate="Lot %{x}<br>等待时长 %{y:.2f} h<extra></extra>",
        )

    for _step_name, group in step_groups:
        specs = _numeric_column(group, "q_spec")
        if not specs.notna().any():
            continue
        figure.add_scatter(
            x=_lot_ids(group),
            y=specs,
            name="QTime规格",
            mode="lines",
            line={"color": SPEC_COLOR, "width": 2},
            hovertemplate="QTime规格 %{y:.2f} h<extra></extra>",
            legendgroup="qtime-spec",
            showlegend=False,
        )

    figure.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        height=430,
        margin={"l": 50, "r": 24, "t": 72, "b": 82},
        legend={"orientation": "h", "x": 0.5, "xanchor": "center", "y": 1.02},
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        bargap=0.35,
        barmode="group",
        xaxis={"title": "Lot ID", "tickangle": -45, "showgrid": False},
        yaxis={
            "title": "等待时长（小时）",
            "rangemode": "tozero",
            "gridcolor": "#d7dde5",
            "zeroline": True,
            "zerolinecolor": "#94a3b8",
            "zerolinewidth": 1.5,
        },
        hovermode="x unified",
    )
    return figure


def _lot_ids(frame: pd.DataFrame) -> pd.Series:
    values = frame.get("lot_id", pd.Series(index=frame.index, dtype="object"))
    return values.fillna("").astype(str)


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")
