"""Public project-count and pass-rate series for capability summaries."""

from __future__ import annotations

import math

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


PERIOD_STYLES = {
    "Y": ("年度", "#577590"),
    "Q": ("季度", "#43AA8B"),
    "M": ("月度", "#F9A03F"),
    "W": ("周度", "#277DA1"),
}


def _number(value: object, *, percent: bool = False) -> float | None:
    if isinstance(value, str):
        value = value.strip()
        if value.endswith("%"):
            value = value[:-1]
            percent = True
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric / 100 if percent else numeric


def create_capability_trend_chart(summary: pd.DataFrame, *, metric: str) -> go.Figure:
    """Preserve display order and never join a line across period types."""
    if metric not in {"cpk", "cpm"}:
        raise ValueError("Unsupported capability metric")
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    periods = [column for column in summary.columns if column != "指标"]
    rows = summary.set_index("指标") if "指标" in summary else pd.DataFrame()
    total_name, rate_name = f"{metric.upper()}总项目数", f"{metric.capitalize()}≥1.33达标率"
    for prefix, (label, color) in PERIOD_STYLES.items():
        selected = [period for period in periods if str(period).startswith(prefix)]
        if not selected:
            continue
        totals = [_number(rows.at[total_name, period]) if total_name in rows.index else None for period in selected]
        rates = [
            1.0 if total == 0 else (_number(rows.at[rate_name, period]) if rate_name in rows.index else None)
            for period, total in zip(selected, totals)
        ]
        figure.add_trace(go.Bar(
            x=selected, y=totals, name=f"{label}总项目数", marker_color=color,
            offsetgroup="project_count",
            hovertemplate="%{x}<br>总项目数：%{y:,.0f}<extra></extra>",
        ), secondary_y=False)
        figure.add_trace(go.Scatter(
            x=selected, y=rates, name=f"{label}达标率", mode="lines+markers",
            connectgaps=False, line={"color": color, "width": 3},
            marker={"size": 7},
            hovertemplate="%{x}<br>达标率：%{y:.2%}<extra></extra>",
        ), secondary_y=True)
    figure.update_xaxes(type="category", categoryorder="array", categoryarray=periods, title=None)
    figure.update_yaxes(title_text="总项目数", rangemode="tozero", secondary_y=False)
    figure.update_yaxes(title_text="达标率", tickformat=".0%", range=[0, 1.05], secondary_y=True)
    figure.update_layout(
        barmode="group", height=440, margin={"t": 20, "b": 75, "l": 55, "r": 55},
        legend={"orientation": "h", "y": -0.2}, hovermode="x unified",
    )
    return figure
