"""Plot preserved weekly capability records without recalculating SPC measurements."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from src.inline_domain.core.monitor.cpk_summary import CPK_THRESHOLD


def build_cpk_ledger_figure(records: pd.DataFrame) -> go.Figure:
    """One indicator, with missing weeks left as gaps instead of inferred values."""
    weekly = records.sort_values("week_start").set_index("week_start")
    weekly = weekly.reindex(pd.date_range(weekly.index.min(), weekly.index.max(), freq="W-MON"))
    labels = weekly.index.strftime("%G-W%V").tolist()
    values = [float(value) if pd.notna(value) else None for value in weekly["cpk"]]
    statuses = weekly["status"].fillna("无台账记录").tolist()
    figure = go.Figure(go.Scatter(
        x=labels, y=values, mode="lines+markers", connectgaps=False,
        line={"color": "#5470c6"},
        marker={"size": 9, "color": ["#d9534f" if s == "预警" else "#5470c6" for s in statuses]},
        customdata=statuses,
        hovertemplate="%{x}<br>CPK：%{y:.4f}<br>%{customdata}<extra></extra>",
        name="台账 CPK",
    ))
    figure.add_hline(y=CPK_THRESHOLD, line_dash="dash", line_color="#d9534f",
                     annotation_text=f"CPK 阈值 {CPK_THRESHOLD:.2f}")
    figure.update_layout(height=320, margin={"l": 40, "r": 30, "t": 30, "b": 40},
                         xaxis_title="ISO 周", yaxis_title="CPK", showlegend=False)
    figure.update_xaxes(type="category")
    return figure
