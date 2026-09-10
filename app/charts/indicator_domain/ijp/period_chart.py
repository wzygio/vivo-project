"""Five period stacks with CODE annotations and reporting ratios."""

import pandas as pd
import plotly.graph_objects as go

from app.charts.indicator_domain.ijp.printer_chart import CODE_COLORS
from src.indicator_domain.core.ijp.period_summary import CODE_DESCRIPTIONS
from src.indicator_domain.core.ijp.printer_summary import PRINTER_CODES


def build_ijp_period_figure(ratios: pd.DataFrame, *, title: str) -> go.Figure:
    periods = list(dict.fromkeys(ratios.period))
    figure = go.Figure()
    for code, color in zip(PRINTER_CODES, CODE_COLORS, strict=True):
        rows = ratios[ratios.rs_code == code].set_index("period").reindex(periods)
        figure.add_bar(
            x=periods, y=rows.display_ratio * 100, name=f"{code}：{CODE_DESCRIPTIONS[code]}",
            marker_color=color,
            customdata=rows[["code_num", "period_start", "period_end"]].to_numpy(),
            hovertemplate=(f"{code}：{CODE_DESCRIPTIONS[code]}<br>" +
                "%{x}<br>%{customdata[1]} 至 %{customdata[2]}"
                "<br>占比 %{y:.2f}%"
                "<br>记录数 %{customdata[0]:,.0f}<extra></extra>"),
        )
    for period in periods:
        if not ratios[ratios.period == period].has_data.any():
            figure.add_annotation(x=period, y=0, text="无数据", showarrow=False, yshift=12)
    figure.update_layout(
        title={"text": title, "x": 0.5}, barmode="stack", height=570,
        margin={"l": 50, "r": 24, "t": 65, "b": 210},
        legend={"orientation": "h", "x": 0, "y": -0.3, "yanchor": "top",
                "font": {"size": 10}},
        xaxis={"title": "月份 / 周（周一开始）", "type": "category",
               "categoryorder": "array", "categoryarray": periods},
        yaxis={"title": "占比（%）", "range": [0, 100], "ticksuffix": "%"},
        plot_bgcolor="white", paper_bgcolor="white", bargap=0.35,
    )
    return figure
