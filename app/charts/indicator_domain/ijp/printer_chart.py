"""Six fixed CODE bars for one Printer in the selected reporting window."""

import pandas as pd
import plotly.graph_objects as go

from src.indicator_domain.core.ijp.printer_summary import PRINTER_CODES

CODE_COLORS = ("#b00040", "#008000", "#9dccff", "#ff6600", "#9467bd", "#80df96")


def build_ijp_printer_figure(ratios: pd.DataFrame, *, title: str) -> go.Figure:
    frame = ratios.set_index("rs_code").reindex(PRINTER_CODES, fill_value=0)
    figure = go.Figure(go.Bar(
        x=list(PRINTER_CODES),
        y=frame["display_ratio"] * 100,
        marker_color=list(CODE_COLORS),
        text=[f"{value:.2%}" for value in frame["display_ratio"]],
        textposition="outside",
        cliponaxis=False,
        customdata=frame[["raw_ratio", "code_num"]].to_numpy(),
        hovertemplate=(
            "%{x}<br>显示占比 %{y:.2f}%<br>实际占比 %{customdata[0]:.2%}"
            "<br>记录数 %{customdata[1]:,.0f}<extra></extra>"
        ),
    ))
    figure.update_layout(
        title={"text": title, "x": 0.5, "xanchor": "center"},
        height=430, margin={"l": 50, "r": 24, "t": 72, "b": 60},
        showlegend=False, bargap=0.45,
        plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
        xaxis={"title": "CODE", "type": "category", "categoryorder": "array",
               "categoryarray": list(PRINTER_CODES)},
        yaxis={"title": "显示占比（%）", "range": [0, 110],
               "tickvals": [0, 20, 40, 60, 80, 100], "ticksuffix": "%"},
    )
    return figure
