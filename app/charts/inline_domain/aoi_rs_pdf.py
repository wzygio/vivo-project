"""Figure styling and report layout entry point for AOI RS PDF exports."""

import plotly.graph_objects as go

from app.charts.inline_domain.indicator_pdf import IndicatorPdfReport as AoiRsPdfReport

__all__ = ["AoiRsPdfReport", "format_aoi_rs_pdf_figure"]


def format_aoi_rs_pdf_figure(source: go.Figure, slot: str) -> go.Figure:
    """Apply the PDF style to a copy of the figure."""
    figure = go.Figure(source)
    figure.update_layout(
        template="plotly_white",
        title={"font": {"size": 17}},
        font={
            "family": "Microsoft YaHei, Noto Sans CJK SC, Arial",
            "size": 15,
        },
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin={"l": 65, "r": 60, "t": 65, "b": 125},
    )
    if slot == "trend":
        figure.update_layout(legend={"y": -0.42})
        figure.update_xaxes(title_standoff=8)
    return figure
