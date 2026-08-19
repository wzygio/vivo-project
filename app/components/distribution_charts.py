from __future__ import annotations

from collections.abc import Iterable

import plotly.graph_objects as go


def create_box_distribution_trace(
    *,
    y_values: Iterable[object],
    name: str,
    color: str,
    x_values: Iterable[object] | None = None,
    fillcolor: str | None = None,
    showlegend: bool = False,
    legendgroup: str | None = None,
    width: float | None = None,
    hovertemplate: str | None = None,
) -> go.Box:
    """Create the shared box-distribution trace used by Inline reports."""
    return go.Box(
        x=x_values,
        y=y_values,
        name=name,
        boxpoints=False,
        marker_color=color,
        fillcolor=fillcolor,
        line={"color": color, "width": 1.4},
        showlegend=showlegend,
        legendgroup=legendgroup,
        width=width,
        hovertemplate=hovertemplate,
    )


def create_point_line_trace(
    *,
    x_values: Iterable[object],
    y_values: Iterable[object],
    name: str,
    color: str,
    customdata: Iterable[object] | None = None,
    hovertemplate: str | None = None,
) -> go.Scatter:
    """Create a point-line trace that preserves every supplied measurement."""
    return go.Scatter(
        x=x_values,
        y=y_values,
        mode="lines+markers",
        name=name,
        line={"color": color, "width": 2},
        marker={"size": 7},
        customdata=customdata,
        hovertemplate=hovertemplate,
    )
