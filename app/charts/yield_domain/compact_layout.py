"""Compact figure layout shared by Yield charts."""

import plotly.graph_objects as go


def apply_compact_chart_layout(fig: go.Figure, height: int) -> go.Figure:
    """Apply the compact layout to a newly built figure."""
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=34, b=26),
        title_font_size=13,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(tickfont=dict(size=10))
    fig.update_yaxes(tickfont=dict(size=10))
    return fig
