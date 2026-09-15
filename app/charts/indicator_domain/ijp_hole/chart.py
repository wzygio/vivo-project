"""FineReport-style stacked glass/day and grouped Total percentages."""

import pandas as pd
import plotly.graph_objects as go

from src.indicator_domain.core.ijp_hole.summary import HOLE_CODES

COLORS = ('#75DF99', '#64C8FA', '#FFDA83')


def build_hole_figure(frame: pd.DataFrame, *, title: str, view: str) -> go.Figure:
    axis = {'glass': 'glass_id', 'day': 'day', 'total': 'printer'}[view]
    figure = go.Figure()
    for code, color in zip(HOLE_CODES, COLORS, strict=True):
        selected = frame[frame.rs_code.eq(code)].sort_values(axis)
        if selected.empty:
            continue
        figure.add_bar(
            name=code, x=selected[axis].astype(str), y=selected.ratio,
            marker_color=color, text=selected.ratio, texttemplate='%{y:.1%}',
            textposition='auto',
            hovertemplate='%{x}<br>' + code + '：%{y:.1%}<extra></extra>',
        )
    figure.update_layout(
        title={'text': title, 'x': 0.5}, height=390,
        barmode='group' if view == 'total' else 'stack',
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=45, r=12, t=85, b=75),
        legend=dict(orientation='h', x=0.5, xanchor='center', y=1.13, traceorder='normal'),
        yaxis=dict(title='占比', tickformat='.0%', range=[0, 1.05], gridcolor='#DDDDDD'),
        xaxis=dict(type='category', title={'glass': 'Glass ID', 'day': '日期', 'total': 'Printer'}[view]),
    )
    return figure
