"""Five ordered monthly/weekly stacks for large-hole monitoring."""

import pandas as pd
import plotly.graph_objects as go

from src.indicator_domain.core.ijp_hole.summary import HOLE_DISPLAY_CODES
from app.utils.ijp_labels import format_ijp_code, get_ijp_code_descriptions

COLORS = ('#B8BBC4', '#75DF99', '#64C8FA', '#FFDA83')


def build_hole_figure(frame: pd.DataFrame, *, title: str) -> go.Figure:
    periods = list(dict.fromkeys(frame.period))
    figure = go.Figure()
    descriptions = get_ijp_code_descriptions('孔区')
    for code, color in zip(HOLE_DISPLAY_CODES, COLORS, strict=True):
        selected = frame[frame.rs_code.eq(code)]
        if selected.empty:
            continue
        selected = selected.set_index('period').reindex(periods)
        figure.add_bar(
            name=format_ijp_code(code, descriptions), x=periods, y=selected.ratio,
            marker_color=color, text=selected.ratio, texttemplate='%{y:.1%}',
            textposition='auto',
            customdata=selected[['period_start', 'period_end']].to_numpy(),
            hovertemplate=('%{x}<br>%{customdata[0]} 至 %{customdata[1]}<br>'
                           + format_ijp_code(code, descriptions) + '：%{y:.1%}<extra></extra>'),
        )
    for period in periods:
        if not frame[frame.period.eq(period)].has_data.any():
            figure.add_annotation(x=period, y=0, text='无数据', showarrow=False, yshift=12)
    figure.update_layout(
        title={'text': title, 'x': 0.5}, height=390,
        barmode='stack',
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=45, r=12, t=85, b=75),
        legend=dict(orientation='h', x=0.5, xanchor='center', y=1.13, traceorder='normal'),
        yaxis=dict(title='占比', tickformat='.0%', range=[0, 1.05], gridcolor='#DDDDDD'),
        xaxis=dict(type='category', title='月份 / 周（周一开始）',
                   categoryorder='array', categoryarray=periods),
    )
    return figure
