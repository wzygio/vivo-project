"""Deterministic lifetime curves; inputs contain no source panel identifiers."""

import pandas as pd
import plotly.graph_objects as go

from src.iqc_domain.core.lifetime.lifetime import (
    FORECAST_ANCHOR_HOURS,
    FORECAST_TARGET_HOURS,
    predict_lifetime_value,
)

COLORS = ('#477da8', '#47a99d', '#6e8dad', '#a2a59b', '#d27479', '#ac8ac4', '#bc944f')


def build_lifetime_chart(group: pd.DataFrame, *, metric: str = '效率衰减') -> go.Figure:
    figure = go.Figure()
    for number, samples in group.groupby('样品编号', sort=True):
        points = samples.sort_values('测试时间', kind='stable')
        color = COLORS[(int(number) - 1) % len(COLORS)]
        figure.add_trace(go.Scatter(
            x=points['测试时间'].tolist(), y=points[metric].tolist(),
            name=str(number), legendgroup=str(number),
            mode='lines+markers', connectgaps=False,
            line={'color': color, 'width': 2},
            marker={'size': 6},
            hovertemplate=(f'样品编号：{number}<br>测试时间：%{{x:g}}'
                           f'<br>{metric}：%{{y:.4f}}<extra></extra>'),
        ))
        prediction = predict_lifetime_value(points, metric=metric)
        if prediction is not None:
            anchor_time = FORECAST_ANCHOR_HOURS[-1]
            anchor_value = points.loc[points['测试时间'].eq(anchor_time), metric].iloc[0]
            figure.add_trace(go.Scatter(
                x=[anchor_time, FORECAST_TARGET_HOURS], y=[anchor_value, prediction],
                name=f'{number}（预测）', legendgroup=str(number), showlegend=False,
                mode='lines+markers', connectgaps=False,
                line={'color': color, 'width': 2, 'dash': 'dash'}, marker={'size': 6},
                customdata=['实测', '预测'],
                hovertemplate=(f'样品编号：{number}<br>测试时间：%{{x:g}}'
                               f'<br>{metric}（%{{customdata}}）：%{{y:.4f}}<extra></extra>'),
            ))
    figure.update_layout(
        height=320, margin={'l': 12, 'r': 12, 't': 74, 'b': 12},
        paper_bgcolor='white', plot_bgcolor='white',
        font={'family': 'Microsoft YaHei, sans-serif', 'color': '#334155'},
        legend={'title': {'text': '样品编号'}, 'orientation': 'h',
                'x': 0, 'xanchor': 'left', 'y': 1.02, 'yanchor': 'bottom'},
        xaxis={'title': '测试时间', 'type': 'linear', 'gridcolor': '#edf1f5',
               'tickmode': 'linear', 'tick0': 0, 'dtick': 100, 'tickformat': '.0f'},
        yaxis={'title': metric, 'tickformat': '.3f', 'gridcolor': '#edf1f5',
               'rangemode': 'normal'},
    )
    return figure
