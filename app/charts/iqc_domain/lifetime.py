"""Deterministic lifetime curves; inputs contain no source panel identifiers."""

import pandas as pd
import plotly.graph_objects as go

COLORS = ('#477da8', '#47a99d', '#6e8dad', '#a2a59b', '#d27479', '#ac8ac4', '#bc944f')


def build_lifetime_chart(group: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    for number, samples in group.groupby('样品编号', sort=True):
        points = samples.sort_values('测试时间', kind='stable')
        figure.add_trace(go.Scatter(
            x=points['测试时间'].tolist(), y=points['效率衰减'].tolist(),
            name=str(number), mode='lines+markers', connectgaps=False,
            line={'color': COLORS[(int(number) - 1) % len(COLORS)], 'width': 2},
            marker={'size': 6},
            hovertemplate=(f'样品编号：{number}<br>测试时间：%{{x:g}}'
                           '<br>效率衰减：%{y:.4f}<extra></extra>'),
        ))
    figure.update_layout(
        height=320, margin={'l': 12, 'r': 12, 't': 12, 'b': 12},
        paper_bgcolor='white', plot_bgcolor='white',
        font={'family': 'Microsoft YaHei, sans-serif', 'color': '#334155'},
        legend={'title': {'text': '样品编号'}, 'orientation': 'h', 'y': 1.15},
        xaxis={'title': '测试时间', 'type': 'linear', 'gridcolor': '#edf1f5'},
        yaxis={'title': '效率衰减', 'tickformat': '.3f', 'gridcolor': '#edf1f5',
               'rangemode': 'normal'},
    )
    return figure
