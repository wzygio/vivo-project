"""Residence over entry time, with only report-visible fields in hover data."""

import pandas as pd
import plotly.graph_objects as go


def build_chamber_figure(details: pd.DataFrame, *, chamber: str) -> go.Figure:
    figure = go.Figure()
    for (product, line), group in details.groupby(['prod_code', 'line'], sort=False):
        measured = group.loc[group['duration_seconds'].notna()].sort_values('entry_time')
        figure.add_trace(go.Scattergl(
            x=measured['entry_time'], y=measured['duration_seconds'], mode='markers',
            name=f'{product} · {line}', marker={'size': 5, 'opacity': 0.75},
            customdata=measured[['glass_id', 'status']].to_numpy(),
            hovertemplate=('进片时间 %{x|%m-%d %H:%M:%S}<br>GlassID %{customdata[0]}'
                           '<br>停留时间 %{y:.1f} 秒<br>%{customdata[1]}<extra>%{fullData.name}</extra>'),
        ))
    for target in details['target_seconds'].dropna().unique():
        figure.add_hline(y=float(target), line_dash='dash', line_color='#ef4444',
                         annotation_text=f'目标 {target:g} 秒')
    figure.update_layout(
        title={'text': chamber.replace('->', ' → '), 'x': 0.5}, height=400,
        xaxis_title='进片时间', yaxis_title='停留时间（秒）',
        margin={'l': 45, 'r': 25, 't': 55, 'b': 55},
        legend={'orientation': 'h', 'y': -0.2},
    )
    figure.update_yaxes(rangemode='tozero')
    figure.update_xaxes(tickformat='%m-%d<br>%H:%M')
    if details['entry_time'].nunique() == 1:
        moment = pd.Timestamp(details['entry_time'].iloc[0])
        figure.update_xaxes(range=[moment - pd.Timedelta(hours=1), moment + pd.Timedelta(hours=1)])
    return figure
