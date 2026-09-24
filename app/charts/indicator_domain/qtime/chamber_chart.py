"""SVG residence markers: one evenly spaced position per measured passage."""

import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative

def build_chamber_figure(
    details: pd.DataFrame, *, chamber: str, product_order: tuple[str, ...],
) -> go.Figure:
    measured = details.loc[details['duration_seconds'].notna()].sort_values(
        ['entry_time', 'glass_id'], kind='stable',
    ).reset_index(drop=True)
    measured = measured.assign(hour=measured['entry_time'].dt.strftime('%m-%d %H时'))
    colors = {product: qualitative.Dark24[index % len(qualitative.Dark24)]
              for index, product in enumerate(product_order)}
    figure = go.Figure()
    for product, group in measured.groupby('prod_code', sort=False):
        figure.add_trace(go.Scatter(
            x=group.index.tolist(), y=group['duration_seconds'].tolist(), mode='markers',
            name=str(product), marker={'color': colors[product], 'size': 4, 'opacity': 0.7}, showlegend=True,
            customdata=group[['hour', 'glass_id', 'status']].to_numpy(),
            hovertemplate=('过货时间 %{customdata[0]}<br>GlassID %{customdata[1]}'
                           '<br>停留时间 %{y:.1f} 秒<br>%{customdata[2]}<extra>%{fullData.name}</extra>'),
        ))
    for target in details['target_seconds'].dropna().unique():
        figure.add_hline(y=float(target), line_dash='dash', line_color='#ef4444',
                         annotation_text=f'{target:g} 秒', annotation_position='bottom right')
    ticks = measured.iloc[::max(1, (len(measured) + 9) // 10)]
    figure.update_layout(
        title={'text': chamber.replace('->', ' → '), 'x': 0.5}, height=400,
        xaxis_title='过货时间（小时，逐片等距）', yaxis_title='停留时间（秒）',
        margin={'l': 45, 'r': 25, 't': 75, 'b': 65},
        legend={'orientation': 'h', 'y': -0.25},
    )
    figure.update_yaxes(range=[0, float(details['target_seconds'].max())], autorange=False)
    figure.update_xaxes(type='linear', tickmode='array', tickvals=ticks.index.tolist(),
                        ticktext=ticks['hour'].tolist(), range=[-0.5, max(len(measured) - 0.5, 0.5)])
    return figure
