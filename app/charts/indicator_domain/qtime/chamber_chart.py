"""SVG residence bars: one evenly spaced position per measured passage."""

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
        figure.add_trace(go.Bar(
            x=group.index.tolist(), y=group['duration_seconds'].tolist(), width=0.8,
            name=str(product), marker_color=colors[product], showlegend=True,
            customdata=group[['hour', 'glass_id', 'status']].to_numpy(),
            hovertemplate=('过货时间 %{customdata[0]}<br>GlassID %{customdata[1]}'
                           '<br>停留时间 %{y:.1f} 秒<br>%{customdata[2]}<extra>%{fullData.name}</extra>'),
        ))
    # Out-of-view specifications remain visible as text without flattening bars.
    upper = max(float(measured['duration_seconds'].max()), 1.0) * 1.1 if not measured.empty else 1.0
    for target in details['target_seconds'].dropna().unique():
        if target <= upper:
            figure.add_hline(y=float(target), line_dash='dash', line_color='#ef4444',
                             annotation_text=f'目标 {target:g} 秒')
        else:
            figure.add_annotation(x=1, y=1.08, xref='paper', yref='paper',
                                  text=f'目标 {target:g} 秒（超出当前纵轴范围）',
                                  showarrow=False, xanchor='right')
    ticks = measured.iloc[::max(1, (len(measured) + 9) // 10)]
    figure.update_layout(
        title={'text': chamber.replace('->', ' → '), 'x': 0.5}, height=400,
        xaxis_title='过货时间（小时，逐片等距）', yaxis_title='停留时间（秒）',
        margin={'l': 45, 'r': 25, 't': 75, 'b': 65},
        legend={'orientation': 'h', 'y': -0.25}, barmode='overlay',
    )
    figure.update_yaxes(range=[0, upper])
    figure.update_xaxes(type='linear', tickmode='array', tickvals=ticks.index.tolist(),
                        ticktext=ticks['hour'].tolist(), range=[-0.5, max(len(measured) - 0.5, 0.5)])
    return figure
