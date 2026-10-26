"""Public Group loss bars and independent physical-throughput line."""
import plotly.graph_objects as go
import pandas as pd

COLORS = ['#1930ff', '#6fb9ff', '#8994f1', '#c9c7c7', '#f0b05a', '#76b6a5']


def _period_positions(periods):
    positions, sections = [], []
    previous = None
    for period in periods:
        kind = '年度（半年）' if '-H' in period else ('季度' if '-Q' in period else '月度')
        position = positions[-1] + (1.9 if kind != previous else 1) if positions else 0
        if kind != previous:
            sections.append({'label': kind, 'start': position, 'end': position})
        else:
            sections[-1]['end'] = position
        positions.append(position)
        previous = kind
    return positions, sections


def create_period_trend_chart(data: pd.DataFrame, periods: list[str], product: str):
    frame = data[['period', 'defect_group', 'defect_rate', 'total_panels']].copy()
    positions, sections = _period_positions(periods)
    figure = go.Figure()
    for index, (group, rows) in enumerate(frame.groupby('defect_group', sort=True)):
        values = rows.set_index('period').defect_rate.reindex(periods)
        figure.add_bar(x=positions, y=values, width=.68, customdata=periods,
                       name=group, marker_color=COLORS[index % len(COLORS)],
                       hovertemplate='%{customdata}<br>%{y:.2%}<extra>%{fullData.name}</extra>')
    inputs = frame[['period', 'total_panels']].drop_duplicates('period').set_index('period').total_panels.reindex(periods)
    line_x, line_y, line_periods = [], [], []
    for index, (position, period, count) in enumerate(zip(positions, periods, inputs)):
        if index and position - positions[index - 1] > 1.1:
            line_x.append((position + positions[index - 1]) / 2)
            line_y.append(None)
            line_periods.append(None)
        line_x.append(position)
        line_y.append(count)
        line_periods.append(period)
    figure.add_scatter(x=line_x, y=line_y, customdata=line_periods, name='过货数', yaxis='y2', mode='lines+markers',
                       line={'color': '#777777', 'dash': 'dot', 'width': 2},
                       connectgaps=False, hovertemplate='%{customdata}<br>过货数：%{y:,.0f}<extra></extra>')
    figure.update_layout(title=f'{product} 不良率趋势', barmode='stack', height=400,
        plot_bgcolor='white', paper_bgcolor='white', font={'family': 'Microsoft YaHei', 'size': 12},
        margin={'l': 55, 'r': 65, 't': 60, 'b': 75},
        legend={'orientation': 'h', 'y': 1.12, 'x': 0},
        xaxis={'type': 'linear', 'tickmode': 'array', 'ticktext': [p.split('-', 1)[1] for p in periods],
               'tickvals': positions, 'range': [-.8, (positions[-1] if positions else 0) + .8]},
        yaxis={'title': '不良率', 'tickformat': '.1%', 'rangemode': 'tozero', 'gridcolor': '#edf0f4'},
        yaxis2={'title': '过货数', 'overlaying': 'y', 'side': 'right', 'rangemode': 'tozero', 'showgrid': False})
    for index, section in enumerate(sections):
        figure.add_annotation(x=(section['start'] + section['end']) / 2, y=-.20,
            xref='x', yref='paper', text=section['label'], showarrow=False,
            font={'color': '#64748b'}, yanchor='top')
        if index:
            boundary = (sections[index-1]['end'] + section['start']) / 2
            figure.add_shape(type='line', x0=boundary, x1=boundary, y0=0, y1=1,
                xref='x', yref='paper', line={'color': '#cbd5e1', 'width': 1, 'dash': 'dot'})
    return figure
