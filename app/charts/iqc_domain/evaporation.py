"""Public IQC/COA inspection markers, styled like chamber Q-Time charts."""

from html import escape

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative

from src.iqc_domain.core.eva_materials.evaporation import TREND_CHARACTERISTICS

POINT_COLUMNS = (
    '物料号', '物料描述', '产品型号', '特性项目', '检验时间',
    'IQC-1', 'COA-1', '规格上限', '规格下限', '规格样式1', '规格样式2',
)
MEASUREMENTS = ('IQC-1', 'COA-1')

def prepare_evaporation_points(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep individual measured inspections; never aggregate or fill missing values."""
    points = frame.loc[:, list(POINT_COLUMNS)].copy()
    points['检验时间'] = pd.to_datetime(points['检验时间'], errors='coerce')
    for column in (*MEASUREMENTS, '规格上限', '规格下限'):
        points[column] = pd.to_numeric(points[column], errors='coerce').replace(
            [np.inf, -np.inf], np.nan,
        )
    return points.loc[
        points['特性项目'].isin(TREND_CHARACTERISTICS)
        & points['检验时间'].notna()
        & points[list(MEASUREMENTS)].notna().any(axis=1)
    ].sort_values('检验时间', kind='stable').reset_index(drop=True)


def _spec_labels(points: pd.DataFrame, column: str, operator: str) -> list[str]:
    return [
        f'{escape(str(op)) if pd.notna(op) else ""} {value:g}'.strip()
        if pd.notna(value) else '未提供'
        for op, value in zip(points[operator], points[column])
    ]


def _add_specification(figure: go.Figure, points: pd.DataFrame, column: str) -> None:
    values = points[column]
    if not values.notna().any():
        return
    if values.notna().all() and values.nunique() == 1:
        value = float(values.iloc[0])
        figure.add_hline(
            y=value, line_dash='dash', line_color='#ef4444',
            annotation_text=f'{column} {value:g}', annotation_position='bottom right',
        )
        return
    # Changing or partially missing bounds belong to individual inspections.
    # Do not draw every historical specification across the whole time range.
    figure.add_trace(go.Scatter(
        x=points.index.tolist(), y=[float(v) if pd.notna(v) else None for v in values],
        mode='lines+markers', name=column, connectgaps=False,
        line={'color': '#ef4444', 'dash': 'dash', 'shape': 'hv', 'width': 1.5},
        marker={'color': '#ef4444', 'size': 3},
        customdata=points['检验时间'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        hovertemplate='检验时间 %{customdata}<br>%{fullData.name} %{y:g}<extra></extra>',
    ))


def build_evaporation_figure(frame: pd.DataFrame, *, characteristic: str) -> go.Figure:
    """One equally spaced position per inspection with the two first-point series."""
    points = prepare_evaporation_points(frame)
    figure = go.Figure()
    if points.empty:
        return figure
    hover = pd.DataFrame({
        'time': points['检验时间'].dt.strftime('%Y-%m-%d %H:%M:%S'),
        'material': points['物料号'].fillna('').map(lambda v: escape(str(v))),
        'product': points['产品型号'].fillna('').map(lambda v: escape(str(v))),
        'upper': _spec_labels(points, '规格上限', '规格样式1'),
        'lower': _spec_labels(points, '规格下限', '规格样式2'),
    })
    for index, measurement in enumerate(MEASUREMENTS):
        measured = points.loc[points[measurement].notna()]
        if measured.empty:
            continue
        figure.add_trace(go.Scatter(
            x=measured.index.tolist(), y=measured[measurement].tolist(), mode='markers',
            name=measurement,
            marker={'color': qualitative.Dark24[index], 'size': 4, 'opacity': 0.7},
            customdata=hover.loc[measured.index].to_numpy(),
            hovertemplate=(
                '检验时间 %{customdata[0]}<br>物料号 %{customdata[1]}'
                '<br>产品型号 %{customdata[2]}<br>测量值 %{y:g}'
                '<br>规格上限 %{customdata[3]}<br>规格下限 %{customdata[4]}'
                '<extra>%{fullData.name}</extra>'
            ),
        ))
    for column in ('规格上限', '规格下限'):
        _add_specification(figure, points, column)
    ticks = points.iloc[::max(1, (len(points) + 9) // 10)]
    figure.update_layout(
        title={'text': escape(str(characteristic)), 'x': 0.5}, height=400,
        xaxis_title='检验时间', yaxis_title='测量值',
        margin={'l': 45, 'r': 25, 't': 75, 'b': 65},
        legend={'orientation': 'h', 'y': -0.25},
    )
    figure.update_xaxes(
        type='linear', tickmode='array', tickvals=ticks.index.tolist(),
        ticktext=ticks['检验时间'].dt.strftime('%Y-%m-%d<br>%H:%M').tolist(),
        range=[-0.5, max(len(points) - 0.5, 0.5)],
    )
    return figure
