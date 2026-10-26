import pandas as pd

from app.charts.yield_domain.period_trend_chart import create_period_trend_chart


def data(periods):
    return pd.DataFrame([
        {'period': period, 'defect_group': group, 'defect_rate': rate,
         'total_panels': 1000 + i, 'flag': 'INTERNAL'}
        for i, period in enumerate(periods)
        for group, rate in [('Array_Line', .01), ('OLED_Mura', .02)]
    ])


def test_period_groups_have_gaps_labels_and_preserve_values_and_hover():
    periods = ['2026-H1', '2026-H2', '2026-Q1', '2026-Q2', '2026-06', '2026-07']
    figure = create_period_trend_chart(data(periods), periods, 'A')
    bars = [trace for trace in figure.data if trace.type == 'bar']
    positions = list(bars[0].x)
    assert positions[2] - positions[1] > positions[1] - positions[0]
    assert positions[4] - positions[3] > positions[3] - positions[2]
    assert list(bars[0].y) == [.01] * 6
    assert list(bars[0].customdata) == periods
    assert {annotation.text for annotation in figure.layout.annotations} == {'年度（半年）', '季度', '月度'}
    assert list(figure.layout.xaxis.tickvals) == positions
    line = next(trace for trace in figure.data if trace.type == 'scatter')
    assert [value for value in line.y if value is not None] == list(range(1000, 1006))
    assert list(line.y).count(None) == 2
    assert 'INTERNAL' not in figure.to_json()


def test_single_granularity_has_no_separator_or_artificial_period():
    periods = ['2026-06', '2026-08']
    figure = create_period_trend_chart(data(periods), periods, 'A')
    assert len(figure.layout.shapes) == 0
    assert [annotation.text for annotation in figure.layout.annotations] == ['月度']
    assert list(figure.data[0].x) == [0, 1]
    assert list(figure.data[-1].customdata) == periods
