import pandas as pd

from app.charts.indicator_domain.qtime.chamber_chart import build_chamber_figure
from src.indicator_domain.core.qtime.chamber import exclude_chamber_outliers

PRODUCTS = ('M626', 'M678')


def readings():
    return pd.DataFrame({
        'prod_code': ['M626', 'M678', 'M626'], 'line': ['3CEE002'] * 3,
        'glass_id': ['late', 'first', 'second'],
        'entry_time': pd.to_datetime(['2026-09-23 20:01:00', '2026-09-23 01:01:00', '2026-09-23 01:59:59']),
        'duration_seconds': [300, 100, 200], 'target_seconds': [6000] * 3,
        'status': ['正常'] * 3,
    })


def test_bars_keep_each_passage_equally_spaced_and_show_hours_only():
    figure = build_chamber_figure(readings(), chamber='OC2->OC3', product_order=PRODUCTS)
    assert {trace.type for trace in figure.data} == {'bar'}
    points = sorted((x, y, extra) for trace in figure.data
                    for x, y, extra in zip(trace.x, trace.y, trace.customdata))
    assert [point[0] for point in points] == [0, 1, 2]
    assert [point[1] for point in points] == [100, 200, 300]
    assert [point[2][0] for point in points] == ['09-23 01时', '09-23 01时', '09-23 20时']
    assert ':59' not in figure.to_json()
    assert figure.layout.yaxis.range[1] < 6000
    assert '6000' in str(figure.layout.annotations)


def test_product_colors_stay_stable_after_filtering():
    details = readings()
    full = build_chamber_figure(details, chamber='OC2->OC3', product_order=PRODUCTS)
    filtered = build_chamber_figure(details.loc[details['prod_code'].eq('M678')], chamber='OC2->OC3', product_order=PRODUCTS)
    expected = next(trace.marker.color for trace in full.data if trace.name == 'M678')
    assert filtered.data[0].marker.color == expected


def test_missing_values_do_not_create_empty_product_legend():
    details = readings()
    details.loc[details['prod_code'].eq('M678'), 'duration_seconds'] = None
    figure = build_chamber_figure(details, chamber='OC2->OC3', product_order=PRODUCTS)
    assert [trace.name for trace in figure.data] == ['M626']


def test_outliers_removed_per_measurement_without_changing_source_or_missing_counts():
    details = pd.DataFrame({'glass_id': ['A'] * 5, 'duration_seconds': [0, 1000, 1000.01, 6000, None]})
    actual = exclude_chamber_outliers(details)
    assert actual['duration_seconds'].iloc[:2].tolist() == [0, 1000]
    assert len(actual) == 3 and actual['duration_seconds'].isna().sum() == 1
    assert details['duration_seconds'].iloc[2] == 1000.01
