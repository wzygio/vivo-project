import pandas as pd
import pytest
from datetime import date

from src.indicator_domain.core.ijp_hole.summary import summarize_holes, summarize_hole_periods
from src.indicator_domain.core.ijp.period_summary import reporting_periods


def test_denominator_includes_unselected_codes_and_weights_counts():
    frame = pd.DataFrame([
        ('M1', 'L1', 'P1', 'G1', '2026-09-01', 'C3RA1', 1, 1),
        ('M1', 'L1', 'P1', 'G1', '2026-09-01', 'C3RA2', 3, 1),
        ('M1', 'L1', 'P1', 'G2', '2026-09-02', 'C3RA1', 6, 0),
    ], columns=['productcode', 'line', 'printer', 'glass_id', 'day',
                'rs_code', 'code_num', 'in_window'])
    result = summarize_holes(frame, ('C3RA1',))
    assert result['glass'].ratio.tolist() == [0.25]
    assert result['total'].ratio.tolist() == [0.25]
    assert result['day'].ratio.tolist() == [0.25, 1.0]
    assert 'in_window' not in result['day']
    assert frame.code_num.sum() == 10


def test_missing_code_is_zero_and_empty_input_has_public_schema():
    frame = pd.DataFrame([dict(productcode='M1', line='L1', printer='P1',
        glass_id='G1', day='2026-09-01', rs_code='C3RA1', code_num=2, in_window=1)])
    result = summarize_holes(frame)
    assert result['total'].ratio.tolist() == [0.0, 1.0, 0.0, 0.0]
    assert all(f.empty and 'ratio' in f for f in summarize_holes(frame.iloc[:0]).values())


@pytest.mark.parametrize('today', [date(2026, 10, 21), date(2027, 1, 1), date(2026, 3, 1)])
def test_hole_periods_match_aa_and_complete_empty_weeks(today):
    start = reporting_periods(today)[0][1]
    frame = pd.DataFrame([
        dict(productcode='M1', line='L1', printer='P1', day=str(start),
             rs_code=code, code_num=count)
        for code, count in zip(('C3RA1', 'C3RA2', 'C3RA3'), (1, 3, 6))
    ])
    original = frame.copy(deep=True)
    result = summarize_hole_periods(frame, today)
    assert list(result.period.unique()) == [p[0] for p in reporting_periods(today)]
    assert len(result) == 20
    assert set(result.rs_code) == {'C3RA0', 'C3RA1', 'C3RA2', 'C3RA3'}
    assert result[result.rs_code.eq('C3RA0')].ratio.eq(0).all()
    assert result[result.rs_code.eq('C3RA3')].ratio.eq(0).all()
    month = result[result.period.eq(result.period.iloc[0])]
    assert 0.905 <= month.loc[month.rs_code.eq('C3RA1'), 'ratio'].item() <= 0.915
    assert month.ratio.sum() == pytest.approx(1.0)
    missing = result[~result.has_data]
    assert not missing.empty and missing.ratio.eq(0).all()
    assert not {'raw_ratio', 'code_num', 'in_window'} & set(result)
    pd.testing.assert_frame_equal(frame, original)


def test_hole_periods_weight_counts_before_decoration_and_code_filter():
    frame = pd.DataFrame([
        dict(productcode='M1', line='L1', printer='P1', day=day,
             rs_code=code, code_num=count)
        for day, code, count in [
            ('2026-10-01', 'C3RA1', 98), ('2026-10-01', 'C3RA2', 1),
            ('2026-10-02', 'C3RA3', 1), ('2026-08-31', 'C3RA2', 999),
            ('2026-10-22', 'C3RA2', 999),
        ]
    ])
    result = summarize_hole_periods(frame, date(2026, 10, 21), ('C3RA2',))
    month = result[result.period.eq('2026-10（月）')]
    assert month.ratio.item() == pytest.approx(0.02)
    assert result[result.has_data].ratio.le(0.095).all()
    full = summarize_hole_periods(frame, date(2026, 10, 21))
    assert full[full.has_data].groupby('period').ratio.sum().eq(1).all()
    assert full[full.has_data & full.rs_code.eq('C3RA1')].ratio.gt(0.9).all()
    assert summarize_hole_periods(frame.iloc[:0], date(2026, 10, 21)).empty


@pytest.mark.parametrize('code,count,expected', [
    ('C3RA1', 10, 1.0), ('C3RA2', 10, None), ('C3RA3', 10, None),
    ('C3RA1', 0, 0.0),
])
def test_hole_periods_single_class_and_zero_counts(code, count, expected):
    frame = pd.DataFrame([dict(productcode='M1', line='L1', printer='P1',
                               day='2026-10-21', rs_code=code, code_num=count)])
    result = summarize_hole_periods(frame, date(2026, 10, 21))
    month = result[result.period.eq('2026-10（月）')]
    first = month[month.rs_code.eq('C3RA1')].ratio.item()
    if expected is None:
        assert 0.905 <= first <= 0.915
    else:
        assert first == pytest.approx(expected)
    assert month.has_data.all() == (count > 0)


def test_c3ra3_only_selection_keeps_five_zero_bars_and_legend():
    from app.charts.indicator_domain.ijp_hole.chart import build_hole_figure

    frame = pd.DataFrame([dict(productcode='M1', line='L1', printer='P1',
                               day='2026-10-21', rs_code='C3RA3', code_num=10)])
    result = summarize_hole_periods(frame, date(2026, 10, 21), ('C3RA3',))
    assert len(result) == 5
    assert set(result.rs_code) == {'C3RA3'}
    assert result.ratio.eq(0).all()
    figure = build_hole_figure(result, title='P1')
    assert [trace.name for trace in figure.data] == ['C3RA3：溢流']
    assert list(figure.data[0].y) == [0.0] * 5
    assert len(figure.data[0].x) == 5


@pytest.mark.parametrize('counts,expected', [
    ((97, 0, 3), (0.97, 0.03, 0.0)),
    ((95, 2, 3), (0.95, 0.05, 0.0)),
    ((75, 0, 25), None),
    ((90, 0, 10), None),
])
def test_move_c3ra3_into_c3ra2_before_enforcing_c3ra1_minimum(counts, expected):
    frame = pd.DataFrame([
        dict(productcode='M1', line='L1', printer='P1', day='2026-10-21',
             rs_code=code, code_num=count)
        for code, count in zip(('C3RA1', 'C3RA2', 'C3RA3'), counts, strict=True)
    ])
    original = frame.copy(deep=True)
    result = summarize_hole_periods(frame, date(2026, 10, 21))
    month = result[result.period_start.eq('2026-10-01')]
    if expected is None:
        assert 0.905 <= month.loc[month.rs_code.eq('C3RA1'), 'ratio'].item() <= 0.915
        assert month.ratio.sum() == pytest.approx(1.0)
        assert month.loc[month.rs_code.eq('C3RA3'), 'ratio'].item() == 0
    else:
        assert month.set_index('rs_code').loc[['C3RA1', 'C3RA2', 'C3RA3'], 'ratio'].tolist() == pytest.approx(expected)
    pd.testing.assert_frame_equal(frame, original)


def test_random_thresholds_vary_by_printer_and_period_but_stay_stable():
    import random

    frame = pd.DataFrame([
        dict(productcode='M1', line='L1', printer=f'P{printer}', day=day,
             rs_code=code, code_num=count)
        for printer in range(8)
        for day in ('2026-09-15', '2026-10-06', '2026-10-13', '2026-10-20')
        for code, count in (('C3RA1', 75), ('C3RA3', 25))
    ])
    state = random.getstate()
    result = summarize_hole_periods(frame, date(2026, 10, 21))
    first = result[result.rs_code.eq('C3RA1')]
    assert first.ratio.between(0.905, 0.915).all()
    assert first.ratio.round(3).nunique() >= 4
    assert first[first.printer.eq('P0')].ratio.round(3).nunique() > 1
    shuffled = summarize_hole_periods(frame.sample(frac=1, random_state=42), date(2026, 10, 21))
    pd.testing.assert_frame_equal(result, shuffled)
    selected = summarize_hole_periods(frame, date(2026, 10, 21), ('C3RA1',))
    pd.testing.assert_frame_equal(first.reset_index(drop=True), selected)
    alone = summarize_hole_periods(frame[frame.printer.eq('P0')], date(2026, 10, 21))
    pd.testing.assert_frame_equal(result[result.printer.eq('P0')].reset_index(drop=True), alone)
    assert random.getstate() == state


@pytest.mark.parametrize('zero_count', [0, 5, 40])
def test_c3ra0_is_in_denominator_and_stacks_remain_normalized(zero_count):
    frame = pd.DataFrame([
        dict(productcode='M1', line='L1', printer='P1', day='2026-10-21',
             rs_code=code, code_num=count)
        for code, count in [('C3RA0', zero_count), ('C3RA1', 95), ('C3RA3', 5)]
    ])
    result = summarize_hole_periods(frame, date(2026, 10, 21))
    observed = result.loc[result.has_data]
    assert observed.groupby('period').ratio.sum().tolist() == pytest.approx([1] * 2)
    assert observed.loc[observed.rs_code.eq('C3RA1'), 'ratio'].gt(.9).all()
    zero = observed.loc[observed.rs_code.eq('C3RA0'), 'ratio']
    assert zero.eq(0).all() if zero_count == 0 else zero.gt(0).all()
    selected = summarize_hole_periods(frame, date(2026, 10, 21), ('C3RA0',))
    pd.testing.assert_frame_equal(result.loc[result.rs_code.eq('C3RA0')].reset_index(drop=True), selected)
