from datetime import date
import random

import pandas as pd
import pytest

from src.indicator_domain.core.qtime.chamber_decoration import decorate_chamber_points


def points(count, *, day='2026-10-12', line='3CEE001', chamber='PT->OC1', duration=400):
    return pd.DataFrame([{
        'prod_code': 'M626', 'line': line, 'chamber': chamber, 'glass_id': f'{day}-{number}',
        'entry_time': pd.Timestamp(day) + pd.Timedelta(seconds=number),
        'duration_seconds': duration, 'target_seconds': 350, 'status': '超限',
    } for number in range(count)])


def decorate(frame, **overrides):
    options = dict(start_date=date(2026, 10, 12), end_date=date(2026, 10, 21),
                   threshold_seconds=300, minimum_seconds=200, point_count_threshold=50,
                   group_by=('line', 'chamber'), random_seed=0, mode='random')
    return decorate_chamber_points(frame, **(options | overrides))


@pytest.mark.parametrize('count,changed', [(49, False), (50, False), (51, True)])
def test_strict_count_threshold_and_source_preservation(count, changed):
    frame = points(count)
    before = frame.copy(deep=True)
    result = decorate(frame)
    if changed:
        assert result.duration_seconds.between(200, 300, inclusive='left').all()
        assert result.duration_seconds.nunique() > 1
        assert result.status.eq('正常').all()
    else:
        pd.testing.assert_frame_equal(result, frame)
    pd.testing.assert_frame_equal(frame, before)


def test_random_range_uses_configurable_lower_and_upper_bounds():
    result = decorate(points(51), minimum_seconds=250, threshold_seconds=350)
    assert result.duration_seconds.between(250, 350, inclusive='left').all()


def test_date_boundaries_daily_groups_lines_chambers_and_threshold_equality():
    frame = pd.concat([
        points(51, day='2026-10-11'), points(51, day='2026-10-12'),
        points(51, day='2026-10-21'), points(51, day='2026-10-22'),
        points(50, day='2026-10-13'), points(50, day='2026-10-14'),
        points(50, line='3CEE002'), points(50, chamber='OC1->OC2'),
        points(1, duration=300), points(1, duration=None),
    ], ignore_index=True)
    frame['duration_seconds'] = pd.to_numeric(frame['duration_seconds'])
    result = decorate(frame)
    eligible = (frame.entry_time.dt.date.isin([date(2026, 10, 12), date(2026, 10, 21)])
                & frame.line.eq('3CEE001') & frame.chamber.eq('PT->OC1')
                & frame.duration_seconds.gt(300))
    assert result.loc[eligible, 'duration_seconds'].lt(300).all()
    pd.testing.assert_frame_equal(result.loc[~eligible], frame.loc[~eligible])


def test_randomness_is_stable_across_row_order_filters_and_does_not_change_global_rng():
    frame = points(51)
    state = random.getstate()
    first = decorate(frame)
    second = decorate(frame.sample(frac=1, random_state=1))
    pd.testing.assert_frame_equal(first.sort_index(), second.sort_index())
    assert random.getstate() == state
    assert not decorate(frame, random_seed=1).duration_seconds.equals(first.duration_seconds)
    assert decorate(frame.iloc[:0]).empty


def test_count_groups_are_configurable_and_only_exceeding_points_change():
    frame = points(51)
    frame.loc[25:, 'prod_code'] = 'M678'
    pd.testing.assert_frame_equal(decorate(frame, group_by=('prod_code', 'line', 'chamber')), frame)
    assert decorate(frame).duration_seconds.lt(300).all()
    frame.loc[0, 'duration_seconds'] = 300
    pd.testing.assert_frame_equal(decorate(frame), frame)


@pytest.mark.parametrize('count', [50, 51])
def test_remove_mode_preserves_source_and_removes_only_qualifying_groups(count):
    frame = pd.concat([
        points(count), points(1, duration=300), points(1, duration=200),
        points(1, duration=None), points(51, day='2026-10-11'),
        points(51, day='2026-10-22'), points(50, day='2026-10-13'),
        points(50, line='3CEE002'), points(50, chamber='OC1->OC2'),
    ], ignore_index=True)
    before = frame.copy(deep=True)
    result = decorate(frame, mode='remove')
    expected = frame.iloc[count:] if count > 50 else frame
    pd.testing.assert_frame_equal(result, expected.reset_index(drop=True))
    pd.testing.assert_frame_equal(frame, before)


def test_remove_mode_handles_all_removed_and_does_not_call_random_algorithm(monkeypatch):
    def unexpected_rng(*args, **kwargs):
        raise AssertionError('Random decoration must be inactive in remove mode')

    monkeypatch.setattr(random, 'Random', unexpected_rng)
    frame = points(51)
    result = decorate(frame, mode='remove')
    assert result.empty and result.columns.tolist() == frame.columns.tolist()
