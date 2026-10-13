from datetime import date

import pandas as pd
import pytest

from src.shared_kernel.config import ConfigLoader
from src.indicator_domain.infrastructure.shared.date_exclusion import (
    apply_indicator_date_exclusion, indicator_source_date_predicate,
)
from src.shared_kernel.data_forward import DataForwardPolicy


@pytest.fixture
def config(monkeypatch):
    section = {'enabled': True, 'start_date': '2026-10-01', 'end_date': '2026-10-06'}
    monkeypatch.setattr(ConfigLoader, 'load_domain_config', lambda domain: {'data_exclusion': section})
    return section


def test_inclusive_dates_filter_copy_and_keep_outside_dates(config):
    frame = pd.DataFrame({'event_time': pd.to_datetime([
        '2026-09-30 23:59:59.999999', '2026-10-01 00:00:00',
        '2026-10-06 23:59:59.999999', '2026-10-07 00:00:00', None,
    ], format='mixed'), 'value': range(5)})
    before = frame.copy(deep=True)
    actual = apply_indicator_date_exclusion(frame, time_column='event_time')
    assert actual.value.tolist() == [0, 3, 4]
    pd.testing.assert_frame_equal(frame, before)
    config['enabled'] = False
    pd.testing.assert_frame_equal(apply_indicator_date_exclusion(frame, time_column='event_time'), frame)


def test_config_supports_today_missing_and_disabled(monkeypatch, config):
    config['end_date'] = 'today'
    assert ConfigLoader.get_indicator_data_exclusion(today=date(2026, 10, 13)) == ('2026-10-01', '2026-10-13')
    config.clear()
    assert ConfigLoader.get_indicator_data_exclusion() is None


@pytest.mark.parametrize('section', [
    [], {'enabled': 'true'}, {'enabled': True},
    {'enabled': True, 'start_date': 'bad', 'end_date': 'today'},
    {'enabled': True, 'start_date': '2026-10-07', 'end_date': '2026-10-06'},
])
def test_invalid_config_is_rejected(monkeypatch, section):
    monkeypatch.setattr(ConfigLoader, 'load_domain_config', lambda domain: {'data_exclusion': section})
    with pytest.raises(ValueError, match='data_exclusion'):
        ConfigLoader.get_indicator_data_exclusion()


@pytest.mark.parametrize('offset,start,end', [
    (4, '2026-09-27 00:00:00', '2026-10-03 00:00:00'),
    (0, '2026-10-01 00:00:00', '2026-10-07 00:00:00'),
])
def test_sql_predicate_translates_display_dates_to_source_window(config, offset, start, end):
    sql, params = indicator_source_date_predicate(
        'D.GLASS_START_TIME', DataForwardPolicy(enabled=bool(offset), offset_days=offset),
    )
    assert 'D.GLASS_START_TIME < :excluded_start_time' in sql
    assert 'D.GLASS_START_TIME >= :excluded_end_time' in sql
    assert params == {'excluded_start_time': start, 'excluded_end_time': end}
    config['enabled'] = False
    assert indicator_source_date_predicate('D.GLASS_START_TIME', DataForwardPolicy()) == ('', {})
