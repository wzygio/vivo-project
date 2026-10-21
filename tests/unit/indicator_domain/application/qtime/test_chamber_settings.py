from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from src.indicator_domain import composition
from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache
from src.indicator_domain.application.qtime.chamber_service import ChamberQTimeService
from src.indicator_domain.application.qtime.chamber_settings import ChamberDecorationSettings
from src.indicator_domain.core.qtime.chamber import CHAMBERS


def test_yaml_target_controls_report_and_invalidates_cache(monkeypatch):
    payload = {
        'measurements': pd.DataFrame([{
            'line': '3CEE001', 'glass_id': 'G', 'entry_time': pd.Timestamp('2026-09-01'),
            'chamber': chamber, 'duration_seconds': 75,
        } for chamber in CHAMBERS]),
        'products': pd.DataFrame({'glass_id': ['G'], 'prod_code': ['M626']}),
        'targets': pd.DataFrame({'line': '3CEE001', 'chamber': CHAMBERS, 'target_seconds': 6000}),
        'invalid_rows': 0,
    }
    source = SimpleNamespace(cache_signature=lambda: ('settings-fixture',), read=lambda **_: payload)
    settings = {'qtime': {'chamber_target_seconds': 50}}
    monkeypatch.setattr(composition, 'DatabaseChamberQTimeRepository', lambda *args, **kwargs: source)
    monkeypatch.setattr(composition.ConfigLoader, 'load_domain_config', lambda domain: settings)
    monkeypatch.setattr(composition.ConfigLoader, 'get_enabled_products', lambda: ['M626'])
    clear_chamber_cache()
    try:
        first = composition.build_chamber_qtime_service(object())
        first_details = cached_chamber_report(first, as_of=date(2026, 9, 24))['details']
        assert first_details['target_seconds'].eq(50).all()
        assert first_details['status'].eq('超限').all()

        settings['qtime']['chamber_target_seconds'] = 100
        second = composition.build_chamber_qtime_service(object())
        second_details = cached_chamber_report(second, as_of=date(2026, 9, 24))['details']
        assert second.cache_signature() != first.cache_signature()
        assert second_details['target_seconds'].eq(100).all()
        assert second_details['status'].eq('正常').all()
        assert payload['targets']['target_seconds'].eq(6000).all()
    finally:
        clear_chamber_cache()


def test_explicit_sources_with_equal_business_signatures_do_not_share_cache():
    def source(duration):
        return SimpleNamespace(
            cache_signature=lambda: ('same-business-signature',),
            read=lambda **_: {
                'measurements': pd.DataFrame([{
                    'line': '3CEE001', 'glass_id': 'G', 'entry_time': pd.Timestamp('2026-09-01'),
                    'chamber': 'PT->OC1', 'duration_seconds': duration,
                }]),
                'products': pd.DataFrame({'glass_id': ['G'], 'prod_code': ['M626']}),
                'targets': pd.DataFrame([{
                    'line': '3CEE001', 'chamber': 'PT->OC1', 'target_seconds': 1200,
                }]),
                'invalid_rows': 0,
            },
        )

    clear_chamber_cache()
    try:
        first = ChamberQTimeService(source(1), enabled_products=('M626',))
        second = ChamberQTimeService(source(2), enabled_products=('M626',))
        assert cached_chamber_report(first, as_of=date(2026, 9, 24))['details'].duration_seconds.tolist() == [1]
        assert cached_chamber_report(second, as_of=date(2026, 9, 24))['details'].duration_seconds.tolist() == [2]
    finally:
        clear_chamber_cache()


@pytest.mark.parametrize('overrides', [
    {'threshold_seconds': 0}, {'threshold_seconds': float('nan')}, {'threshold_seconds': True},
    {'minimum_seconds': -1}, {'minimum_seconds': 300}, {'minimum_seconds': 301},
    {'minimum_seconds': float('nan')}, {'minimum_seconds': True},
    {'point_count_threshold': -1}, {'point_count_threshold': 1.5},
    {'start_date': 'invalid'}, {'end_date': '2026-10-11'},
    {'enabled': 'false'}, {'group_by': ['line', 'line']}, {'group_by': ['unknown']},
    {'mode': 'unknown'},
])
def test_decoration_settings_reject_invalid_configuration(overrides):
    with pytest.raises(ValidationError):
        ChamberDecorationSettings.model_validate({'mode': 'random', **overrides})


def test_decoration_today_window_and_config_changes_invalidate_cached_payload(monkeypatch):
    measurements = pd.DataFrame([{
        'line': '3CEE001', 'glass_id': 'G', 'entry_time': pd.Timestamp('2026-10-12') + pd.Timedelta(seconds=i),
        'chamber': 'PT->OC1', 'duration_seconds': 400,
    } for i in range(51)])
    payload = {
        'measurements': measurements,
        'products': pd.DataFrame({'glass_id': ['G'], 'prod_code': ['M626']}),
        'targets': pd.DataFrame([{'line': '3CEE001', 'chamber': 'PT->OC1', 'target_seconds': 350}]),
        'invalid_rows': 0,
    }
    source = SimpleNamespace(cache_signature=lambda: ('stable-source',), read=lambda **_: payload)
    settings = {'qtime': {'chamber_target_seconds': 350, 'chamber_point_decoration': {
        'enabled': True, 'mode': 'random', 'start_date': '2026-10-12', 'end_date': 'today',
    }}}
    monkeypatch.setattr(composition, 'DatabaseChamberQTimeRepository', lambda *args, **kwargs: source)
    monkeypatch.setattr(composition.ConfigLoader, 'load_domain_config', lambda _: settings)
    monkeypatch.setattr(composition.ConfigLoader, 'get_enabled_products', lambda: ['M626'])
    clear_chamber_cache()
    try:
        first = composition.build_chamber_qtime_service(object())
        before_window = cached_chamber_report(first, as_of=date(2026, 10, 11))
        assert before_window['details'].duration_seconds.eq(400).all()
        decorated = cached_chamber_report(first, as_of=date(2026, 10, 12))
        assert decorated['details'].duration_seconds.between(200, 300, inclusive='left').all()
        assert decorated['details'].status.eq('正常').all()
        settings['qtime']['chamber_point_decoration']['enabled'] = False
        second = composition.build_chamber_qtime_service(object())
        assert first.cache_signature() != second.cache_signature()
        assert cached_chamber_report(second, as_of=date(2026, 10, 12))['details'].duration_seconds.eq(400).all()
        settings['qtime']['chamber_point_decoration'].update(enabled=True, point_count_threshold=51)
        third = composition.build_chamber_qtime_service(object())
        assert third.cache_signature() != first.cache_signature()
        assert cached_chamber_report(third, as_of=date(2026, 10, 12))['details'].duration_seconds.eq(400).all()
        settings['qtime']['chamber_point_decoration'].update(point_count_threshold=50, minimum_seconds=280)
        fourth = composition.build_chamber_qtime_service(object())
        assert fourth.cache_signature() != first.cache_signature()
        assert cached_chamber_report(fourth, as_of=date(2026, 10, 12))['details'].duration_seconds.between(
            280, 300, inclusive='left',
        ).all()
        settings['qtime']['chamber_point_decoration']['mode'] = 'remove'
        fifth = composition.build_chamber_qtime_service(object())
        assert fifth.cache_signature() != fourth.cache_signature()
        assert cached_chamber_report(fifth, as_of=date(2026, 10, 12))['details'].empty
        assert measurements.duration_seconds.eq(400).all()
    finally:
        clear_chamber_cache()


def test_only_enabled_products_contribute_to_daily_decoration_counts():
    rows = [{
        'line': '3CEE001', 'glass_id': str(i), 'entry_time': pd.Timestamp('2026-10-12'),
        'chamber': 'PT->OC1', 'duration_seconds': 400,
    } for i in range(51)]
    payload = {
        'measurements': pd.DataFrame(rows),
        'products': pd.DataFrame({'glass_id': [str(i) for i in range(51)],
                                 'prod_code': ['M626'] * 50 + ['DISABLED']}),
        'targets': pd.DataFrame([{'line': '3CEE001', 'chamber': 'PT->OC1', 'target_seconds': 350}]),
        'invalid_rows': 0,
    }
    source = SimpleNamespace(cache_signature=lambda: (), read=lambda **_: payload)
    svc = ChamberQTimeService(source, enabled_products=('M626',),
                             decoration_settings=ChamberDecorationSettings(enabled=True))
    details = svc.load(as_of=date(2026, 10, 12))['details']
    assert len(details) == 50
    assert details.duration_seconds.eq(400).all()


def test_fixed_decoration_end_is_inclusive_and_today_rolls_forward():
    settings = ChamberDecorationSettings(enabled=True, end_date=date(2026, 10, 15))
    assert settings.window_end(date(2026, 10, 14)) == date(2026, 10, 14)
    assert settings.window_end(date(2026, 10, 21)) == date(2026, 10, 15)
    dynamic = ChamberDecorationSettings(enabled=True)
    assert dynamic.window_end(date(2026, 10, 21)) == date(2026, 10, 21)
    assert dynamic.window_end(date(2026, 10, 22)) == date(2026, 10, 22)
