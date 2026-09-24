from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from src.indicator_domain import composition
from src.indicator_domain.application.qtime.chamber_cache import cached_chamber_report, clear_chamber_cache
from src.indicator_domain.core.qtime.chamber import CHAMBERS


def test_yaml_target_controls_report_and_invalidates_cache(monkeypatch):
    payload = {
        'measurements': pd.DataFrame([{
            'line': '3CEE001', 'glass_id': 'G', 'entry_time': pd.Timestamp('2026-09-01'),
            **dict.fromkeys(CHAMBERS, 75),
        }]),
        'products': pd.DataFrame({'glass_id': ['G'], 'prod_code': ['M626']}),
        'targets': pd.DataFrame({'line': '3CEE001', 'chamber': CHAMBERS, 'target_seconds': 6000}),
        'invalid_rows': 0,
    }
    source = SimpleNamespace(cache_signature=lambda: ('settings-fixture',), read=lambda **_: payload)
    settings = {'qtime': {'chamber_target_seconds': 50}}
    monkeypatch.setattr(composition, 'ChamberQTimeRepository', lambda *args: source)
    monkeypatch.setattr(composition.ConfigLoader, 'load_domain_config', lambda domain: settings)
    monkeypatch.setattr(composition.ConfigLoader, 'get_enabled_products', lambda: ['M626'])
    monkeypatch.setattr(composition.ConfigLoader, 'get_domain_resource_path', lambda *args: Path('fixture.xlsx'))
    clear_chamber_cache()
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
    clear_chamber_cache()
