"""The active Yield alert UI consumes publication health, without raw fallback."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest
from streamlit.testing.v1 import AppTest

from src.yield_domain import composition
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore


@pytest.mark.parametrize('source_status', ['fresh', 'stale', 'unknown', 'partial'])
def test_published_health_controls_saved_yield_centres(monkeypatch, tmp_path, source_status):
    day = date(2026, 11, 2)
    now = datetime(2026, 11, 2, 10, tzinfo=ZoneInfo('Asia/Shanghai'))
    paths = {'lot': tmp_path / 'lots.xlsx', 'trend': tmp_path / 'trends.xlsx'}
    service = YieldAlertResults(YieldAlertWorkbookStore(paths), policy_signature=lambda _: 'fixture',
                               clock=lambda: now, ttl_seconds=43200)
    for kind in paths:
        service.publish('P1', kind, [], reference_date=day,
                        coverage_start='2026-09-01', coverage_end=str(day), has_data=True,
                        health={'status': source_status, 'refreshed_at': now.isoformat()},
                        calculation_signature='fixture')
    before = {kind: path.read_bytes() for kind, path in paths.items()}
    monkeypatch.setattr(composition, 'build_yield_alert_results', lambda: service)
    app = AppTest.from_string('''
from datetime import date
from app.sections.yield_domain.published_alert_center import render_saved_yield_alerts
render_saved_yield_alerts('P1', date(2026, 11, 2))
''').run()
    assert not app.exception and len(app.expander) == 2
    if source_status == 'fresh':
        assert not app.error and len(app.success) == 2
    else:
        assert not app.success and len(app.error) == 2
        for kind in paths:
            result = service.read('P1', kind, day)
            assert result.status == 'failed'
            assert result.metadata['error_code'] == 'YIELD_SOURCE_NOT_CURRENT'
    assert {kind: path.read_bytes() for kind, path in paths.items()} == before
