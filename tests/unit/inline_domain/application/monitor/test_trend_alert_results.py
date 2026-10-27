from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from src.inline_domain.application.monitor.trend_alert_results import TrendAlertResults
from src.inline_domain.infrastructure.monitor.trend_alert_workbook import TrendAlertWorkbook
from src.inline_domain.core.shared.density_trend_alerts import EVALUATION_VALUE_COLUMNS


def evaluation(product='M678', alert=True):
    row = dict.fromkeys(EVALUATION_VALUE_COLUMNS, 1)
    row.update(prod_code=product, factory='ARRAY', step_id='100', rs_code='RS',
               target_date='2026-10-26', is_alert=alert, daily_alert=alert, daily_evaluable=True)
    return pd.DataFrame([row])


def test_publish_read_and_second_product_are_independent(tmp_path):
    now = datetime(2026, 10, 27, 10)
    store = TrendAlertWorkbook(tmp_path / 'trend.xlsx')
    service = TrendAlertResults(store, scope='aoi_rs', ttl_seconds=3600, policy='p1', clock=lambda: now)
    day = date(2026, 10, 27)
    assert service.read('M678', day)['state'] == 'error'
    service.publish('M678', day, evaluation())
    version = store.signature()
    service.publish('M678', day, evaluation())
    assert version == store.signature()
    service.publish('M626', day, evaluation('M626', False))
    assert service.read('M678', day)['state'] == 'alert'
    assert service.read('M626', day)['state'] == 'ok'
    assert service.read('M678', day)['evaluation']['rs_code'].tolist() == ['RS']
    assert service.read('M678', day + timedelta(days=1))['state'] == 'error'


def test_expiry_policy_and_empty_evaluation(tmp_path):
    now = datetime(2026, 10, 27, 10)
    store = TrendAlertWorkbook(tmp_path / 'trend.xlsx')
    day = date(2026, 10, 27)
    service = TrendAlertResults(store, scope='aoi_tt', ttl_seconds=3600, policy='p1', clock=lambda: now)
    service.publish('M678', day, pd.DataFrame())
    assert service.read('M678', day)['state'] == 'no_data'
    expired = TrendAlertResults(store, scope='aoi_tt', ttl_seconds=3600, policy='p1', clock=lambda: now + timedelta(hours=2))
    assert expired.read('M678', day)['state'] == 'error'
    changed = TrendAlertResults(store, scope='aoi_tt', ttl_seconds=3600, policy='p2', clock=lambda: now)
    assert changed.read('M678', day)['state'] == 'error'


def test_invalid_product_does_not_publish(tmp_path):
    store = TrendAlertWorkbook(tmp_path / 'trend.xlsx')
    service = TrendAlertResults(store, scope='aoi_rs', ttl_seconds=3600, policy='p1')
    with pytest.raises(ValueError, match='Invalid'):
        service.publish('M678', date(2026, 10, 27), evaluation('M626'))
    assert not store.path.exists()
