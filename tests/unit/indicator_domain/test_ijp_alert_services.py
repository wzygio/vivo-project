from datetime import date

import pandas as pd
import pytest

from src.indicator_domain.application.ijp.service import IjpReportService
from src.indicator_domain.application.ijp_hole.service import IjpHoleReportService
from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS


class Store:
    def __init__(self):
        self.calls = []

    def save(self, region, alerts, reference_date):
        self.calls.append((region, alerts.copy(), reference_date))


class Source:
    def __init__(self):
        self.queries = []

    def fetch_daily_counts(self, query):
        self.queries.append(query)
        return pd.DataFrame([dict(productcode=p, line='L1', printer='P1',
            day='2026-10-20', rs_code=c, code_num=1)
            for p in ('M1', 'DISABLED') for c in ('C3DM1', 'C3DM2')])

    def fetch_counts(self, query):
        self.queries.append(query)
        return pd.DataFrame([dict(productcode=p, line='L1', printer='P1',
            day='2026-10-20', rs_code=c, code_num=1)
            for p in ('M1', 'DISABLED') for c in ('C3RA1', 'C3RA2')])


@pytest.mark.parametrize('region', ['AA区', '孔区'])
def test_automatic_scope_public_payload_and_publication(region):
    source, store = Source(), Store()
    kwargs = dict(work_order_types=('MP',), today_provider=lambda: date(2026, 10, 26), alert_store=store)
    service = (IjpReportService(source, enabled_product_codes=('M1',), **kwargs)
               if region == 'AA区' else IjpHoleReportService(source, enabled_products=('M1',), **kwargs))
    report = service.get_alert_report()
    assert set(report['ratios'].productcode) == {'M1'}
    assert not {'raw_ratio', 'code_num', 'in_window', 'flag'} & set(report['ratios'])
    # Existing decoration keeps the reported target above the warning limit.
    assert report['alerts'].empty
    assert list(report['alerts']) == ALERT_COLUMNS
    assert len(store.calls) == 1 and store.calls[0][0] == region
    assert source.queries[0].product_codes == ('M1',)
    assert source.queries[0].work_order_types == ('MP',)
    assert not source.queries[0].lines and not source.queries[0].picis


@pytest.mark.parametrize('region', ['AA区', '孔区'])
def test_calculation_failure_does_not_publish_success(region):
    store = Store()
    kwargs = dict(today_provider=lambda: date(2026, 10, 26), alert_store=store)
    service = (IjpReportService(object(), enabled_product_codes=('M1',), **kwargs)
               if region == 'AA区' else IjpHoleReportService(object(), enabled_products=('M1',), **kwargs))
    with pytest.raises(AttributeError):
        service.get_alert_report()
    assert not store.calls
