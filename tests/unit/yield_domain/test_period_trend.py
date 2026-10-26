from datetime import datetime

import pandas as pd
import pytest

from src.yield_domain.core.mwd_trend.period_trend import aggregate_periods, project_report
from src.yield_domain.application.period_trend import PeriodTrendService


def monthly():
    return pd.DataFrame([
        [m, level, 'G', code, code, rate]
        for m, rate in [('2026-01', .1), ('2026-02', .2)]
        for level, code in [('Group', ''), ('Code', 'C')]
    ], columns=['month', 'level', 'defect_group', 'defect_code', 'defect_desc', 'defect_rate'])


def test_weighted_periods_and_code_filter_do_not_change_totals_or_input():
    inputs = pd.DataFrame({'month': ['2026-01', '2026-02'], 'total_panels': [100, 300]})
    rates, volumes = aggregate_periods(monthly(), inputs, 2026, 2)
    assert rates.query("period == '2026-H1' and level == 'Group'").defect_rate.iloc[0] == pytest.approx(.175)
    table, chart = project_report(rates, volumes, ['G'], [], ['2026-H1', '2026-Q1', '2026-01'])
    assert table.iloc[0]['2026-H1'] == pytest.approx(.175)
    assert 'C' not in table['Defect Code'].values
    assert chart.query("period == '2026-H1'").total_panels.iloc[0] == 400


def test_missing_positive_input_rate_does_not_become_zero():
    inputs = pd.DataFrame({'month': ['2026-01', '2026-02'], 'total_panels': [100, 300]})
    rates, _ = aggregate_periods(monthly().query("month == '2026-01'"), inputs, 2026, 2)
    assert rates.query("period == '2026-H1'").defect_rate.isna().all()


class Store:
    def __init__(self):
        self.data = {}

    def read(self, product):
        return self.data.get(product)

    def save(self, product, rates, inputs, meta):
        self.data[product] = (rates.copy(), inputs.copy(), dict(meta))


class Source:
    signature = 'policy-1'
    maintenance_signature = 'table-1'

    def __init__(self):
        self.calls = []

    def throughput(self, product, month, now):
        self.calls.append(('input', month))
        return 100

    def raw_rates(self, product, month, total, now):
        assert month < '2026-06'
        self.calls.append(('raw', month))
        return monthly().query("month == '2026-01'").assign(month=month)

    def maintained_rates(self, product, month):
        self.calls.append(('maintained', month))
        return monthly().query("month == '2026-01'").assign(month=month)


def test_closed_months_reused_current_month_only_and_june_has_no_defect_query():
    source, store = Source(), Store()
    service = PeriodTrendService(source, store, maintained_start='2026-06', ttl_seconds=3600)
    service.load('A', datetime(2026, 7, 2, 12))
    assert len([c for c in source.calls if c[0] == 'raw']) == 5
    source.calls.clear()
    service.load('A', datetime(2026, 7, 2, 12, 10))
    assert source.calls == []
    service.load('A', datetime(2026, 7, 3, 12))
    assert source.calls == [('input', '2026-07'), ('maintained', '2026-07')]
    source.calls.clear()
    service.load('A', datetime(2026, 8, 1, 12))
    # July was open: close it once; August alone remains refreshable afterwards.
    assert [c[1] for c in source.calls if c[0] == 'input'] == ['2026-07', '2026-08']


def test_failed_refresh_preserves_committed_history():
    source, store = Source(), Store()
    service = PeriodTrendService(source, store, maintained_start='2026-06', ttl_seconds=1)
    service.load('A', datetime(2026, 6, 2))
    before = store.read('A')
    source.throughput = lambda *args: (_ for _ in ()).throw(OSError('source unavailable'))
    with pytest.raises(OSError):
        service.load('A', datetime(2026, 6, 3))
    assert store.read('A') is before
