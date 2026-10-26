"""Annual period trends, assembled from immutable monthly history and live month."""
from __future__ import annotations

from datetime import datetime
from typing import Protocol

import pandas as pd

from src.yield_domain.core.mwd_trend.period_trend import RATE_COLUMNS, aggregate_periods


class PeriodTrendSource(Protocol):
    signature: str
    maintenance_signature: str

    def throughput(self, product: str, month: str, now: datetime) -> int: ...
    def raw_rates(self, product: str, month: str, total: int, now: datetime) -> pd.DataFrame: ...
    def maintained_rates(self, product: str, month: str) -> pd.DataFrame: ...


class PeriodTrendStore(Protocol):
    def read(self, product: str) -> tuple[pd.DataFrame, pd.DataFrame, dict] | None: ...
    def save(self, product: str, rates: pd.DataFrame, inputs: pd.DataFrame, meta: dict) -> None: ...


class PeriodTrendService:
    def __init__(self, source: PeriodTrendSource, store: PeriodTrendStore, *,
                 maintained_start: str, ttl_seconds: int):
        self.source, self.store = source, store
        self.maintained_start, self.ttl_seconds = maintained_start, ttl_seconds

    def load(self, product: str, now: datetime | None = None) -> dict:
        now = now or datetime.now()
        current = now.strftime('%Y-%m')
        maintenance_version = self.source.maintenance_signature
        previous = self.store.read(product)
        rates, inputs, meta = previous if previous else (
            pd.DataFrame(columns=RATE_COLUMNS), pd.DataFrame(columns=['month', 'total_panels']), {})
        rates, inputs, meta = rates.copy(), inputs.copy(), dict(meta)
        if meta and meta.get('policy') != self.source.signature:
            raise ValueError('History policy changed; explicit migration required')
        modified = meta.get('maintenance') != maintenance_version
        months = [str(p) for p in pd.period_range(f'{now.year}-01', current, freq='M')]
        refreshed = pd.Timestamp(meta.get('refreshed', '1970-01-01'))
        fresh = (pd.Timestamp(now) - refreshed).total_seconds() < self.ttl_seconds and refreshed.date() == now.date()
        changed = False
        for month in months:
            exists = month in set(inputs.month)
            closing = exists and month == meta.get('open_month') and month != current
            refresh = not exists or closing or (month == current and not fresh)
            if refresh:
                total = self.source.throughput(product, month, now)
                inputs = pd.concat([inputs.loc[inputs.month.ne(month)], pd.DataFrame([{'month': month, 'total_panels': total}])], ignore_index=True)
            else:
                total = int(inputs.loc[inputs.month.eq(month), 'total_panels'].iloc[0])
            if refresh or (modified and month >= self.maintained_start):
                new = (self.source.maintained_rates(product, month) if month >= self.maintained_start
                       else self.source.raw_rates(product, month, total, now))
                if total == 0:
                    new = new.assign(defect_rate=0.0)
                kept = rates.loc[rates.month.ne(month)]
                rates = pd.concat([kept, new[RATE_COLUMNS]], ignore_index=True) if not kept.empty else new[RATE_COLUMNS].copy()
                changed = True
        if changed:
            if self.source.maintenance_signature != maintenance_version:
                raise RuntimeError('Maintained workbook changed during calculation; retry')
            meta['expected_version'] = meta.get('version')
            meta.update(policy=self.source.signature, maintenance=maintenance_version,
                        refreshed=now.isoformat(), open_month=current, schema=1)
            self.store.save(product, rates, inputs, meta)
        selected_rates = rates.loc[rates.month.isin(months)]
        selected_inputs = inputs.loc[inputs.month.isin(months)]
        aggregated, volumes = aggregate_periods(selected_rates, selected_inputs, now.year, now.month)
        return {'rates': aggregated, 'volumes': volumes, 'monthly_rates': selected_rates,
                'monthly_inputs': selected_inputs, 'refreshed_at': meta.get('refreshed')}
