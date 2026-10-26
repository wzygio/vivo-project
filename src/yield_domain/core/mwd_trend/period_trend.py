"""Monthly L1 facts and weighted half-year/quarter projections, without I/O."""
from __future__ import annotations

import numpy as np
import pandas as pd

RATE_COLUMNS = ['month', 'level', 'defect_group', 'defect_code', 'defect_desc', 'defect_rate']
IDENTITY = ['level', 'defect_group', 'defect_code', 'defect_desc']


def period_months(year: int, last_month: int) -> dict[str, list[str]]:
    months = [f'{year}-{m:02}' for m in range(1, last_month + 1)]
    periods = {f'{year}-H{h}': [m for m in months if (int(m[-2:]) - 1) // 6 + 1 == h]
               for h in range(1, (last_month - 1) // 6 + 2)}
    periods.update({f'{year}-Q{q}': [m for m in months if (int(m[-2:]) - 1) // 3 + 1 == q]
                    for q in range(1, (last_month - 1) // 3 + 2)})
    periods.update({m: [m] for m in months})
    return periods


def aggregate_periods(monthly: pd.DataFrame, inputs: pd.DataFrame, year: int,
                      last_month: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Missing rates with positive input propagate NaN; zero-input months weigh zero."""
    rates = monthly[RATE_COLUMNS].copy()
    rates[IDENTITY] = rates[IDENTITY].fillna('')
    if rates.duplicated(['month', *IDENTITY]).any() or inputs.duplicated('month').any():
        raise ValueError('Duplicate monthly business key')
    weights = pd.to_numeric(inputs.set_index('month')['total_panels'], errors='raise').astype(float)
    if (weights < 0).any() or not np.isfinite(weights).all():
        raise ValueError('Invalid monthly throughput')
    matrix = rates.pivot(index=IDENTITY, columns='month', values='defect_rate')
    presence = rates.assign(_present=1).pivot(index=IDENTITY, columns='month', values='_present').notna()
    present_months = set(rates.month)
    result, volumes = [], []
    for period, months in period_months(year, last_month).items():
        counts = weights.reindex(months)
        total = counts.sum() if counts.notna().all() else np.nan
        volumes.append({'period': period, 'total_panels': total})
        values = matrix.reindex(columns=months).copy()
        known = presence.reindex(columns=months, fill_value=False)
        # Keep explicit unavailable values; only absent identities in a complete
        # month represent zero. Vectorizing avoids per-Code period recalculation.
        for month in months:
            if month in present_months:
                values.loc[~known[month].astype(bool), month] = 0.0
        valid = values.loc[:, counts.gt(0)].notna().all(axis=1) & counts.notna().all()
        weighted = values.fillna(0).mul(counts, axis=1).sum(axis=1)
        period_rates = weighted / total if total > 0 else weighted * 0
        period_rates = period_rates.where(valid & pd.notna(total), np.nan)
        frame = period_rates.rename('defect_rate').reset_index().assign(period=period)
        result.append(frame)
    return pd.concat(result, ignore_index=True).reindex(columns=[*IDENTITY, 'period', 'defect_rate']), pd.DataFrame(volumes)


def project_report(rates: pd.DataFrame, volumes: pd.DataFrame, groups: list[str],
                   codes: list[str], periods: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Code selection only controls detail rows; Group totals retain their scope."""
    public = rates[[*IDENTITY, 'period', 'defect_rate']].copy()
    group_rows = public.loc[public.level.eq('Group') & public.defect_group.isin(groups) & public.period.isin(periods)]
    code_rows = public.loc[public.level.eq('Code') & public.defect_group.isin(groups) & public.defect_code.isin(codes) & public.period.isin(periods)]
    table_rows = []
    total = {'指标': '总不良', 'Defect Group': '', 'Defect Code': '', '缺陷名称': ''}
    for p in periods:
        values = group_rows.loc[group_rows.period.eq(p), 'defect_rate']
        total[p] = values.sum(min_count=len(values)) if len(values) else np.nan
    table_rows.append(total)
    for subset, label in ((group_rows, 'Group'), (code_rows, 'Code')):
        for identity, rows in subset.groupby(IDENTITY, sort=True, dropna=False):
            row = dict(zip(['指标', 'Defect Group', 'Defect Code', '缺陷名称'], [label, *identity[1:]]))
            row.update(rows.set_index('period').defect_rate.to_dict())
            table_rows.append(row)
    table = pd.DataFrame(table_rows).reindex(columns=['指标', 'Defect Group', 'Defect Code', '缺陷名称', *periods])
    chart = group_rows[['period', 'defect_group', 'defect_rate']].merge(volumes[['period', 'total_panels']], on='period', how='left')
    return table, chart


def rates_from_counts(counts: pd.DataFrame, total: int, month: str,
                      multipliers: dict[str, float] | None = None) -> pd.DataFrame:
    """Same distinct-Panel monthly rate as MWD, with a fixed physical denominator."""
    data = counts.copy()
    factors = data.defect_desc.map(multipliers or {}).fillna(1.0)
    data['defect_rate'] = np.floor(data.defect_panel_count * factors) / total if total else 0.0
    code = data.assign(month=month, level='Code')
    group = data.groupby('defect_group', as_index=False).defect_rate.sum().assign(
        month=month, level='Group', defect_code='', defect_desc='')
    return pd.concat([group[RATE_COLUMNS], code[RATE_COLUMNS]], ignore_index=True)
