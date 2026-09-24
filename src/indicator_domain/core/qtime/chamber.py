"""Glass identity and residence statistics; independent of persistence and UI."""

from __future__ import annotations

import numpy as np
import pandas as pd

CHAMBERS = (
    'PT->OC1', 'OC1->OC2', 'OC2->OC3', 'OC3->OC4', 'OC4->OC5',
    'OC5->OC6', 'OC6->OC7', 'OC7->OC8', 'OC8->MC', 'MC->OC9', 'OC9->OC10',
)
DETAIL_COLUMNS = ['prod_code', 'line', 'glass_id', 'entry_time', 'chamber',
                  'duration_seconds', 'target_seconds', 'status']
MAX_DISPLAY_DURATION_SECONDS = 1000


def exclude_chamber_outliers(details: pd.DataFrame) -> pd.DataFrame:
    """Exclude individual readings above the display limit; retain missing values."""
    durations = details['duration_seconds']
    return details.loc[durations.isna() | durations.le(MAX_DISPLAY_DURATION_SECONDS)].copy()


def build_chamber_report(
    measurements: pd.DataFrame, products: pd.DataFrame,
    targets: pd.DataFrame, enabled_products: tuple[str, ...],
) -> dict[str, object]:
    """Exclude uncertain product identities before expanding each glass's chambers."""
    mapping = products[['glass_id', 'prod_code']].dropna().drop_duplicates()
    conflicts = mapping.groupby('glass_id')['prod_code'].nunique()
    ambiguous = set(conflicts[conflicts.gt(1)].index)
    source_ids = set(measurements['glass_id'])
    unmatched = source_ids - set(mapping['glass_id'])
    mapping = mapping.loc[~mapping['glass_id'].isin(ambiguous)]
    mapping = mapping.loc[mapping['prod_code'].isin(enabled_products)]
    selected = measurements.merge(mapping, on='glass_id', how='inner', validate='many_to_one')
    details = selected.melt(
        id_vars=['prod_code', 'line', 'glass_id', 'entry_time'],
        value_vars=list(CHAMBERS), var_name='chamber', value_name='duration_seconds',
    ).merge(targets, on=['line', 'chamber'], how='left', validate='many_to_one')
    durations = pd.to_numeric(details['duration_seconds'], errors='coerce')
    details['duration_seconds'] = durations.where(np.isfinite(durations) & durations.ge(0))
    details['status'] = np.select(
        [details['duration_seconds'].isna(), details['duration_seconds'].gt(details['target_seconds'])],
        ['缺失', '超限'], default='正常',
    )
    return {
        'details': details[DETAIL_COLUMNS].sort_values(['entry_time', 'line', 'glass_id', 'chamber']).reset_index(drop=True),
        'unmatched_glasses': len(unmatched),
        'ambiguous_glasses': len(source_ids & ambiguous),
    }


def summarize_chambers(details: pd.DataFrame) -> pd.DataFrame:
    """Only valid measured intervals contribute to the exceedance denominator."""
    frame = details.assign(exceeded=details['status'].eq('超限'), missing=details['status'].eq('缺失'))
    summary = frame.groupby(['prod_code', 'line', 'chamber'], sort=False).agg(
        measured_count=('duration_seconds', 'count'),
        missing_count=('missing', 'sum'),
        exceeded_count=('exceeded', 'sum'),
        mean_seconds=('duration_seconds', 'mean'),
        max_seconds=('duration_seconds', 'max'),
        target_seconds=('target_seconds', 'first'),
    ).reset_index()
    summary['exceeded_ratio'] = summary['exceeded_count'].div(summary['measured_count'].where(summary['measured_count'].gt(0)))
    return summary
