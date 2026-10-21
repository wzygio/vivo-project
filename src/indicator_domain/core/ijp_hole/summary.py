"""Large-hole count ratios and the public two-month/three-week projection."""

from datetime import date
import hashlib
import json
import random

import pandas as pd

from src.indicator_domain.core.ijp.period_summary import reporting_periods

HOLE_CODES = ('C3RA1', 'C3RA2', 'C3RA3')
HOLE_DISPLAY_CODES = HOLE_CODES
C3RA1_TARGET_MINIMUM = 0.905
C3RA1_TARGET_MAXIMUM = 0.915
HOLE_PRINTERS = (
    '3CEE01-IK2-PR1', '3CEE01-IK2-PR2',
    '3CEE02-IK2-PR1', '3CEE02-IK2-PR2',
)
DIMENSIONS = ['productcode', 'line', 'printer']
COUNT_COLUMNS = [*DIMENSIONS, 'glass_id', 'day', 'rs_code', 'code_num', 'in_window']
PERIOD_COLUMNS = [
    *DIMENSIONS, 'period', 'period_start', 'period_end', 'rs_code', 'ratio', 'has_data',
]


def summarize_hole_periods(
    counts: pd.DataFrame, today: date, codes: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Decorate weighted period totals before applying the visible CODE filter.

    Assign C3RA3 to C3RA2, then apply a stable random C3RA1 threshold
    around 91%. C3RA3 and empty periods stay zero; input facts stay untouched.
    """
    if counts.empty:
        return pd.DataFrame(columns=PERIOD_COLUMNS)
    periods = reporting_periods(today)
    frame = counts[counts.rs_code.isin(HOLE_CODES)].copy()
    frame['day'] = pd.to_datetime(frame.day, errors='raise').dt.date
    frame['code_num'] = pd.to_numeric(frame.code_num, errors='raise')
    if frame.code_num.isna().any() or frame.code_num.lt(0).any():
        raise ValueError('CODE 记录数必须为非负数')
    frame = frame[frame.day.between(periods[0][1], today)]
    groups = frame[DIMENSIONS].drop_duplicates().sort_values(DIMENSIONS)
    grid = groups.merge(pd.DataFrame({'rs_code': HOLE_CODES}), how='cross')
    outputs = []
    for label, start, end in periods:
        selected = frame[frame.day.between(start, end)]
        sums = selected.groupby([*DIMENSIONS, 'rs_code'], as_index=False).code_num.sum()
        result = grid.merge(sums, on=[*DIMENSIONS, 'rs_code'], how='left')
        result['code_num'] = result.code_num.fillna(0)
        totals = result.groupby(DIMENSIONS).code_num.transform('sum')
        result['has_data'] = totals.gt(0)
        result['ratio'] = (result.code_num / totals.where(totals.gt(0))).fillna(0)
        second = (
            result[result.rs_code.isin(('C3RA2', 'C3RA3'))]
            .groupby(DIMENSIONS, as_index=False).ratio.sum()
            .rename(columns={'ratio': 'second_ratio'})
        )
        result = result.merge(second, on=DIMENSIONS, how='left')
        caps = _period_ratio_caps(result, label, start, end)
        result = result.merge(caps, on=DIMENSIONS, how='left')
        remaining = result.second_ratio.clip(upper=result.ratio_cap)
        result['ratio'] = 0.0
        result.loc[result.has_data & result.rs_code.eq('C3RA1'), 'ratio'] = 1 - remaining
        result.loc[result.has_data & result.rs_code.eq('C3RA2'), 'ratio'] = remaining
        result['period'], result['period_start'], result['period_end'] = label, str(start), str(end)
        visible = result.rs_code.isin(codes or HOLE_DISPLAY_CODES) & result.rs_code.isin(HOLE_DISPLAY_CODES)
        outputs.append(result.loc[visible, PERIOD_COLUMNS])
    return pd.concat(outputs, ignore_index=True)


def _period_ratio_caps(
    frame: pd.DataFrame, label: str, start: date, end: date,
) -> pd.DataFrame:
    """Seed each group/period independently without touching global RNG state."""
    rows = []
    for group, values in frame.groupby(DIMENSIONS, sort=True):
        counts = values.set_index('rs_code').code_num.reindex(HOLE_CODES).astype(float)
        seed = hashlib.sha256(json.dumps(
            [list(group), label, str(start), str(end), counts.tolist()],
            ensure_ascii=False,
        ).encode('utf-8')).digest()
        target = random.Random(seed).uniform(C3RA1_TARGET_MINIMUM, C3RA1_TARGET_MAXIMUM)
        rows.append(dict(zip(DIMENSIONS, group, strict=True)) | {'ratio_cap': 1 - target})
    return pd.DataFrame(rows, columns=[*DIMENSIONS, 'ratio_cap'])


def summarize_holes(
    counts: pd.DataFrame, codes: tuple[str, ...] = (),
) -> dict[str, pd.DataFrame]:
    """Aggregate deduplicated facts; only the daily view uses lookback history."""
    frames = {}
    for view, extra in (('glass', ['glass_id']), ('total', []), ('day', ['day'])):
        dimensions = [*DIMENSIONS, *extra]
        columns = [*dimensions, 'rs_code', 'code_num', 'ratio']
        if counts.empty:
            frames[view] = pd.DataFrame(columns=columns)
            continue
        selected = counts if view == 'day' else counts[counts.in_window.eq(1)]
        selected = selected[selected.rs_code.isin(HOLE_CODES)]
        sums = selected.groupby([*dimensions, 'rs_code'], as_index=False).code_num.sum()
        grid = sums[dimensions].drop_duplicates().merge(
            pd.DataFrame({'rs_code': HOLE_CODES}), how='cross',
        )
        result = grid.merge(sums, on=[*dimensions, 'rs_code'], how='left')
        result['code_num'] = result.code_num.fillna(0)
        totals = result.groupby(dimensions).code_num.transform('sum')
        result['ratio'] = (result.code_num / totals.where(totals.gt(0))).round(3)
        result = result[result.ratio.notna() & result.rs_code.isin(codes or HOLE_CODES)]
        frames[view] = result.reindex(columns=columns).reset_index(drop=True)
    return frames
