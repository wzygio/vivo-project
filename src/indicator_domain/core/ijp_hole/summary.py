"""Ratios use all three large-hole classifications before display filtering."""

import pandas as pd

HOLE_CODES = ('C3RA1', 'C3RA2', 'C3RA3')
HOLE_PRINTERS = (
    '3CEE01-IK2-PR1', '3CEE01-IK2-PR2',
    '3CEE02-IK2-PR1', '3CEE02-IK2-PR2',
)
DIMENSIONS = ['productcode', 'line', 'printer']
COUNT_COLUMNS = [*DIMENSIONS, 'glass_id', 'day', 'rs_code', 'code_num', 'in_window']


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
