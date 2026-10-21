"""Daily chamber point decoration on report copies, independent of persistence."""

from datetime import date
import hashlib
import random
from typing import Literal

import numpy as np
import pandas as pd


def decorate_chamber_points(
    details: pd.DataFrame, *, start_date: date, end_date: date,
    threshold_seconds: float, minimum_seconds: float, point_count_threshold: int,
    group_by: tuple[str, ...], random_seed: int, mode: Literal['random', 'remove'],
) -> pd.DataFrame:
    """Remove or replace exceeding points when their daily group exceeds the count."""
    result = details.copy()
    if result.empty or start_date > end_date:
        return result
    days = pd.to_datetime(result['entry_time']).dt.normalize()
    eligible = (days.between(pd.Timestamp(start_date), pd.Timestamp(end_date))
                & result['duration_seconds'].gt(threshold_seconds))
    exceeding = result.loc[eligible]
    if exceeding.empty:
        return result
    dimensions = [days.loc[eligible], *(exceeding[column] for column in group_by)]
    counts = exceeding.groupby(dimensions, dropna=False)['duration_seconds'].transform('size')
    selected = eligible.copy()
    selected.loc[eligible] = counts.gt(point_count_threshold)
    if not selected.any():
        return result
    if mode == 'remove':
        return result.loc[~selected].reset_index(drop=True)
    # Stable per-point identities prevent refreshes, sorting or other days from
    # changing random values; a local RNG leaves the global generator untouched.
    identity_columns = ['prod_code', 'line', 'chamber', 'glass_id', 'entry_time', 'duration_seconds']
    values = []
    for identity in result.loc[selected, identity_columns].itertuples(index=False, name=None):
        seed = hashlib.sha256(repr((random_seed, *identity)).encode('utf-8')).digest()
        value = minimum_seconds + random.Random(seed).random() * (threshold_seconds - minimum_seconds)
        values.append(min(value, np.nextafter(threshold_seconds, minimum_seconds)))
    result['duration_seconds'] = result['duration_seconds'].astype(float)
    result.loc[selected, 'duration_seconds'] = values
    result.loc[selected, 'status'] = np.where(
        result.loc[selected, 'duration_seconds'].gt(result.loc[selected, 'target_seconds']),
        '超限', '正常',
    )
    return result
