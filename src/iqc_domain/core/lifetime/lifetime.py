"""Project source measurements to anonymous, numerically ordered public facts."""

import numpy as np
import pandas as pd

REPORT_FIELDS = {
    'product_group': '产品型号', 'product_id': '产品状态', 'wo_name': '量产',
    'date_key': '批次号', 'panel_id': '样品编号', 'test_time': '测试时间',
    'bu': '测试画面', 'luminance': '亮度', 'lumi_decay': '亮度衰减',
    'cie_x': 'CIEx', 'cie_xy': 'CIEy', 'iss': 'Iss',
    'curr_decay': '电流衰减', 'efficiency': '效率', 'eff_decay': '效率衰减',
    'jncd': 'JNCD',
}
GROUP_COLUMNS = ['产品型号', '批次号', '测试画面']
MEASUREMENT_COLUMNS = list(REPORT_FIELDS.values())[7:]
SOURCE_GROUP = ['product_group', 'date_key', 'bu']
FORECAST_ANCHOR_HOURS = (100, 400)
FORECAST_TARGET_HOURS = 1200


class LifetimeDataInvalid(ValueError):
    """Measurements cannot be assigned to unambiguous lifetime curves."""


def predict_lifetime_value(points: pd.DataFrame, *, metric: str) -> float | None:
    """Extrapolate one sample's metric using its exact 100H and 400H points."""
    anchors = points.loc[points['测试时间'].isin(FORECAST_ANCHOR_HOURS)]
    if len(anchors) != 2 or anchors['测试时间'].nunique() != 2:
        return None
    values = anchors.set_index('测试时间')[metric].reindex(FORECAST_ANCHOR_HOURS)
    if not np.isfinite(values.to_numpy(dtype=float)).all():
        return None
    start, end = FORECAST_ANCHOR_HOURS
    slope = (float(values.loc[end]) - float(values.loc[start])) / (end - start)
    prediction = float(values.loc[end]) + slope * (FORECAST_TARGET_HOURS - end)
    return prediction if np.isfinite(prediction) else None


def project_report(
    source: pd.DataFrame, *, enabled_products: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    if not set(REPORT_FIELDS).issubset(source.columns):
        raise LifetimeDataInvalid('LIFETIME_COLUMNS_MISSING')
    frame = source.loc[:, list(REPORT_FIELDS)].copy()
    frame.attrs = {}
    if enabled_products is not None:
        frame = frame.loc[frame['product_group'].isin(enabled_products)].copy()
    for column in [*SOURCE_GROUP, 'panel_id', 'product_id', 'wo_name']:
        frame[column] = frame[column].astype('string')
    # Normalize absent batches before grouping, deduplication and sample numbering.
    missing_batch = frame['date_key'].isna() | frame['date_key'].str.strip().eq('')
    frame['date_key'] = frame['date_key'].mask(missing_batch, '未填写批次')
    identifiers = frame[[*SOURCE_GROUP, 'panel_id']]
    if identifiers.isna().any(axis=None) or any(
        identifiers[column].str.strip().eq('').any() for column in identifiers
    ):
        raise LifetimeDataInvalid('LIFETIME_IDENTITY_MISSING')
    for column in ['test_time', *list(REPORT_FIELDS)[7:]]:
        frame[column] = pd.to_numeric(frame[column], errors='coerce').astype(float)
        frame[column] = frame[column].where(np.isfinite(frame[column]))
    if frame.test_time.isna().any() or frame.test_time.lt(0).any():
        raise LifetimeDataInvalid('LIFETIME_TIME_INVALID')
    frame = frame.drop_duplicates()
    key = [*SOURCE_GROUP, 'panel_id', 'test_time']
    if frame.duplicated(key).any():
        raise LifetimeDataInvalid('LIFETIME_MEASUREMENT_CONFLICT')
    frame = frame.sort_values(key, kind='stable')
    # Rank complete source groups before any interactive filtering/pagination.
    frame['panel_id'] = frame.groupby(SOURCE_GROUP, sort=False)['panel_id'].rank(
        method='dense'
    ).astype('int64')
    public = frame.rename(columns=REPORT_FIELDS).reset_index(drop=True)
    public.attrs = {}
    return public


def filter_report(
    frame: pd.DataFrame, products: list[str], batches: list[str],
    statuses: list[str] | None = None,
) -> pd.DataFrame:
    mask = pd.Series(True, index=frame.index)
    for column, values in [('产品型号', products), ('批次号', batches),
                           ('产品状态', statuses)]:
        if values:
            mask &= frame[column].isin(values)
    return frame.loc[mask].copy()
