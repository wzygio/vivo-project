"""Project source measurements to anonymous, numerically ordered public facts."""

import numpy as np
import pandas as pd

REPORT_FIELDS = {
    'product_group': '产品型号', 'product_id': '产品状态', 'wo_name': '量产',
    'date_key': '批次号', 'panel_id': '样品编号', 'test_time': '测试时间',
    'bu': '测试画面', 'luminance': '亮度', 'lumi_decay': '亮度衰减',
    'cie_x': 'CIEx', 'cie_xy': 'CIEy', 'iss': 'Iss',
    'curr_decay': '电流衰减', 'efficiency': '效率', 'eff_decay': '效率衰减',
}
GROUP_COLUMNS = ['产品型号', '批次号', '测试画面']
MEASUREMENT_COLUMNS = list(REPORT_FIELDS.values())[7:]
SOURCE_GROUP = ['product_group', 'date_key', 'bu']


class LifetimeDataInvalid(ValueError):
    """Measurements cannot be assigned to unambiguous lifetime curves."""


def project_report(source: pd.DataFrame) -> pd.DataFrame:
    if not set(REPORT_FIELDS).issubset(source.columns):
        raise LifetimeDataInvalid('LIFETIME_COLUMNS_MISSING')
    frame = source.loc[:, list(REPORT_FIELDS)].copy()
    frame.attrs = {}
    for column in [*SOURCE_GROUP, 'panel_id', 'product_id', 'wo_name']:
        frame[column] = frame[column].astype('string')
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
