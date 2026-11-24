"""Evaluate product/screen lifetime limits on each sample's decay curves."""

from collections.abc import Mapping
from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd

from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.iqc_domain.core.lifetime.lifetime import FORECAST_ANCHOR_HOURS, GROUP_COLUMNS, predict_lifetime_value

LIFETIME_THRESHOLD = 0.95
LIFETIME_ALERT_COLUMNS = (
    *GROUP_COLUMNS, '样品编号', '指标', '规格时间(H)', '规格点数值', '取值方式', '判定阈值',
)
LIFETIME_LEDGER_COLUMNS = (*LIFETIME_ALERT_COLUMNS, 'Flag', '本期状态')
LIFETIME_KEY_COLUMNS = [*GROUP_COLUMNS, '样品编号', '指标']
DECAY_METRICS = ('效率衰减', '亮度衰减')


class LifetimeLedgerInvalid(ValueError):
    """A validation message suitable for the ledger editor."""


def normalize_lifetime_ledger(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate saved/uploaded cells and expand a nine-column legacy workbook."""
    legacy = set(frame) == set(LIFETIME_ALERT_COLUMNS)
    if not frame.columns.is_unique or (not legacy and set(frame) != set(LIFETIME_LEDGER_COLUMNS)):
        raise LifetimeLedgerInvalid('请保留“寿命预警”工作表及下载台账的全部原始列。')
    rows = frame.reindex(columns=LIFETIME_LEDGER_COLUMNS).copy()
    rows['批次号'] = rows['批次号'].map(
        lambda v: f'{v.year}/{v.month}/{v.day}' if isinstance(v, date) and pd.notna(v) else v
    )
    for col in ('产品型号', '批次号', '测试画面', '指标', '取值方式'):
        rows[col] = rows[col].astype('string').str.strip()
        if rows[col].isna().any() or rows[col].eq('').any():
            raise LifetimeLedgerInvalid(f'“{col}”不能为空。')
    for col, allowed in [('测试画面', tuple('WRGB')),
                         ('指标', DECAY_METRICS), ('取值方式', ('实测', '插值', '预测'))]:
        if not rows[col].isin(allowed).all():
            raise LifetimeLedgerInvalid(f'“{col}”包含无效值。')
    for col in ('样品编号', '规格时间(H)', '规格点数值', '判定阈值'):
        rows[col] = pd.to_numeric(rows[col], errors='coerce')
        if not np.isfinite(rows[col].to_numpy(dtype=float)).all():
            raise LifetimeLedgerInvalid(f'“{col}”需要填写有效数值。')
    samples = rows['样品编号']
    if not (samples.gt(0) & samples.mod(1).eq(0)).all() or not rows['规格时间(H)'].gt(0).all():
        raise LifetimeLedgerInvalid('样品编号须为正整数，规格时间须大于零。')
    if not rows['判定阈值'].eq(LIFETIME_THRESHOLD).all():
        raise LifetimeLedgerInvalid('判定阈值须为 0.95。')
    if rows.duplicated(LIFETIME_KEY_COLUMNS).any():
        raise LifetimeLedgerInvalid('同一产品、批次、画面的样品与指标只能有一条预警。')
    if legacy:
        rows['Flag'] = False
        rows['本期状态'] = np.where(rows['规格点数值'].lt(LIFETIME_THRESHOLD), '超规', '达标')
    else:
        try:
            flags = rows['Flag'].map(lambda value: value.strip() if isinstance(value, str) else value)
            rows['Flag'] = flags.replace({'0': False, '1': True}).map(parse_decoration_flag)
        except ValueError as exc:
            raise LifetimeLedgerInvalid('Flag 须填写 True、False 或 Delete。') from exc
        if not rows['本期状态'].isin(('超规', '达标', '未判定')).all():
            raise LifetimeLedgerInvalid('本期状态须为超规、达标或未判定。')
    return rows.reset_index(drop=True)


def reconcile_lifetime_ledger(existing: pd.DataFrame, evaluations: pd.DataFrame) -> pd.DataFrame:
    """Refresh the measurement at its current spec time while preserving manual decisions."""
    previous = normalize_lifetime_ledger(existing).set_index(LIFETIME_KEY_COLUMNS)
    current = normalize_lifetime_ledger(evaluations.loc[:, list(LIFETIME_ALERT_COLUMNS)])
    current = current.set_index(LIFETIME_KEY_COLUMNS)
    matched = current.index.isin(previous.index)
    updated = current.loc[matched | current['本期状态'].eq('超规')].copy()
    updated['Flag'] = previous['Flag'].reindex(updated.index, fill_value=False)
    history = previous.loc[~previous.index.isin(updated.index)].assign(本期状态='未判定')
    order = previous.index.union(updated.index, sort=False)
    return pd.concat([updated, history]).reindex(order).reset_index().loc[:, list(LIFETIME_LEDGER_COLUMNS)]


def merge_lifetime_upload(
    existing: pd.DataFrame, uploaded: pd.DataFrame, products: tuple[str, ...],
) -> pd.DataFrame:
    """Patch only existing decisions. The producer owns facts, eligibility and product scope."""
    del products
    previous = normalize_lifetime_ledger(existing).set_index(LIFETIME_KEY_COLUMNS)
    incoming = uploaded.assign(本期状态='未判定') if 'Flag' in uploaded else uploaded
    edited = normalize_lifetime_ledger(incoming).set_index(LIFETIME_KEY_COLUMNS)
    if not edited.index.isin(previous.index).all():
        raise LifetimeLedgerInvalid('上传包含未发布的预警记录，请下载最新台账后修改。')
    if 'Flag' in uploaded:
        previous['Flag'] = previous['Flag'].astype(object)
        previous.loc[edited.index, 'Flag'] = edited['Flag']
    return previous.reset_index().loc[:, list(LIFETIME_LEDGER_COLUMNS)]


def released_lifetime_alerts(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = normalize_lifetime_ledger(ledger)
    public = rows['Flag'].eq(False) & rows['本期状态'].eq('超规') & rows['规格点数值'].lt(LIFETIME_THRESHOLD)
    return rows.loc[public, list(LIFETIME_ALERT_COLUMNS)].reset_index(drop=True)


def _spec_point(points: pd.DataFrame, metric: str, hour: float) -> tuple[float, str] | None:
    valid = points.loc[points[metric].notna(), ['测试时间', metric]].sort_values('测试时间')
    if valid.empty:
        return None
    times = valid['测试时间'].to_numpy()
    values = valid[metric].to_numpy()
    exact = values[times == hour]
    if exact.size:
        return float(exact[0]), '实测'
    if times[0] < hour < times[-1]:
        right = np.searchsorted(times, hour)
        start, end = (Decimal(str(t)) for t in times[right - 1:right + 1])
        first, last = (Decimal(str(y)) for y in values[right - 1:right + 1])
        elapsed = Decimal(str(hour)) - start
        return float(first + (last - first) * elapsed / (end - start)), '插值'
    if hour > FORECAST_ANCHOR_HOURS[-1]:
        value = predict_lifetime_value(points, metric=metric, target_hour=hour)
        if value is not None:
            return value, '预测'
    return None


def evaluate_lifetime_specs(
    report: pd.DataFrame, specs: Mapping[tuple[str, str], float],
) -> pd.DataFrame:
    rows = []
    for key, points in report.groupby([*GROUP_COLUMNS, '样品编号'], sort=True):
        product, _, screen, _ = key
        hour = specs.get((product, screen))
        if hour is None:
            continue
        for metric in DECAY_METRICS:
            point = _spec_point(points, metric, hour)
            if point is not None:
                value, method = point
                rows.append((*key, metric, hour, value, method, LIFETIME_THRESHOLD))
    return pd.DataFrame(rows, columns=LIFETIME_ALERT_COLUMNS)


def lifetime_spec_alerts(evaluations: pd.DataFrame) -> pd.DataFrame:
    return evaluations.loc[evaluations['规格点数值'].lt(LIFETIME_THRESHOLD)].reset_index(drop=True)
