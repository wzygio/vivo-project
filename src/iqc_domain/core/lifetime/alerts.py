"""Evaluate product/screen lifetime limits on each sample's decay curves."""

from collections.abc import Mapping
from decimal import Decimal

import numpy as np
import pandas as pd

from src.iqc_domain.core.lifetime.lifetime import FORECAST_ANCHOR_HOURS, GROUP_COLUMNS, predict_lifetime_value

LIFETIME_THRESHOLD = 0.95
LIFETIME_ALERT_COLUMNS = (
    *GROUP_COLUMNS, '样品编号', '指标', '规格时间(H)', '规格点数值', '取值方式', '判定阈值',
)
DECAY_METRICS = ('效率衰减', '亮度衰减')


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
