"""Read the maintained WRGB hour limits and publish the latest lifetime failures."""

from datetime import date
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from src.iqc_domain.core.lifetime.alerts import LIFETIME_ALERT_COLUMNS
from src.iqc_domain.infrastructure.alert_workbook import write_alert_snapshot
from src.shared_kernel.utils.excel_tools import read_workbook_sheet


def read_lifetime_specs(path: Path) -> dict[tuple[str, str], float]:
    path = path.resolve()
    stat = path.stat()
    return _read_spec_generation(path, stat.st_mtime_ns, stat.st_size).copy()


@lru_cache(maxsize=8)
def _read_spec_generation(path: Path, mtime_ns: int, size: int) -> dict[tuple[str, str], float]:
    frame = read_workbook_sheet(path, '800nit寿命规格')
    screens = ('W', 'R', 'G', 'B')
    columns = [f'{screen}-Spec (H)' for screen in screens]
    if frame.empty or not {'产品型号', *columns}.issubset(frame.columns):
        raise ValueError('LIFETIME_SPEC_SCHEMA_INVALID')
    products = frame['产品型号'].astype('string').str.strip()
    if products.isna().any() or products.eq('').any() or products.duplicated().any():
        raise ValueError('LIFETIME_SPEC_PRODUCT_INVALID')
    hours = frame[columns].apply(pd.to_numeric, errors='raise').to_numpy(dtype=float)
    if not np.isfinite(hours).all() or (hours <= 0).any():
        raise ValueError('LIFETIME_SPEC_HOUR_INVALID')
    return {
        (product, screen): float(hour)
        for product, row in zip(products, hours)
        for screen, hour in zip(screens, row)
    }


class LifetimeAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(
        self, alerts: pd.DataFrame, *, reference_date: date, products: tuple[str, ...],
    ) -> None:
        write_alert_snapshot(self.path, '寿命预警', alerts.loc[:, list(LIFETIME_ALERT_COLUMNS)], {
            'reference_date': str(reference_date), 'products': '、'.join(products),
        })
