"""Atomic publication of the latest failed evaporation inspections."""

from datetime import date
from pathlib import Path

import pandas as pd

from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.iqc_domain.infrastructure.alert_workbook import write_alert_snapshot


class EvaporationAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(
        self, alerts: pd.DataFrame, *, start: date, end: date, products: tuple[str, ...],
    ) -> None:
        write_alert_snapshot(self.path, '超规材料', alerts.reindex(columns=REPORT_COLUMNS), {
            'start_date': str(start), 'end_date': str(end), 'products': '、'.join(products),
        })
