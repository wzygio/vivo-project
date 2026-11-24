"""Publish failed material inspections with their durable decisions."""

from datetime import date
from pathlib import Path

import pandas as pd

from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.shared_kernel.infrastructure.alert_decision_sheet import AlertDecisionSheet
from src.shared_kernel.infrastructure.alert_workbook import edit_alert_workbook, snapshot_metadata

EVAPORATION_DECISIONS = AlertDecisionSheet('超规材料', (
    '工厂', '物料号', '物料分类', '特性项目', '报检日期', '检验时间',
))


class EvaporationAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, alerts: pd.DataFrame, *, start: date, end: date,
             products: tuple[str, ...]) -> pd.DataFrame:
        with edit_alert_workbook(self.path, EVAPORATION_DECISIONS.sheet, preserve_text=True) as sheets:
            public = EVAPORATION_DECISIONS.publish(sheets, alerts.reindex(columns=REPORT_COLUMNS))
            sheets['__meta__'] = snapshot_metadata({
                'start_date': str(start), 'end_date': str(end), 'products': '、'.join(products),
            }, len(public)).assign(candidate_count=len(alerts))
        return public
