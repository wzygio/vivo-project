"""Publish the current over-limit parts and retain decisions per physical part."""

from pathlib import Path

import pandas as pd

from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.shared_kernel.infrastructure.alert_decision_sheet import AlertDecisionSheet
from src.shared_kernel.infrastructure.alert_workbook import edit_alert_workbook, snapshot_metadata

PARTS_DECISIONS = AlertDecisionSheet('超规备件', (
    '厂别', '备件类型', '设备类型', '膜层', '制程', '站点', '机台号-腔室',
))


class PartsAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, alerts: pd.DataFrame, *, as_of_date: str, last_update: str) -> pd.DataFrame:
        with edit_alert_workbook(self.path, PARTS_DECISIONS.sheet, preserve_text=True) as sheets:
            public = PARTS_DECISIONS.publish(sheets, alerts.reindex(columns=PARTS_PUBLIC_COLUMNS))
            sheets['__meta__'] = snapshot_metadata(
                {'as_of_date': as_of_date, 'last_update': last_update}, len(public),
            ).assign(candidate_count=len(alerts))
        return public
