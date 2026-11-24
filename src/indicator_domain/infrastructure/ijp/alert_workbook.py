"""Publish both IJP regions independently and preserve their manual decisions."""

from datetime import date
from pathlib import Path

import pandas as pd

from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS, ALERT_CODES, previous_week_range
from src.shared_kernel.infrastructure.alert_decision_sheet import AlertDecisionSheet
from src.shared_kernel.infrastructure.alert_workbook import edit_alert_workbook, snapshot_metadata


def ijp_decisions(region: str) -> AlertDecisionSheet:
    if region not in ALERT_CODES:
        raise ValueError('Unknown IJP region')
    return AlertDecisionSheet(region, ('region', 'productcode', 'line', 'printer', 'rs_code',
                                       'period_start', 'period_end'))


class IjpAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, region: str, alerts: pd.DataFrame, reference_date: date) -> pd.DataFrame:
        layout = ijp_decisions(region)
        with edit_alert_workbook(self.path, region, preserve_text=True) as sheets:
            for name in ALERT_CODES:
                sheets.setdefault(name, pd.DataFrame(columns=ALERT_COLUMNS))
            public = layout.publish(sheets, alerts.reindex(columns=ALERT_COLUMNS))
            metadata = sheets.get('__meta__', pd.DataFrame(columns=['region']))
            start, end = previous_week_range(reference_date)
            row = snapshot_metadata({
                'region': region, 'reference_date': str(reference_date),
                'period_start': str(start), 'period_end': str(end),
            }, len(public)).assign(candidate_count=len(alerts))
            sheets['__meta__'] = pd.concat([metadata.loc[metadata.region.ne(region)], row], ignore_index=True)
        return public
