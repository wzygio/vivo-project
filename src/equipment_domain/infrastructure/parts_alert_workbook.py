"""Publish the current over-limit parts and retain decisions per physical part."""

from pathlib import Path

import pandas as pd

from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.shared_kernel.infrastructure.alert_decision_sheet import AlertDecisionSheet
from src.shared_kernel.infrastructure.alert_workbook import edit_alert_workbook, snapshot_metadata

PARTS_DECISIONS = AlertDecisionSheet('超规备件', (
    '厂别', '备件类型', '设备类型', '膜层', '制程', '站点', '机台号-腔室',
))
PARTS_ALERT_SHEETS = (PARTS_DECISIONS.sheet, PARTS_DECISIONS.candidates, PARTS_DECISIONS.decisions)


def has_parts_alert_history(sheets: dict[str, pd.DataFrame]) -> bool:
    return any(len(sheets[name]) for name in PARTS_ALERT_SHEETS if name in sheets)


def read_parts_publication(sheets: dict[str, pd.DataFrame], meta: pd.Series, *,
                           verify_publication: bool = False) -> pd.DataFrame:
    if has_parts_alert_history(sheets):
        return PARTS_DECISIONS.read(sheets, meta, verify_publication=verify_publication)

    # Before the first breach only the successful inspection receipt is required.
    # Older publishers also wrote empty sheets. Validate any that actually exist.
    candidate_count = meta.get('candidate_count')
    if pd.isna(candidate_count) or candidate_count == '':
        if verify_publication and PARTS_DECISIONS.sheet not in sheets:
            raise ValueError('PARTS_EMPTY_PUBLICATION_MISSING_COUNT')
        candidate_count = 0  # Legacy public-only workbooks did not store this count.
    if float(meta.alert_count) != 0 or float(candidate_count) != 0:
        raise ValueError('PARTS_EMPTY_PUBLICATION_COUNT_MISMATCH')
    for name in PARTS_ALERT_SHEETS:
        if name in sheets:
            required = (*PARTS_DECISIONS.keys, 'Flag') if name == PARTS_DECISIONS.decisions else PARTS_PUBLIC_COLUMNS
            if not set(required) <= set(sheets[name]):
                raise ValueError('PARTS_EMPTY_PUBLICATION_SCHEMA_INVALID')
    return pd.DataFrame(columns=PARTS_PUBLIC_COLUMNS)


class PartsAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, alerts: pd.DataFrame, *, as_of_date: str, last_update: str) -> pd.DataFrame:
        verify_sheet = '__meta__' if alerts.empty else PARTS_DECISIONS.sheet
        with edit_alert_workbook(self.path, verify_sheet, preserve_text=True) as sheets:
            public = alerts.reindex(columns=PARTS_PUBLIC_COLUMNS)
            if len(public) or has_parts_alert_history(sheets):
                public = PARTS_DECISIONS.publish(sheets, public)
            sheets['__meta__'] = snapshot_metadata(
                {'as_of_date': as_of_date, 'last_update': last_update}, len(public),
            ).assign(candidate_count=len(alerts))
        return public
