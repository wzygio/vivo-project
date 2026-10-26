"""Atomic, locked snapshots; each region owns one replaceable result sheet."""

from datetime import date, datetime
from io import BytesIO
import os
from pathlib import Path
import tempfile

import pandas as pd

from src.indicator_domain.core.ijp.alerts import ALERT_COLUMNS, ALERT_CODES, previous_week_range
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets


class IjpAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, region: str, alerts: pd.DataFrame, reference_date: date) -> None:
        if region not in ALERT_CODES:
            raise ValueError('Unknown IJP region')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            sheets = read_workbook_sheets(self.path) if self.path.exists() else {}
            for name in ALERT_CODES:
                sheets.setdefault(name, pd.DataFrame(columns=ALERT_COLUMNS))
            sheets[region] = alerts.reindex(columns=ALERT_COLUMNS).copy()
            metadata = sheets.get('__meta__', pd.DataFrame(columns=['region']))
            start, end = previous_week_range(reference_date)
            row = pd.DataFrame([{
                'region': region, 'reference_date': str(reference_date),
                'period_start': str(start), 'period_end': str(end),
                'updated_at': datetime.now().isoformat(timespec='seconds'),
                'alert_count': len(alerts), 'status': 'success',
            }])
            sheets['__meta__'] = pd.concat(
                [metadata.loc[metadata.region.ne(region)], row], ignore_index=True,
            )
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={
                'options': {'strings_to_formulas': False, 'strings_to_urls': False},
            }) as writer:
                for name, frame in sheets.items():
                    frame.to_excel(writer, sheet_name=name, index=False)
            payload = buffer.getvalue()
            verified = pd.read_excel(BytesIO(payload), sheet_name=region)
            if len(verified) != len(alerts) or list(verified) != ALERT_COLUMNS:
                raise ValueError('IJP snapshot verification failed')
            descriptor, temporary = tempfile.mkstemp(
                prefix='.ijp-alert-', suffix='.xlsx', dir=self.path.parent,
            )
            temporary_path = Path(temporary)
            try:
                with os.fdopen(descriptor, 'wb') as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary_path, self.path)
            finally:
                temporary_path.unlink(missing_ok=True)
