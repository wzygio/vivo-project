"""Locked, atomic publication of the latest over-limit parts snapshot."""

from datetime import datetime
from io import BytesIO
import os
from pathlib import Path
import tempfile

import pandas as pd

from src.equipment_domain.core.parts_alerts import PARTS_PUBLIC_COLUMNS
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets


class PartsAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, alerts: pd.DataFrame, *, as_of_date: str, last_update: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            sheets = read_workbook_sheets(self.path) if self.path.exists() else {}
            sheets['超规备件'] = alerts.reindex(columns=PARTS_PUBLIC_COLUMNS).copy()
            sheets['__meta__'] = pd.DataFrame([{
                'as_of_date': as_of_date, 'last_update': last_update,
                'updated_at': datetime.now().isoformat(timespec='seconds'),
                'alert_count': len(alerts), 'status': 'success',
            }])
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={
                'options': {'strings_to_formulas': False, 'strings_to_urls': False},
            }) as writer:
                for name, frame in sheets.items():
                    frame.to_excel(writer, sheet_name=name, index=False)
            payload = buffer.getvalue()
            verified = pd.read_excel(BytesIO(payload), sheet_name='超规备件')
            if len(verified) != len(alerts) or list(verified) != list(PARTS_PUBLIC_COLUMNS):
                raise ValueError('Parts alert snapshot verification failed')
            descriptor, temporary = tempfile.mkstemp(
                prefix='.parts-alert-', suffix='.xlsx', dir=self.path.parent,
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
