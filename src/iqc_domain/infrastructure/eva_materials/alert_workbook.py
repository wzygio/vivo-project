"""Atomic publication of the latest failed evaporation inspections."""

from datetime import date, datetime
from io import BytesIO
import os
from pathlib import Path
import tempfile

import pandas as pd

from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets


class EvaporationAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(
        self, alerts: pd.DataFrame, *, start: date, end: date, products: tuple[str, ...],
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            sheets = read_workbook_sheets(self.path) if self.path.exists() else {}
            sheets['超规材料'] = alerts.reindex(columns=REPORT_COLUMNS).copy()
            sheets['__meta__'] = pd.DataFrame([{
                'start_date': str(start), 'end_date': str(end),
                'products': '、'.join(products),
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
            verified = pd.read_excel(BytesIO(payload), sheet_name='超规材料')
            if len(verified) != len(alerts) or list(verified) != list(REPORT_COLUMNS):
                raise ValueError('IQC_ALERT_SNAPSHOT_VERIFICATION_FAILED')
            descriptor, temporary = tempfile.mkstemp(
                prefix='.eva-alert-', suffix='.xlsx', dir=self.path.parent,
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
