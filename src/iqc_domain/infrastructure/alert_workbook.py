"""Atomic publication of IQC alert rows and matching scope metadata."""

from datetime import datetime
from io import BytesIO
import os
from pathlib import Path
import tempfile

import pandas as pd

from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets


def write_alert_snapshot(path: Path, sheet_name: str, alerts: pd.DataFrame, scope: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with publication_lock(path):
        sheets = read_workbook_sheets(path) if path.exists() else {}
        sheets[sheet_name] = alerts
        sheets['__meta__'] = pd.DataFrame([{
            **scope,
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
        verified = pd.read_excel(BytesIO(payload), sheet_name=sheet_name)
        if len(verified) != len(alerts) or list(verified) != list(alerts):
            raise ValueError('IQC_ALERT_SNAPSHOT_VERIFICATION_FAILED')
        descriptor, temporary = tempfile.mkstemp(
            prefix='.iqc-alert-', suffix='.xlsx', dir=path.parent,
        )
        temporary_path = Path(temporary)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, path)
        finally:
            temporary_path.unlink(missing_ok=True)
