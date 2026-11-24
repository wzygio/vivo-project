"""Locked, atomic edits of alert workbooks and their metadata."""

from datetime import datetime
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from io import BytesIO
import os
from pathlib import Path
import tempfile

import pandas as pd

from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets


class SnapshotChangedError(ValueError):
    """The displayed workbook was replaced before the user confirmed the upload."""


def snapshot_metadata(scope: dict[str, str], alert_count: int, *, ledger_count: int | None = None) -> pd.DataFrame:
    record = {**scope, 'updated_at': datetime.now().isoformat(timespec='seconds'),
              'alert_count': alert_count, 'status': 'success'}
    if ledger_count is not None:
        record['ledger_count'] = ledger_count
    return pd.DataFrame([record])


@contextmanager
def edit_alert_workbook(path: Path, sheet_name: str, *, expected_digest: str | None = None,
                        preserve_text: bool = False) -> Iterator[dict[str, pd.DataFrame]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with publication_lock(path):
        if expected_digest is not None:
            digest = sha256(path.read_bytes()).hexdigest() if path.exists() else ''
            if digest != expected_digest:
                raise SnapshotChangedError('台账已更新，请下载最新台账后重新提交。')
        if preserve_text:
            sheets = read_workbook_sheets(path, preserve_text=True) if path.exists() else {}
        else:
            sheets = read_workbook_sheets(path) if path.exists() else {}
        yield sheets
        write_locked_workbook(path, sheets, sheet_name)


def write_locked_workbook(path: Path, sheets: dict, sheet_name: str) -> None:
    """Atomically commit under the caller's publication lock."""
    rows = sheets[sheet_name]
    payload = workbook_bytes(sheets)
    verified = pd.read_excel(BytesIO(payload), sheet_name=sheet_name)
    if len(verified) != len(rows) or list(verified) != list(rows):
        raise ValueError('ALERT_SNAPSHOT_VERIFICATION_FAILED')
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.alert-', suffix='.xlsx', dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def workbook_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={
        'options': {'strings_to_formulas': False, 'strings_to_urls': False},
    }) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    return buffer.getvalue()
