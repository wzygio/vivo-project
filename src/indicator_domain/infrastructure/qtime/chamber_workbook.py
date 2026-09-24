"""Read the supplier's fixed row/column workbook contract without changing it."""

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import BadZipFile

import openpyxl
import pandas as pd
from fr_file_decryption import decrypt_file, is_encrypted_file
from openpyxl.utils.datetime import from_excel

from src.indicator_domain.core.qtime.chamber import CHAMBERS


def read_chamber_workbook(path: Path, *, line: str, temporary_root: Path) -> dict:
    if not is_encrypted_file(path):
        try:
            return _read_standard(path, line)
        except BadZipFile:
            # Enterprise transparent encryption can change visibility between opens.
            pass
    temporary_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='qtime-chamber-', dir=temporary_root) as directory:
        result = decrypt_file(path, output_dir=directory, strategy='excel-values')
        return _read_standard(Path(result.output_path), line)


def _read_standard(path: Path, line: str) -> dict:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook.worksheets[0]
        # Supplier exports declare A1:A1 even when tens of thousands of rows exist.
        sheet.reset_dimensions()
        # Values-only normalization uses a write-only workbook without dimensions.
        rows = sheet.iter_rows(max_col=15, values_only=True)
        header_rows = [next(rows, ()) for _ in range(9)]
        header = header_rows[8]
        if (len(header) != 15 or 'GlassID' not in str(header[1])
                or 'LL1Time' not in str(header[2])
                or tuple(str(value).strip() for value in header[4:15]) != CHAMBERS):
            raise ValueError('Unexpected chamber workbook header')
        targets = pd.to_numeric(pd.Series(header_rows[4][4:15]), errors='coerce')
        if targets.isna().any() or (~targets.between(0, float('inf'), inclusive='neither')).any():
            raise ValueError('Invalid chamber targets')
        records, invalid = [], 0
        for row in rows:
            if all(value is None for value in row[1:15]):
                continue
            glass = str(row[1]).strip() if row[1] is not None else ''
            timestamp = _entry_time(row[2], workbook.epoch)
            if not glass or pd.isna(timestamp):
                invalid += 1
                continue
            records.append((line, glass, timestamp, *row[4:15]))
        frame = pd.DataFrame(records, columns=['line', 'glass_id', 'entry_time', *CHAMBERS]).drop_duplicates()
        if frame.duplicated(['line', 'glass_id', 'entry_time']).any():
            raise ValueError('Conflicting duplicate chamber readings')
        return {
            'measurements': frame,
            'targets': pd.DataFrame({'line': line, 'chamber': CHAMBERS, 'target_seconds': targets}),
            'invalid_rows': invalid,
        }
    finally:
        workbook.close()


def _entry_time(value: object, epoch: datetime) -> pd.Timestamp:
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return pd.Timestamp(from_excel(value, epoch=epoch))
        return pd.to_datetime(value, errors='coerce')
    except (ValueError, TypeError, OverflowError):
        return pd.NaT
