"""Edit capability rows on a staged copy of the maintained worksheet."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import gc
from math import isinf
import os
from pathlib import Path
import shutil
import tempfile
from typing import Iterator
import zipfile

import openpyxl
from openpyxl.compat.strings import safe_string
from openpyxl.utils.datetime import from_excel, to_excel
import pandas as pd
import psutil

from src.shared_kernel.utils.excel_tools import WorkbookWriteResult, _normalize_excel_com_value


def workbook_version(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    # Enterprise protection can virtualize byte contents and size after COM access.
    return stat.st_mtime_ns, stat.st_ino


def _cell(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    value = value.item() if hasattr(value, "item") else value
    if isinstance(value, float) and isinf(value):
        return "inf" if value > 0 else "-inf"
    return value


def _openxml_value(value: object, epoch: datetime) -> object:
    """Compare against the same numeric precision and date serial Excel can store."""
    if isinstance(value, float):
        return float(safe_string(value))
    if isinstance(value, datetime):
        serial = float(safe_string(to_excel(value, epoch)))
        return from_excel(serial, epoch)
    return value


def _verify_rows(actual, expected, columns: list[str], first_row: int) -> None:
    if len(actual) != len(expected):
        raise ValueError("Capability appended row count failed verification")
    for row_number, (saved, intended) in enumerate(zip(actual, expected), start=first_row):
        if len(saved) != len(intended):
            raise ValueError(f"Capability row {row_number} width failed verification")
        for name, value, target in zip(columns, saved, intended):
            if value != target or isinstance(value, bool) != isinstance(target, bool):
                raise ValueError(f"Capability row {row_number} column {name} failed verification")


@dataclass
class CapabilityRowEdits:
    """Physical Excel rows, numbered before any deletions."""
    updates: dict[int, dict[str, object]] = field(default_factory=dict)
    deletes: list[int] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.updates or self.deletes)


def _append_openxml(staged: Path, sheet: str, columns: list[str], rows: list[tuple],
                    edits: CapabilityRowEdits) -> bool:
    workbook = openpyxl.load_workbook(staged) if staged.stat().st_size else openpyxl.Workbook()
    try:
        if sheet in workbook.sheetnames:
            worksheet = workbook[sheet]
            headers = [cell.value for cell in worksheet[1]]
            if headers != columns:
                raise ValueError("Existing capability headers require explicit manual maintenance")
            if not rows and not edits:
                return False
            for number, fields in edits.updates.items():
                for name, value in fields.items():
                    worksheet.cell(number, columns.index(name) + 1).value = _cell(value)
            for number in reversed(edits.deletes):
                worksheet.delete_rows(number)
        else:
            if staged.stat().st_size == 0:
                workbook.remove(workbook.active)
            worksheet = workbook.create_sheet(sheet)
            worksheet.append(columns)
        first_row = worksheet.max_row + 1
        for row in rows:
            worksheet.append(row)
            for cell, value in zip(worksheet[worksheet.max_row], row):
                if isinstance(value, str):
                    cell.data_type = "s"
        workbook.save(staged)
    finally:
        workbook.close()
    check = openpyxl.load_workbook(staged, data_only=False)
    try:
        actual = list(check[sheet].iter_rows(min_row=first_row, max_row=first_row + len(rows) - 1,
                                           values_only=True)) if rows else []
        expected = [tuple(_openxml_value(value, check.epoch) for value in row) for row in rows]
        _verify_rows(actual, expected, columns, first_row)
    finally:
        check.close()
    return True


def _excel_process_ids() -> set[int]:
    return {process.pid for process in psutil.process_iter(["name"])
            if (process.info["name"] or "").lower() == "excel.exe"}


@contextmanager
def _excel_application() -> Iterator[object]:
    import pythoncom
    import win32com.client
    import win32process

    pythoncom.CoInitialize()
    baseline = _excel_process_ids()
    excel = owned = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        pid = win32process.GetWindowThreadProcessId(excel.Hwnd)[1]
        if pid in baseline:
            raise RuntimeError("Excel instance is not task-owned")
        owned = psutil.Process(pid)
        created_at = owned.create_time()
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.AutomationSecurity = 3
        yield excel
    finally:
        try:
            if excel is not None and owned is not None:
                excel.Quit()
        finally:
            excel = None
            gc.collect()
            try:
                if owned is not None:
                    try:
                        owned.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        if owned.create_time() != created_at:
                            raise RuntimeError("Excel process identity changed during cleanup")
                        owned.terminate()
                        owned.wait(timeout=5)
            finally:
                pythoncom.CoUninitialize()


def _append_excel(staged: Path, sheet: str, columns: list[str], rows: list[tuple],
                  edits: CapabilityRowEdits | None = None) -> bool:
    with _excel_application() as excel:
        workbook = None
        try:
            workbook = excel.Workbooks.Open(str(staged.resolve()), ReadOnly=False, UpdateLinks=0)
            if workbook.ReadOnly:
                raise PermissionError("Capability staging workbook is read-only")
            names = [item.Name for item in workbook.Worksheets]
            if sheet in names:
                worksheet = workbook.Worksheets(sheet)
                used = worksheet.UsedRange
                if used.Row != 1 or used.Column != 1 or used.Columns.Count != len(columns):
                    raise ValueError("Existing capability sheet layout requires manual maintenance")
                headers = worksheet.Range(worksheet.Cells(1, 1), worksheet.Cells(1, len(columns))).Value
                if list(headers[0]) != columns:
                    raise ValueError("Existing capability headers require explicit manual maintenance")
                if not rows and not edits:
                    return False
                first_row = used.Rows.Count + 1
                if edits:
                    for number, fields in edits.updates.items():
                        for name, value in fields.items():
                            cell = worksheet.Cells(number, columns.index(name) + 1)
                            value = _cell(value)
                            if isinstance(value, datetime):
                                cell.NumberFormat = "yyyy-mm-dd hh:mm:ss"
                                value = value.replace(tzinfo=timezone.utc)
                            cell.Value = value
                    for number in reversed(edits.deletes):
                        worksheet.Rows(number).Delete()
                    first_row -= len(edits.deletes)
            else:
                worksheet = workbook.Worksheets.Add(After=workbook.Worksheets(workbook.Worksheets.Count))
                worksheet.Name = sheet
                worksheet.Range(worksheet.Cells(1, 1), worksheet.Cells(1, len(columns))).Value = (tuple(columns),)
                first_row = 2
            if rows:
                for row_offset, row in enumerate(rows):
                    for column_offset, value in enumerate(row):
                        if isinstance(value, datetime):
                            worksheet.Cells(first_row + row_offset, column_offset + 1).NumberFormat = "yyyy-mm-dd hh:mm:ss"
                        elif isinstance(value, str):
                            worksheet.Cells(first_row + row_offset, column_offset + 1).NumberFormat = "@"
                target = worksheet.Range(worksheet.Cells(first_row, 1),
                    worksheet.Cells(first_row + len(rows) - 1, len(columns)))
                # Excel dates are wall-clock values. Avoid pywin32 interpreting a
                # naive timestamp in the host timezone and shifting it to UTC.
                target.Value = tuple(tuple(
                    value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) else value
                    for value in row
                ) for row in rows)
                actual = tuple(tuple(_normalize_excel_com_value(value) for value in row) for row in target.Value)
                _verify_rows(actual, rows, columns, first_row)
            workbook.Save()
            return True
        finally:
            if workbook is not None:
                workbook.Close(SaveChanges=False)


def create_capability_rows(path: Path, sheet_name: str, new_rows: pd.DataFrame) -> WorkbookWriteResult:
    return write_capability_rows(path, sheet_name, new_rows, CapabilityRowEdits(), workbook_version(path))


def write_capability_rows(path: Path, sheet_name: str, new_rows: pd.DataFrame,
                          edits: CapabilityRowEdits, version: tuple[int, int] | None) -> WorkbookWriteResult:
    """Publish one locked read/modify/write, using the version captured before reading."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.stem}-append-", suffix=".xlsx", dir=path.parent)
    os.close(descriptor)
    staged = Path(name)
    backup_stage = None
    try:
        if workbook_version(path) != version:
            raise RuntimeError("Capability workbook changed before staging")
        if version is not None:
            shutil.copy2(path, staged)
        columns = list(new_rows.columns)
        rows = [tuple(_cell(value) for value in row) for row in new_rows.itertuples(index=False, name=None)]
        if version is None or zipfile.is_zipfile(staged):
            changed = _append_openxml(staged, sheet_name, columns, rows, edits)
        else:
            changed = _append_excel(staged, sheet_name, columns, rows, edits)
        if not changed:
            return WorkbookWriteResult(False, path, ())
        if workbook_version(path) != version:
            raise RuntimeError("Capability workbook changed during publication; original retained")
        if version is not None:
            descriptor, name = tempfile.mkstemp(prefix=f".{path.stem}-backup-", suffix=".tmp", dir=path.parent)
            os.close(descriptor)
            backup_stage = Path(name)
            shutil.copy2(path, backup_stage)
            os.replace(backup_stage, path.with_suffix(path.suffix + ".bak"))
        os.replace(staged, path)
        return WorkbookWriteResult(True, path, (sheet_name,))
    except Exception as exc:
        return WorkbookWriteResult(False, path, (), f"Capability row publication failed: {exc}")
    finally:
        staged.unlink(missing_ok=True)
        if backup_stage is not None:
            backup_stage.unlink(missing_ok=True)
