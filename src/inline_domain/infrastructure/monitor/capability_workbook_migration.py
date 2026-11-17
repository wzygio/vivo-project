"""Explicit, backed-up split of the maintained combined summary workbook."""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
import gc
import hashlib
import json
import os
import shutil
from uuid import uuid4

import pandas as pd


def _category(name: str) -> str:
    if name.endswith(" CPK"):
        return "cpk"
    if name.endswith(" CPM"):
        return "cpm"
    if name.endswith("报警率"):
        return "alarm"
    raise ValueError(f"Unclassified source sheet must be preserved explicitly: {name}")


def _values(frame: pd.DataFrame) -> tuple:
    converted = frame.astype(object).where(frame.notna(), None)
    return (tuple(frame.columns), *tuple(converted.itertuples(index=False, name=None)))


def split_summary_workbook(source: Path, targets: Mapping[str, Path], *,
                           revised_sheets: Mapping[str, pd.DataFrame],
                           expected_source_signature: str | None = None) -> dict:
    """Serialize source/target publication with their existing workbook locks."""
    from src.inline_domain.infrastructure.monitor.summary_workbook_store import _interprocess_lock

    paths = sorted({Path(source).resolve(), *(Path(path).resolve() for path in targets.values())}, key=str)
    with ExitStack() as locks:
        for path in paths:
            locks.enter_context(_interprocess_lock(path))
        return _split_summary_workbook_locked(source, targets, revised_sheets=revised_sheets,
                                              expected_source_signature=expected_source_signature)


def _split_summary_workbook_locked(source: Path, targets: Mapping[str, Path], *,
                                 revised_sheets: Mapping[str, pd.DataFrame],
                                 expected_source_signature: str | None) -> dict:
    """Publish three verified files; preserve a byte-for-byte original backup.

    Revised capability sheets have been validated by the application/core before
    this adapter is called. Alarm sheets retain values, formulas and formatting.
    A source changed by another writer during staging aborts publication.
    """
    source = source.resolve()
    if set(targets) != {"alarm", "cpk", "cpm"}:
        raise ValueError("Exactly three independently configured targets are required")
    targets = {key: Path(path).resolve() for key, path in targets.items()}
    if len(set(targets.values())) != 3 or source in targets.values():
        raise ValueError("Split targets must be distinct from the source")
    if any(path.exists() for path in targets.values()):
        raise FileExistsError("Split target exists; refusing to overwrite maintained inputs")
    signature = hashlib.sha256(source.read_bytes()).hexdigest()
    original_mtime = source.stat().st_mtime_ns
    token = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid4().hex[:6]
    backup = source.with_name(source.name + f".before-capability-split-{token}.bak")
    shutil.copy2(source, backup)
    stages = {key: path.with_name(path.stem + f".staging-{token}.xlsx") for key, path in targets.items()}
    names = {}
    try:
        if source.read_bytes()[:2] == b"PK":
            from openpyxl import load_workbook
            from src.inline_domain.application.monitor.capability_rebuild_provenance import workbook_signature
            if (expected_source_signature is not None
                    and workbook_signature(pd.read_excel(source, sheet_name=None)) != expected_source_signature):
                raise RuntimeError("Source workbook changed after historical planning")
            for category, stage in stages.items():
                stage.parent.mkdir(parents=True, exist_ok=True)
                book = load_workbook(source)
                try:
                    for name in list(book.sheetnames):
                        if _category(name) != category:
                            del book[name]
                    for name, frame in revised_sheets.items():
                        if _category(name) != category:
                            continue
                        sheet = book[name] if name in book else book.create_sheet(name)
                        for cell in list(sheet[1]):
                            if cell.value == "显示标签":
                                sheet.delete_cols(cell.column)
                                break
                        for row in sheet:
                            for cell in row:
                                cell.value = None
                        for row_index, row in enumerate(_values(frame), 1):
                            for column_index, value in enumerate(row, 1):
                                sheet.cell(row_index, column_index, value)
                        for column_index, column in enumerate(frame.columns, 1):
                            if "达标率" in column:
                                for row_index in range(2, len(frame) + 2):
                                    sheet.cell(row_index, column_index).number_format = "0.00%"
                    names[category] = list(book.sheetnames)
                    book.save(stage)
                finally:
                    book.close()
        else:
            names = _split_com(source, stages, revised_sheets, expected_source_signature=expected_source_signature)
        if source.stat().st_mtime_ns != original_mtime:
            raise RuntimeError("Source workbook changed during split; staged files were not published")
        published = {}
        try:
            for category, stage in stages.items():
                # A same-directory hard link is atomic and fails if the target
                # was created by another writer; os.replace would overwrite it.
                stat = stage.stat()
                identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                os.link(stage, targets[category])
                published[targets[category]] = identity
        except BaseException:
            for path, identity in published.items():
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns) == identity:
                    path.unlink()
            raise
        return {"source": str(source), "backup": str(backup), "source_sha256": signature,
                "targets": {key: str(path) for key, path in targets.items()}, "sheets": names}
    finally:
        for stage in stages.values():
            stage.unlink(missing_ok=True)


def _split_com(source: Path, stages: Mapping[str, Path], revised: Mapping[str, pd.DataFrame], *,
               expected_source_signature: str | None = None) -> dict:
    import pythoncom
    import psutil
    import win32com.client
    import win32process
    from src.shared_kernel.config import ConfigLoader

    pythoncom.CoInitialize()
    excel = book = original = sheet = None
    result = {}
    identity = None
    previous = {item.pid for item in psutil.process_iter(["name"])
                if (item.info["name"] or "").lower() == "excel.exe"}
    evidence = ConfigLoader.get_project_root() / "output/test-results/capability-summary/migration-process.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    record = {"cleanup_confirmed": False}
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        _, pid = win32process.GetWindowThreadProcessId(excel.Hwnd)
        if pid in previous:
            excel = None
            raise RuntimeError("Refusing to use a preexisting Excel process")
        identity = (pid, psutil.Process(pid).create_time())
        record.update(pid=pid, created=identity[1])
        evidence.write_text(json.dumps(record, indent=2), encoding="utf-8")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.EnableEvents = False
        excel.AutomationSecurity = 3
        original = excel.Workbooks.Open(str(source), UpdateLinks=0, ReadOnly=True)
        if expected_source_signature is not None:
            from src.inline_domain.application.monitor.capability_rebuild_provenance import workbook_signature
            frames = {}
            for worksheet in original.Worksheets:
                values = worksheet.UsedRange.Value
                if values is None:
                    frames[worksheet.Name] = pd.DataFrame()
                    continue
                values = [[datetime(value.year, value.month, value.day, value.hour, value.minute,
                                    value.second, value.microsecond) if isinstance(value, datetime) else value
                           for value in row] for row in values]
                frames[worksheet.Name] = pd.DataFrame(values[1:], columns=values[0])
            if workbook_signature(frames) != expected_source_signature:
                raise RuntimeError("Source workbook changed after historical planning")
        source_names = [worksheet.Name for worksheet in original.Worksheets]
        source_content = {worksheet.Name: (worksheet.UsedRange.Address, worksheet.UsedRange.Formula)
                          for worksheet in original.Worksheets}
        for name in source_names:
            _category(name)
        for category, stage in stages.items():
            stage.parent.mkdir(parents=True, exist_ok=True)
            original.SaveCopyAs(str(stage))
            book = excel.Workbooks.Open(str(stage), UpdateLinks=0, ReadOnly=False)
            if book.ReadOnly:
                raise PermissionError("Staged summary workbook is read-only")
            for name in source_names:
                if _category(name) != category:
                    book.Worksheets(name).Delete()
            for name, frame in revised.items():
                if _category(name) != category:
                    continue
                existing = [worksheet.Name for worksheet in book.Worksheets]
                sheet = book.Worksheets(name) if name in existing else book.Worksheets.Add()
                sheet.Name = name
                headers = sheet.UsedRange.Rows(1).Value
                if headers and isinstance(headers[0], tuple) and "显示标签" in headers[0]:
                    sheet.Columns(headers[0].index("显示标签") + 1).Delete()
                sheet.UsedRange.ClearContents()
                values = _values(frame)
                sheet.Range(sheet.Cells(1, 1), sheet.Cells(len(values), len(values[0]))).Value = values
                for column_index, column in enumerate(frame.columns, 1):
                    if "达标率" in column:
                        sheet.Range(sheet.Cells(2, column_index), sheet.Cells(max(2, len(values)), column_index)).NumberFormat = "0.00%"
                sheet = None
            result[category] = [worksheet.Name for worksheet in book.Worksheets]
            if category == "alarm":
                for name in result[category]:
                    if book.Worksheets(name).UsedRange.Formula != original.Worksheets(name).UsedRange.Formula:
                        raise RuntimeError("Alarm sheet content changed during split")
            book.Save()
            book.Close(SaveChanges=False)
            book = None
        # Enterprise protection can re-encrypt bytes while preserving workbook
        # content/mtime. Compare a fresh logical read, not unstable cipher bytes.
        original.Close(SaveChanges=False)
        original = None
        original = excel.Workbooks.Open(str(source), UpdateLinks=0, ReadOnly=True)
        verified = {worksheet.Name: (worksheet.UsedRange.Address, worksheet.UsedRange.Formula)
                    for worksheet in original.Worksheets}
        if verified != source_content:
            raise RuntimeError("Source workbook content changed during split")
        return result
    finally:
        cleanup_errors = []
        for owned_book in (book, original):
            if owned_book is not None:
                try:
                    owned_book.Close(SaveChanges=False)
                except Exception as exc:
                    cleanup_errors.append(type(exc).__name__)
        sheet = book = original = owned_book = None
        try:
            if excel is not None:
                excel.Quit()
        except Exception as exc:
            cleanup_errors.append(type(exc).__name__)
        finally:
            excel = None
            gc.collect()
            pythoncom.CoUninitialize()
        if identity is not None:
            try:
                process = psutil.Process(identity[0])
                if process.create_time() == identity[1]:
                    try:
                        process.wait(timeout=10)
                    except psutil.TimeoutExpired:
                        if process.create_time() == identity[1] and process.pid not in previous:
                            process.terminate()
                            process.wait(timeout=10)
            except psutil.NoSuchProcess:
                pass
            try:
                record["cleanup_confirmed"] = psutil.Process(identity[0]).create_time() != identity[1]
            except psutil.NoSuchProcess:
                record["cleanup_confirmed"] = True
        record["cleanup_errors"] = cleanup_errors
        evidence.write_text(json.dumps(record, indent=2), encoding="utf-8")
        if identity is not None and not record["cleanup_confirmed"]:
            raise RuntimeError("Owned Excel process cleanup could not be verified")
