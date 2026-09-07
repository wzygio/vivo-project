"""Incremental Excel read model for automatic-warning period summaries."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd

from src.inline_domain.core.monitor.period_summary import (
    MONITOR_SUMMARY_COLUMNS,
    current_period_windows,
)
from src.shared_kernel.utils.excel_tools import (
    WorkbookWriteResult,
    read_workbook_sheet,
    replace_workbook_sheets,
)

SUMMARY_SHEET = "报警率"
SUMMARY_KEY_COLUMNS = ["产品", "监控类型", "厂别", "周期类型", "时间标签"]
LEGACY_COLUMNS = [
    "产品",
    "周期类型",
    "时间标签",
    "显示标签",
    "过货量",
    "OOC报警率",
    "SOOS报警率",
    "OOS报警率",
]
COUNT_RATE_PAIRS = [
    ("OOC报警片数", "OOC报警率"),
    ("SOOS报警片数", "SOOS报警率"),
    ("OOS报警片数", "OOS报警率"),
]


class MonitorSummaryWorkbookError(RuntimeError):
    """Raised when the persisted summary cannot be safely read or updated."""


class MonitorSummaryWorkbookStore:
    _update_lock = threading.Lock()

    def __init__(self, workbook_path: Path | str) -> None:
        self._workbook_path = Path(workbook_path)

    @property
    def workbook_path(self) -> Path:
        return self._workbook_path

    def source_signature(self) -> str:
        """Return a cache signature that changes after external workbook edits."""
        try:
            stat = self._workbook_path.stat()
        except OSError:
            return "missing"
        return f"{stat.st_mtime_ns}:{stat.st_size}"

    def read(self) -> pd.DataFrame:
        try:
            source = read_workbook_sheet(self._workbook_path, SUMMARY_SHEET)
        except Exception as exc:
            raise MonitorSummaryWorkbookError(
                f"无法读取报警汇总工作簿：{exc}"
            ) from exc
        return self._normalize(source)

    def upsert_current_periods(
        self, rows: pd.DataFrame, *, as_of: pd.Timestamp
    ) -> pd.DataFrame:
        incoming = self._normalize(rows)
        allowed_periods = {
            (window.period_type, window.time_label, window.display_label)
            for window in current_period_windows(as_of)
        }
        incoming_periods = set(
            incoming[["周期类型", "时间标签", "显示标签"]]
            .itertuples(index=False, name=None)
        )
        if not incoming_periods.issubset(allowed_periods):
            raise ValueError("only current year/quarter/month/week rows may be updated")
        if incoming.duplicated(SUMMARY_KEY_COLUMNS).any():
            raise ValueError("summary rows contain duplicate business keys")

        with self._update_lock, _interprocess_lock(self._workbook_path):
            current = self.read()
            merged = self._merge(current, incoming)
            if current.reset_index(drop=True).equals(merged.reset_index(drop=True)):
                return current
            result = _replace_summary_sheet(self._workbook_path, merged)
            if not result.written:
                raise MonitorSummaryWorkbookError(
                    result.error or "报警汇总工作簿写入失败"
                )
            return self.read()

    @staticmethod
    def _merge(current: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
        incoming_keys = pd.MultiIndex.from_frame(incoming[SUMMARY_KEY_COLUMNS])
        current_keys = pd.MultiIndex.from_frame(current[SUMMARY_KEY_COLUMNS])
        preserved = current.loc[~current_keys.isin(incoming_keys)].copy()
        frames = [frame for frame in (preserved, incoming) if not frame.empty]
        combined = (
            pd.concat([frame.astype(object) for frame in frames], ignore_index=True)
            if frames
            else pd.DataFrame(columns=MONITOR_SUMMARY_COLUMNS)
        )
        return MonitorSummaryWorkbookStore._normalize(
            MonitorSummaryWorkbookStore._sort(combined)
        )

    @staticmethod
    def _normalize(source: pd.DataFrame) -> pd.DataFrame:
        frame = source.copy() if isinstance(source, pd.DataFrame) else pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=MONITOR_SUMMARY_COLUMNS)
        missing_legacy = set(LEGACY_COLUMNS).difference(frame.columns)
        if missing_legacy:
            raise MonitorSummaryWorkbookError(
                f"报警率 sheet 缺少字段：{sorted(missing_legacy)}"
            )
        for column in ("监控类型", "厂别"):
            if column not in frame.columns:
                frame[column] = "ALL"
            frame[column] = frame[column].fillna("ALL").astype(str).str.strip()
            frame.loc[frame[column].eq(""), column] = "ALL"
        for column in ["产品", "周期类型", "时间标签", "显示标签"]:
            frame[column] = frame[column].fillna("").astype(str).str.strip()
        if frame[SUMMARY_KEY_COLUMNS].eq("").any(axis=None):
            raise MonitorSummaryWorkbookError("报警率 sheet 包含空业务键")
        frame["过货量"] = (
            pd.to_numeric(frame["过货量"], errors="coerce").fillna(0).astype(int)
        )
        for count_column, rate_column in COUNT_RATE_PAIRS:
            rate = (
                pd.to_numeric(frame[rate_column], errors="coerce")
                .fillna(0.0)
                .astype("Float64")
            )
            frame[rate_column] = rate
            if count_column not in frame.columns:
                # Rates in the legacy workbook may be rounded and do not carry
                # the physical-Sheet identity needed for exact counts.
                frame[count_column] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
            else:
                count = pd.to_numeric(frame[count_column], errors="coerce")
                frame[count_column] = count.astype("Int64")
        if "Total报警片数" not in frame.columns:
            frame["Total报警片数"] = pd.Series(
                pd.NA, index=frame.index, dtype="Int64"
            )
        else:
            total = pd.to_numeric(frame["Total报警片数"], errors="coerce")
            frame["Total报警片数"] = total.astype("Int64")
        if "Total报警率" not in frame.columns:
            frame["Total报警率"] = pd.NA
        else:
            total_rate = pd.to_numeric(frame["Total报警率"], errors="coerce")
            frame["Total报警率"] = total_rate.astype("Float64")
        if frame.duplicated(SUMMARY_KEY_COLUMNS).any():
            raise MonitorSummaryWorkbookError("报警率 sheet 存在重复业务键")
        return frame[MONITOR_SUMMARY_COLUMNS].copy()

    @staticmethod
    def _sort(frame: pd.DataFrame) -> pd.DataFrame:
        period_order = {"年度": 0, "季度": 1, "月度": 2, "周度": 3}
        sortable = frame.assign(
            _period_order=frame["周期类型"].map(period_order).fillna(99)
        )
        return (
            sortable.sort_values(
                ["产品", "监控类型", "厂别", "_period_order", "时间标签"],
                kind="stable",
            )
            .drop(columns="_period_order")
            .reset_index(drop=True)
        )


@contextmanager
def _interprocess_lock(workbook_path: Path, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Serialize read/merge/write across Streamlit and scheduler processes."""
    digest = hashlib.sha256(str(workbook_path.resolve()).encode("utf-8")).hexdigest()[:16]
    lock_path = Path(tempfile.gettempdir()) / f"vivo-monitor-summary-{digest}.lock"
    handle = lock_path.open("a+b")
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    deadline = time.monotonic() + timeout_seconds
    locked = False
    try:
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise MonitorSummaryWorkbookError("等待汇总工作簿写锁超时")
                time.sleep(0.05)
        yield
    finally:
        try:
            if locked:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _replace_summary_sheet(path: Path, frame: pd.DataFrame) -> WorkbookWriteResult:
    """Update only 报警率, preserving encrypted workbooks and every other object."""
    writable = frame.astype(object).where(frame.notna(), None)
    if not path.exists():
        return replace_workbook_sheets(path, {SUMMARY_SHEET: writable})
    try:
        import openpyxl

        workbook = openpyxl.load_workbook(path, read_only=True)
        workbook.close()
    except Exception:
        return _replace_encrypted_summary_sheet_via_com(path, writable)
    return replace_workbook_sheets(path, {SUMMARY_SHEET: writable})


def _replace_encrypted_summary_sheet_via_com(
    path: Path, frame: pd.DataFrame
) -> WorkbookWriteResult:
    """Atomically edit a protected workbook without rebuilding its other sheets."""
    try:
        import pythoncom
        import win32com.client
    except ImportError as exc:
        return WorkbookWriteResult(False, path, (), f"缺少 Excel COM 支持: {exc}")

    source_was_protected = not _has_zip_signature(path)
    backup_path = path.with_suffix(f"{path.suffix}.bak")
    temp_handle = tempfile.NamedTemporaryFile(
        prefix=f".{path.stem}-",
        suffix=".tmp.xlsx",
        dir=path.parent,
        delete=False,
    )
    temp_path = Path(temp_handle.name)
    temp_handle.close()
    try:
        _copy_bytes(path, backup_path)
        _copy_bytes(path, temp_path)
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        return WorkbookWriteResult(False, path, (), f"创建加密工作簿备份失败: {exc}")

    excel = workbook = worksheet = None
    com_initialized = False
    write_error: Exception | None = None
    try:
        pythoncom.CoInitialize()
        com_initialized = True
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        workbook = excel.Workbooks.Open(str(temp_path.resolve()), ReadOnly=False)
        worksheet = workbook.Worksheets(SUMMARY_SHEET)
        worksheet.UsedRange.ClearContents()
        data_rows = frame.astype(object).where(frame.notna(), None).values.tolist()
        values = [list(frame.columns)] + data_rows
        target = worksheet.Range("A1").Resize(len(values), len(frame.columns))
        target.Value = tuple(tuple(value for value in row) for row in values)
        workbook.Save()
        workbook.Close(SaveChanges=False)
        workbook = None
    except Exception as exc:
        write_error = exc
    finally:
        worksheet = None
        if workbook is not None:
            try:
                workbook.Close(SaveChanges=False)
            except Exception:
                pass
        if excel is not None:
            try:
                excel.Quit()
            except Exception:
                pass
        if com_initialized:
            pythoncom.CoUninitialize()

    if write_error is not None:
        temp_path.unlink(missing_ok=True)
        return WorkbookWriteResult(
            False, path, (), f"Excel COM 更新报警率失败: {write_error}"
        )

    try:
        if source_was_protected and not _wait_for_protected_bytes(temp_path):
            raise ValueError("企业加密保护未能保留，正式文件未变更")
        persisted = read_workbook_sheet(temp_path, SUMMARY_SHEET)
        normalized_actual = MonitorSummaryWorkbookStore._normalize(persisted)
        normalized_expected = MonitorSummaryWorkbookStore._normalize(frame)
        if not normalized_actual.reset_index(drop=True).equals(
            normalized_expected.reset_index(drop=True)
        ):
            raise ValueError("写回后的报警率数据与预期不一致")
        os.replace(temp_path, path)
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        return WorkbookWriteResult(False, path, (), f"报警率写回校验失败: {exc}")
    return WorkbookWriteResult(True, path, (SUMMARY_SHEET,))


def _copy_bytes(source: Path, destination: Path) -> None:
    """Copy protected bytes without asking the shell to decrypt by file extension."""
    with source.open("rb") as source_stream, destination.open("wb") as target_stream:
        shutil.copyfileobj(source_stream, target_stream)
    shutil.copystat(source, destination)


def _wait_for_protected_bytes(path: Path, timeout_seconds: float = 5.0) -> bool:
    """Allow the endpoint protection agent time to re-wrap an Excel COM save."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        if not _has_zip_signature(path):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.1)


def _has_zip_signature(path: Path) -> bool:
    with path.open("rb") as stream:
        return stream.read(4) == b"PK\x03\x04"


__all__ = [
    "MonitorSummaryWorkbookError",
    "MonitorSummaryWorkbookStore",
    "SUMMARY_KEY_COLUMNS",
    "SUMMARY_SHEET",
]
