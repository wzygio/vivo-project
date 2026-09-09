"""Incremental Excel read model for automatic-warning period summaries."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
import time
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

import pandas as pd
import numpy as np

from src.inline_domain.core.monitor.weekly_replacement import (
    WEEK_MARKER, WEEK_TRACKING_COLUMNS, CONTRIBUTION_COLUMNS, plan_weekly_replacement,
)

from src.inline_domain.core.monitor.period_summary import (
    MONITOR_SUMMARY_COLUMNS,
    current_period_windows,
)
from src.shared_kernel.utils.excel_tools import (
    WorkbookWriteResult,
    list_workbook_sheet_names,
    read_workbook_sheet,
    replace_workbook_sheets,
)

SUMMARY_SHEET = "报警率"
ALARM_SHEET_SUFFIX = "报警率"
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
    """Shared period-sheet persistence; subclasses supply their sheet contract."""

    _update_lock = threading.Lock()
    sheet_name = SUMMARY_SHEET
    legacy_sheet_name = SUMMARY_SHEET
    sheet_suffix = ALARM_SHEET_SUFFIX
    columns = MONITOR_SUMMARY_COLUMNS + WEEK_TRACKING_COLUMNS

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
            sheet_names = list_workbook_sheet_names(self._workbook_path)
            if sheet_names is None:
                raise MonitorSummaryWorkbookError("无法枚举汇总工作簿的 sheet")
            product_sheets = sorted(
                name
                for name in sheet_names
                if name != self.legacy_sheet_name and name.endswith(self.sheet_suffix)
            )
            if not product_sheets:
                source = read_workbook_sheet(
                    self._workbook_path, self.legacy_sheet_name
                )
                return self._normalize(source)
            frames = [self._read_product_sheet(name) for name in product_sheets]
            source = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        except Exception as exc:
            if isinstance(exc, MonitorSummaryWorkbookError):
                raise
            raise MonitorSummaryWorkbookError(
                f"无法读取 {self.sheet_suffix} 汇总工作簿：{exc}"
            ) from exc
        return self._normalize(source)

    def _read_product_sheet(self, sheet_name: str) -> pd.DataFrame:
        product = sheet_name[: -len(self.sheet_suffix)].strip()
        if not product:
            raise MonitorSummaryWorkbookError(f"无效的产品 sheet 名：{sheet_name}")
        frame = self._normalize(read_workbook_sheet(self._workbook_path, sheet_name))
        products = set(frame["产品"].astype(str)) if not frame.empty else set()
        if products and products != {product}:
            raise MonitorSummaryWorkbookError(
                f"{sheet_name} 只能包含产品 {product}，实际为 {sorted(products)}"
            )
        return frame

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
            if incoming.empty:
                return current
            merged = self._merge(current, incoming)
            if current.reset_index(drop=True).equals(merged.reset_index(drop=True)):
                return current
            result = self._write(merged)
            if not result.written:
                raise MonitorSummaryWorkbookError(
                    result.error or f"{self.sheet_name} 汇总工作簿写入失败"
                )
            return self.read()

    def refresh_current_week(
        self, alerts: pd.DataFrame, *, products: Iterable[str], scope_key: str,
        factory_key: str, as_of: pd.Timestamp,
        available_types: Mapping[str, set[str]],
    ) -> tuple[pd.DataFrame, list[str]]:
        """Replace the open week's contribution using the locked persisted baseline."""
        with self._update_lock, _interprocess_lock(self._workbook_path):
            current = self.read()
            incoming, warnings = plan_weekly_replacement(
                current, alerts, products=products, scope_key=scope_key,
                factory_key=factory_key, as_of=as_of, available_types=available_types,
            )
            if incoming.empty:
                return current, warnings
            merged = self._merge(current, self._normalize(incoming))
            if current.reset_index(drop=True).equals(merged.reset_index(drop=True)):
                return current, warnings
            result = self._write(merged)
            if not result.written:
                raise MonitorSummaryWorkbookError(result.error or "报警率汇总工作簿写入失败")
            return self.read(), warnings

    def _write(self, frame: pd.DataFrame) -> WorkbookWriteResult:
        return replace_product_period_sheets(
            self._workbook_path,
            frame,
            sheet_suffix=self.sheet_suffix,
            normalizer=self._normalize,
        )

    @classmethod
    def _merge(cls, current: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
        incoming_keys = pd.MultiIndex.from_frame(incoming[SUMMARY_KEY_COLUMNS])
        current_keys = pd.MultiIndex.from_frame(current[SUMMARY_KEY_COLUMNS])
        preserved = current.loc[~current_keys.isin(incoming_keys)].copy()
        frames = [frame for frame in (preserved, incoming) if not frame.empty]
        combined = (
            pd.concat([frame.astype(object) for frame in frames], ignore_index=True)
            if frames
            else pd.DataFrame(columns=cls.columns)
        )
        return cls._normalize(cls._sort(combined))

    @staticmethod
    def _normalize(source: pd.DataFrame) -> pd.DataFrame:
        frame = source.copy() if isinstance(source, pd.DataFrame) else pd.DataFrame()
        if frame.empty:
            return pd.DataFrame(columns=MonitorSummaryWorkbookStore.columns)
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
            pd.to_numeric(frame["过货量"], errors="coerce").astype("Int64")
        )
        for count_column, rate_column in COUNT_RATE_PAIRS:
            rate = (
                pd.to_numeric(frame[rate_column], errors="coerce")
                .astype("Float64")
            )
            frame[rate_column] = rate
            if count_column not in frame.columns:
                frame[count_column] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
            count = pd.to_numeric(frame[count_column], errors="coerce")
            # User-approved initialization of historical counts from maintained rates.
            initialized = np.floor(frame["过货量"] * rate + 0.5)
            frame[count_column] = count.fillna(initialized).astype("Int64")
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
        total_initialized = np.floor(frame["过货量"] * pd.to_numeric(
            frame["Total报警率"], errors="coerce"
        ) + 0.5)
        frame["Total报警片数"] = frame["Total报警片数"].fillna(total_initialized).astype("Int64")
        for column in WEEK_TRACKING_COLUMNS:
            if column not in frame:
                frame[column] = pd.NA
        frame[WEEK_MARKER] = frame[WEEK_MARKER].astype("string")
        for column in CONTRIBUTION_COLUMNS.values():
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
        if frame.duplicated(SUMMARY_KEY_COLUMNS).any():
            raise MonitorSummaryWorkbookError("报警率 sheet 存在重复业务键")
        return frame[MonitorSummaryWorkbookStore.columns].copy()

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
    canonical_path = os.path.normcase(str(workbook_path.resolve()))
    digest = hashlib.sha256(canonical_path.encode("utf-8")).hexdigest()[:16]
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
    """Update product alarm sheets, preserving every unrelated workbook object."""
    return replace_product_period_sheets(
        path,
        frame,
        sheet_suffix=ALARM_SHEET_SUFFIX,
        normalizer=MonitorSummaryWorkbookStore._normalize,
    )


def replace_product_period_sheets(
    path: Path,
    frame: pd.DataFrame,
    *,
    sheet_suffix: str,
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
) -> WorkbookWriteResult:
    """Atomically write one sheet per product for a summary data type."""
    normalized = normalizer(frame)
    sheets = {
        _product_sheet_name(product, sheet_suffix): rows.reset_index(drop=True)
        for product, rows in normalized.groupby("产品", sort=True)
    }
    if not sheets:
        return WorkbookWriteResult(False, path, (), "没有可写入的产品汇总数据")
    writable = {
        name: rows.astype(object).where(rows.notna(), None)
        for name, rows in sheets.items()
    }
    if not path.exists():
        return replace_workbook_sheets(path, writable)
    try:
        import openpyxl

        workbook = openpyxl.load_workbook(path, read_only=True)
        workbook.close()
    except Exception:
        return _replace_encrypted_period_sheets_via_com(
            path, writable, normalizer=normalizer
        )
    return replace_workbook_sheets(path, writable)


def _product_sheet_name(product: object, sheet_suffix: str) -> str:
    normalized = str(product).strip()
    name = f"{normalized}{sheet_suffix}"
    if not normalized or len(name) > 31 or any(char in name for char in "[]:*?/\\"):
        raise MonitorSummaryWorkbookError(f"产品名无法生成合法 Excel sheet：{product!r}")
    return name


def replace_period_sheet(
    path: Path, frame: pd.DataFrame, *, sheet_name: str,
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
) -> WorkbookWriteResult:
    """Write one summary sheet while retaining the workbook's other sheets."""
    writable = frame.astype(object).where(frame.notna(), None)
    if not path.exists():
        return replace_workbook_sheets(path, {sheet_name: writable})
    try:
        import openpyxl

        workbook = openpyxl.load_workbook(path, read_only=True)
        workbook.close()
    except Exception:
        if sheet_name == SUMMARY_SHEET:
            return _replace_encrypted_summary_sheet_via_com(path, writable)
        return _replace_encrypted_summary_sheet_via_com(
            path, writable, sheet_name=sheet_name, normalizer=normalizer
        )
    return replace_workbook_sheets(path, {sheet_name: writable})


def _replace_encrypted_summary_sheet_via_com(
    path: Path, frame: pd.DataFrame, *, sheet_name: str = SUMMARY_SHEET,
    normalizer: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> WorkbookWriteResult:
    """Compatibility wrapper for one protected summary sheet."""
    normalize = normalizer or MonitorSummaryWorkbookStore._normalize
    return _replace_encrypted_period_sheets_via_com(
        path, {sheet_name: frame}, normalizer=normalize
    )


def _replace_encrypted_period_sheets_via_com(
    path: Path,
    sheets: Mapping[str, pd.DataFrame],
    *,
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
) -> WorkbookWriteResult:
    """Atomically edit protected product sheets without rebuilding the workbook."""
    try:
        import pythoncom
        import win32com.client
    except ImportError as exc:
        return WorkbookWriteResult(False, path, (), f"缺少 Excel COM 支持: {exc}")

    target_sheets = tuple(sheets)
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

    excel = workbook = None
    com_initialized = False
    write_error: Exception | None = None
    try:
        pythoncom.CoInitialize()
        com_initialized = True
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        workbook = excel.Workbooks.Open(str(temp_path.resolve()), ReadOnly=False)
        for sheet_name, frame in sheets.items():
            try:
                worksheet = workbook.Worksheets(sheet_name)
            except Exception:
                worksheet = workbook.Worksheets.Add(
                    After=workbook.Worksheets(workbook.Worksheets.Count)
                )
                worksheet.Name = sheet_name
            worksheet.UsedRange.ClearContents()
            data_rows = frame.astype(object).where(frame.notna(), None).values.tolist()
            values = [list(frame.columns)] + data_rows
            target = worksheet.Range("A1").Resize(len(values), len(frame.columns))
            target.Value = tuple(tuple(value for value in row) for row in values)
            worksheet = None
        workbook.Save()
        workbook.Close(SaveChanges=False)
        workbook = None
    except Exception as exc:
        write_error = exc
    finally:
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
            False,
            path,
            (),
            f"Excel COM 更新 {', '.join(target_sheets)} 失败: {write_error}",
        )

    try:
        if source_was_protected and not _wait_for_protected_bytes(temp_path):
            raise ValueError("企业加密保护未能保留，正式文件未变更")
        for sheet_name, frame in sheets.items():
            persisted = read_workbook_sheet(temp_path, sheet_name)
            normalized_actual = normalizer(persisted)
            normalized_expected = normalizer(frame)
            if not normalized_actual.reset_index(drop=True).equals(
                normalized_expected.reset_index(drop=True)
            ):
                raise ValueError(f"写回后的 {sheet_name} 数据与预期不一致")
        os.replace(temp_path, path)
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        return WorkbookWriteResult(False, path, (), f"产品 sheet 写回校验失败: {exc}")
    return WorkbookWriteResult(True, path, target_sheets)


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
    "replace_product_period_sheets",
]
