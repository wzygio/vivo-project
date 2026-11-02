"""Process-local bounded file-version cache for read-only API workbook inputs."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from threading import RLock
from zipfile import BadZipFile
from xml.etree.ElementTree import ParseError

import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException

try:
    from pywintypes import com_error, error as windows_error
except ImportError:
    _OFFICE_ERRORS: tuple[type[Exception], ...] = ()
else:
    _OFFICE_ERRORS = (com_error, windows_error)

try:
    from lxml.etree import XMLSyntaxError
except ImportError:
    _XML_ERRORS: tuple[type[Exception], ...] = ()
else:
    _XML_ERRORS = (XMLSyntaxError,)

_READ_ERRORS = (OSError, ValueError, ImportError, BadZipFile, ParseError, InvalidFileException) + _OFFICE_ERRORS + _XML_ERRORS

DEFAULT_MAX_BYTES = 64 * 1024 * 1024


class WorkbookVersionError(OSError):
    """The requested source version no longer matches the filesystem."""


class WorkbookLimitError(ValueError):
    """An input exceeds configured file, row or parsed-cache limits."""


class WorkbookReadError(OSError):
    """Expected parser or protected-file failures at the local read boundary."""


def read_api_workbook(path: Path) -> dict[str, pd.DataFrame]:
    """Preserve stored text identifiers; reuse protected-file support on fallback.

    Ordinary XLSX does not import Streamlit. The existing protected-file helper
    may use Excel COM and its platform-specific lifecycle; callers must bound
    concurrent reads and must not assume an HTTP timeout terminates Office.
    """
    try:
        return pd.read_excel(path, sheet_name=None, engine="openpyxl", dtype=object)
    except _READ_ERRORS:
        from src.shared_kernel.utils.excel_tools import read_workbook_sheets

        try:
            return read_workbook_sheets(path)
        except _READ_ERRORS as exc:
            raise WorkbookReadError('Published workbook unavailable') from exc


class BoundedWorkbookCache:
    """Serialize cold parses and return independent frames from verified versions.

    Cache hits still stat the source; stale version requests never retrieve an
    older entry. Parser failures leave prior entries intact but never serve them
    as a fallback for the failed version. Limits constrain retained bytes, not
    the parser's transient memory or CPU usage.
    """

    def __init__(
        self,
        *,
        max_entries: int = 16,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_file_bytes: int = DEFAULT_MAX_BYTES,
        max_rows: int = 200_000,
        reader: Callable[[Path], dict[str, pd.DataFrame]] | None = None,
    ) -> None:
        for value in (max_entries, max_bytes, max_file_bytes, max_rows):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError("Workbook cache limits must be positive integers")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._max_file_bytes = max_file_bytes
        self._max_rows = max_rows
        self._reader = reader or read_api_workbook
        self._entries: OrderedDict[tuple[str, int, int], tuple[dict[str, pd.DataFrame], int]] = OrderedDict()
        self._bytes = 0
        self._lock = RLock()

    def __call__(self, path: str, mtime_ns: int, size: int) -> dict[str, pd.DataFrame]:
        source = Path(path).resolve()
        key = (str(source), mtime_ns, size)
        with self._lock:
            self._verify_version(source, mtime_ns, size)
            if size > self._max_file_bytes:
                raise WorkbookLimitError("Workbook exceeds the source-size limit")
            cached = self._entries.get(key)
            if cached is not None:
                self._entries.move_to_end(key)
                return self._copies(cached[0])
            sheets = self._reader(source)
            self._verify_version(source, mtime_ns, size)
            memory = self._validate_and_size(sheets)
            retained = self._copies(sheets)
            self._remove_prior_versions(str(source))
            while self._entries and (
                len(self._entries) >= self._max_entries or self._bytes + memory > self._max_bytes
            ):
                _, (_, removed_bytes) = self._entries.popitem(last=False)
                self._bytes -= removed_bytes
            self._entries[key] = retained, memory
            self._bytes += memory
            return self._copies(retained)

    def clear(self) -> None:
        """Drop this API process's entries without touching files or page caches."""
        with self._lock:
            self._entries.clear()
            self._bytes = 0

    @staticmethod
    def _verify_version(source: Path, mtime_ns: int, size: int) -> None:
        stat = source.stat()
        if (stat.st_mtime_ns, stat.st_size) != (mtime_ns, size):
            raise WorkbookVersionError("Workbook changed during the read")

    def _validate_and_size(self, sheets: dict[str, pd.DataFrame]) -> int:
        if not isinstance(sheets, dict) or any(
            not isinstance(name, str) or not isinstance(frame, pd.DataFrame)
            for name, frame in sheets.items()
        ):
            raise ValueError("Workbook parser returned an invalid sheet collection")
        if sum(len(frame) for frame in sheets.values()) > self._max_rows:
            raise WorkbookLimitError("Workbook exceeds the row limit")
        memory = sum(int(frame.memory_usage(index=True, deep=True).sum()) for frame in sheets.values())
        if memory > self._max_bytes:
            raise WorkbookLimitError("Workbook exceeds the parsed-cache byte limit")
        return memory

    def _remove_prior_versions(self, path: str) -> None:
        for key in [key for key in self._entries if key[0] == path]:
            _, memory = self._entries.pop(key)
            self._bytes -= memory

    @staticmethod
    def _copies(sheets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        return {name: frame.copy(deep=True) for name, frame in sheets.items()}
