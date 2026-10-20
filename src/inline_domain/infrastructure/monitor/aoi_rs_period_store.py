"""Atomic AOI_RS aggregate workbook, with one product per sheet and no raw facts."""

from collections.abc import Callable
from io import BytesIO
from pathlib import Path
import os
import re
import tempfile
import threading

import pandas as pd
import streamlit as st

from src.inline_domain.application.monitor.aoi_rs_monitor_service import HISTORY_COLUMNS
from src.inline_domain.infrastructure.monitor.excel_alarm_store import read_cached_alarm_workbook
from src.inline_domain.infrastructure.shared.rolling_snapshot import snapshot_process_lock


@st.cache_data(show_spinner=False, max_entries=8)
def read_period_workbook(path: str, mtime_ns: int, size: int) -> dict[str, pd.DataFrame]:
    try:
        return pd.read_excel(path, sheet_name=None, dtype={"step_id": str, "rs_code": str})
    except Exception:
        return read_cached_alarm_workbook(path, mtime_ns, size)


class AoiRsPeriodStore:
    _guard = threading.Lock()
    _locks: dict[str, threading.RLock] = {}

    def __init__(self, path: Path) -> None:
        self.path = Path(path).resolve()
        with self._guard:
            self._lock = self._locks.setdefault(str(self.path), threading.RLock())

    def read(self) -> pd.DataFrame:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return pd.DataFrame(columns=HISTORY_COLUMNS)
        sheets = read_period_workbook(str(self.path), stat.st_mtime_ns, stat.st_size)
        frames = []
        for product, frame in sheets.items():
            if set(HISTORY_COLUMNS) - set(frame.columns):
                raise ValueError("Invalid AOI_RS history schema")
            if not frame["prod_code"].eq(product).all():
                raise ValueError("AOI_RS history product mismatch")
            frame = frame.copy()
            for column in ("prod_code", "factory", "step_id", "rs_code", "code_desc", "dependency_month"):
                frame[column] = frame[column].fillna("").astype(str)
            frames.append(frame[HISTORY_COLUMNS])
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=HISTORY_COLUMNS)

    def signature(self) -> str:
        try:
            stat = self.path.stat()
            return f"{self.path}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{self.path}:missing"

    def update(self, updater: Callable[[pd.DataFrame], pd.DataFrame]) -> pd.DataFrame:
        with self._lock, snapshot_process_lock(self.path):
            current = self.read()
            updated = updater(current.copy()).reindex(columns=HISTORY_COLUMNS)
            if updated.equals(current):
                return current
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter", engine_kwargs={"options": {"strings_to_formulas": False, "strings_to_urls": False}}) as writer:
                for product, frame in updated.groupby("prod_code", sort=False):
                    if not re.fullmatch(r"[A-Za-z0-9_-]{1,31}", str(product)):
                        raise ValueError("Invalid product sheet name")
                    frame.to_excel(writer, sheet_name=str(product), index=False)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle, temporary = tempfile.mkstemp(prefix=f".{self.path.stem}-", suffix=".xlsx", dir=self.path.parent)
            try:
                with os.fdopen(handle, "wb") as output:
                    output.write(buffer.getvalue())
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temporary, self.path)
            finally:
                Path(temporary).unlink(missing_ok=True)
            return updated
