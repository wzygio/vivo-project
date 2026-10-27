"""Atomic per-shop Q-Time result publication, independent from raw source snapshots."""

from __future__ import annotations

from io import BytesIO
import os
from pathlib import Path
import tempfile

import pandas as pd
import streamlit as st

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets
from src.indicator_domain.application.qtime.alert_results import QTIME_ALERT_COLUMNS, QTIME_METADATA_COLUMNS, QTIME_SHOPS


@st.cache_data(max_entries=16, ttl=ConfigLoader.get_cache_ttl_seconds(), show_spinner=False)
def read_qtime_alert_workbook(path: str, mtime_ns: int, size: int) -> dict[str, pd.DataFrame]:
    return read_workbook_sheets(Path(path))


class QTimeAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize_empty_template(self) -> bool:
        """Create an explicit header-only template, preserving every existing file."""
        from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            if self.path.exists():
                return False
            sheets = {"__meta__": pd.DataFrame(columns=QTIME_METADATA_COLUMNS)}
            for shop in QTIME_SHOPS:
                sheets[f"alerts_{shop}"] = pd.DataFrame(columns=QTIME_ALERT_COLUMNS)
                sheets[f"products_{shop}"] = pd.DataFrame(columns=["prodcode", "step_desc", "lot_id", "timekey"])
            result = replace_workbook_sheets(self.path, sheets)
            if not result.written:
                raise OSError("Q-Time empty template initialization failed")
        return True

    def read(self) -> dict[str, pd.DataFrame] | None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return None
        return read_qtime_alert_workbook(str(self.path.resolve()), stat.st_mtime_ns, stat.st_size)

    def source_signature(self) -> str:
        try:
            stat = self.path.stat()
            return f"{self.path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{self.path.resolve()}:missing"

    def save(self, shop: str, alerts: pd.DataFrame, products: pd.DataFrame, metadata: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            sheets = read_workbook_sheets(self.path) if self.path.exists() else {}
            meta = sheets.get("__meta__", pd.DataFrame(columns=["shop", "fingerprint"]))
            prior = meta.loc[meta["shop"].eq(shop)]
            if len(prior) == 1 and prior.iloc[0].get("fingerprint") == metadata["fingerprint"]:
                return
            sheets[f"alerts_{shop}"] = alerts.copy()
            sheets[f"products_{shop}"] = products.copy()
            sheets["__meta__"] = pd.concat([
                meta.loc[meta["shop"].ne(shop)], pd.DataFrame([metadata]),
            ], ignore_index=True)
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter", engine_kwargs={
                "options": {"strings_to_formulas": False, "strings_to_urls": False},
            }) as writer:
                for name, frame in sheets.items():
                    frame.to_excel(writer, sheet_name=name, index=False)
            payload = buffer.getvalue()
            checked = pd.read_excel(BytesIO(payload), sheet_name=f"alerts_{shop}")
            if len(checked) != len(alerts) or list(checked) != list(alerts):
                raise ValueError("Q-Time alert publication verification failed")
            handle, temporary = tempfile.mkstemp(prefix=".qtime-alert-", suffix=".xlsx", dir=self.path.parent)
            path = Path(temporary)
            try:
                with os.fdopen(handle, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(path, self.path)
            finally:
                path.unlink(missing_ok=True)
