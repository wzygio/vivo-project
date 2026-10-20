"""Atomic product-sheet publication; memory caches only parsed workbook bytes."""
from pathlib import Path
import threading
from typing import Mapping

import pandas as pd
import streamlit as st

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils import excel_tools
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets


@st.cache_data(show_spinner=False, max_entries=32, ttl=ConfigLoader.get_cache_ttl_seconds())
def read_yield_alert_workbook(path: str, mtime_ns: int, size: int) -> dict[str, pd.DataFrame]:
    try:
        return pd.read_excel(path, sheet_name=None, engine="openpyxl")
    except Exception:
        try:
            return excel_tools._read_all_sheets_via_com(Path(path))
        except Exception:
            raise OSError("Yield alert workbook could not be read") from None


def clear_yield_alert_workbook(path: Path) -> None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return
    read_yield_alert_workbook.clear(str(path.resolve()), stat.st_mtime_ns, stat.st_size)


class YieldAlertWorkbookStore:
    _write_lock = threading.RLock()

    def __init__(self, paths: Mapping[str, Path]) -> None:
        self.paths = {key: Path(path) for key, path in paths.items()}
        if set(self.paths) != {"lot", "trend"}:
            raise ValueError("Expected Lot and trend result paths")
        if self.paths["lot"].resolve() == self.paths["trend"].resolve():
            raise ValueError("Lot and trend results require separate workbooks")

    @staticmethod
    def _meta_sheet(product: str) -> str:
        if (not product or len(product) > 25 or any(char in product for char in '[]:*?/\\')):
            raise ValueError("Invalid product worksheet name")
        return f"{product}__meta"

    def _sheets(self, kind: str) -> dict[str, pd.DataFrame]:
        path = self.paths[kind].resolve()
        try:
            stat = path.stat()
        except FileNotFoundError:
            return {}
        return read_yield_alert_workbook(str(path), stat.st_mtime_ns, stat.st_size)

    def read_product(self, product: str, kind: str) -> dict | None:
        sheets = self._sheets(kind)
        meta = sheets.get(self._meta_sheet(product))
        if meta is None:
            return None
        if len(meta) != 1:
            raise ValueError("Invalid Yield publication metadata")
        return {"records": sheets.get(product, pd.DataFrame()).copy(),
                "metadata": meta.iloc[0].where(meta.iloc[0].notna(), None).to_dict()}

    def source_signature(self, kind: str) -> str:
        path = self.paths[kind].resolve()
        try:
            stat = path.stat()
            return f"{path}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{path}:missing"

    def write_product(self, product: str, kind: str, records: pd.DataFrame | None,
                      metadata: dict) -> bool:
        path = self.paths[kind]
        meta_name = self._meta_sheet(product)
        with self._write_lock, publication_lock(path):
            previous = self.read_product(product, kind)
            old = previous["metadata"] if previous else {}
            same_records = records is None or (previous is not None
                and list(previous["records"].columns) == list(records.columns)
                and previous["records"].astype(object).where(previous["records"].notna(), None).to_dict("records")
                    == records.astype(object).where(records.notna(), None).to_dict("records"))
            if old.get("fingerprint") == metadata["fingerprint"] and same_records:
                return False
            merged = {**old, **metadata} if records is None else metadata
            sheets = {meta_name: pd.DataFrame([merged])}
            if records is not None:
                sheets[product] = records
            outcome = replace_workbook_sheets(path, sheets)
            if not outcome.written:
                raise OSError("Yield alert workbook could not be published")
            return True
