"""Read-only, workbook-granularity cache for generated monitor Excel inputs."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Mapping

import pandas as pd
import streamlit as st

from src.shared_kernel.utils import excel_tools
from src.shared_kernel.config import ConfigLoader
from src.inline_domain.core.monitor.excel_contract import ExcelAlarmReadError


@st.cache_data(ttl=ConfigLoader.get_cache_ttl_seconds(), max_entries=32, show_spinner=False)
def read_cached_alarm_workbook(
    path: str, mtime_ns: int, size: int
) -> dict[str, pd.DataFrame]:
    """Only native frames enter the cache; no COM objects or business results."""
    workbook = Path(path)
    try:
        return pd.read_excel(workbook, sheet_name=None)
    except Exception:
        try:
            return excel_tools._read_all_sheets_via_com(workbook)
        except Exception as exc:
            raise ExcelAlarmReadError(
                f"Unable to read generated alarm workbook: {workbook.name}"
            ) from exc


def clear_excel_alarm_cache() -> None:
    read_cached_alarm_workbook.clear()


def clear_alarm_workbook_cache(path: Path | str) -> None:
    """Invalidate only this workbook's current cache entry."""
    workbook = Path(path).resolve()
    try:
        stat = workbook.stat()
    except FileNotFoundError:
        return
    read_cached_alarm_workbook.clear(str(workbook), stat.st_mtime_ns, stat.st_size)


class ExcelAlarmStore:
    """Missing files/sheets are explicit, not an authoritative zero result."""

    def __init__(self, file_paths: Mapping[str, Path | str]) -> None:
        self.file_paths = {scope: Path(path) for scope, path in file_paths.items()}

    def source_path(self, scope: str) -> Path:
        return self.file_paths[scope]

    def read(self, scope: str, product: str) -> dict | None:
        path = self.source_path(scope)
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        sheets = read_cached_alarm_workbook(str(path.resolve()), stat.st_mtime_ns, stat.st_size)
        if product not in sheets:
            return None
        return {
            "frame": sheets[product].copy(),
            "refresh_meta": sheets.get("__refresh_meta__", pd.DataFrame()).copy(),
        }

    def source_signature(self, products: list[str], scopes: list[str]) -> str:
        parts = [",".join(sorted(set(products)))]
        for scope in sorted(set(scopes)):
            path = self.source_path(scope).resolve()
            try:
                stat = path.stat()
                parts.append(f"{scope}:{path}:{stat.st_mtime_ns}:{stat.st_size}")
            except FileNotFoundError:
                parts.append(f"{scope}:{path}:missing")
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
