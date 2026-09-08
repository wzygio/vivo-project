"""Cached, read-only access to product CPK sheets (never CPM sheets)."""
from pathlib import Path

import pandas as pd

from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk
from src.inline_domain.infrastructure.monitor.excel_alarm_store import read_cached_alarm_workbook


class CpkLatestExcelStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def source_updated_at(self) -> str | None:
        try:
            timestamp = self.path.stat().st_mtime
        except FileNotFoundError:
            return None
        return str(pd.Timestamp(timestamp, unit="s", tz="UTC").tz_convert("Asia/Shanghai"))

    def source_signature(self) -> str:
        try:
            stat = self.path.stat()
            return f"{self.path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{self.path.resolve()}:missing"

    def read_product(self, product: str) -> pd.DataFrame | None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return None
        sheets = read_cached_alarm_workbook(str(self.path.resolve()), stat.st_mtime_ns, stat.st_size)
        if product not in sheets:
            return None
        return normalize_latest_cpk(sheets[product], product)
