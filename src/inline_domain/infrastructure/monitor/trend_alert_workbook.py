"""Atomic product-scoped density evaluation publication; no raw-data access."""
from pathlib import Path

import pandas as pd

from src.inline_domain.infrastructure.monitor.excel_alarm_store import read_cached_alarm_workbook
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_workbook_sheets, replace_workbook_sheets


class TrendAlertWorkbook:
    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self, product: str) -> tuple[pd.DataFrame, dict] | None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return None
        frames = read_cached_alarm_workbook(str(self.path.resolve()), stat.st_mtime_ns, stat.st_size)
        if product not in frames or "__meta__" not in frames:
            return None
        meta = frames["__meta__"]
        if "product" not in meta:
            raise ValueError("Missing trend product metadata")
        selected = meta.loc[meta["product"].astype(str).eq(product)]
        if len(selected) != 1:
            return None
        return frames[product].copy(), selected.iloc[0].to_dict()

    def save(self, product: str, evaluation: pd.DataFrame, metadata: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with publication_lock(self.path):
            frames = read_workbook_sheets(self.path) if self.path.exists() else {}
            meta = frames.get("__meta__", pd.DataFrame(columns=["product"]))
            meta = pd.concat([meta.loc[~meta["product"].astype(str).eq(product)], pd.DataFrame([metadata])], ignore_index=True)
            outcome = replace_workbook_sheets(self.path, {product: evaluation, "__meta__": meta})
            if not outcome.written:
                raise OSError("Trend result publication failed")

    def signature(self) -> str:
        try:
            stat = self.path.stat()
            return f"{self.path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{self.path.resolve()}:missing"
