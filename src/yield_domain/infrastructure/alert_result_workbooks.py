"""Atomic product-sheet publication; memory caches only parsed workbook bytes."""
from pathlib import Path
from io import BytesIO
import threading
from typing import Mapping
from collections.abc import Callable

import pandas as pd
import streamlit as st

from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils import excel_tools
from src.shared_kernel.utils.excel_tools import replace_workbook_sheets
from src.yield_domain.application.alert_results import RELEASE_KEYS, merge_release_controls
from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.shared_kernel.core.decoration_revision import (
    VERSION_SHEET, decision_revision, upload_revision, validate_decision_update,
)

WorkbookReader = Callable[[str, int, int], dict[str, pd.DataFrame]]


@st.cache_data(show_spinner=False, max_entries=32, ttl=ConfigLoader.get_cache_ttl_seconds())
def read_yield_alert_workbook(path: str, mtime_ns: int, size: int) -> dict[str, pd.DataFrame]:
    try:
        return pd.read_excel(path, sheet_name=None, engine="openpyxl", dtype=object)
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

    def __init__(self, paths: Mapping[str, Path], *, workbook_reader: WorkbookReader | None = None) -> None:
        self.paths = {key: Path(path) for key, path in paths.items()}
        self.workbook_reader = workbook_reader or read_yield_alert_workbook
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
        return self.workbook_reader(str(path), stat.st_mtime_ns, stat.st_size)

    @classmethod
    def _flags_sheet(cls, product: str) -> str:
        cls._meta_sheet(product)
        if len(product) > 24:
            raise ValueError("Invalid product release worksheet name")
        return f"{product}__flags"

    def read_product(self, product: str, kind: str) -> dict | None:
        sheets = self._sheets(kind)
        meta = sheets.get(self._meta_sheet(product))
        if meta is None:
            return None
        if len(meta) != 1:
            raise ValueError("Invalid Yield publication metadata")
        decisions = sheets.get(self._flags_sheet(product))
        initialized = decisions is not None
        records = sheets.get(product, pd.DataFrame()).copy()
        if not initialized and meta.iloc[0].get("release_schema_version") == 1:
            raise ValueError("Missing Yield release controls")
        if not initialized and "flag" in records:
            keys = RELEASE_KEYS[kind]
            decisions = records[[*keys, "flag"]].drop_duplicates().copy()
            decisions["flag"] = decisions["flag"].map(parse_decoration_flag)
        return {"records": records,
                "metadata": meta.iloc[0].where(meta.iloc[0].notna(), None).to_dict(),
                "decisions": decisions.copy() if decisions is not None else None,
                "controls_initialized": initialized}

    def is_product_absent(self, product: str, kind: str) -> bool:
        """Missing workbook/product is normal; an unreadable or partial publication is not."""
        names = {product, self._meta_sheet(product), self._flags_sheet(product)}
        sheets = self._sheets(kind)
        return names.isdisjoint(sheets)

    def initialize_lot_controls(self, product: str) -> bool:
        return self.initialize_controls(product, "lot")

    def initialize_controls(self, product: str, kind: str) -> bool:
        """Upgrade existing product sheets without changing their measurement or publication date."""
        path = self.paths[kind]
        flags_name = self._flags_sheet(product)
        with self._write_lock, publication_lock(path):
            previous = self.read_product(product, kind)
            if previous is None:
                raise ValueError("Missing Yield publication for release initialization")
            controlled, decisions = merge_release_controls(kind, previous["records"], previous["decisions"])
            if (previous["controls_initialized"] and "flag" in previous["records"]
                    and previous["metadata"].get("release_schema_version") == 1):
                return False
            metadata = {**previous["metadata"], "release_schema_version": 1}
            outcome = replace_workbook_sheets(path, {
                product: controlled, flags_name: decisions,
                self._meta_sheet(product): pd.DataFrame([metadata]),
            })
            if not outcome.written:
                raise OSError("Yield release initialization could not be published")
            return True

    def source_signature(self, kind: str) -> str:
        path = self.paths[kind].resolve()
        try:
            stat = path.stat()
            return f"{path}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{path}:missing"

    def decision_workbook(self, product: str, kind: str) -> bytes:
        payload = self.read_product(product, kind)
        keys = RELEASE_KEYS[kind]
        records = payload["records"] if payload else pd.DataFrame(columns=keys)
        _, decisions = merge_release_controls(kind, records, payload["decisions"] if payload else None)
        stream = BytesIO()
        with pd.ExcelWriter(stream, engine="xlsxwriter", engine_kwargs={
            "options": {"strings_to_formulas": False, "strings_to_urls": False},
        }) as writer:
            decisions.to_excel(writer, sheet_name=self._flags_sheet(product), index=False)
            records.drop(columns="flag", errors="ignore").to_excel(writer, sheet_name="预警候选", index=False)
            revision = decision_revision(f"yield/{product}/{kind}", decisions, keys)
            pd.DataFrame({"revision": [revision]}).to_excel(writer, sheet_name=VERSION_SHEET, index=False)
            writer.sheets[VERSION_SHEET].hide()
        return stream.getvalue()

    def update_decisions(self, product: str, kind: str, data: bytes) -> None:
        sheet = self._flags_sheet(product)
        edits = excel_tools.read_uploaded_excel_sheet(data, sheet_name=sheet)
        revision = upload_revision(excel_tools.read_uploaded_excel_sheet(data, sheet_name=VERSION_SHEET))
        keys = RELEASE_KEYS[kind]
        if set(edits) != {*keys, "flag"}:
            raise ValueError("决策表的业务键或 flag 列不完整。")
        edits["flag"] = edits["flag"].map(parse_decoration_flag)
        path = self.paths[kind]
        with self._write_lock, publication_lock(path):
            previous = self.read_product(product, kind)
            if previous is None:
                raise ValueError("当前产品尚无发布结果。")
            records = previous["records"]
            _, ledger = merge_release_controls(kind, records, previous["decisions"])
            _, changes = merge_release_controls(kind, records.iloc[:0], edits)
            # A legacy publication may not yet have a persisted decisions sheet.
            existing = ledger if previous["controls_initialized"] or revision else ledger.iloc[:0]
            validate_decision_update(f"yield/{product}/{kind}", existing, changes, keys, revision)
            decisions = ledger.set_index(keys)
            updates = changes.set_index(keys)
            if not updates.index.isin(decisions.index).all():
                raise ValueError("上传表包含未知的预警业务键。")
            decisions["flag"] = decisions["flag"].astype(object)
            decisions.loc[updates.index, "flag"] = updates["flag"]
            effective, saved = merge_release_controls(kind, records, decisions.reset_index())
            result = replace_workbook_sheets(path, {
                product: effective, sheet: saved,
                self._meta_sheet(product): pd.DataFrame([{**previous["metadata"], "release_schema_version": 1}]),
            })
            if not result.written:
                raise OSError("Yield decisions could not be saved")

    def write_product(self, product: str, kind: str, records: pd.DataFrame | None,
                      metadata: dict) -> bool:
        path = self.paths[kind]
        meta_name = self._meta_sheet(product)
        with self._write_lock, publication_lock(path):
            previous = self.read_product(product, kind)
            old = previous["metadata"] if previous else {}
            decisions = None
            if records is not None:
                records, decisions = merge_release_controls(
                    kind, records, previous["decisions"] if previous else None,
                )
                metadata = {**metadata, "release_schema_version": 1}
            same_records = records is None or (previous is not None
                and list(previous["records"].columns) == list(records.columns)
                and previous["records"].astype(object).where(previous["records"].notna(), None).to_dict("records")
                    == records.astype(object).where(records.notna(), None).to_dict("records"))
            controls_ready = records is None or (previous is not None and previous["controls_initialized"]
                                                and old.get("release_schema_version") == 1)
            if controls_ready and old.get("fingerprint") == metadata["fingerprint"] and same_records:
                return False
            merged = {**old, **metadata} if records is None else metadata
            sheets = {meta_name: pd.DataFrame([merged])}
            if records is not None:
                sheets[product] = records
            if decisions is not None:
                sheets[self._flags_sheet(product)] = decisions
            outcome = replace_workbook_sheets(path, sheets)
            if not outcome.written:
                raise OSError("Yield alert workbook could not be published")
            return True
