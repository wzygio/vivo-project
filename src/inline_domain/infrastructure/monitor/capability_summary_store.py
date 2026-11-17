"""Independent, atomic workbook storage for CPK or CPM trend history."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
import threading

import pandas as pd

from src.inline_domain.core.monitor.capability_trend import (
    MonthKey,
    capability_history_for_storage,
    normalize_capability_history,
    update_weekly_history,
)
from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns
from src.inline_domain.infrastructure.monitor.excel_alarm_store import read_cached_alarm_workbook
from src.inline_domain.infrastructure.monitor.summary_workbook_store import (
    MonitorSummaryWorkbookError,
    _interprocess_lock,
    replace_product_period_sheets,
)


class CapabilitySummaryStore:
    """Keep one metric's product sheets independent from alarm-rate persistence.

    Reads use one bounded workbook cache call, including enterprise-protected
    workbooks. Writes share the established process/file transaction lock and
    atomic workbook adapter, preserving unrelated product sheets and objects.
    Display labels are derived on reads and omitted from the durable schema.
    """

    _update_lock = threading.Lock()

    def __init__(
        self, workbook_path: Path | str, *, metric: str = "cpk",
        workbook_reader: Callable | None = None,
    ) -> None:
        capability_summary_columns(metric)
        self.workbook_path = Path(workbook_path)
        self.metric = metric
        self._reader = workbook_reader or read_cached_alarm_workbook

    def source_signature(self) -> str:
        try:
            stat = self.workbook_path.stat()
            return f"{self.workbook_path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
        except FileNotFoundError:
            return f"{self.workbook_path.resolve()}:missing"

    def read(self) -> pd.DataFrame:
        try:
            stat = self.workbook_path.stat()
        except FileNotFoundError:
            return normalize_capability_history(pd.DataFrame(), metric=self.metric)
        sheets = self._reader(str(self.workbook_path.resolve()), stat.st_mtime_ns, stat.st_size)
        suffix = f" {self.metric.upper()}"
        frames = []
        for name, frame in sheets.items():
            if not name.endswith(suffix):
                continue
            product = name.removesuffix(suffix).strip()
            if not product:
                raise MonitorSummaryWorkbookError(f"Invalid product summary sheet: {name}")
            try:
                normalized = normalize_capability_history(frame, metric=self.metric)
            except (ValueError, TypeError) as exc:
                raise MonitorSummaryWorkbookError(f"{name}: {exc}") from exc
            if not normalized.empty and not normalized["产品"].eq(product).all():
                raise MonitorSummaryWorkbookError(f"{name} contains another product")
            frames.append(normalized)
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return normalize_capability_history(combined, metric=self.metric)

    def _write(self, rows: pd.DataFrame) -> None:
        result = replace_product_period_sheets(
            self.workbook_path, rows, sheet_suffix=f" {self.metric.upper()}",
            normalizer=lambda frame: capability_history_for_storage(frame, metric=self.metric),
        )
        if not result.written:
            raise MonitorSummaryWorkbookError(result.error or "Capability summary write failed")

    def write_history(self, rows: pd.DataFrame) -> pd.DataFrame:
        """Replace supplied products' complete histories; preserve other products."""
        normalized = normalize_capability_history(rows, metric=self.metric)
        if normalized.empty:
            return self.read()
        with self._update_lock, _interprocess_lock(self.workbook_path):
            current = self.read()
            products = normalized["产品"].unique()
            previous = current.loc[current["产品"].isin(products)].reset_index(drop=True)
            if previous.equals(normalized):
                return current
            self._write(normalized)
            return self.read()

    def refresh_weeks(
        self, weekly_rows: pd.DataFrame, *, complete_months: Iterable[MonthKey] = (),
        as_of: object | None = None,
    ) -> pd.DataFrame:
        """Commit week replacements and their owning aggregates as one transaction."""
        with self._update_lock, _interprocess_lock(self.workbook_path):
            current = self.read()
            updated = update_weekly_history(
                current, weekly_rows, metric=self.metric, complete_months=complete_months,
                as_of=as_of,
            )
            if current.equals(updated):
                return current
            self._write(updated)
            return self.read()
