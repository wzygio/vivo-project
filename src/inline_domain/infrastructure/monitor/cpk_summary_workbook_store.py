"""CPK period read model using the shared warning-workbook transaction."""

from __future__ import annotations

import pandas as pd
from collections.abc import Iterable

from src.inline_domain.core.monitor.cpk_latest import plan_latest_cpk

from src.inline_domain.core.monitor.cpk_summary import (
    CPK_SUMMARY_COLUMNS,
    normalize_cpk_records,
)
from src.inline_domain.infrastructure.monitor.summary_workbook_store import (
    MonitorSummaryWorkbookError,
    MonitorSummaryWorkbookStore,
    replace_period_sheet,
    _interprocess_lock,
)
from src.shared_kernel.utils.excel_tools import WorkbookWriteResult


class CpkSummaryWorkbookStore(MonitorSummaryWorkbookStore):
    """Update CPK under the same file lock as the alarm-rate sheet."""

    sheet_name = "CPK"
    columns = CPK_SUMMARY_COLUMNS

    def refresh_latest(
        self, detail: pd.DataFrame, *, products: Iterable[str],
        factories: Iterable[str], factory_key: str, as_of: pd.Timestamp,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        with self._update_lock, _interprocess_lock(self.workbook_path):
            current = self.read()
            incoming, status = plan_latest_cpk(
                current, detail, products=products, factories=factories,
                factory_key=factory_key, as_of=as_of,
            )
            if incoming.empty:
                return current, status
            merged = self._merge(current, self._normalize(incoming))
            # Compare sorted values to avoid rewriting an unchanged unsorted workbook.
            if self._sort(current).equals(self._sort(merged)):
                return current, status
            result = self._write(merged)
            if not result.written:
                raise MonitorSummaryWorkbookError(result.error or "CPK 汇总工作簿写入失败")
            return self.read(), status

    @staticmethod
    def _normalize(source: pd.DataFrame) -> pd.DataFrame:
        try:
            return normalize_cpk_records(source)
        except (ValueError, TypeError) as exc:
            raise MonitorSummaryWorkbookError(f"CPK 汇总格式错误：{exc}") from exc

    def _write(self, frame: pd.DataFrame) -> WorkbookWriteResult:
        return replace_period_sheet(
            self.workbook_path, frame, sheet_name=self.sheet_name,
            normalizer=self._normalize,
        )


__all__ = ["CpkSummaryWorkbookStore"]
