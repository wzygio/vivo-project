"""CPK period read model using the shared warning-workbook transaction."""

from __future__ import annotations

import pandas as pd

from src.inline_domain.core.monitor.cpk_summary import (
    CPK_SUMMARY_COLUMNS,
    normalize_cpk_records,
)
from src.inline_domain.infrastructure.monitor.summary_workbook_store import (
    MonitorSummaryWorkbookError,
    MonitorSummaryWorkbookStore,
    replace_period_sheet,
)
from src.shared_kernel.utils.excel_tools import WorkbookWriteResult


class CpkSummaryWorkbookStore(MonitorSummaryWorkbookStore):
    """Update CPK under the same file lock as the alarm-rate sheet."""

    sheet_name = "CPK"
    columns = CPK_SUMMARY_COLUMNS

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
