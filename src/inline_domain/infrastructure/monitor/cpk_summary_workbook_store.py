"""CPK period read model using the shared warning-workbook transaction."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.inline_domain.core.monitor.cpk_latest import plan_latest_cpk
from src.inline_domain.core.monitor.cpk_summary import (
    CPK_SUMMARY_COLUMNS,
    normalize_cpk_records,
)
from src.inline_domain.core.shared.date_exclusion import InlineDateExclusion
from src.inline_domain.infrastructure.monitor.summary_workbook_store import (
    MonitorSummaryWorkbookError,
    MonitorSummaryWorkbookStore,
    _interprocess_lock,
)


class CpkSummaryWorkbookStore(MonitorSummaryWorkbookStore):
    """Update CPK under the same file lock as the alarm-rate sheet."""

    sheet_name = "CPK"
    sheet_suffix = " CPK"
    columns = CPK_SUMMARY_COLUMNS
    metric = "cpk"

    def refresh_latest(
        self, detail: pd.DataFrame, *, products: Iterable[str],
        factories: Iterable[str], factory_key: str, as_of: pd.Timestamp,
        date_exclusion: InlineDateExclusion | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        with self._update_lock, _interprocess_lock(self.workbook_path):
            current = self.read()
            incoming, status = plan_latest_cpk(
                current, detail, products=products, factories=factories,
                factory_key=factory_key, as_of=as_of,
                date_exclusion=date_exclusion,
                metric=self.metric,
            )
            if incoming.empty:
                return current, status
            merged = self._merge(current, self._normalize(incoming))
            # Compare sorted values to avoid rewriting an unchanged unsorted workbook.
            if self._sort(current).equals(self._sort(merged)):
                return current, status
            result = self._write(merged)
            if not result.written:
                raise MonitorSummaryWorkbookError(result.error or f"{self.metric.upper()} 汇总工作簿写入失败")
            return self.read(), status

    @classmethod
    def _normalize(cls, source: pd.DataFrame) -> pd.DataFrame:
        try:
            return normalize_cpk_records(source, metric=cls.metric)
        except (ValueError, TypeError) as exc:
            raise MonitorSummaryWorkbookError(f"{cls.metric.upper()} 汇总格式错误：{exc}") from exc

__all__ = ["CpkSummaryWorkbookStore"]
