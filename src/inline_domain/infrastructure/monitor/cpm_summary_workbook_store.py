"""CPM summaries share the alarm/CPK transaction while owning separate sheets."""
import pandas as pd

from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns
from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
from src.inline_domain.infrastructure.monitor.summary_workbook_store import MonitorSummaryWorkbookError
from src.shared_kernel.utils.excel_tools import list_workbook_sheet_names, read_workbook_sheet


class CpmSummaryWorkbookStore(CpkSummaryWorkbookStore):
    metric = "cpm"
    sheet_name = "CPM"
    legacy_sheet_name = "CPM"
    sheet_suffix = " CPM"
    columns = capability_summary_columns("cpm")

    def read(self) -> pd.DataFrame:
        """Keep unified-sheet compatibility local to CPM."""
        try:
            names = list_workbook_sheet_names(self.workbook_path)
            if names is None:
                raise MonitorSummaryWorkbookError("无法枚举 CPM 汇总工作簿的 sheet")
            if self.legacy_sheet_name in names and not any(
                name.endswith(self.sheet_suffix) for name in names
            ):
                return self._normalize(
                    read_workbook_sheet(self.workbook_path, self.legacy_sheet_name)
                )
        except MonitorSummaryWorkbookError:
            raise
        except Exception as exc:
            raise MonitorSummaryWorkbookError(f"无法读取 CPM 汇总工作簿：{exc}") from exc
        return super().read()
