"""CPM summaries share the alarm/CPK transaction while owning separate sheets."""
from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns
from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore


class CpmSummaryWorkbookStore(CpkSummaryWorkbookStore):
    metric = "cpm"
    sheet_name = "CPM"
    legacy_sheet_name = "CPM"
    sheet_suffix = " CPM"
    columns = capability_summary_columns("cpm")
