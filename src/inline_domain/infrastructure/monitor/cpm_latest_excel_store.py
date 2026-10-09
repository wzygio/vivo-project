"""Read product CPM sheets only from the shared capability workbook."""
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore


class CpmLatestExcelStore(CpkLatestExcelStore):
    metric = "cpm"
    sheet_suffix = "_cpm"
