import pandas as pd

from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.core.monitor.cpk_summary import normalize_cpk_records


class ReadOnlyStore:
    def read(self):
        return normalize_cpk_records(pd.DataFrame([
            {"产品": "M626", "监控类型": "SPC", "厂别": "ALL",
             "周期类型": "年度", "时间标签": "2026", "显示标签": "Y26",
             "CPK总项目数": 100, "Cpk≥1.33达标率": .8},
        ]))


def test_reads_maintained_year_without_raw_analysis_or_writing():
    result = CpkWorkbookMonitorService(ReadOnlyStore()).build_dashboard(
        products=["M626"], factories=["ARRAY", "OLED", "TP"], end_date="2026-09-08",
    )
    assert result.detail_df.empty
    assert result.summary_df.Y26.iloc[0] == 100
    assert result.summary_df.Y26.iloc[3] == "80.00%"
    assert result.summary_df.Q3.iloc[0] == "—"


def test_missing_selected_product_does_not_show_partial_total():
    result = CpkWorkbookMonitorService(ReadOnlyStore()).build_dashboard(
        products=["M626", "M678"], factories=["ARRAY", "OLED", "TP"], end_date="2026-09-08",
    )
    assert result.summary_df.Y26.iloc[0] == "—"
