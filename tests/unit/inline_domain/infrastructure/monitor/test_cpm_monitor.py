"""CPM uses its own sheet and summary while retaining the CPK update policy."""
import pandas as pd
import pytest

from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.infrastructure.monitor.cpm_latest_excel_store import CpmLatestExcelStore
from src.inline_domain.infrastructure.monitor.cpm_summary_workbook_store import CpmSummaryWorkbookStore
from src.inline_domain.infrastructure.monitor.summary_workbook_store import MonitorSummaryWorkbookError
from src.shared_kernel.config import ConfigLoader


def _source():
    return pd.DataFrame([
        dict(prod_code="M626", factory=factory, step_id="1", param_name="CD",
             period_type="week", period_label="2026-W40", cpm_corrected=value, flag=False,
             period_start="2026-09-28", period_end="2026-09-30")
        for factory, value in [("ARRAY", 1.2), ("OLED", 1.5)]
    ])


def _baseline():
    return pd.DataFrame([
        {"产品": "M626", "监控类型": "SPC", "厂别": "ALL", "周期类型": kind,
         "时间标签": label, "显示标签": label, "CPM总项目数": 10,
         "Cpm≥1.33达标率": .8, "达标项目数": 8, "预警项目数": 2}
        for kind, label in [("年度", "2026"), ("季度", "2026-Q4"),
                            ("月度", "2026-10"), ("周度", "2026-W40"), ("周度", "2026-W39")]
    ])


def test_cpm_reader_does_not_consume_cpk_sheet(tmp_path):
    path = tmp_path / "source.xlsx"
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame({"invalid_cpk": [1]}).to_excel(writer, sheet_name="M626", index=False)
        _source().to_excel(writer, sheet_name="M626_cpm", index=False)
    reader = CpmLatestExcelStore(path)
    detail = reader.read_product("M626")
    assert detail["cpm"].tolist() == [1.2, 1.5]
    assert detail["status"].tolist() == ["预警", "已修饰或达标"]
    assert "cpk" not in detail and "cpk_corrected" not in detail
    assert reader.read_product("M678") is None


def test_cpm_update_preserves_cpk_and_month_and_counts_all_factories(tmp_path, monkeypatch):
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    path, source_path = tmp_path / "summary.xlsx", tmp_path / "source.xlsx"
    maintained = pd.DataFrame({"cpk_only": [123]})
    with pd.ExcelWriter(path) as writer:
        _baseline().to_excel(writer, sheet_name="M626 CPM", index=False)
        maintained.to_excel(writer, sheet_name="M626 CPK", index=False)
    _source().to_excel(source_path, sheet_name="M626_cpm", index=False)
    store = CpmSummaryWorkbookStore(path)
    service = CpkWorkbookMonitorService(store, latest_reader=CpmLatestExcelStore(source_path), metric="cpm")
    view = service.build_dashboard(products=["M626"], factories=["OLED"], end_date="2026-10-09")
    assert view.summary_df["指标"].tolist() == ["CPM总项目数", "达标项目数", "预警项目数", "Cpm≥1.33达标率"]
    assert view.summary_df.W40.tolist() == [10, 9, 1, "90.00%"]
    assert view.summary_df.M10.tolist() == [10, 8, 2, "80.00%"]
    assert view.detail_df["factory"].tolist() == ["OLED"]
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="M626 CPK"), maintained)
    before = path.read_bytes()
    service.build_dashboard(products=["M626"], factories=[], end_date="2026-10-09")
    assert path.read_bytes() == before


def test_missing_cpm_baseline_zero_completion_preserves_cpk_sheet(tmp_path):
    path = tmp_path / "summary.xlsx"
    maintained = pd.DataFrame({"cpk_only": [123]})
    maintained.to_excel(path, sheet_name="CPK", index=False)
    records, _ = CpmSummaryWorkbookStore(path).refresh_latest(
        pd.DataFrame(), products=["M626"], factories=["ARRAY", "OLED", "TP"],
        factory_key="ALL", as_of=pd.Timestamp("2026-10-09"),
    )
    assert records["时间标签"].eq("2026-W40").any()
    assert records.iloc[:, 6:].eq(0).all().all()
    pd.testing.assert_frame_equal(pd.read_excel(path, sheet_name="CPK"), maintained)


def test_cpm_date_exclusion_changes_only_latest_week(tmp_path):
    path = tmp_path / "summary.xlsx"
    _baseline().to_excel(path, sheet_name="CPM", index=False)
    store = CpmSummaryWorkbookStore(path)
    before = store.read()
    records, _ = store.refresh_latest(
        pd.DataFrame(), products=["M626"], factories=["ARRAY", "OLED", "TP"],
        factory_key="ALL", as_of=pd.Timestamp("2026-10-09"),
        date_exclusion=(("OLED", "TP"), "2026-10-01", "2026-10-06"),
    )
    assert records.loc[records["时间标签"].eq("2026-W40")].iloc[:, 6:].eq(0).all().all()
    preserved = before.loc[before["时间标签"].ne("2026-W40")].reset_index(drop=True)
    actual = records.loc[records["时间标签"].isin(preserved["时间标签"])].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, preserved)


def test_malformed_cpm_sheet_reports_cpm_contract(tmp_path):
    path = tmp_path / "summary.xlsx"
    pd.DataFrame({"invalid": [1]}).to_excel(path, sheet_name="CPM", index=False)
    with pytest.raises(MonitorSummaryWorkbookError, match="CPM"):
        CpmSummaryWorkbookStore(path).read()
