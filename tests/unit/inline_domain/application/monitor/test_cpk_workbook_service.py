import pandas as pd

from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.core.monitor.cpk_summary import normalize_cpk_records


def test_factory_selection_changes_detail_only_and_excluded_summary_is_shown_as_zero(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
    from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk
    from src.shared_kernel.config import ConfigLoader

    rule = (("OLED", "TP"), "2026-10-01", "2026-10-06")
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: rule)
    source = normalize_latest_cpk(pd.DataFrame([
        dict(prod_code="M626", factory=factory, step_id="1", param_name="CD",
             period_type="week", period_label="2026-W40", cpk_corrected=1.2, flag=False)
        for factory in ["ARRAY", "OLED"]
    ]), "M626")
    store = CpkSummaryWorkbookStore(tmp_path / "summary.xlsx")
    class Latest:
        def read_product(self, product):
            return source.copy()
    service = CpkWorkbookMonitorService(store, latest_reader=Latest())
    array = service.build_dashboard(products=["M626"], factories=["ARRAY"], end_date="2026-10-09")
    oled = service.build_dashboard(products=["M626"], factories=["OLED"], end_date="2026-10-09")
    pd.testing.assert_frame_equal(array.summary_df, oled.summary_df)
    assert array.summary_df.W40.tolist() == [0, 0, 0, "0.00%"]
    assert array.detail_df["factory"].tolist() == ["ARRAY"]
    assert oled.detail_df.empty
    assert store.read()["厂别"].eq("ALL").all()


def test_summary_counts_all_factories_and_exclusion_preserves_existing_year_quarter_month(tmp_path, monkeypatch):
    from src.inline_domain.infrastructure.monitor.cpk_summary_workbook_store import CpkSummaryWorkbookStore
    from src.inline_domain.core.monitor.cpk_latest import normalize_latest_cpk
    from src.shared_kernel.config import ConfigLoader

    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion", lambda: None)
    path = tmp_path / "summary.xlsx"
    baseline = pd.DataFrame([
        {"产品": "M626", "监控类型": "SPC", "厂别": "ALL", "周期类型": kind,
         "时间标签": label, "显示标签": label, "CPK总项目数": 10,
         "Cpk≥1.33达标率": .8, "达标项目数": 8, "预警项目数": 2}
        for kind, label in [("年度", "2026"), ("季度", "2026-Q4"),
                            ("月度", "2026-10"), ("周度", "2026-W40"), ("周度", "2026-W39")]
    ])
    baseline.to_excel(path, sheet_name="CPK", index=False)
    source = normalize_latest_cpk(pd.DataFrame([
        dict(prod_code="M626", factory=factory, step_id="1", param_name="CD",
             period_type="week", period_label="2026-W40", cpk_corrected=1.2, flag=False)
        for factory in ["ARRAY", "OLED"]
    ]), "M626")
    class Latest:
        def read_product(self, product):
            return source.copy()
    store = CpkSummaryWorkbookStore(path)
    service = CpkWorkbookMonitorService(store, latest_reader=Latest())
    view = service.build_dashboard(products=["M626"], factories=["ARRAY"], end_date="2026-10-09")
    assert view.summary_df.W40.tolist() == [10, 8, 2, "80.00%"]
    assert view.detail_df["factory"].tolist() == ["ARRAY"]
    before = store.read()
    monkeypatch.setattr(ConfigLoader, "get_inline_data_exclusion",
                        lambda: (("OLED", "TP"), "2026-10-01", "2026-10-06"))
    view = service.build_dashboard(products=["M626"], factories=[], end_date="2026-10-09")
    assert view.summary_df.W40.tolist() == [0, 0, 0, "0.00%"]
    assert view.summary_df.M10.tolist() == [10, 8, 2, "80.00%"]
    assert view.summary_df.Q4.tolist() == [10, 8, 2, "80.00%"]
    assert view.summary_df.Y26.tolist() == [10, 8, 2, "80.00%"]
    assert view.detail_df.empty
    preserved = before.loc[before["时间标签"].ne("2026-W40")].reset_index(drop=True)
    after = store.read().loc[lambda rows: rows["时间标签"].ne("2026-W40")].reset_index(drop=True)
    pd.testing.assert_frame_equal(after, preserved)


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
