"""Runtime weekly refresh reuses monitored totals and the ledger's release flags."""

import pandas as pd
import pytest

from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
from src.inline_domain.core.monitor.capability_trend import normalize_capability_history
from src.inline_domain.infrastructure.monitor.capability_summary_store import CapabilitySummaryStore


class WeeklySource:
    def __init__(self, totals=None):
        self.calls = []
        self.totals = totals or {}

    def read_weekly_totals(self, product, weeks, *, metric, as_of):
        self.calls.append((product, tuple(weeks), metric))
        return pd.DataFrame([{"prod_code": product, "period_label": week,
                              "total_projects": self.totals.get(week, 10)} for week in weeks])


class Ledger:
    def read_product(self, product):
        rows = [
            {"prod_code": product, "factory": "ARRAY", "step_id": "1", "param_name": name,
             "period_type": "week", "period_label": "2026-W46", "flag": flag,
             "cpk_corrected": 999, "period_start": "2026-11-15", "period_end": "2026-11-15",
             "status": "已修饰或达标"}
            for name, flag in [("released", False), ("released", "false"), ("hidden", True)]
        ]
        return pd.DataFrame(rows)


def seed(store, *, metric="cpk"):
    rows = [{"产品": "M626", "周期类型": kind, "时间标签": label,
             f"{metric.upper()}总项目数": total, "达标项目数": total,
             "预警项目数": 0, f"{metric.capitalize()}≥1.33达标率": 1.0}
            for kind, label, total in [("周度", "2026-W46", 5), ("月度", "2026-11", 25),
                                       ("季度", "2026-Q4", 25), ("年度", "2026", 25),
                                       ("月度", "2026-10", 100)]]
    store.write_history(normalize_capability_history(pd.DataFrame(rows), metric=metric))


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_latest_week_counts_false_flags_without_reapplying_threshold_or_span(tmp_path, metric):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx", metric=metric)
    seed(store, metric=metric)
    source = WeeklySource()
    service = CpkWorkbookMonitorService(store, latest_reader=Ledger(), metric=metric, weekly_source=source)
    view = service.build_dashboard(products=["M626"], factories=["OLED"], end_date="2026-11-17")
    assert source.calls == [("M626", ("2026-W46",), metric)]
    persisted = store.read()
    week = persisted.loc[persisted["时间标签"].eq("2026-W46")].iloc[0]
    assert week[f"{metric.upper()}总项目数"] == 10
    assert week["预警项目数"] == 1
    assert week["达标项目数"] == 9
    assert week[f"{metric.capitalize()}≥1.33达标率"] == .9
    assert persisted.loc[persisted["时间标签"].eq("2026-11"), f"{metric.upper()}总项目数"].item() == 30
    assert persisted.loc[persisted["时间标签"].eq("2026-10"), f"{metric.upper()}总项目数"].item() == 100
    assert view.summary_df.W46.tolist() == [10, 9, 1, "90.00%"]
    assert view.detail_df.empty
    before = store.workbook_path.stat().st_mtime_ns
    service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    assert store.workbook_path.stat().st_mtime_ns == before


def test_new_week_requires_full_owned_month_and_preserves_other_months(tmp_path):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx")
    source = WeeklySource()
    service = CpkWorkbookMonitorService(store, latest_reader=Ledger(), weekly_source=source)
    service.build_dashboard(products=["M626"], factories=["ARRAY"], end_date="2026-11-17")
    assert source.calls == [("M626", ("2026-W44", "2026-W45", "2026-W46"), "cpk")]
    result = store.read()
    month = result.loc[result["时间标签"].eq("2026-11")].iloc[0]
    assert month["CPK总项目数"] == 30
    assert month["预警项目数"] == 1
    assert not result["时间标签"].eq("2026-10").any()


def test_incomplete_totals_cannot_publish_zero_or_change_history(tmp_path):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx")
    seed(store)
    before = store.workbook_path.read_bytes()
    class Missing:
        def read_weekly_totals(self, product, weeks, *, metric, as_of):
            return pd.DataFrame(columns=["prod_code", "period_label", "total_projects"])
    service = CpkWorkbookMonitorService(store, latest_reader=Ledger(), weekly_source=Missing())
    with pytest.raises(ValueError, match="Incomplete weekly"):
        service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    assert store.workbook_path.read_bytes() == before


def test_warning_count_above_total_is_rejected_before_any_write(tmp_path):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx")
    seed(store)
    before = store.workbook_path.read_bytes()
    source = WeeklySource({"2026-W46": 0})
    service = CpkWorkbookMonitorService(store, latest_reader=Ledger(), weekly_source=source)
    with pytest.raises(ValueError):
        service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    assert store.workbook_path.read_bytes() == before


def test_missing_alert_sheet_counts_zero_but_missing_workbook_fails(tmp_path):
    from src.inline_domain.infrastructure.monitor.cpm_latest_excel_store import CpmLatestExcelStore

    store = CapabilitySummaryStore(tmp_path / "history.xlsx", metric="cpm")
    seed(store, metric="cpm")
    ledger = tmp_path / "ledger.xlsx"
    service = CpkWorkbookMonitorService(
        store, latest_reader=CpmLatestExcelStore(ledger, require_workbook=True),
        weekly_source=WeeklySource(), metric="cpm",
    )
    before = store.workbook_path.read_bytes()
    with pytest.raises(FileNotFoundError):
        service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    assert store.workbook_path.read_bytes() == before

    # A CPK sheet can exist before this product's first CPM warning.
    pd.DataFrame({"flag": [False]}).to_excel(ledger, sheet_name="M626", index=False)
    workbook_before = ledger.read_bytes()
    view = service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    assert view.summary_df.W46.tolist() == [10, 10, 0, "100.00%"]
    assert ledger.read_bytes() == workbook_before


@pytest.mark.parametrize("sheet_exists", [False, True])
def test_zero_projects_can_have_no_alerts_without_inventing_a_missing_monitored_sheet(tmp_path, sheet_exists):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx")
    class NoAlerts:
        def read_product(self, product):
            return pd.DataFrame() if sheet_exists else None
    source = WeeklySource(dict.fromkeys(["2026-W44", "2026-W45", "2026-W46"], 0))
    service = CpkWorkbookMonitorService(store, latest_reader=NoAlerts(), weekly_source=source)
    view = service.build_dashboard(products=["Z622"], factories=[], end_date="2026-11-17")
    assert view.summary_df.W46.tolist() == [0, 0, 0, "100.00%"]
    assert store.read()["Cpk≥1.33达标率"].eq(1).all()


def test_existing_empty_alert_sheet_is_valid_zero_alerts_for_monitored_projects(tmp_path):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx")
    class EmptyLedger:
        def read_product(self, product):
            return pd.DataFrame()
    service = CpkWorkbookMonitorService(store, latest_reader=EmptyLedger(), weekly_source=WeeklySource())
    view = service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    assert view.summary_df.W46.tolist() == [10, 10, 0, "100.00%"]


def test_runtime_complete_month_preserves_but_does_not_aggregate_open_week(tmp_path):
    store = CapabilitySummaryStore(tmp_path / "history.xlsx")
    store.write_history(normalize_capability_history(pd.DataFrame([
        {"产品": "M626", "周期类型": "周度", "时间标签": "2026-W47",
         "CPK总项目数": 100, "达标项目数": 100, "预警项目数": 0, "Cpk≥1.33达标率": 1.0},
    ])))
    service = CpkWorkbookMonitorService(store, latest_reader=Ledger(), weekly_source=WeeklySource())
    service.build_dashboard(products=["M626"], factories=[], end_date="2026-11-17")
    records = store.read()
    assert records.loc[records["时间标签"].eq("2026-W47"), "CPK总项目数"].item() == 100
    for label in ("2026-11", "2026-Q4", "2026"):
        assert records.loc[records["时间标签"].eq(label), "CPK总项目数"].item() == 30


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_product_trends_use_same_query_records_without_reloading_sources(tmp_path, metric):
    class ProductSource(WeeklySource):
        def read_weekly_totals(self, product, weeks, *, metric, as_of):
            rows = super().read_weekly_totals(product, weeks, metric=metric, as_of=as_of)
            rows["total_projects"] = 10 if product == "M626" else 20
            return rows

    store = CapabilitySummaryStore(tmp_path / "history.xlsx", metric=metric)
    source = ProductSource()
    service = CpkWorkbookMonitorService(store, latest_reader=Ledger(), metric=metric, weekly_source=source)
    view = service.build_dashboard(products=["M626", "M678"], factories=[], end_date="2026-11-17")
    assert len(source.calls) == 2
    assert set(view.product_summary_dfs) == {"M626", "M678"}
    assert view.product_summary_dfs["M626"].W46.tolist() == [10, 9, 1, "90.00%"]
    assert view.product_summary_dfs["M678"].W46.tolist() == [20, 19, 1, "95.00%"]
    assert view.summary_df.W46.tolist() == [30, 28, 2, "93.33%"]
    assert all(list(frame.columns) == list(view.summary_df.columns)
               for frame in view.product_summary_dfs.values())
    selected = service.build_dashboard(products=["M678"], factories=[], end_date="2026-11-17")
    assert list(selected.product_summary_dfs) == ["M678"]
