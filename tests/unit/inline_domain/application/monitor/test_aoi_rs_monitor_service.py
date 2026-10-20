from pathlib import Path

import pandas as pd
import pytest

from src.inline_domain.application.monitor.aoi_rs_monitor_service import (
    AoiRsMonitorQuery, AoiRsMonitorService,
)
from src.inline_domain.infrastructure.monitor.aoi_rs_period_store import AoiRsPeriodStore


class Source:
    def __init__(self):
        self.calls = []
        self.fail = False

    def policy_signature(self, product):
        return "fixture-v1"

    def history_signature(self, product):
        return self.policy_signature(product)

    def read_projected(self, product, factory, start, end):
        self.calls.append((product, start, end))
        if self.fail:
            raise RuntimeError("fixture source failure")
        common = dict(prod_code=product, factory=factory, step_id="11629", lot_id="L1")
        details = pd.DataFrame([
            dict(common, start_time=pd.Timestamp("2026-01-10"), sheet_id="S1", rs_code="A1", code_qty=6),
            dict(common, start_time=pd.Timestamp("2026-02-10"), sheet_id="S1", rs_code="A1", code_qty=2),
        ])
        passed = pd.DataFrame([
            dict(common, start_time=pd.Timestamp("2026-01-10"), sheet_id="S1"),
            dict(common, start_time=pd.Timestamp("2026-02-10"), sheet_id="S1"),
            dict(common, start_time=pd.Timestamp("2026-02-10"), sheet_id="S2"),
        ])
        specs = pd.DataFrame([dict(common, rs_code=code, code_desc=code) for code in ("A1", "A2")])
        return {"rs_details_df": details, "pass_through_df": passed, "indicators_df": specs,
                "special_factories": (), "decoration_window": None}


def query(**kwargs):
    return AoiRsMonitorQuery(products=("M626",), factory="ARRAY", halfyears=(1,),
                             quarters=(1,), months=(1, 2), week_count=5, **kwargs)


def test_history_reuse_period_distinct_counts_and_failure_preservation(tmp_path):
    source = Source()
    path = tmp_path / "rs.xlsx"
    service = AoiRsMonitorService(source, AoiRsPeriodStore(path), enabled_products=("M626",))
    result = service.build_dashboard(query(), as_of="2026-02-20")
    table = result["tables"]["M626"].set_index("RSCode")
    assert table.loc["A1", "2026-H1"] == 4.0  # 8 defects / 2 unique sheets
    assert table.loc["A1", "2026-M01"] == 6.0
    assert table.loc["A2", "2026-H1"] == 0.0
    service.build_dashboard(query(), as_of="2026-02-20")
    assert len(source.calls) == 1
    source.calls.clear()
    before_history = service.store.read()
    january = before_history.loc[before_history["period_label"].eq("2026-M01")].reset_index(drop=True)
    service.build_dashboard(query(), as_of="2026-02-21", refresh=True)
    after_history = service.store.read()
    pd.testing.assert_frame_equal(january, after_history.loc[after_history["period_label"].eq("2026-M01")].reset_index(drop=True))
    before = path.read_bytes()
    source.fail = True
    with pytest.raises(RuntimeError, match="fixture source failure"):
        service.build_dashboard(query(), as_of="2026-02-21", refresh=True)
    assert path.read_bytes() == before


def test_disabled_products_rejected_and_filters_do_not_replace_history(tmp_path):
    source = Source()
    service = AoiRsMonitorService(source, AoiRsPeriodStore(tmp_path / "rs.xlsx"), enabled_products=("M626",))
    service.build_dashboard(query(), as_of="2026-02-20")
    filtered = service.build_dashboard(query(codes=("A1",)), as_of="2026-02-20")
    assert filtered["tables"]["M626"]["RSCode"].tolist() == ["A1"]
    assert set(service.build_dashboard(query(), as_of="2026-02-20")["tables"]["M626"]["RSCode"]) == {"A1", "A2"}
    with pytest.raises(ValueError):
        service.build_dashboard(AoiRsMonitorQuery(products=("DISABLED",)), as_of="2026-02-20")


def test_closed_week_with_current_month_dependency_updates_and_old_month_is_preserved(tmp_path):
    class SpecialSource(Source):
        def read_projected(self, *args):
            result = super().read_projected(*args)
            result["special_factories"] = ("ARRAY",)
            return result

    source = SpecialSource()
    service = AoiRsMonitorService(source, AoiRsPeriodStore(tmp_path / "rs.xlsx"), enabled_products=("M626",))
    service.build_dashboard(query(), as_of="2026-02-20")
    before = service.store.read()
    service.build_dashboard(query(), as_of="2026-02-21")
    after = service.store.read()
    old_week = "2026-W06"
    original = before.loc[before["period_label"].eq(old_week) & before["record_kind"].eq("coverage")].iloc[0]
    updated = after.loc[after["period_label"].eq(old_week) & after["record_kind"].eq("coverage")].iloc[0]
    assert original["covered_through"] == "2026-02-20T12:00:00"
    assert updated["covered_through"] == "2026-02-21T12:00:00"


def test_successfully_empty_periods_are_covered_and_signature_change_recalculates(tmp_path):
    class EmptySource(Source):
        version = "v1"

        def policy_signature(self, product):
            return self.version

        def read_projected(self, *args):
            result = super().read_projected(*args)
            for key in ("rs_details_df", "pass_through_df", "indicators_df"):
                result[key] = result[key].iloc[:0]
            return result

    source = EmptySource()
    service = AoiRsMonitorService(source, AoiRsPeriodStore(tmp_path / "rs.xlsx"), enabled_products=("M626",))
    assert service.build_dashboard(query(), as_of="2026-02-20")["tables"]["M626"].empty
    service.build_dashboard(query(), as_of="2026-02-20")
    assert len(source.calls) == 1
    source.version = "v2"
    service.build_dashboard(query(), as_of="2026-02-20")
    assert len(source.calls) == 2


def test_concurrent_products_are_both_preserved(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "rs.xlsx"
    def publish(product):
        service = AoiRsMonitorService(Source(), AoiRsPeriodStore(path), enabled_products=("M626", "M678"))
        service.build_dashboard(AoiRsMonitorQuery(products=(product,), months=(1,)), as_of="2026-02-20")

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(publish, ["M626", "M678"]))
    assert set(AoiRsPeriodStore(path).read()["prod_code"]) == {"M626", "M678"}


def test_closed_week_outside_special_window_has_no_month_dependency(tmp_path):
    class WindowSource(Source):
        def read_projected(self, *args):
            result = super().read_projected(*args)
            result.update(special_factories=("ARRAY",), decoration_window=("2026-02-16", "2026-02-28"))
            return result

    service = AoiRsMonitorService(WindowSource(), AoiRsPeriodStore(tmp_path / "rs.xlsx"), enabled_products=("M626",))
    service.build_dashboard(query(), as_of="2026-02-20")
    rows = service.store.read()
    week = rows.loc[rows["period_label"].eq("2026-W06") & rows["record_kind"].eq("coverage")].iloc[0]
    assert week["dependency_month"] == ""
    assert week["covered_through"] == "2026-02-08T23:59:59"
