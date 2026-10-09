import pandas as pd

from src.inline_domain.core.monitor.period_summary import current_period_windows
from src.inline_domain.core.monitor.weekly_replacement import plan_weekly_replacement


def baseline():
    rows = []
    for window, count in zip(current_period_windows(pd.Timestamp("2026-09-08")), [300, 200, 100, 10]):
        rows.append({"产品": "M626", "监控类型": "ALL", "厂别": "ALL",
                     "周期类型": window.period_type, "时间标签": window.time_label,
                     "显示标签": window.display_label, "过货量": 1000,
                     "OOS报警片数": count, "OOC报警片数": 0,
                     "Total报警片数": count, "SOOS报警片数": 0})
    return pd.DataFrame(rows)


def open_rows(records, as_of="2026-09-08"):
    labels = {window.time_label for window in current_period_windows(pd.Timestamp(as_of))}
    return records[records["时间标签"].isin(labels)]


def test_replaces_only_current_week_contribution_not_historical_week_sum():
    current = baseline()
    # A historical week's unrelated value must never participate in rebuilding totals.
    history = current.iloc[-1].copy()
    history["时间标签"] = "2026-W36"
    history["OOS报警片数"] = 999
    current = pd.concat([current, history.to_frame().T], ignore_index=True)
    alerts = pd.DataFrame([{"prod_code": "M626", "factory": "ARRAY", "item_id": f"S{i}",
                            "alarm_type": "OOS", "event_time": pd.Timestamp("2026-09-08")}
                           for i in range(12)])
    updates, problems = plan_weekly_replacement(
        current, alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-09-08"), available_types={"M626": {"OOS", "OOC"}},
    )
    assert not problems
    assert open_rows(updates)["OOS报警片数"].tolist() == [302, 202, 102, 12]
    assert open_rows(updates)["过货量"].tolist() == [1000] * 4
    merged = pd.concat([current[~current["时间标签"].isin(updates["时间标签"])], updates])
    assert merged.set_index("时间标签").loc["2026-W36", "OOS报警片数"] == 999
    again, problems = plan_weekly_replacement(
        merged, alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-09-08"), available_types={"M626": {"OOS", "OOC"}},
    )
    assert not problems
    assert open_rows(again)["OOS报警片数"].tolist() == [302, 202, 102, 12]


def test_missing_week_is_initialized_to_zero_without_erasing_existing_totals():
    updates, problems = plan_weekly_replacement(
        baseline().iloc[:3], pd.DataFrame(), products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-09-08"), available_types={"M626": {"OOS", "OOC"}},
    )
    assert not problems
    current = updates[updates["时间标签"].isin(["2026", "2026-Q3", "2026-09", "2026-W37"])]
    assert current["OOS报警片数"].tolist() == [300, 200, 100, 0]
    week = current[current["时间标签"].eq("2026-W37")].iloc[0]
    assert week["过货量"] == 0
    assert week["OOS报警率"] == 0


def test_rollover_fills_continuous_periods_and_counts_only_latest_week():
    original = baseline()
    before = original.copy(deep=True)
    alerts = pd.DataFrame([
        dict(prod_code="M626", factory="ARRAY", item_id="old", alarm_type="OOS", event_time="2026-09-28"),
        dict(prod_code="M626", factory="ARRAY", item_id="new", alarm_type="OOS", event_time="2026-10-08"),
    ])
    updates, problems = plan_weekly_replacement(
        original, alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-10-09"), available_types={"M626": {"OOS", "OOC"}},
    )
    assert not problems
    pd.testing.assert_frame_equal(original, before)
    indexed = updates.set_index("时间标签")
    assert indexed.loc["2026", "OOS报警片数"] == 301
    for label in ("2026-Q4", "2026-10", "2026-W41"):
        assert indexed.loc[label, "OOS报警片数"] == 1
        assert indexed.loc[label, "过货量"] == 0
        assert indexed.loc[label, "OOS报警率"] == 0
    for label in ("2026-W38", "2026-W39", "2026-W40"):
        assert indexed.loc[label, "OOS报警片数"] == 0
    merged = pd.concat([original[~original["时间标签"].isin(updates["时间标签"])], updates])
    repeated, problems = plan_weekly_replacement(
        merged, alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-10-09"), available_types={"M626": {"OOS", "OOC"}},
    )
    assert not problems
    assert repeated.set_index("时间标签").loc["2026", "OOS报警片数"] == 301


def test_iso_year_rollover_does_not_count_previous_iso_week():
    alerts = pd.DataFrame([
        dict(prod_code="M626", factory="ARRAY", item_id="old", alarm_type="OOS", event_time="2026-12-31"),
        dict(prod_code="M626", factory="ARRAY", item_id="new", alarm_type="OOS", event_time="2027-01-05"),
    ])
    updates, problems = plan_weekly_replacement(
        baseline(), alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2027-01-05"), available_types={"M626": {"OOS", "OOC"}},
    )
    assert not problems
    indexed = updates.set_index("时间标签")
    for label in ("2027", "2027-Q1", "2027-01", "2027-W01"):
        assert indexed.loc[label, "OOS报警片数"] == 1
    assert indexed.loc["2026-W53", "OOS报警片数"] == 0
    assert "2026" not in indexed.index


def test_sheet_count_excludes_aoi_rs_lot_points():
    facts = pd.DataFrame([
        dict(prod_code="M626", factory="ARRAY", item_id="LOT1", alarm_type="OOS", event_time="2026-09-08", scope="aoi_rs", chart_kind="lot"),
        dict(prod_code="M626", factory="ARRAY", item_id="S1", alarm_type="OOS", event_time="2026-09-08", scope="aoi_rs", chart_kind="sheet"),
    ])
    updates, _ = plan_weekly_replacement(baseline(), facts, products=["M626"], scope_key="ALL", factory_key="ALL", as_of=pd.Timestamp("2026-09-08"), available_types={"M626": {"OOS"}})
    assert open_rows(updates)["OOS报警片数"].tolist() == [291, 191, 91, 1]


def test_invalid_old_contribution_is_not_masked_by_new_large_count():
    current = baseline()
    current.loc[0, "OOS报警片数"] = 5
    facts = pd.DataFrame([dict(prod_code="M626", factory="ARRAY", item_id=str(i), alarm_type="OOS", event_time="2026-09-08") for i in range(20)])
    updates, warnings = plan_weekly_replacement(current, facts, products=["M626"], scope_key="ALL", factory_key="ALL", as_of=pd.Timestamp("2026-09-08"), available_types={"M626": {"OOS"}})
    assert open_rows(updates)["OOS报警片数"].tolist() == [5, 200, 100, 10]
    assert any("超出汇总" in warning for warning in warnings)


def test_cross_month_week_requires_explicit_period_contribution_and_never_edits_closed_month():
    from src.inline_domain.core.monitor.weekly_replacement import WEEK_MARKER, CONTRIBUTION_COLUMNS
    current = baseline()
    for index, window in enumerate(current_period_windows(pd.Timestamp("2026-09-03"))):
        current.loc[index, ["周期类型", "时间标签", "显示标签"]] = [window.period_type, window.time_label, window.display_label]
    alerts = pd.DataFrame([{"prod_code": "M626", "factory": "ARRAY", "item_id": "A",
                            "alarm_type": "OOS", "event_time": pd.Timestamp("2026-08-31")},
                           {"prod_code": "M626", "factory": "ARRAY", "item_id": "B",
                            "alarm_type": "OOS", "event_time": pd.Timestamp("2026-09-02")}])
    updates, problems = plan_weekly_replacement(
        current, alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-09-03"), available_types={"M626": {"OOS"}},
    )
    assert open_rows(updates, "2026-09-03")["OOS报警片数"].tolist() == [300, 200, 100, 10]
    assert any("跨月" in item for item in problems)
    current[WEEK_MARKER] = "2026-W36"
    current[CONTRIBUTION_COLUMNS["OOS"]] = [10, 10, 4, 10]
    updates, _ = plan_weekly_replacement(
        current, alerts, products=["M626"], scope_key="ALL", factory_key="ALL",
        as_of=pd.Timestamp("2026-09-03"), available_types={"M626": {"OOS"}},
    )
    assert open_rows(updates, "2026-09-03")["OOS报警片数"].tolist() == [292, 192, 97, 2]
    assert open_rows(updates, "2026-09-03")["时间标签"].tolist() == ["2026", "2026-Q3", "2026-09", "2026-W36"]
