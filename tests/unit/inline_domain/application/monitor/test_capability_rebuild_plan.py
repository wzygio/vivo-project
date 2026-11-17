import pandas as pd
import pytest

from tools.rebuild_capability_summaries import (
    anomalous_history_weeks, plan_history, validate_anomalous_scope, validate_weekly_scope,
)


def row(kind, label, total, warning, *, qualified=None):
    return {"产品": "M626", "监控类型": "SPC", "厂别": "ALL", "周期类型": kind,
            "时间标签": label, "CPK总项目数": total,
            "达标项目数": total - warning if qualified is None else qualified,
            "预警项目数": warning, "Cpk≥1.33达标率": None}


def test_plan_replaces_bad_ancestors_after_monthly_rebuild_and_preserves_other_weeks():
    source = pd.DataFrame([row("周度", "2026-W40", 4, 1), row("周度", "2026-W41", 6, 1),
                           row("周度", "2026-W42", 0, 0), row("月度", "2026-10", 10, 2),
                           row("季度", "2026-Q4", 0, 99), row("年度", "2026", 0, 99)])
    original = source.copy(deep=True)
    weekly = pd.DataFrame([row("周度", "2026-W42", 7, 1)])
    result = plan_history({"M626 CPK": source}, weekly, metric="cpk", as_of="2026-10-20")
    aggregate = result.loc[result["周期类型"].isin(["月度", "季度", "年度"])]
    assert aggregate["CPK总项目数"].tolist() == [17, 17, 17]
    assert aggregate["预警项目数"].tolist() == [3, 3, 3]
    assert aggregate["达标项目数"].tolist() == [14, 14, 14]
    assert result.loc[result["时间标签"].eq("2026-W40"), "CPK总项目数"].item() == 4
    pd.testing.assert_frame_equal(source, original)


def test_plan_refuses_to_invent_out_of_scope_historical_denominator():
    source = pd.DataFrame([row("周度", "2026-W35", 0, 2, qualified=0)])
    weekly = pd.DataFrame([row("周度", "2026-W40", 1, 0), row("周度", "2026-W41", 1, 0),
                           row("周度", "2026-W42", 1, 0)])
    with pytest.raises(ValueError, match="Historical totals unavailable.*2026-W35"):
        plan_history({"M626 CPK": source}, weekly, metric="cpk", as_of="2026-10-20")


def test_plan_preserves_older_aggregates_without_retained_monthly_children():
    source = pd.DataFrame([row("季度", "2025-Q4", 100, 3), row("年度", "2025", 100, 3),
                           row("周度", "2026-W40", 4, 1), row("周度", "2026-W41", 6, 1)])
    source.loc[source["时间标签"].eq("2025-Q4"), "Cpk≥1.33达标率"] = .969
    weekly = pd.DataFrame([row("周度", "2026-W42", 7, 1)])
    result = plan_history({"M626 CPK": source}, weekly, metric="cpk", as_of="2026-10-20")
    old = result.loc[result["时间标签"].isin(["2025-Q4", "2025"])]
    assert len(old) == 2
    assert old["CPK总项目数"].tolist() == [100, 100]
    assert result.loc[result["时间标签"].eq("2025-Q4"), "Cpk≥1.33达标率"].item() == .97


@pytest.mark.parametrize("invalid", ["not-a-number", 1.5, float("inf"), -1])
def test_plan_rejects_invalid_nonblank_maintained_counts_before_repair(invalid):
    source = pd.DataFrame([row("周度", "2026-W35", 0, 0),
                           row("周度", "2026-W40", 4, 1), row("周度", "2026-W41", 6, 1)])
    source["CPK总项目数"] = source["CPK总项目数"].astype(object)
    source.loc[source["时间标签"].eq("2026-W35"), "CPK总项目数"] = invalid
    with pytest.raises(ValueError):
        plan_history({"M626 CPK": source}, pd.DataFrame([row("周度", "2026-W42", 7, 1)]),
                     metric="cpk", as_of="2026-10-20")


def test_prepared_weekly_requires_cartesian_scope_not_just_product_and_week_sets():
    frame = pd.DataFrame([row("周度", "2026-W40", 4, 1), row("周度", "2026-W41", 6, 1),
                          {**row("周度", "2026-W40", 4, 1), "产品": "M678"}])
    frame["Cpk≥1.33达标率"] = frame["达标项目数"] / frame["CPK总项目数"]
    with pytest.raises(ValueError, match="every requested product/week"):
        validate_weekly_scope(frame, products=["M626", "M678"], weeks=["2026-W40", "2026-W41"], metric="cpk")


def test_prepared_weekly_rejects_wrong_rates_and_duplicate_keys():
    frame = pd.DataFrame([row("周度", "2026-W40", 4, 1)])
    frame["Cpk≥1.33达标率"] = .5
    with pytest.raises(ValueError, match="rates disagree"):
        validate_weekly_scope(frame, products=["M626"], weeks=["2026-W40"], metric="cpk")
    with pytest.raises(ValueError, match="duplicate business keys"):
        validate_weekly_scope(pd.concat([frame, frame]), products=["M626"], weeks=["2026-W40"], metric="cpk")


def complete_rows(items):
    frame = pd.DataFrame(items)
    frame["Cpk≥1.33达标率"] = frame["达标项目数"] / frame["CPK总项目数"].replace(0, 1)
    frame.loc[frame["CPK总项目数"].eq(0), "Cpk≥1.33达标率"] = 1.0
    return frame


def test_anomaly_selection_is_sparse_per_product_and_excludes_base_open_or_healthy_weeks():
    source = pd.DataFrame([row("周度", "2026-W35", 0, 2, qualified=0),
                           row("周度", "2026-W34", 0, 0),
                           row("周度", "2026-W42", 0, 2, qualified=0),
                           row("周度", "2026-W43", 0, 2, qualified=0)])
    healthy = pd.DataFrame([{**row("周度", "2026-W35", 10, 0), "产品": "M678"}])
    disabled = pd.DataFrame([{**row("周度", "2026-W35", 0, 2, qualified=0), "产品": "OLD"}])
    result = anomalous_history_weeks({"M626 CPK": source, "M678 CPK": healthy, "OLD CPK": disabled},
        metric="cpk", base_weeks=["2026-W42"], products=["M626", "M678"],
        as_of="2026-10-20", include_missing_coverage=False)
    assert result == {"M626": ["2026-W35"]}


def test_anomalous_month_missing_week_coverage_is_explicit_not_assumed_zero():
    source = pd.DataFrame([row("周度", f"2026-W{week}", 0, 2, qualified=0) for week in (32, 33, 34, 35)])
    result = anomalous_history_weeks({"M626 CPK": source}, metric="cpk", base_weeks=["2026-W42"],
                                    products=["M626"], as_of="2026-10-20")
    assert result == {"M626": [f"2026-W{week}" for week in (31, 32, 33, 34, 35)]}


def test_approved_extra_weeks_repair_unknown_month_and_ancestors_without_changing_healthy_weeks():
    source = pd.DataFrame([
        row("周度", "2026-W31", 10, 0), row("周度", "2026-W32", 0, 2, qualified=0),
        row("周度", "2026-W33", 5, 1), row("周度", "2026-W34", 0, 0),
        row("周度", "2026-W35", 0, 3, qualified=0), row("月度", "2026-08", 0, 6, qualified=0),
        row("周度", "2026-W40", 4, 1), row("周度", "2026-W41", 6, 1),
        row("月度", "2026-07", 20, 0),
    ])
    source.loc[source["时间标签"].eq("2026-W33"), "Cpk≥1.33达标率"] = .8
    weekly = complete_rows([row("周度", "2026-W42", 7, 1)])
    extra = complete_rows([row("周度", "2026-W32", 8, 2), row("周度", "2026-W35", 9, 3)])
    with pytest.raises(ValueError, match="Historical totals unavailable"):
        plan_history({"M626 CPK": source}, weekly, metric="cpk", as_of="2026-10-20")
    result = plan_history({"M626 CPK": source}, weekly, additional_weekly=extra,
                          metric="cpk", as_of="2026-10-20")
    by_label = result.set_index("时间标签")
    assert by_label.loc["2026-08", "CPK总项目数"] == 32
    assert by_label.loc["2026-08", "预警项目数"] == 6
    assert by_label.loc["2026-Q3", "CPK总项目数"] == 52
    assert by_label.loc["2026", "CPK总项目数"] == 69
    assert by_label.loc["2026-W33", "CPK总项目数"] == 5
    assert by_label.loc["2026-W33", "预警项目数"] == 1
    assert by_label.loc["2026-W33", "Cpk≥1.33达标率"] == .8
    assert by_label.loc["2026-07", "CPK总项目数"] == 20


def test_anomalous_results_cannot_replace_healthy_product_or_week():
    expected = {"M626": ["2026-W32"]}
    good = complete_rows([row("周度", "2026-W32", 8, 2)])
    assert len(validate_anomalous_scope(good, expected=expected, metric="cpk")) == 1
    for extra in [row("周度", "2026-W33", 5, 0), {**row("周度", "2026-W32", 5, 0), "产品": "M678"}]:
        unauthorized = pd.concat([good, complete_rows([extra])], ignore_index=True)
        with pytest.raises(ValueError, match="authorized product/week scope"):
            validate_anomalous_scope(unauthorized, expected=expected, metric="cpk")


def test_missing_anomalous_month_week_must_have_a_verified_computation():
    source = pd.DataFrame([row("周度", f"2026-W{week}", 0, 2, qualified=0) for week in (32, 33, 34, 35)]
                          + [row("周度", "2026-W40", 4, 1), row("周度", "2026-W41", 6, 1)])
    weekly = complete_rows([row("周度", "2026-W42", 7, 1)])
    extra = complete_rows([row("周度", f"2026-W{week}", 8, 2) for week in (32, 33, 34, 35)])
    with pytest.raises(ValueError, match="authorized product/week scope"):
        plan_history({"M626 CPK": source}, weekly, additional_weekly=extra,
                     metric="cpk", as_of="2026-10-20")
    extra = pd.concat([extra, complete_rows([row("周度", "2026-W31", 4, 0)])], ignore_index=True)
    result = plan_history({"M626 CPK": source}, weekly, additional_weekly=extra,
                          metric="cpk", as_of="2026-10-20")
    assert result.loc[result["时间标签"].eq("2026-08"), "CPK总项目数"].item() == 36


def test_cli_records_and_reuses_sparse_extra_provenance_without_cross_product_recalculation(tmp_path, monkeypatch):
    import json
    import sys
    import tools.rebuild_capability_summaries as command

    source, ledger = tmp_path / "summary.xlsx", tmp_path / "ledger.xlsx"
    products = ["M626", "M678"]
    with pd.ExcelWriter(source) as writer:
        for product in products:
            for metric in ("CPK", "CPM"):
                rows = [row("周度", f"2026-W{week}", 5, 0) for week in range(31, 48)]
                broken = "2026-W39" if metric == "CPK" else "2026-W32"
                if product == "M626":
                    for item in rows:
                        if item["时间标签"] == broken:
                            item.update({"CPK总项目数": 0, "预警项目数": 2, "达标项目数": 0})
                frame = pd.DataFrame(rows).assign(产品=product)
                if metric == "CPM":
                    frame = frame.rename(columns={"CPK总项目数": "CPM总项目数", "Cpk≥1.33达标率": "Cpm≥1.33达标率"})
                frame.to_excel(writer, sheet_name=f"{product} {metric}", index=False)
    ledger_columns = ["prod_code", "factory", "step_id", "param_name", "period_type", "period_label",
                      "flag", "cpk_corrected", "cpm_corrected"]
    with pd.ExcelWriter(ledger) as writer:
        for product in products:
            for suffix in ("", "_cpm"):
                pd.DataFrame(columns=ledger_columns).to_excel(writer, sheet_name=product + suffix, index=False)

    class Source:
        calls = []

        def source_signature(self, selected):
            return "stable-source-" + ",".join(sorted(selected))

        def read_weekly_totals(self, product, weeks, *, metric, as_of):
            self.calls.append((product, tuple(weeks), metric))
            return pd.DataFrame({"prod_code": product, "period_label": weeks, "total_projects": 10})

    fake = Source()
    monkeypatch.setattr(command, "SnapshotCapabilitySource", lambda *args: fake)
    monkeypatch.setattr(command.ConfigLoader, "get_enabled_products", lambda: products)
    monkeypatch.setattr(command.ConfigLoader, "get_domain_resource_path", lambda *args: ledger)
    output = tmp_path / "first"
    args = ["rebuild", "--source-workbook", str(source), "--as-of", "2026-11-17",
            "--repair-anomalous-history"]
    monkeypatch.setattr(sys, "argv", [*args, "--output", str(output)])
    command.main()
    extra_calls = [call for call in fake.calls if len(call[1]) == 1]
    assert extra_calls == [("M626", ("2026-W39",), "cpk"), ("M626", ("2026-W32",), "cpm")]
    assert not (output / "anomalous/M678").exists()
    manifest = json.loads((output / "anomalous/M626/rebuild-inputs.json").read_text())
    assert manifest["scope"]["products"] == ["M626"]
    assert manifest["scope"]["weeks_by_metric"] == {"cpk": ["2026-W39"], "cpm": ["2026-W32"]}
    before = list(fake.calls)
    second = tmp_path / "second"
    monkeypatch.setattr(sys, "argv", [*args, "--output", str(second), "--prepared-weekly", str(output),
                                     "--prepared-anomalous", str(output / "anomalous")])
    command.main()
    assert fake.calls == before
    plan = json.loads((second / "rebuild-plan.json").read_text())
    assert plan["additional_history"] == {"cpk": {"M626": ["2026-W39"]}, "cpm": {"M626": ["2026-W32"]}}
