"""Weekly replacement preserves unrelated maintained period contributions."""

import pandas as pd
import pytest

from src.inline_domain.core.monitor.capability_trend import (
    capability_history_for_storage,
    normalize_capability_history,
    rollup_calendar_history,
    update_weekly_history,
    week_owner_month,
)


def record(kind, label, total=10, warnings=2, *, product="M626", metric="cpk"):
    return {"产品": product, "周期类型": kind, "时间标签": label,
            f"{metric.upper()}总项目数": total, "达标项目数": total - warnings,
            "预警项目数": warnings, f"{metric.capitalize()}≥1.33达标率": (total - warnings) / total if total else 1.0}


def row(frame, kind, label, product="M626"):
    return frame.loc[frame["周期类型"].eq(kind) & frame["时间标签"].eq(label)
                     & frame["产品"].eq(product)].iloc[0]


@pytest.mark.parametrize("metric", ["cpk", "cpm"])
def test_repairs_missing_values_and_derives_display_labels_without_storage(metric):
    source = pd.DataFrame([
        {"产品": "M626", "周期类型": "月度", "时间标签": "2026-09", f"{metric.upper()}总项目数": 12},
        {"产品": "M626", "周期类型": "周度", "时间标签": "2026-W40"},
    ])
    original = source.copy(deep=True)
    normalized = normalize_capability_history(source, metric=metric, repair_missing=True)
    assert normalized["显示标签"].tolist() == ["M9", "W40"]
    assert normalized[f"{metric.upper()}总项目数"].tolist() == [12, 0]
    assert normalized["达标项目数"].tolist() == [12, 0]
    assert normalized["预警项目数"].tolist() == [0, 0]
    assert normalized[f"{metric.capitalize()}≥1.33达标率"].tolist() == [1.0, 1.0]
    assert "显示标签" not in capability_history_for_storage(normalized, metric=metric)
    pd.testing.assert_frame_equal(source, original)


@pytest.mark.parametrize("column,value", [("CPK总项目数", -1), ("CPK总项目数", 2.5),
                                         ("预警项目数", 11), ("达标项目数", "bad")])
def test_rejects_invalid_or_inconsistent_counts(column, value):
    bad = record("月度", "2026-09")
    bad[column] = value
    with pytest.raises(ValueError):
        normalize_capability_history(pd.DataFrame([bad]))


@pytest.mark.parametrize("label,month", [("2026-W40", "2026-10"), ("2020-W53", "2021-01"),
                                        ("2026-W01", "2026-01")])
def test_week_owner_uses_calendar_sunday(label, month):
    assert week_owner_month(label) == month


def test_month_rollups_sum_counts_and_weight_rates_preserving_other_years():
    data = pd.DataFrame([
        record("月度", "2026-01", 10, 2), record("月度", "2026-02", 90, 9),
        record("月度", "2026-04", 20, 4), record("季度", "2026-Q1", 999, 0),
        record("季度", "2026-Q2", 999, 0), record("年度", "2026", 999, 0),
        record("年度", "2025", 40, 4),
    ])
    result = rollup_calendar_history(data)
    assert row(result, "季度", "2026-Q1")["CPK总项目数"] == 100
    assert row(result, "季度", "2026-Q1")["Cpk≥1.33达标率"] == .89
    assert row(result, "年度", "2026")["CPK总项目数"] == 120
    assert row(result, "年度", "2026")["预警项目数"] == 15
    assert row(result, "年度", "2025")["CPK总项目数"] == 40


def test_partial_month_replacement_applies_delta_and_is_idempotent():
    data = pd.DataFrame([
        record("周度", "2026-W40", 10, 2), record("周度", "2026-W41", 20, 4),
        record("月度", "2026-10", 100, 20), record("月度", "2026-11", 50, 5),
        record("季度", "2026-Q4", 150, 25), record("年度", "2026", 150, 25),
        record("月度", "2026-09", 80, 8), record("季度", "2026-Q3", 80, 8),
        record("月度", "2026-10", 7, 1, product="M678"),
    ])
    update = pd.DataFrame([record("周度", "2026-W40", 15, 1)])
    result = update_weekly_history(data, update)
    assert row(result, "月度", "2026-10")["CPK总项目数"] == 105
    assert row(result, "月度", "2026-10")["预警项目数"] == 19
    assert row(result, "季度", "2026-Q4")["CPK总项目数"] == 155
    assert row(result, "年度", "2026")["CPK总项目数"] == 235
    assert row(result, "月度", "2026-11")["预警项目数"] == 5
    assert row(result, "季度", "2026-Q3")["CPK总项目数"] == 80
    assert row(result, "月度", "2026-10", "M678")["CPK总项目数"] == 7
    pd.testing.assert_frame_equal(update_weekly_history(result, update), result)


def test_unknown_old_contribution_requires_explicit_complete_month_coverage():
    data = pd.DataFrame([record("月度", "2026-10", 100, 20)])
    update = pd.DataFrame([record("周度", "2026-W40", 10, 1)])
    with pytest.raises(ValueError, match="complete month coverage required"):
        update_weekly_history(data, update)
    result = update_weekly_history(data, update, complete_months=[("M626", "SPC", "ALL", "2026-10")])
    assert row(result, "月度", "2026-10")["CPK总项目数"] == 10
    assert row(result, "月度", "2026-10")["Cpk≥1.33达标率"] == .9


def test_complete_month_aggregates_all_weekly_records_and_owns_calendar_year():
    data = pd.DataFrame([record("周度", "2021-W01", 20, 2), record("年度", "2020", 80, 8)])
    update = pd.DataFrame([record("周度", "2020-W53", 10, 1)])
    result = update_weekly_history(data, update, complete_months=[("M626", "SPC", "ALL", "2021-01")])
    assert row(result, "月度", "2021-01")["CPK总项目数"] == 30
    assert row(result, "年度", "2021")["CPK总项目数"] == 30
    assert row(result, "年度", "2020")["CPK总项目数"] == 80


def test_zero_project_rollups_are_full_compliance_and_metric_specific():
    data = pd.DataFrame([record("月度", "2026-09", 0, 0, metric="cpm")])
    result = rollup_calendar_history(data, metric="cpm")
    assert result["Cpm≥1.33达标率"].tolist() == [1.0, 1.0, 1.0]
    assert "CPK总项目数" not in result


def test_legacy_zero_rate_is_upgraded_and_percent_alias_is_readable():
    empty = record("月度", "2026-09", 0, 0)
    empty["Cpk≥1.33达标率"] = 0
    populated = record("月度", "2026-10", 10, 2)
    populated["CPK≥1.33达标率"] = "80%"
    populated.pop("Cpk≥1.33达标率")
    zero = normalize_capability_history(pd.DataFrame([empty]))
    percentage = normalize_capability_history(pd.DataFrame([populated]))
    assert zero["Cpk≥1.33达标率"].item() == 1.0
    assert percentage["Cpk≥1.33达标率"].item() == .8


def test_affected_month_filter_keeps_other_quarters_maintained():
    history = pd.DataFrame([
        record("月度", "2026-01", 10, 2), record("季度", "2026-Q1", 100, 10),
        record("月度", "2026-10", 20, 4), record("季度", "2026-Q4", 200, 20),
    ])
    result = rollup_calendar_history(history, affected_months=["2026-10"])
    assert row(result, "季度", "2026-Q1")["CPK总项目数"] == 100
    assert row(result, "季度", "2026-Q4")["CPK总项目数"] == 20
    assert row(result, "年度", "2026")["CPK总项目数"] == 120


def test_week_replacement_rejects_negative_remaining_monthly_contributions():
    history = pd.DataFrame([
        record("周度", "2026-W40", 100, 20), record("月度", "2026-10", 10, 2),
    ])
    update = pd.DataFrame([record("周度", "2026-W40", 1, 0)])
    with pytest.raises(ValueError, match="exceeds maintained baseline"):
        update_weekly_history(history, update)


@pytest.mark.parametrize("label", ["2025-W53", "2026-W1", "2026-Q2", ""])
def test_invalid_week_identity_fails_instead_of_choosing_a_month(label):
    with pytest.raises(ValueError):
        week_owner_month(label)


@pytest.mark.parametrize("as_of", ["2026-11-17", "2026-11-22"])
def test_complete_month_excludes_open_and_future_weeks_but_preserves_their_rows(as_of):
    history = pd.DataFrame([
        record("周度", "2026-W45", 20, 2),
        record("周度", "2026-W47", 100, 10),
        record("周度", "2026-W48", 200, 20),
    ])
    update = pd.DataFrame([record("周度", "2026-W46", 10, 1)])
    result = update_weekly_history(history, update, as_of=as_of,
                                   complete_months=[("M626", "SPC", "ALL", "2026-11")])
    for kind, label in [("月度", "2026-11"), ("季度", "2026-Q4"), ("年度", "2026")]:
        assert row(result, kind, label)["CPK总项目数"] == 30
        assert row(result, kind, label)["预警项目数"] == 3
    assert row(result, "周度", "2026-W47")["CPK总项目数"] == 100
    assert row(result, "周度", "2026-W48")["CPK总项目数"] == 200


def test_partial_month_with_smaller_baseline_fails_even_when_replacement_is_large():
    history = pd.DataFrame([
        record("周度", "2026-W46", 100, 20), record("月度", "2026-11", 10, 2),
    ])
    update = pd.DataFrame([record("周度", "2026-W46", 200, 40)])
    with pytest.raises(ValueError, match="exceeds maintained baseline"):
        update_weekly_history(history, update, as_of="2026-11-17")
