"""Behavior of the two independent yesterday-density warning rules."""

from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.aoi_rs.trend_alerts import evaluate_aoi_rs_trends


def test_daily_rule_alerts_on_yesterday_even_when_two_day_rise_does_not() -> None:
    indicators = pd.DataFrame([dict(prod_code="M678", factory="ARRAY", step_id="S", rs_code="A")])
    # T=18th; the 17th declined, so this is only the original daily rule.
    trend = pd.DataFrame([
        dict(factory="ARRAY", step_id="S", rs_code="A", period_type="day",
             period_label=day, value=value, sheet_qty=100)
        for day, value in [("2026-10-16", 0.10), ("2026-10-17", 0.08),
                           ("2026-10-18", 0.15), ("2026-10-19", 0.01)]
    ])

    result = evaluate_aoi_rs_trends(trend, indicators, reference_date=date(2026, 10, 19))

    row = result.iloc[0]
    assert row["target_date"] == "2026-10-18"
    assert row["target_density"] == 0.15
    assert row["daily_alert"]
    assert not row["rising_alert"]
    assert row["is_alert"]


def _evaluate(values, *, reference=date(2026, 10, 19), overrides=None):
    dates = pd.date_range(end=pd.Timestamp(reference) - pd.Timedelta(days=1), periods=7)
    rows = [
        dict(factory="ARRAY", step_id="S", rs_code="A", period_type="day",
             period_label=day.date().isoformat(), value=value, sheet_qty=100)
        for day, value in zip(dates, values) if value is not None
    ]
    for row in rows:
        row.update((overrides or {}).get(row["period_label"], {}))
    indicators = pd.DataFrame([dict(prod_code="M678", factory="ARRAY", step_id="S", rs_code="A")])
    return evaluate_aoi_rs_trends(pd.DataFrame(rows), indicators, reference_date=reference).iloc[0]


def _evaluate_dates(observations, *, reference=date(2026, 10, 20)):
    indicators = pd.DataFrame([dict(prod_code="M678", factory="ARRAY", step_id="S", rs_code="A")])
    rows = [dict(factory="ARRAY", step_id="S", rs_code="A", period_type="day",
                 period_label=day, value=value, sheet_qty=100)
            for day, value in observations]
    return evaluate_aoi_rs_trends(pd.DataFrame(rows), indicators, reference_date=reference).iloc[0]


def test_five_day_mean_skips_zero_and_missing_but_includes_oldest_day():
    row = _evaluate_dates([
        ("2026-10-11", 999), ("2026-10-12", 999), ("2026-10-13", .2),
        ("2026-10-14", 0), ("2026-10-16", .4), ("2026-10-17", .6),
        ("2026-10-18", .65), ("2026-10-19", 1), ("2026-10-20", 999),
    ])
    assert row.baseline_density == pytest.approx(.4)
    assert row.baseline_days == 3
    assert row.daily_alert


def test_missing_calendar_days_use_last_two_valid_records_with_actual_dates():
    row = _evaluate_dates([
        ("2026-10-19", .17), ("2026-10-14", .10), ("2026-10-17", .13),
    ])
    assert row.previous_date == "2026-10-17"
    assert row.two_days_before_date == "2026-10-14"
    assert row.previous_density == .13
    assert row.two_days_before_density == .10
    assert row.daily_alert and row.rising_alert


@pytest.mark.parametrize("day,expected", [("2026-10-12", True), ("2026-10-11", False)])
def test_predecessor_can_be_exactly_seven_days_old_but_not_eight(day, expected):
    row = _evaluate_dates([("2026-10-11", .1), ("2026-10-19", .2)]
                          + ([] if day == "2026-10-11" else [(day, .1)]))
    assert row.previous_date == (day if expected else "")
    # The seven-day-old record is eligible as a comparator, but in this
    # example there is no positive observation in the separate five-day mean.
    assert not row.daily_evaluable and not row.daily_alert


@pytest.mark.parametrize("day,expected", [("2026-10-12", True), ("2026-10-11", False)])
def test_third_record_uses_same_seven_day_limit_without_chained_backtracking(day, expected):
    row = _evaluate_dates([(day, .1), ("2026-10-15", .13), ("2026-10-19", .17)])
    assert row.rising_alert == expected
    assert row.rising_evaluable == expected


def test_zero_comparator_is_retained_but_zero_only_mean_is_unavailable():
    row = _evaluate_dates([("2026-10-16", 0), ("2026-10-18", .13), ("2026-10-19", .17)])
    assert pd.isna(row.baseline_density)
    assert row.baseline_days == 0
    assert not row.daily_evaluable
    assert row.rising_alert
    row = _evaluate_dates([("2026-10-16", .1), ("2026-10-18", 0), ("2026-10-19", .17)])
    assert row.previous_date == "2026-10-18"
    assert row.previous_density == 0
    assert row.daily_alert and not row.rising_alert


def test_missing_target_does_not_roll_back_to_last_valid_observation():
    row = _evaluate_dates([("2026-10-14", .1), ("2026-10-17", .13), ("2026-10-18", .17)])
    assert row.target_date == "2026-10-19"
    assert pd.isna(row.target_density)
    assert not row.is_alert


@pytest.mark.parametrize("values,expected", [
    ([.1, .1, .1, .1, .1, .08, .15], (True, False)),
    ([10, 10, 10, 10, .1, .13, .17], (False, True)),
    ([.1, .1, .1, .1, .1, .13, .17], (True, True)),
    ([.1, .1, .1, .1, .1, .1, .1], (False, False)),
    ([.5, .5, .5, .5, .5, .1, .13], (False, False)),
    ([.05, .05, .05, .05, .05, .2, .21], (False, False)),
    ([.1, .1, .1, .1, .1, .125, .15], (False, False)),
    ([.1, .1, .1, .1, .1, .05, .12], (False, False)),
    ([10, 10, 10, 10, .1, .12, .15], (False, False)),
    ([0, 0, 0, 0, 0, .13, .17], (False, True)),
    ([None, None, None, None, .1, .13, .17], (True, True)),
    ([None, None, None, None, None, .13, .17], (False, False)),
    ([.1, .1, .1, .1, .1, None, .17], (True, False)),
    ([.1, .1, .1, .1, .1, .13, None], (False, False)),
    ([.1, .1, .1, .1, None, .13, .17], (True, True)),
    ([.1, .1, .1, .1, .1, 0, .17], (True, False)),
    ([1 / 6] * 6 + [1 / 5], (False, False)),
    ([1 / 6] * 5 + [1 / 5, .4], (True, False)),
])
def test_rules_are_independent_and_use_strict_calendar_comparisons(values, expected):
    row = _evaluate(values)
    assert (row.daily_alert, row.rising_alert) == expected
    assert row.is_alert == any(expected)


@pytest.mark.parametrize("override", [
    {"sheet_qty": 0}, {"sheet_qty": -1}, {"sheet_qty": float("inf")},
    {"value": float("nan")}, {"value": float("inf")}, {"value": -1}, {"value": "invalid"},
])
def test_invalid_target_is_insufficient_not_a_zero_density(override):
    row = _evaluate([.1, .1, .1, .1, .1, .13, .17], overrides={"2026-10-18": override})
    assert not row.is_alert
    assert not row.daily_evaluable and not row.rising_evaluable
    assert pd.isna(row.target_density)
    assert row.valid_days == 6


def test_empty_days_do_not_report_all_clear():
    row = _evaluate([None] * 7)
    assert not row.daily_evaluable and not row.rising_evaluable
    assert not row.is_alert


def test_calendar_window_crosses_year_and_keeps_valid_zero():
    row = _evaluate([0, 0, 0, 0, 0, .13, .17], reference=date(2027, 1, 2))
    assert row.target_date == "2027-01-01"
    assert row.valid_days == 7
    assert pd.isna(row.baseline_density)
    assert row.baseline_days == 0
    assert row.rising_evaluable and row.rising_alert
    assert not row.daily_evaluable


def test_outside_dates_and_other_indicators_never_supply_missing_comparators():
    indicators = pd.DataFrame([
        dict(prod_code="M678", factory=factory, step_id=step, rs_code=code)
        for factory, step, code in [("ARRAY", "S", "A"), ("OLED", "S", "A"),
                                    ("ARRAY", "OTHER", "A"), ("ARRAY", "S", "B")]
    ])
    rows = [
        dict(factory="ARRAY", step_id="S", rs_code="A", period_type="day",
             period_label=day, value=value, sheet_qty=100)
        for day, value in [("2026-10-01", .1), ("2026-10-17", .13),
                           ("2026-10-18", .17), ("2026-10-19", 999)]
    ]
    result = evaluate_aoi_rs_trends(pd.DataFrame(rows[::-1]), indicators, reference_date=date(2026, 10, 19))
    assert result.valid_days.tolist() == [2, 0, 0, 0]
    assert not result.is_alert.any()


def test_malformed_or_duplicate_daily_input_fails_instead_of_reporting_normal():
    indicators = pd.DataFrame([dict(prod_code="M678", factory="ARRAY", step_id="S", rs_code="A")])
    with pytest.raises(ValueError, match="required columns"):
        evaluate_aoi_rs_trends(pd.DataFrame([{"value": 1}]), indicators, reference_date=date(2026, 10, 19))
    row = dict(factory="ARRAY", step_id="S", rs_code="A", period_type="day",
               period_label="2026-10-18", value=1, sheet_qty=1)
    with pytest.raises(ValueError, match="duplicate"):
        evaluate_aoi_rs_trends(pd.DataFrame([row, row]), indicators, reference_date=date(2026, 10, 19))
