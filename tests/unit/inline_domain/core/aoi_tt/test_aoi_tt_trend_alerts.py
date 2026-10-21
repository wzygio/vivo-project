"""TT density alerts use Total only and preserve the RS calendar rules."""
from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.aoi_tt.trend_alerts import evaluate_aoi_tt_trends


@pytest.mark.parametrize("total_values,particle_values,expected_alert", [
    ([.1, .1, .1], [.1, .13, .17], False),
    ([.1, .2, .3], [.1, .1, .1], True),
    ([.1, .2, .3], [.1, .13, .17], True),
    ([.1, .13, .17], [.1, .13, .17], False),
])
def test_sparse_tt_rise_evaluates_only_total(total_values, particle_values, expected_alert):
    indicators = pd.DataFrame([
        dict(prod_code="M678", factory="ARRAY", step_id="S", tt_name="TDSUM", particle_size=size)
        for size in ("Total", "S")
    ])
    trend = pd.DataFrame([
        dict(factory="ARRAY", step_id="S", tt_name="TDSUM", particle_size=size,
             period_type="day", period_label=day, value=value, sheet_qty=100)
        for size, values in [("Total", total_values), ("S", particle_values)]
        for day, value in zip(["2026-10-12", "2026-10-15", "2026-10-18"], values)
    ])
    result = evaluate_aoi_tt_trends(trend, indicators, reference_date=date(2026, 10, 19))
    assert len(result) == 1
    assert "particle_size" not in result.columns
    assert bool(result.iloc[0].daily_alert) == expected_alert
    assert "rising_alert" not in result.columns and "rising_evaluable" not in result.columns
    assert result.iloc[0].target_density == total_values[-1]
    assert result.iloc[0].previous_date == "2026-10-15"
    assert result.iloc[0].two_days_before_date == "2026-10-12"


def test_particle_only_indicator_does_not_replace_missing_total():
    indicators = pd.DataFrame([
        dict(factory="ARRAY", step_id="S", tt_name="TDSUM", particle_size="S")
    ])
    trend = pd.DataFrame([
        dict(factory="ARRAY", step_id="S", tt_name="TDSUM", particle_size="S",
             period_type="day", period_label=day, value=value, sheet_qty=100)
        for day, value in zip(["2026-10-12", "2026-10-15", "2026-10-18"], [.1, .13, .17])
    ])
    assert evaluate_aoi_tt_trends(trend, indicators, reference_date=date(2026, 10, 19)).empty
