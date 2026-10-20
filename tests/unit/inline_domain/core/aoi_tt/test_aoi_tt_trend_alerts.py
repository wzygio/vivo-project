"""TT density alerts preserve particle identity and the RS calendar rules."""
from datetime import date

import pandas as pd

from src.inline_domain.core.aoi_tt.trend_alerts import evaluate_aoi_tt_trends


def test_sparse_tt_rise_is_evaluated_separately_for_each_particle_size():
    indicators = pd.DataFrame([
        dict(prod_code="M678", factory="ARRAY", step_id="S", tt_name="TDSUM", particle_size=size)
        for size in ("Total", "S")
    ])
    trend = pd.DataFrame([
        dict(factory="ARRAY", step_id="S", tt_name="TDSUM", particle_size=size,
             period_type="day", period_label=day, value=value, sheet_qty=100)
        for size, values in [("Total", [.1, .1, .1]), ("S", [.1, .13, .17])]
        for day, value in zip(["2026-10-12", "2026-10-15", "2026-10-18"], values)
    ])
    result = evaluate_aoi_tt_trends(trend, indicators, reference_date=date(2026, 10, 19)).set_index("particle_size")
    assert not result.loc["Total", "is_alert"]
    assert result.loc["S", "daily_alert"] and result.loc["S", "rising_alert"]
    assert result.loc["S", "previous_date"] == "2026-10-15"
    assert result.loc["S", "two_days_before_date"] == "2026-10-12"
