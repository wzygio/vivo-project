from datetime import date

import pandas as pd
import pytest

from src.indicator_domain.core.ijp.period_summary import reporting_periods, summarize_periods, CODE_DESCRIPTIONS
from app.charts.indicator_domain.ijp.period_chart import build_ijp_period_figure


def test_months_and_monday_weeks_at_year_boundary():
    periods = reporting_periods(date(2027, 1, 1))
    assert len(periods) == 5
    assert periods[0][1:] == (date(2026, 12, 1), date(2026, 12, 31))
    assert periods[1][1:] == (date(2027, 1, 1), date(2027, 1, 1))
    assert periods[-1][1:] == (date(2026, 12, 28), date(2027, 1, 1))


def test_period_counts_empty_periods_decoration_and_chart_contract():
    frame = pd.DataFrame({
        "productcode": ["M626"] * 3, "line": ["L1"] * 3, "printer": ["PR1"] * 3,
        "day": ["2026-08-31", "2026-09-01", "2026-09-01"],
        "rs_code": ["C3DM1", "C3DM1", "C3DM2"], "code_num": [2, 1, 7],
    })
    result = summarize_periods(frame, date(2026, 9, 8))
    assert len(result) == 30
    periods = reporting_periods(date(2026, 9, 8))
    month = result[result.period == periods[1][0]].set_index("rs_code")
    week = result[result.period == periods[3][0]].set_index("rs_code")
    assert month.loc["C3DM1", "raw_ratio"] == 1/8
    assert week.loc["C3DM1", "raw_ratio"] == 3/10
    assert 0.9 < month.loc["C3DM1", "display_ratio"] < 0.92
    assert month.display_ratio.sum() == pytest.approx(1)
    empty = result[result.period == periods[4][0]]
    assert not empty.has_data.any()
    assert empty.display_ratio.sum() == 0
    pd.testing.assert_frame_equal(result, summarize_periods(frame.iloc[::-1], date(2026, 9, 8)))
    figure = build_ijp_period_figure(result, title="PR1")
    assert figure.layout.barmode == "stack"
    assert len(figure.data) == 6
    assert all(len(trace.x) == 5 for trace in figure.data)
    assert all(description in trace.name for description, trace in zip(CODE_DESCRIPTIONS.values(), figure.data))
    assert len(figure.layout.annotations) == 2


def test_empty_input_and_unselected_dm1_do_not_create_decorated_data():
    assert summarize_periods(pd.DataFrame(), date(2026, 9, 8)).empty
    frame = pd.DataFrame([dict(productcode="M626", line="L1", printer="P1",
                               day="2026-09-08", rs_code="C3DM2", code_num=1)])
    result = summarize_periods(frame, date(2026, 9, 8), decorate=False)
    assert (result[result.rs_code == "C3DM1"].display_ratio == 0).all()
