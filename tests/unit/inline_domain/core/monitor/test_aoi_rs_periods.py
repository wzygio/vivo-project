"""Calendar axes and whole-period AOI RS density contracts."""

from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.monitor.aoi_rs_periods import build_monitor_period_axis
from src.inline_domain.core.aoi_rs.aoi_rs_calculator import build_period_density_df
from src.inline_domain.core.aoi_rs.aoi_rs_special_decoration import build_decorated_period_trend_df


KEY = dict(factory="ARRAY", step_id="100", rs_code="RS")


def test_axis_orders_elapsed_year_periods_and_clips_current_ends():
    axis = build_monitor_period_axis(
        date(2026, 10, 20), halfyears=(2, 1), quarters=(4, 1), months=(10, 1), week_count=5,
    )
    assert axis.period_label.tolist() == [
        "2026-H1", "2026-H2", "2026-Q1", "2026-Q4", "2026-M01", "2026-M10",
        "2026-W39", "2026-W40", "2026-W41", "2026-W42", "2026-W43",
    ]
    assert axis.period_sort.is_monotonic_increasing and axis.period_sort.is_unique
    rows = axis.set_index("period_label")
    assert rows.loc["2026-H1", "period_end"] == pd.Timestamp("2026-06-30")
    assert rows.loc["2026-H2", "period_end"] == pd.Timestamp("2026-10-20")
    assert rows.loc["2026-Q4", "period_start"] == pd.Timestamp("2026-10-01")
    assert rows.loc["2026-W43", "period_end"] == pd.Timestamp("2026-10-20")


def test_axis_uses_iso_year_for_cross_year_weeks_and_leap_month():
    axis = build_monitor_period_axis(date(2027, 1, 1), halfyears=(), quarters=(), months=(), week_count=2)
    assert axis.period_label.tolist() == ["2026-W52", "2026-W53"]
    assert axis.period_start.tolist() == [pd.Timestamp("2026-12-21"), pd.Timestamp("2026-12-28")]
    leap = build_monitor_period_axis(date(2024, 3, 1), halfyears=(), quarters=(), months=(2,), week_count=1)
    assert leap.iloc[0].period_end == pd.Timestamp("2024-02-29")


@pytest.mark.parametrize("overrides", [
    {"halfyears": (3,)}, {"quarters": (0,)}, {"months": (13,)},
    {"halfyears": (2,)}, {"quarters": (2,)}, {"months": (2,)},
    {"week_count": 0}, {"week_count": 54}, {"months": (True,)}, {"week_count": 1.5},
])
def test_axis_rejects_invalid_or_future_selection(overrides):
    options = dict(halfyears=(), quarters=(), months=(), week_count=1)
    options.update(overrides)
    with pytest.raises(ValueError):
        build_monitor_period_axis(date(2026, 1, 15), **options)


def test_density_deduplicates_whole_quarter_and_completes_indicator_universe():
    axis = build_monitor_period_axis(date(2026, 2, 15), halfyears=(1,), quarters=(1,), months=(1, 2), week_count=1)
    source = pd.DataFrame([
        dict(**KEY, start_time="2026-01-10", code_qty=2),
        dict(**KEY, start_time="2026-02-10", code_qty=4),
        dict(**KEY, start_time="2026-02-16", code_qty=100),
    ])
    throughput = pd.DataFrame([
        dict(factory="ARRAY", step_id="100", sheet_id=sheet, start_time=day)
        for sheet, day in [("S1", "2026-01-10"), ("S1", "2026-02-10"), ("S2", "2026-02-10")]
    ])
    indicators = pd.DataFrame([KEY, {**KEY, "rs_code": "ZERO"}, {**KEY, "step_id": "EMPTY"}])
    before = source.copy(deep=True)
    result = build_period_density_df(source, throughput, axis, indicators=indicators)
    quarter = result.loc[result.period_type.eq("quarter")].set_index(["step_id", "rs_code"])
    assert quarter.loc[("100", "RS"), ["rs_qty", "sheet_qty", "value"]].tolist() == [6, 2, 3]
    assert quarter.loc[("100", "ZERO"), ["rs_qty", "sheet_qty", "value"]].tolist() == [0, 2, 0]
    assert pd.isna(quarter.loc[("EMPTY", "RS"), "value"])
    assert len(result) == len(axis) * len(indicators)
    pd.testing.assert_frame_equal(source, before)


def test_density_handles_empty_sources_and_preserves_sparse_default():
    axis = build_monitor_period_axis(date(2026, 2, 15), halfyears=(), quarters=(), months=(1,), week_count=1)
    result = build_period_density_df(pd.DataFrame(), pd.DataFrame(), axis, indicators=pd.DataFrame([KEY]))
    assert result.rs_qty.eq(0).all() and result.sheet_qty.eq(0).all() and result.value.isna().all()
    assert build_period_density_df(pd.DataFrame(), pd.DataFrame(), axis).empty
    source = pd.DataFrame([dict(**KEY, start_time="2026-01-10", code_qty=2)])
    sparse = build_period_density_df(source, pd.DataFrame(), axis)
    assert sparse.period_label.tolist() == ["2026-M01"]
    assert sparse.value.isna().all()


def test_density_retains_zero_defect_indicators_with_throughput_only():
    axis = build_monitor_period_axis(date(2026, 2, 15), halfyears=(), quarters=(), months=(1,), week_count=1)
    throughput = pd.DataFrame([
        dict(factory="ARRAY", step_id="100", sheet_id="S1", start_time="2026-01-10"),
        dict(factory="ARRAY", step_id="100", sheet_id="BAD", start_time="invalid"),
    ])
    result = build_period_density_df(pd.DataFrame(), throughput, axis, indicators=pd.DataFrame([KEY, KEY]))
    january = result.loc[result.period_label.eq("2026-M01")].iloc[0]
    assert [january.rs_qty, january.sheet_qty, january.value] == [0, 1, 0]
    assert len(result) == len(axis)
    assert build_period_density_df(pd.DataFrame(), throughput, axis, indicators=pd.DataFrame()).empty
    assert build_period_density_df(pd.DataFrame(), throughput, axis.iloc[:0], indicators=pd.DataFrame([KEY])).empty


def test_arbitrary_axis_week_cap_reads_endpoint_month_with_new_month_label():
    axis = build_monitor_period_axis(date(2026, 9, 2), halfyears=(), quarters=(), months=(8, 9), week_count=1)
    source = pd.DataFrame([
        dict(**KEY, start_time="2026-08-31", code_qty=10, sheet_id="S1"),
        dict(**KEY, start_time="2026-09-01", code_qty=1, sheet_id="S2"),
    ])
    result = build_decorated_period_trend_df(
        source, source, date(2026, 9, 2), period_axis=axis, indicators=pd.DataFrame([KEY]),
        special_factories=("ARRAY",),
    ).set_index("period_label")
    assert result.loc["2026-W36", "value"] == pytest.approx(1.3)
    assert result.loc["2026-W36", "rs_qty"] == pytest.approx(2.6)
    assert result.loc["2026-M08", "value"] == 10
