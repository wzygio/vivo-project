from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.aoi_rs.aoi_rs_calculator import build_lot_point_df, build_sheet_point_df
from src.inline_domain.core.aoi_rs import aoi_rs_special_decoration as rules


WINDOW = ("2026-09-21", "2026-09-23")


def details():
    return pd.DataFrame([
        dict(prod_code="P", factory="OLED", step_id="1", rs_code="RS",
             sheet_id=sheet, lot_id=sheet, start_time=pd.Timestamp(day), code_qty=100.)
        for sheet, day in [("BEFORE", "2026-09-20 23:59:59"), ("START", "2026-09-21"),
                           ("END", "2026-09-23 23:59:59"), ("AFTER", "2026-09-24")]
    ])


def specs():
    return pd.DataFrame([
        dict(factory="OLED", step_id="1", rs_code="RS", type_flag=kind, spec=limit)
        for kind, limit in [("SHEET_ID", 10.), ("LOT_RATIO", 6.)]
    ])


def decorate(source, flags=None):
    return rules.apply_factory_scoped_decoration(
        build_lot_point_df(source, source), build_sheet_point_df(source), specs(), "P",
        pd.DataFrame() if flags is None else flags,
        rs_details_df=source, special_factories=("OLED",), decoration_window=WINDOW,
    )


def test_special_sheet_values_are_limited_to_inclusive_display_dates():
    source = details()
    before = source.copy(deep=True)
    _, sheets = decorate(source)
    values = sheets.set_index("sheet_id").rs_qty
    assert 0 <= values.START <= 5
    assert 0 <= values.END <= 5
    assert 8.5 <= values.BEFORE <= 9.5
    assert 8.5 <= values.AFTER <= 9.5
    projected = rules.project_factory_scoped_details(
        source, sheets, ("OLED",), decoration_window=WINDOW,
    )
    assert projected.set_index("sheet_id").loc[["BEFORE", "AFTER"], "code_qty"].tolist() == [100., 100.]
    pd.testing.assert_frame_equal(source, before)


def test_cross_window_lot_does_not_zero_outside_sheets():
    source = details().iloc[[0, 1]].assign(lot_id="MIXED", code_qty=8.)
    lots, sheets = decorate(source)
    assert lots.value.iloc[0] > 0
    assert sheets.rs_qty.tolist() == [8., 8.]


@pytest.mark.parametrize("delete_all", [False, True])
def test_deleted_window_sheets_do_not_return_in_projected_details(delete_all):
    source = details()
    deleted = ["START", "END"] if delete_all else ["START"]
    flags = pd.DataFrame([
        dict(prod_code="P", factory="OLED", step_id="1", rs_code="RS",
             chart_kind="sheet", point_id=sheet, flag="Delete")
        for sheet in deleted
    ])
    _, sheets = decorate(source, flags)
    projected = rules.project_factory_scoped_details(
        source, sheets, ("OLED",), decoration_window=WINDOW,
    )
    assert not projected.sheet_id.isin(deleted).any()
    assert projected.set_index("sheet_id").loc[["BEFORE", "AFTER"], "code_qty"].tolist() == [100., 100.]
    if not delete_all:
        assert 0 <= projected.set_index("sheet_id").loc["END", "code_qty"] <= 5


def test_special_period_cap_leaves_outside_days_and_partial_weeks_unchanged():
    source = details().assign(code_qty=[1., 100., 100., 1.])
    original = rules.build_decorated_period_trend_df(source, source, date(2026, 9, 24), special_factories=())
    actual = rules.build_decorated_period_trend_df(
        source, source, date(2026, 9, 24), special_factories=("OLED",), decoration_window=WINDOW,
    )
    month = actual.loc[actual.period_type.eq("month"), "value"].iloc[0]
    days = actual.loc[actual.period_type.eq("day")].set_index("period_label").value
    assert days["2026-09-21"] == pytest.approx(month * 1.3)
    assert days["2026-09-23"] == pytest.approx(month * 1.3)
    assert days["2026-09-20"] == days["2026-09-24"] == 1.
    pd.testing.assert_frame_equal(actual[actual.period_type.eq("week")], original[original.period_type.eq("week")])
