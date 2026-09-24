from datetime import date

import pandas as pd
import pytest

from src.inline_domain.core.aoi_rs.aoi_rs_calculator import (
    build_lot_point_df,
    build_sheet_point_df,
)
from src.inline_domain.core.aoi_rs import aoi_rs_special_decoration as rules


def _details() -> pd.DataFrame:
    return pd.DataFrame([
        dict(prod_code="P", factory="ARRAY", step_id="100", rs_code="RS",
             sheet_id=sheet, lot_id=lot, start_time=pd.Timestamp(day), code_qty=qty)
        for sheet, lot, day, qty in [
            ("S1", "L1", "2026-09-01", 80),
            ("S1", "L1", "2026-09-02", 20),
            ("S2", "L2", "2026-09-20", 8),
            ("S3", "L2", "2026-09-20", 6),
            ("S4", "L3", "2026-09-21", 1),
        ]
    ])


def _specs(lot_limit=6) -> pd.DataFrame:
    return pd.DataFrame([
        dict(factory="ARRAY", step_id="100", rs_code="RS", type_flag=kind, spec=value)
        for kind, value in [("SHEET_ID", 10), ("LOT_RATIO", lot_limit)]
    ])


def _decorate(details, specs=None, flags=None, exemptions=None):
    return rules.apply_aoi_rs_decoration(
        build_lot_point_df(details, details), build_sheet_point_df(details),
        _specs() if specs is None else specs, "P",
        pd.DataFrame() if flags is None else flags, exemptions,
        rs_details_df=details,
    )


def test_sheet_then_lot_zeroing_and_detail_projection():
    source = _details()
    original = source.copy(deep=True)
    lots, sheets = _decorate(source)
    by_sheet = sheets.set_index("sheet_id")["rs_qty"]
    by_lot = lots.set_index("lot_id")
    assert 0 <= by_sheet["S1"] <= 5
    assert by_lot.loc["L1", "value"] == pytest.approx(by_sheet["S1"])
    assert by_sheet[["S2", "S3"]].tolist() == [0, 0]
    assert by_lot.loc["L2", ["value", "rs_qty"]].tolist() == [0, 0]
    assert by_sheet["S4"] == 1
    projected = rules.project_aoi_rs_details(source, sheets)
    s1 = projected[projected.sheet_id.eq("S1")].code_qty
    assert s1.sum() == pytest.approx(by_sheet["S1"])
    assert s1.iloc[0] == pytest.approx(by_sheet["S1"] * .8)
    pd.testing.assert_frame_equal(source, original)
    _, shuffled = _decorate(source.sample(frac=1, random_state=42))
    pd.testing.assert_series_equal(
        sheets.set_index("sheet_id").rs_qty.sort_index(),
        shuffled.set_index("sheet_id").rs_qty.sort_index(),
    )
    assert not {"flag", "spec", "point_id", "chart_kind"} & set(sheets.columns)


@pytest.mark.parametrize("flag, expected", [(False, 7), ("Delete", None), (True, 0)])
def test_lot_flags_apply_after_sheet_stage(flag, expected):
    source = _details()
    flags = pd.DataFrame([dict(prod_code="P", factory="ARRAY", step_id="100",
                               rs_code="RS", chart_kind="lot", point_id="L2", flag=flag)])
    lots, sheets = _decorate(source, flags=flags)
    lot = lots[lots.lot_id.eq("L2")]
    if expected is None:
        assert lot.empty
    else:
        assert lot.value.iloc[0] == expected
    assert sheets.set_index("sheet_id").loc["S2", "rs_qty"] == (0 if flag is True else 8)


def test_sheet_delete_excluded_from_lot_and_projected_details():
    source = _details()
    flags = pd.DataFrame([dict(prod_code="P", factory="ARRAY", step_id="100",
                               rs_code="RS", chart_kind="sheet", point_id="S2", flag="Delete")])
    lots, sheets = _decorate(source, flags=flags)
    assert lots.set_index("lot_id").loc["L2", "value"] == 3  # original throughput = 2
    assert "S2" not in set(rules.project_aoi_rs_details(source, sheets).sheet_id)


def test_exemptions_and_missing_specs_keep_values():
    for kwargs in [dict(exemptions=["rs"]), dict(specs=pd.DataFrame())]:
        lots, sheets = _decorate(_details(), **kwargs)
        assert sheets.set_index("sheet_id").loc["S1", "rs_qty"] == 100
        assert lots.set_index("lot_id").loc["L2", "value"] == 7


def test_period_caps_use_decorated_month_without_changing_details():
    source = _details()
    _, sheets = _decorate(source)
    projected = rules.project_aoi_rs_details(source, sheets)
    before = projected.copy(deep=True)
    trend = rules.build_decorated_period_trend_df(projected, source, date(2026, 9, 24))
    month = trend[trend.period_type.eq("month")].value.iloc[0]
    short = trend[trend.period_type.isin(["week", "day"])]
    assert short.value.max() <= month * 1.3
    assert short.value.max() == pytest.approx(month * 1.3)
    assert (short.rs_qty - short.value * short.sheet_qty).abs().max() < 1e-9
    pd.testing.assert_frame_equal(projected, before)


def test_lot_zeroing_is_scoped_by_factory_step_and_code():
    source = _details()
    copies = [source]
    for column, value in [("factory", "TP"), ("step_id", "200"), ("rs_code", "OTHER")]:
        copies.append(source.assign(**{column: value}))
    lots, sheets = _decorate(pd.concat(copies, ignore_index=True))
    other = sheets[~(sheets.factory.eq("ARRAY") & sheets.step_id.eq("100") & sheets.rs_code.eq("RS"))]
    assert set(other[other.sheet_id.eq("S2")].rs_qty) == {8}
    assert lots.loc[lots.factory.eq("TP") & lots.lot_id.eq("L2"), "value"].iloc[0] == 7


def test_zero_specs_and_all_deleted_sheets():
    source = _details()
    lots, sheets = _decorate(source, specs=_specs().assign(spec=0))
    assert lots.value.eq(0).all()
    assert sheets.rs_qty.eq(0).all()
    flags = pd.DataFrame([
        dict(prod_code="P", factory="ARRAY", step_id="100", rs_code="RS",
             chart_kind="sheet", point_id=sheet, flag="Delete")
        for sheet in source.sheet_id.unique()
    ])
    lots, sheets = _decorate(source, flags=flags)
    assert lots.empty and sheets.empty
    assert rules.project_aoi_rs_details(source, sheets).empty


def test_exact_spec_values_and_missing_denominator():
    source = _details().iloc[[0]].assign(code_qty=10)
    lots, sheets = _decorate(source, specs=_specs(lot_limit=10))
    assert sheets.rs_qty.iloc[0] == lots.value.iloc[0] == 10
    lots, sheets = rules.apply_aoi_rs_decoration(
        build_lot_point_df(source, pd.DataFrame()), build_sheet_point_df(source),
        _specs(), "P", pd.DataFrame(), rs_details_df=source,
    )
    assert pd.isna(lots.value.iloc[0])
    assert sheets.rs_qty.iloc[0] == 10
    trend = rules.build_decorated_period_trend_df(source, pd.DataFrame(), date(2026, 9, 24))
    assert trend.value.isna().all()


def test_cross_month_week_uses_month_of_period_end():
    source = _details().iloc[[0, 2]].copy()
    source["start_time"] = pd.to_datetime(["2026-08-31", "2026-09-01"])
    source["code_qty"] = [10., 1.]
    trend = rules.build_decorated_period_trend_df(source, source, date(2026, 9, 2))
    week = trend[trend.period_type.eq("week")]
    assert week.value.iloc[0] == pytest.approx(1.3)
    days = trend[trend.period_type.eq("day")].set_index("period_label")
    assert days.loc["2026-08-31", "value"] == 10
    assert days.loc["2026-09-01", "value"] == 1
