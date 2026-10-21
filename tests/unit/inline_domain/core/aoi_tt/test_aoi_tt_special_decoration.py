import pandas as pd
import pytest

from src.inline_domain.core.aoi_tt.aoi_tt_decoration import apply_aoi_tt_decoration


WINDOW = ("2026-09-21", "2026-09-23")


def _details(values=(49, 50, 51, 101)):
    return pd.DataFrame([
        dict(prod_code="P", factory="OLED", step_id="100", tt_name="TT",
             sheet_id=f"S{i}", lot_id=f"L{i}", start_time=pd.Timestamp("2026-09-22"), tt_qty=value)
        for i, value in enumerate(values)
    ])


def _specs(ucl=100):
    return pd.DataFrame([dict(step_id="100", tt_name="TT", usl=200, ucl=ucl)])


def _decorate(details, decisions=None, **kwargs):
    return apply_aoi_tt_decoration(
        details, _specs(), pd.DataFrame() if decisions is None else decisions,
        special_factories=("OLED",), decoration_window=WINDOW, **kwargs,
    )


def test_special_half_ucl_trigger_and_quarter_ucl_integer_hash():
    source = _details()
    original = source.copy(deep=True)
    result = _decorate(source)
    assert result.tt_qty.iloc[:2].tolist() == [49, 50]
    assert result.tt_qty.iloc[2:].between(0, 25).all()
    assert result.tt_qty.mod(1).eq(0).all()
    assert result.columns.tolist() == source.columns.tolist()
    pd.testing.assert_frame_equal(source, original)
    shuffled = _decorate(source.sample(frac=1, random_state=42))
    pd.testing.assert_frame_equal(
        result.sort_values("sheet_id").reset_index(drop=True),
        shuffled.sort_values("sheet_id").reset_index(drop=True),
    )


def test_custom_sheet_ratios_control_trigger_and_hash_range():
    source = _details((60, 61))
    result = _decorate(source, sheet_spec_ratios=(0.6, 0.1))
    assert result.tt_qty.iloc[0] == 60
    assert 0 <= result.tt_qty.iloc[1] <= 10
    assert _decorate(source, sheet_spec_ratios=(0.5, 0)).tt_qty.eq(0).all()


def test_factory_and_inclusive_date_boundaries_preserve_standard_rules():
    rows = _details((120, 120, 120, 120, 120))
    rows["start_time"] = pd.to_datetime([
        "2026-09-20 23:59:59", "2026-09-21 00:00:00", "2026-09-23 23:59:59",
        "2026-09-24 00:00:00", "2026-09-22 00:00:00",
    ])
    rows.loc[4, "factory"] = "TP"
    result = _decorate(rows).set_index("sheet_id")
    assert result.loc[["S1", "S2"], "tt_qty"].between(0, 25).all()
    assert result.loc[["S0", "S3", "S4"], "tt_qty"].between(85, 95).all()


def test_special_respects_false_delete_and_parameter_exemptions():
    source = _details((300, 300, 300))
    flags = source.iloc[:2].assign(flag=[False, "Delete"])
    result = _decorate(source, flags)
    assert result.sheet_id.tolist() == ["S0", "S2"]
    assert result.tt_qty.iloc[0] == 300
    assert 0 <= result.tt_qty.iloc[1] <= 25
    exempt = _decorate(source, flags, exempt_param_name_contains=["tt"])
    assert exempt.tt_qty.tolist() == [300, 300]


@pytest.mark.parametrize("ucl", [None, float("nan"), float("inf"), -1, "invalid"])
def test_special_skips_invalid_ucl_without_usl_fallback(ucl):
    source = _details((300,))
    result = apply_aoi_tt_decoration(
        source, _specs(ucl), pd.DataFrame(), special_factories=("OLED",), decoration_window=WINDOW,
    )
    assert result.tt_qty.iloc[0] == 300


@pytest.mark.parametrize("ucl", [0, 3, 5.5])
def test_integer_replacement_stays_below_fractional_cap(ucl):
    result = apply_aoi_tt_decoration(
        _details((300,)), _specs(ucl), pd.DataFrame(),
        special_factories=("OLED",), decoration_window=WINDOW,
    )
    assert 0 <= result.tt_qty.iloc[0] <= ucl * 0.25
    assert result.tt_qty.iloc[0] % 1 == 0
