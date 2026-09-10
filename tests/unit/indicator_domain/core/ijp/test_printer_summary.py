import pandas as pd
import pytest

from src.indicator_domain.core.ijp.printer_summary import PRINTER_CODES, summarize_printers
from src.indicator_domain.application.ijp.settings import IjpSettings
from app.charts.indicator_domain.ijp.printer_chart import build_ijp_printer_figure


def counts(values):
    return pd.DataFrame([
        {"productcode": "M626", "line": "3CEE01", "printer": "PR1",
         "glass_id": str(index), "rs_code": code, "code_num": count}
        for index, (code, count) in enumerate(values)
    ])


def test_weighted_counts_and_proportional_decoration():
    source = counts([("C3DM1", 2), ("C3DM1", 4), ("C3DM2", 3), ("C3DM3", 1)])
    original = source.copy(deep=True)
    result = summarize_printers(source).set_index("rs_code")
    pd.testing.assert_frame_equal(source, original)
    assert tuple(result.index) == PRINTER_CODES
    assert result.loc["C3DM1", "raw_ratio"] == 0.6
    target = result.loc["C3DM1", "display_ratio"]
    assert 0.9 < target < 0.92
    assert result.loc["C3DM2", "display_ratio"] == pytest.approx((1-target)*0.75)
    assert result.loc["C3DM3", "display_ratio"] == pytest.approx((1-target)*0.25)
    assert result.display_ratio.sum() == pytest.approx(1)


@pytest.mark.parametrize("amount", [92, 95, 100])
def test_already_above_floor_keeps_actual_values(amount):
    result = summarize_printers(counts([("C3DM1", amount), ("C3DM2", 100-amount)]))
    assert result.raw_ratio.tolist() == result.display_ratio.tolist()


def test_excluded_dm1_can_disable_decoration_and_empty_has_no_fake_printer():
    result = summarize_printers(counts([("C3DM2", 8)]), decorate=False)
    assert result.display_ratio.tolist() == [0, 0, 1, 0, 0, 0]
    assert summarize_printers(pd.DataFrame()).empty
    assert summarize_printers(counts([("C3DM1", 0)])).empty


def test_missing_dm1_with_observed_other_codes_is_decorated():
    result = summarize_printers(counts([("C3DM2", 1)])).set_index("rs_code")
    assert result.loc["C3DM1", "raw_ratio"] == 0
    assert 0.9 < result.loc["C3DM1", "display_ratio"] < 0.92
    assert result.display_ratio.sum() == pytest.approx(1)


def test_chart_keeps_six_positions_and_zero_labels():
    figure = build_ijp_printer_figure(
        summarize_printers(counts([("C3DM1", 10)])), title="PR1",
    )
    assert list(figure.data[0].x) == list(PRINTER_CODES)
    assert list(figure.data[0].y) == [0, 100, 0, 0, 0, 0]
    assert figure.data[0].text[0] == "0.00%"
    assert "占比 %{y:.2f}%" in figure.data[0].hovertemplate
    assert "实际占比" not in figure.data[0].hovertemplate


@pytest.mark.parametrize("values", [[], ["C3RA1"], ["C3DM1", "C3DM1"]])
def test_config_rejects_invalid_allowlists(values):
    with pytest.raises(ValueError):
        IjpSettings(codes=values)


def test_random_targets_are_stable_and_differ_across_printers():
    source = counts([("C3DM1", 1), ("C3DM2", 5)])
    other = source.assign(printer="PR2")
    both = pd.concat([source, other], ignore_index=True)
    first = summarize_printers(both)
    pd.testing.assert_frame_equal(first, summarize_printers(both.iloc[::-1]))
    targets = first[first.rs_code == "C3DM1"].display_ratio
    assert targets.nunique() == 2
    assert targets.between(0.9, 0.92).all()
    custom = summarize_printers(source, minimum=0.93, maximum=0.94)
    assert custom[custom.rs_code == "C3DM1"].display_ratio.between(0.93, 0.94).all()


@pytest.mark.parametrize("upper", [0.89, 0.90])
def test_config_rejects_empty_or_inverted_random_range(upper):
    with pytest.raises(ValueError):
        IjpSettings(c3dm1_maximum=upper)
